"""
Statham Trading Bot — RENDER (Unified v1.0)
============================================
Объединяет всё в одном месте:
  • Приём TradingView-сигналов → уведомление в Telegram → исполнение на Bybit
  • Все Telegram-команды (статистика, F&G, сессии, отчёты, управление)
  • Планировщик (Fear & Greed каждые 6ч, сессии, дневные/недельные/месячные отчёты)
  • Bybit: Market-ордера, 4 TP, SL, Trailing Stop

Эндпоинты:
  POST /webhook/telegram  — Telegram-апдейты
  POST /webhook/bybit     — TradingView-сигналы (secret_key в теле или заголовке)
  GET  /health            — Healthcheck
  GET  /setup             — Переустановить вебхук Telegram
  GET  /debug             — Последние 100 строк лога
  GET  /trades            — Активные сделки (JSON)
  GET  /stats             — Статистика (JSON)
  GET  /history           — История сделок (JSON)
  POST /close_all         — Аварийное закрытие всех позиций (admin)
  GET  /test_bybit        — Проверка подключения к Bybit

ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ (задавать в Render Dashboard):
  TG_TOKEN            — токен Telegram-бота
  TG_CHAT             — ID чата (-100...)
  TG_SIGNALS_TOPIC    — ID ветки сигналов (например 6314)
  TG_SESSIONS_TOPIC   — ID ветки сессий/F&G (например 1)
  WEBHOOK_SECRET      — секрет для /webhook/bybit (из TradingView/скрипта)
  RENDER_URL          — свой URL на Render (для keepalive)
  BYBIT_API_KEY       — ключ Bybit
  BYBIT_API_SECRET    — секрет Bybit
  TESTNET             — "true" для тестнета, "false" для реального
  ADMIN_IDS           — через запятую: 123456,789012
  ALLOWED_PAIRS       — через запятую: BTCUSDT,ETHUSDT,...
  DEFAULT_LEVERAGE    — плечо по умолчанию (10)
  DEFAULT_SIZE_USDT   — размер позиции в USDT (50)
  PAIR_SETTINGS_JSON  — '{"BTCUSDT":{"leverage":10,"size_usdt":100}}'
  TRAIL_PCT           — трейлинг % (0.5)
  DATA_DIR            — директория для JSON-файлов (Render Disk → /data, иначе /tmp)

⚠️ Для постоянного хранения данных (stats, history) добавь Render Disk и
   установи DATA_DIR=/data в переменных окружения.
"""
from __future__ import annotations
import json, os, re, time, math, threading, random, uuid, datetime, logging
from flask import Flask, request, jsonify
import requests
import telebot

# ── Попытка импорта Bybit (может отсутствовать на старых установках) ──
try:
    from pybit.unified_trading import HTTP as BybitHTTP
    BYBIT_AVAILABLE = True
except ImportError:
    BYBIT_AVAILABLE = False

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════════════════════════════
TG_TOKEN          = os.environ.get("TG_TOKEN",          "")
TG_CHAT           = os.environ.get("TG_CHAT",           "")
TG_SIGNALS_TOPIC  = os.environ.get("TG_SIGNALS_TOPIC",  "6314")
TG_SESSIONS_TOPIC = os.environ.get("TG_SESSIONS_TOPIC", "1")
WEBHOOK_SECRET    = os.environ.get("WEBHOOK_SECRET",    "statham_secret")
RENDER_URL        = os.environ.get("RENDER_URL",        "")

BYBIT_API_KEY     = os.environ.get("BYBIT_API_KEY",     "")
BYBIT_API_SECRET  = os.environ.get("BYBIT_API_SECRET",  "")
TESTNET           = os.environ.get("TESTNET",           "true").lower() == "true"

ADMIN_IDS = set(
    int(x.strip()) for x in os.environ.get("ADMIN_IDS", "").split(",")
    if x.strip().lstrip("-").isdigit()
)

ALLOWED_PAIRS = set(
    p.strip().upper().replace(".P", "")
    for p in os.environ.get("ALLOWED_PAIRS", "YBUSDT,ASTERUSDT,EDGEUSDT,STOUSDT").split(",")
    if p.strip()
)

DEFAULT_LEVERAGE  = int(os.environ.get("DEFAULT_LEVERAGE",  "10"))
DEFAULT_SIZE_USDT = float(os.environ.get("DEFAULT_SIZE_USDT","1"))

try:
    PAIR_SETTINGS: dict = json.loads(os.environ.get("PAIR_SETTINGS_JSON", "{}"))
except Exception:
    PAIR_SETTINGS = {}

TRAIL_PCT = float(os.environ.get("TRAIL_PCT", "0.5")) / 100.0

# Секрет для браузерных admin-эндпоинтов (/close_all, /positions, /test_bybit и т.д.)
# Передавать как ?secret=XXX или заголовок X-Secret
RENDER_SECRET = os.environ.get("RENDER_SECRET", "")

def _http_auth(req) -> bool:
    """Проверяет секрет для HTTP admin-эндпоинтов. Если RENDER_SECRET не задан — пропускает."""
    if not RENDER_SECRET:
        return True
    token = req.headers.get("X-Secret") or req.args.get("secret", "")
    return token == RENDER_SECRET

# TP: доля закрытия позиции при каждом TP (25+20+25+15+10+5 = 100%)
TP_CLOSE_PCT = {1: 0.25, 2: 0.20, 3: 0.25, 4: 0.15, 5: 0.10, 6: 0.05}

# Файлы данных
DATA_DIR       = os.environ.get("DATA_DIR", "/tmp")
os.makedirs(DATA_DIR, exist_ok=True)

STATS_FILE      = os.path.join(DATA_DIR, "trade_stats.json")
TRADES_FILE     = os.path.join(DATA_DIR, "active_trades.json")
HISTORY_FILE    = os.path.join(DATA_DIR, "trade_history.json")
POSITIONS_FILE  = os.path.join(DATA_DIR, "positions.json")
FG_STATE_FILE   = os.path.join(DATA_DIR, "fg_state.json")
SENT_FLAGS_FILE = os.path.join(DATA_DIR, "sent_flags.json")
LOG_FILE        = os.path.join(DATA_DIR, "bot.log")

# Retry для очереди сигналов
MAX_QUEUE_ATTEMPTS = 3
QUEUE_RETRY_DELAY  = 15
_signal_queue: list[dict] = []   # in-memory очередь
_queue_lock   = threading.Lock()

# Блокировка позиций
_pos_lock = threading.Lock()

# Блокировка новых сигналов (/emergency_stop)
_emergency_stop = threading.Event()

# Планировщик: часы МСК для F&G
FG_SCHEDULED_HOURS = {4, 10, 16, 22}

bot = telebot.TeleBot(TG_TOKEN, threaded=False) if TG_TOKEN else None

# ══════════════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ══════════════════════════════════════════════════════════════════════════════
def write_log(entry: str):
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {entry}\n"
    log.info(entry)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) > 1000:
            with open(LOG_FILE, "w", encoding="utf-8") as f:
                f.writelines(lines[-1000:])
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════════════════
# JSON ХЕЛПЕРЫ
# ══════════════════════════════════════════════════════════════════════════════
_file_lock = threading.Lock()

def load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with _file_lock:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        write_log(f"LOAD_JSON_ERR | {path} | {e}")
        return default

def save_json(path: str, data):
    try:
        with _file_lock:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        write_log(f"SAVE_JSON_ERR | {path} | {e}")

def load_stats()    : return load_json(STATS_FILE,    {"wins": 0, "losses": 0, "total": 0})
def load_trades()   : return load_json(TRADES_FILE,   {})
def load_history()  : return load_json(HISTORY_FILE,  [])
def load_positions(): return load_json(POSITIONS_FILE,{})
def save_stats(d)   : save_json(STATS_FILE,    d)
def save_trades(d)  : save_json(TRADES_FILE,   d)
def save_history(d) : save_json(HISTORY_FILE,  d)
def save_positions(d): save_json(POSITIONS_FILE, d)

# ── Флаги планировщика ────────────────────────────────────────────────────────
def _was_sent(key: str) -> bool:
    return load_json(SENT_FLAGS_FILE, {}).get(key, False)

def _mark_sent(key: str):
    flags = load_json(SENT_FLAGS_FILE, {})
    flags[key] = True
    save_json(SENT_FLAGS_FILE, flags)

# ══════════════════════════════════════════════════════════════════════════════
# ХЕЛПЕРЫ
# ══════════════════════════════════════════════════════════════════════════════
def _msk() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)

def calc_winrate(wins: int, total: int) -> float:
    return round(wins / total * 100, 1) if total > 0 else 0.0

def fmt_duration(seconds: int) -> str:
    if seconds < 3600:  return f"{seconds // 60}м"
    if seconds < 86400: return f"{seconds // 3600}ч {(seconds % 3600) // 60}м"
    return f"{seconds // 86400}д {(seconds % 86400) // 3600}ч"

def is_admin_user(user_id: int) -> bool:
    """Проверяет, является ли user_id глобальным администратором."""
    if ADMIN_IDS and user_id in ADMIN_IDS:
        return True
    # Fallback: проверяем через Telegram API (если ADMIN_IDS не заданы)
    if not ADMIN_IDS and bot and TG_CHAT:
        try:
            admins = bot.get_chat_administrators(TG_CHAT)
            return user_id in [a.user.id for a in admins]
        except Exception:
            pass
    return False

# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM — ОТПРАВКА
# ══════════════════════════════════════════════════════════════════════════════
def send_tg(text: str, thread_id: str | int | None = None,
            chat_id: str | None = None, reply_to: int | None = None) -> dict:
    if not TG_TOKEN or not TG_CHAT:
        return {}
    if chat_id is None:
        chat_id = TG_CHAT
    payload: dict = {
        "chat_id":                  chat_id,
        "text":                     text,
        "parse_mode":               "HTML",
        "disable_web_page_preview": True,
    }
    if thread_id is not None:
        try: payload["message_thread_id"] = int(thread_id)
        except (ValueError, TypeError): pass
    if reply_to:
        payload["reply_parameters"] = {
            "message_id": reply_to,
            "allow_sending_without_reply": True,
        }
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json=payload, timeout=10,
        )
        data = resp.json()
        if not data.get("ok"):
            raise RuntimeError(f"TG error: {data.get('description')}")
        return data["result"]
    except Exception as e:
        write_log(f"SEND_TG_ERR | {e}")
        return {}

def send_signals(text: str, reply_to: int | None = None) -> dict:
    return send_tg(text, thread_id=TG_SIGNALS_TOPIC, reply_to=reply_to)

def send_sessions(text: str) -> dict:
    try:
        return send_tg(text, thread_id=TG_SESSIONS_TOPIC)
    except Exception as e:
        write_log(f"SESSIONS_SEND_ERR | {e}")
        return send_tg(text)

def send_fg(text: str) -> dict:
    try:
        return send_tg(text, thread_id=TG_SESSIONS_TOPIC)
    except Exception as e:
        write_log(f"FG_SEND_ERR | {e}")
        return send_tg(text)

# ══════════════════════════════════════════════════════════════════════════════
# BYBIT КЛИЕНТ
# ══════════════════════════════════════════════════════════════════════════════
_bybit_session = None

def bybit():
    global _bybit_session
    if not BYBIT_AVAILABLE:
        raise RuntimeError("pybit не установлен")
    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        raise RuntimeError("BYBIT_API_KEY / BYBIT_API_SECRET не заданы")
    if _bybit_session is None:
        _bybit_session = BybitHTTP(
            testnet=TESTNET,
            api_key=BYBIT_API_KEY,
            api_secret=BYBIT_API_SECRET,
        )
        write_log(f"BYBIT | connected | testnet={TESTNET}")
    return _bybit_session

_instrument_cache: dict = {}

def get_instrument(symbol: str) -> dict:
    if symbol not in _instrument_cache:
        resp = bybit().get_instruments_info(category="linear", symbol=symbol)
        _instrument_cache[symbol] = resp["result"]["list"][0]
    return _instrument_cache[symbol]

def round_qty(symbol: str, qty: float) -> str:
    try:
        step = float(get_instrument(symbol)["lotSizeFilter"]["qtyStep"])
        decimals = max(0, round(-math.log10(step)))
        return str(round(math.floor(qty / step) * step, decimals))
    except Exception:
        return str(round(qty, 4))

def round_price(symbol: str, price: float) -> str:
    try:
        tick = float(get_instrument(symbol)["priceFilter"]["tickSize"])
        decimals = max(0, round(-math.log10(tick)))
        return str(round(price, decimals))
    except Exception:
        return str(round(price, 4))

def min_qty(symbol: str) -> float:
    try:
        return float(get_instrument(symbol)["lotSizeFilter"]["minOrderQty"])
    except Exception:
        return 0.001

def get_price(symbol: str) -> float:
    resp = bybit().get_tickers(category="linear", symbol=symbol)
    return float(resp["result"]["list"][0]["lastPrice"])

def set_leverage(symbol: str, leverage: int):
    try:
        bybit().set_leverage(
            category="linear", symbol=symbol,
            buyLeverage=str(leverage), sellLeverage=str(leverage),
        )
    except Exception as e:
        write_log(f"LEVERAGE_WARN | {symbol} | {e}")

def calc_qty(symbol: str, size_usdt: float, leverage: int, price: float) -> float:
    qty = (size_usdt * leverage) / price
    qty_rounded = float(round_qty(symbol, qty))
    return max(qty_rounded, min_qty(symbol))

def place_market(symbol: str, side: str, qty: float, reduce_only: bool = False) -> dict:
    resp = bybit().place_order(
        category="linear", symbol=symbol,
        side=side, orderType="Market",
        qty=round_qty(symbol, qty),
        reduceOnly=reduce_only, timeInForce="IOC",
    )
    write_log(f"MARKET | {symbol} {side} qty={qty} ro={reduce_only} | ret={resp['retCode']}")
    return resp

def place_stop(symbol: str, side: str, qty: float, trigger_price: float,
               reduce_only: bool = True) -> dict:
    resp = bybit().place_order(
        category="linear", symbol=symbol,
        side=side, orderType="Market",
        qty=round_qty(symbol, qty),
        triggerPrice=round_price(symbol, trigger_price),
        triggerBy="LastPrice", orderFilter="StopOrder",
        reduceOnly=reduce_only, timeInForce="IOC",
    )
    write_log(f"STOP | {symbol} {side} qty={qty} trigger={trigger_price} | ret={resp['retCode']}")
    return resp

def cancel_all_orders(symbol: str):
    try:
        bybit().cancel_all_orders(category="linear", symbol=symbol)
    except Exception as e:
        write_log(f"CANCEL_ALL_ERR | {symbol} | {e}")

def get_bybit_balance() -> str:
    try:
        resp    = bybit().get_wallet_balance(accountType="UNIFIED")
        equity  = resp["result"]["list"][0]["totalEquity"]
        avail   = resp["result"]["list"][0]["totalAvailableBalance"]
        mode    = "🧪 TESTNET" if TESTNET else "🔴 LIVE"
        return (f"💰 <b>Баланс Bybit</b> {mode}\n"
                f"Equity: <b>{float(equity):.2f} USDT</b>\n"
                f"Доступно: <b>{float(avail):.2f} USDT</b>")
    except Exception as e:
        return f"❌ Ошибка получения баланса: {e}"

# ── Парсинг цены из текста ────────────────────────────────────────────────────
def parse_price(text: str, *prefixes: str) -> float | None:
    for prefix in prefixes:
        idx = text.find(prefix)
        if idx == -1:
            continue
        substr = text[idx + len(prefix):].strip()
        m = re.search(r"[\d]+\.?[\d]*", substr)
        if m:
            try: return float(m.group())
            except ValueError: pass
    return None

# ── Управление позицией (SL) ──────────────────────────────────────────────────
def move_sl(pos: dict, new_sl: float) -> str:
    symbol    = pos["symbol"]
    opp_side  = pos["opp_side"]
    remaining = pos.get("remaining_qty", pos["total_qty"])
    if remaining <= 0:
        return ""
    cancel_all_orders(symbol)
    time.sleep(0.3)
    try:
        resp = place_stop(symbol, opp_side, remaining, new_sl)
        if resp["retCode"] == 0:
            oid = resp["result"].get("orderId", "")
            write_log(f"SL_MOVED | {symbol} → {new_sl} | order={oid}")
            return oid
    except Exception as e:
        write_log(f"SL_MOVE_ERR | {symbol} | {e}")
    return ""

# ── Ключ позиции ──────────────────────────────────────────────────────────────
def pos_key(ticker: str, direction: str) -> str:
    return f"{ticker}_{direction}"

# ══════════════════════════════════════════════════════════════════════════════
# СТАТИСТИКА И ИСТОРИЯ СДЕЛОК
# ══════════════════════════════════════════════════════════════════════════════
def update_stats(is_win: bool):
    s = load_stats()
    s["total"]  = s.get("total", 0) + 1
    if is_win:  s["wins"]   = s.get("wins",   0) + 1
    else:       s["losses"] = s.get("losses", 0) + 1
    save_stats(s)

def save_trade_to_history(payload: dict, trade_data: dict | None, result: str, tp_num: int):
    now        = int(time.time())
    entry_time = trade_data.get("created_at", now) if trade_data else now
    record = {
        "trade_id":     payload.get("trade_id", ""),
        "ticker":       payload.get("ticker", ""),
        "direction":    payload.get("direction", ""),
        "timeframe":    payload.get("timeframe", ""),
        "is_strong":    payload.get("is_strong", False),
        "entry_time":   entry_time,
        "close_time":   now,
        "duration_sec": max(0, now - entry_time),
        "result":       result,
        "tp_num":       tp_num,
        "date_msk":     _msk().strftime("%Y-%m-%d"),
        "week_msk":     f"{_msk().year}-W{_msk().isocalendar()[1]:02d}",
    }
    history = load_history()
    history.append(record)
    if len(history) > 2000:
        history = history[-2000:]
    save_history(history)

def _build_report(trades: list, title: str) -> str:
    wins    = [r for r in trades if r["result"] == "win"]
    losses  = [r for r in trades if r["result"] == "loss"]
    total   = len(trades)
    wr      = calc_winrate(len(wins), total)
    avg_dur = int(sum(r.get("duration_sec", 0) for r in trades) / total) if total else 0
    tp_counts: dict = {}
    for r in wins:
        k = f"TP{r['tp_num']}"
        tp_counts[k] = tp_counts.get(k, 0) + 1
    text  = f"{title}\n\n"
    text += f"📊 Win Rate: <b>{wr}%</b>\n"
    text += f"✅ TP: {len(wins)}   ❌ SL: {len(losses)}\n"
    text += f"📈 Всего закрыто: {total}\n"
    text += f"⏱ Ср. время: {fmt_duration(avg_dur)}\n"
    if tp_counts:
        text += "\n<b>Разбивка по TP:</b>\n"
        for tp, cnt in sorted(tp_counts.items()):
            text += f"  {tp}: {cnt} ✅\n"
    return text

# ══════════════════════════════════════════════════════════════════════════════
# АКТИВНЫЕ СДЕЛКИ (для Telegram-нотификаций и replied messages)
# ══════════════════════════════════════════════════════════════════════════════
def build_trade_key(payload: dict) -> str:
    tid = payload.get("trade_id")
    if tid and tid != "null" and str(tid).strip():
        return str(tid).strip()
    ticker    = payload.get("ticker", "").strip()
    direction = payload.get("direction", "").strip()
    tf        = payload.get("timeframe", "").strip()
    return f"{ticker}_{direction}_{tf}" if ticker else ""

def put_trade(key: str, data: dict):
    t = load_trades(); t[key] = data; save_trades(t)

def get_trade(key: str) -> dict | None:
    trades = load_trades()
    if key in trades:
        return trades[key]
    # Fuzzy-поиск по ticker+direction
    if key and "_" in key:
        parts = key.split("_")
        if len(parts) >= 2:
            tk, dr = parts[0], parts[1]
            for k, v in trades.items():
                if v.get("ticker","") == tk and v.get("direction","") == dr:
                    return v
    return None

def remove_trade(key: str):
    t = load_trades()
    if key in t:
        del t[key]; save_trades(t)

def dedup_entry(ticker: str, direction: str):
    trades = load_trades()
    keys_to_remove = [
        k for k, v in trades.items()
        if v.get("ticker","") == ticker and v.get("direction","") == direction
    ]
    for k in keys_to_remove:
        del trades[k]
    if keys_to_remove:
        save_trades(trades)
        write_log(f"DEDUP | removed {len(keys_to_remove)} old keys for {ticker}_{direction}")

def cleanup_old_trades() -> int:
    trades  = load_trades()
    cutoff  = int(time.time()) - 7 * 86400
    removed = [k for k, v in trades.items() if v.get("created_at", 0) < cutoff]
    for k in removed:
        del trades[k]
    if removed:
        save_trades(trades)
    return len(removed)

# ══════════════════════════════════════════════════════════════════════════════
# ОБРАБОТЧИКИ СИГНАЛОВ (TG уведомление + Bybit исполнение)
# ══════════════════════════════════════════════════════════════════════════════
def handle_entry(payload: dict):
    ticker    = payload.get("ticker", "").replace(".P", "").upper()
    direction = payload.get("direction", "").upper()
    text      = payload.get("text", "").strip()
    trade_id  = str(payload.get("trade_id") or "")

    # 1. Telegram-уведомление
    dedup_entry(ticker, direction)
    key  = build_trade_key(payload)
    sent = send_signals(text or f"📥 Вход {ticker} {direction}")
    if key:
        put_trade(key, {
            "message_id": sent.get("message_id"),
            "chat_id":    TG_CHAT,
            "thread_id":  int(TG_SIGNALS_TOPIC),
            "event":      "entry",
            "ticker":     ticker,
            "direction":  direction,
            "timeframe":  payload.get("timeframe", ""),
            "is_strong":  payload.get("is_strong", False),
            "created_at": int(time.time()),
        })

    # 2. Bybit исполнение
    if not BYBIT_AVAILABLE or not BYBIT_API_KEY:
        write_log(f"ENTRY_SKIP_BYBIT | {ticker} | Bybit не настроен")
        return
    if ALLOWED_PAIRS and ticker not in ALLOWED_PAIRS:
        write_log(f"ENTRY_SKIP | {ticker} not in ALLOWED_PAIRS")
        return

    side     = "Buy"  if direction == "BUY"  else "Sell"
    opp_side = "Sell" if side == "Buy" else "Buy"

    cfg      = {**{"leverage": DEFAULT_LEVERAGE, "size_usdt": DEFAULT_SIZE_USDT},
                **PAIR_SETTINGS.get(ticker, {})}
    leverage  = int(cfg["leverage"])
    size_usdt = float(cfg["size_usdt"])

    sl_price  = parse_price(text, "SL:", "⛔ SL:")
    tp1_price = parse_price(text, "TP1:", "✅ TP1:")
    tp2_price = parse_price(text, "TP2:", "✅ TP2:")
    tp3_price = parse_price(text, "TP3:", "✅ TP3:")
    tp4_price = parse_price(text, "TP4:", "✅ TP4:")

    set_leverage(ticker, leverage)

    try:
        price = get_price(ticker)
    except Exception as e:
        write_log(f"ENTRY_ERR | get_price | {ticker} | {e}")
        send_signals(f"❌ <b>Ошибка входа {ticker}</b>\nНе удалось получить цену: {e}")
        return

    qty = calc_qty(ticker, size_usdt, leverage, price)
    write_log(f"ENTRY | {ticker} {direction} | price={price} qty={qty} lev={leverage}x")

    try:
        resp = place_market(ticker, side, qty)
        if resp["retCode"] != 0:
            raise Exception(f"{resp['retCode']}: {resp.get('retMsg','')}")
    except Exception as e:
        write_log(f"ENTRY_FAIL | {ticker} | {e}")
        send_signals(f"❌ <b>Ошибка входа {ticker} {direction}</b>\n{e}")
        return

    time.sleep(0.5)

    sl_order_id = ""
    if sl_price:
        try:
            sl_resp = place_stop(ticker, opp_side, qty, sl_price)
            if sl_resp["retCode"] == 0:
                sl_order_id = sl_resp["result"].get("orderId", "")
        except Exception as e:
            write_log(f"SL_PLACE_ERR | {ticker} | {e}")

    pkey = pos_key(ticker, direction)
    with _pos_lock:
        positions = load_positions()
        positions[pkey] = {
            "symbol":       ticker,
            "direction":    direction,
            "side":         side,
            "opp_side":     opp_side,
            "entry_price":  price,
            "total_qty":    qty,
            "remaining_qty": qty,
            "leverage":     leverage,
            "sl_price":     sl_price,
            "sl_order_id":  sl_order_id,
            "tp1_price":    tp1_price,
            "tp2_price":    tp2_price,
            "tp3_price":    tp3_price,
            "tp4_price":    tp4_price,
            "trade_id":     trade_id,
            "created_at":   int(time.time()),
            "trail_active": False,
            "trail_sl":     None,
        }
        save_positions(positions)

    net_tag = "🧪 TESTNET" if TESTNET else "🔴 LIVE"
    arrow   = "🟢" if direction == "BUY" else "🔴"
    send_signals(
        f"{arrow} <b>{'LONG' if direction=='BUY' else 'SHORT'} ОТКРЫТ</b>  {net_tag}\n"
        f"#{ticker}  |  {leverage}x  |  {size_usdt} USDT\n"
        f"📍 Вход: <b>{price}</b>  |  ⛔ SL: {sl_price or '—'}\n"
        f"✅ TP1: {tp1_price or '—'}  |  TP2: {tp2_price or '—'}\n"
        f"📦 Объём: {qty} контр."
    )


def handle_tp_hit(payload: dict):
    ticker    = payload.get("ticker", "").replace(".P", "").upper()
    direction = (payload.get("direction") or "").upper()
    tp_num    = int(payload.get("tp_num") or 0)
    text      = payload.get("text", "").strip()
    key       = build_trade_key(payload)

    # TG уведомление
    trade    = get_trade(key) if key else None
    reply_id = trade["message_id"] if trade else None
    send_signals(text or f"✅ TP{tp_num} {ticker}", reply_to=reply_id)
    if tp_num >= 3:
        update_stats(True)
        save_trade_to_history(payload, trade, "win", tp_num)
        if key: remove_trade(key)

    # Bybit исполнение
    if not BYBIT_AVAILABLE or not BYBIT_API_KEY:
        return
    pkey = pos_key(ticker, direction)
    with _pos_lock:
        positions = load_positions()
        pos = positions.get(pkey)
        if not pos:
            return
        if pos.get(f"tp{tp_num}_hit"):
            return

        total_qty = pos["total_qty"]
        remaining = pos["remaining_qty"]
        opp_side  = pos["opp_side"]
        close_qty = round(total_qty * TP_CLOSE_PCT.get(tp_num, 0.0), 8)
        close_qty = min(close_qty, remaining)

        if close_qty < min_qty(ticker):
            return

        try:
            resp = place_market(ticker, opp_side, close_qty, reduce_only=True)
            if resp["retCode"] != 0:
                raise Exception(f"{resp['retCode']}: {resp.get('retMsg','')}")
        except Exception as e:
            write_log(f"TP_HIT_ERR | {ticker} TP{tp_num} | {e}")
            return

        new_remaining = max(0.0, remaining - close_qty)
        pos[f"tp{tp_num}_hit"] = True
        pos["remaining_qty"]   = new_remaining

        # SL → TP1 после TP2
        if tp_num == 2 and pos.get("tp1_price"):
            new_sl = pos["tp1_price"]
            oid = move_sl(pos, new_sl)
            pos["sl_price"] = new_sl
            pos["sl_order_id"] = oid

        # Трейлинг после TP3
        if tp_num >= 3 and not pos["trail_active"]:
            pos["trail_active"] = True
            pos["trail_sl"]     = pos.get("sl_price")

        if new_remaining < min_qty(ticker) or tp_num >= 6:
            cancel_all_orders(ticker)
            del positions[pkey]
        else:
            positions[pkey] = pos
        save_positions(positions)


def handle_sl_hit(payload: dict):
    ticker    = payload.get("ticker", "").replace(".P", "").upper()
    direction = (payload.get("direction") or "").upper()
    text      = payload.get("text", "").strip()
    key       = build_trade_key(payload)

    trade    = get_trade(key) if key else None
    reply_id = trade["message_id"] if trade else None
    send_signals(text or f"🛑 SL {ticker}", reply_to=reply_id)
    update_stats(False)
    save_trade_to_history(payload, trade, "loss", 0)
    if key: remove_trade(key)

    if not BYBIT_AVAILABLE or not BYBIT_API_KEY:
        return
    cancel_all_orders(ticker)
    with _pos_lock:
        positions = load_positions()
        positions.pop(pos_key(ticker, direction), None)
        save_positions(positions)


def handle_sl_moved(payload: dict):
    ticker    = payload.get("ticker", "").replace(".P", "").upper()
    direction = (payload.get("direction") or "").upper()
    text      = payload.get("text", "").strip()
    key       = build_trade_key(payload)

    trade    = get_trade(key) if key else None
    reply_id = trade["message_id"] if trade else None
    send_signals(text or f"🔒 SL сдвинут {ticker}", reply_to=reply_id)

    if not BYBIT_AVAILABLE or not BYBIT_API_KEY:
        return
    new_sl = parse_price(text, "✅ Стало:", "Стало:")
    if not new_sl:
        return
    pkey = pos_key(ticker, direction)
    with _pos_lock:
        positions = load_positions()
        pos = positions.get(pkey)
        if not pos:
            return
        oid = move_sl(pos, new_sl)
        pos["sl_price"]    = new_sl
        pos["sl_order_id"] = oid
        positions[pkey] = pos
        save_positions(positions)


def process_signal(payload: dict):
    """Главный роутер сигналов."""
    if _emergency_stop.is_set():
        write_log("EMERGENCY_STOP active — signal dropped")
        return

    event = payload.get("event", "unknown")
    if event == "enty":
        event = "entry"
    write_log(f"PROCESS | event={event} | ticker={payload.get('ticker','?')}")

    text = payload.get("text", "").strip()
    key  = build_trade_key(payload)

    if event in ("entry", "smart_entry"):
        handle_entry(payload)
    elif event == "limit_hit":
        # Лимитная отработана — открываем как вход
        handle_entry(payload)
    elif event == "limit_order":
        # Лимитный ордер выставлен — просто публикуем уведомление
        send_signals(text or f"📋 Лимитный ордер {payload.get('ticker','')}")
    elif event == "tp_hit":
        handle_tp_hit(payload)
    elif event == "sl_hit":
        handle_sl_hit(payload)
    elif event == "sl_moved":
        handle_sl_moved(payload)
    elif event == "scale_in":
        trade    = get_trade(key) if key else None
        reply_id = trade["message_id"] if trade else None
        send_signals(text or f"📈 Scale in {payload.get('ticker','')}", reply_to=reply_id)
    else:
        send_signals(text or json.dumps(payload, ensure_ascii=False))

# ── Фоновая очередь сигналов ──────────────────────────────────────────────────
def enqueue_signal(payload: dict):
    with _queue_lock:
        _signal_queue.append({"payload": payload, "attempts": 0, "queued_at": time.time()})

def _queue_worker():
    write_log("QUEUE_WORKER | start")
    while True:
        with _queue_lock:
            if _signal_queue:
                item = _signal_queue.pop(0)
            else:
                item = None
        if item:
            payload  = item["payload"]
            attempts = item["attempts"]
            try:
                process_signal(payload)
            except Exception as e:
                write_log(f"QUEUE_ERR | attempt={attempts} | {e}")
                if attempts < MAX_QUEUE_ATTEMPTS - 1:
                    item["attempts"] += 1
                    time.sleep(QUEUE_RETRY_DELAY)
                    with _queue_lock:
                        _signal_queue.insert(0, item)
                else:
                    write_log(f"QUEUE_DEAD | payload={json.dumps(payload)[:200]}")
        else:
            time.sleep(1)

threading.Thread(target=_queue_worker, daemon=True, name="queue_worker").start()

# ══════════════════════════════════════════════════════════════════════════════
# FEAR & GREED INDEX
# ══════════════════════════════════════════════════════════════════════════════
FEAR_GREED_URL = "https://api.alternative.me/fng/?limit=1"

_FG_LABELS = [(0,24,"Extreme Fear"),(25,44,"Fear"),(45,55,"Neutral"),
              (56,74,"Greed"),(75,100,"Extreme Greed")]

def _fg_emoji(v: int) -> str:
    if v <= 24: return "😱"
    if v <= 44: return "😨"
    if v <= 55: return "😐"
    if v <= 74: return "🤑"
    return "🚀"

def _fg_label(v: int) -> str:
    for lo, hi, label in _FG_LABELS:
        if lo <= v <= hi: return label
    return "Unknown"

def _fg_bar(v: int, n: int = 20) -> str:
    filled = round(v / 100 * n)
    return "█" * filled + "░" * (n - filled)

def _fetch_fg() -> dict | None:
    try:
        resp  = requests.get(FEAR_GREED_URL, timeout=8)
        entry = resp.json()["data"][0]
        value = int(entry["value"])
        return {"value": value, "label": _fg_label(value)}
    except Exception as e:
        write_log(f"FG_FETCH_ERR | {e}")
        return None

def _build_fg_message(fg: dict, trigger: str = "scheduled") -> str:
    v  = fg["value"]; label = fg["label"]
    bar = _fg_bar(v); msk = _msk().strftime("%d.%m.%Y %H:%M")
    header = "🔔 <b>Fear & Greed — смена зоны!</b>" if trigger == "change" else "📊 <b>Fear & Greed Index</b>"
    text  = f"{header}\n\n"
    text += f"{_fg_emoji(v)}  <b>{v} / 100</b>  —  {label}\n"
    text += f"<code>[{bar}]</code>\n\n"
    if v <= 24:   text += "🔻 Рынок в панике. Часто — точка разворота вверх.\n"
    elif v <= 44: text += "📉 Преобладает страх. Возможны покупки на откатах.\n"
    elif v <= 55: text += "➡️ Рынок нейтрален. Ждём определённости.\n"
    elif v <= 74: text += "📈 Жадность растёт. Следим за перекупленностью.\n"
    else:         text += "🔺 Эйфория. Высокий риск разворота вниз.\n"
    text += f"\n🕐 Обновлено: {msk} МСК"
    return text

def _check_fg(force: bool = False) -> bool:
    fg = _fetch_fg()
    if fg is None:
        return False
    state      = load_json(FG_STATE_FILE, {})
    last_label = state.get("label", "")
    trigger    = "change" if (last_label and last_label != fg["label"]) else "scheduled"
    if not force and trigger != "change":
        return False
    try:
        send_fg(_build_fg_message(fg, trigger))
        save_json(FG_STATE_FILE, {
            "value":       fg["value"],
            "label":       fg["label"],
            "sent_at":     int(time.time()),
            "sent_at_msk": _msk().strftime("%Y-%m-%d %H:%M"),
        })
        return True
    except Exception as e:
        write_log(f"FG_SEND_ERR | {e}")
        return False

# ══════════════════════════════════════════════════════════════════════════════
# РАСПИСАНИЕ ТОРГОВЫХ СЕССИЙ
# ══════════════════════════════════════════════════════════════════════════════
SESSIONS = [
    {"name": "🇦🇺 Австралия",   "open": "02:00", "close": "09:00"},
    {"name": "🇯🇵 Азия (Токио)","open": "03:00", "close": "09:00"},
    {"name": "🇬🇧 Европа",      "open": "10:00", "close": "18:30"},
    {"name": "🇺🇸 Америка",     "open": "16:30", "close": "23:00"},
]

def _sessions_status() -> str:
    msk = _msk()
    cur = msk.hour * 60 + msk.minute
    lines = [f"🕐 <b>Торговые сессии</b> ({msk.strftime('%H:%M')} МСК)\n"]
    for s in SESSIONS:
        oh, om = map(int, s["open"].split(":"))
        ch, cm = map(int, s["close"].split(":"))
        o_min  = oh * 60 + om
        c_min  = ch * 60 + cm
        # обработка ночных сессий (close < open)
        if c_min < o_min:
            is_open = cur >= o_min or cur < c_min
        else:
            is_open = o_min <= cur < c_min
        status = "🟢 Открыта" if is_open else "🔴 Закрыта"
        lines.append(f"{s['name']}  {s['open']}–{s['close']}  {status}")
    return "\n".join(lines)

# ══════════════════════════════════════════════════════════════════════════════
# ФОНОВЫЙ ПЛАНИРОВЩИК
# ══════════════════════════════════════════════════════════════════════════════
def _scheduler():
    write_log("SCHEDULER | start")
    while True:
        try:
            msk    = _msk()
            cur    = msk.hour * 60 + msk.minute
            h, m   = msk.hour, msk.minute
            today  = msk.strftime("%Y-%m-%d")
            in_min = abs(m)   # минут от начала часа

            # ── Fear & Greed каждые 6 часов ──────────────────────────────
            if h in FG_SCHEDULED_HOURS and 0 <= m <= 1:
                fg_key = f"fg_{today}_{h:02d}"
                if not _was_sent(fg_key):
                    _mark_sent(fg_key)
                    _check_fg(force=True)

            # ── Дневной отчёт 23:55 МСК ───────────────────────────────────
            if h == 23 and 55 <= m <= 59:
                key = f"daily_{today}"
                if not _was_sent(key):
                    _mark_sent(key)
                    history = load_history()
                    day_trades = [r for r in history if r.get("date_msk") == today]
                    if day_trades:
                        send_signals(_build_report(day_trades, f"📅 <b>Дневной отчёт {today}</b>"))

            # ── Недельный отчёт в воскресенье 23:55 МСК ─────────────────
            if msk.weekday() == 6 and h == 23 and 55 <= m <= 59:
                week_key = f"{msk.year}-W{msk.isocalendar()[1]:02d}"
                key = f"weekly_{week_key}"
                if not _was_sent(key):
                    _mark_sent(key)
                    history = load_history()
                    week_trades = [r for r in history if r.get("week_msk") == week_key]
                    if week_trades:
                        send_signals(_build_report(week_trades, f"📅 <b>Недельный отчёт {week_key}</b>"))

            # ── Месячный отчёт в последний день месяца 23:59 МСК ────────
            import calendar
            last_day = calendar.monthrange(msk.year, msk.month)[1]
            if msk.day == last_day and h == 23 and 59 <= m <= 59:
                month_key = f"{msk.year}-{msk.month:02d}"
                key = f"monthly_{month_key}"
                if not _was_sent(key):
                    _mark_sent(key)
                    history = load_history()
                    month_trades = [r for r in history if r.get("date_msk","").startswith(month_key)]
                    if month_trades:
                        send_signals(_build_report(month_trades, f"📅 <b>Месячный отчёт {month_key}</b>"))

            # ── Сессии: предупреждения и уведомления ─────────────────────
            for s in SESSIONS:
                oh, om = map(int, s["open"].split(":"))
                ch, cm = map(int, s["close"].split(":"))
                open_min  = oh * 60 + om
                close_min = ch * 60 + cm

                # Предупреждение об открытии (~15 мин)
                if 14 <= open_min - cur <= 16:
                    k = f"sess_open_warn_{today}_{s['open']}"
                    if not _was_sent(k):
                        _mark_sent(k)
                        send_sessions(f"⏰ {s['name']} открывается через 15 мин! ({s['open']} МСК)")

                # Уведомление об открытии
                if -1 <= open_min - cur <= 1:
                    k = f"sess_open_{today}_{s['open']}"
                    if not _was_sent(k):
                        _mark_sent(k)
                        send_sessions(f"🟢 {s['name']} открылась! ({s['open']} МСК)")

                # Предупреждение о закрытии (~15 мин)
                if 14 <= close_min - cur <= 16:
                    k = f"sess_close_warn_{today}_{s['close']}"
                    if not _was_sent(k):
                        _mark_sent(k)
                        send_sessions(f"⏰ {s['name']} закрывается через 15 мин! ({s['close']} МСК)")

                # Уведомление о закрытии
                is_midnight = (ch == 0 and cm == 0)
                at_close = (-1 <= cur <= 1) if is_midnight else (-1 <= close_min - cur <= 1)
                if at_close:
                    key_date = (msk - datetime.timedelta(days=1)).strftime("%Y-%m-%d") \
                               if is_midnight else today
                    k = f"sess_close_{key_date}_{s['close']}"
                    if not _was_sent(k):
                        _mark_sent(k)
                        send_sessions(f"🔴 {s['name']} закрылась! ({s['close']} МСК)")

        except Exception as e:
            write_log(f"SCHEDULER_ERR | {e}")

        time.sleep(60)

threading.Thread(target=_scheduler, daemon=True, name="scheduler").start()

# ── Trailing Stop фоновый поток ───────────────────────────────────────────────
def _position_manager():
    write_log("POSITION_MANAGER | start")
    while True:
        try:
            with _pos_lock:
                positions = load_positions()
            for pkey, pos in list(positions.items()):
                if not pos.get("trail_active"):
                    continue
                ticker  = pos["symbol"]
                cur_sl  = pos.get("trail_sl") or pos.get("sl_price") or 0
                try:
                    price = get_price(ticker)
                except Exception:
                    continue
                if pos["direction"] == "BUY":
                    new_trail = price * (1 - TRAIL_PCT)
                    if new_trail > cur_sl:
                        oid = move_sl(pos, new_trail)
                        with _pos_lock:
                            p2 = load_positions()
                            if pkey in p2:
                                p2[pkey]["trail_sl"] = new_trail
                                p2[pkey]["sl_price"] = new_trail
                                p2[pkey]["sl_order_id"] = oid
                                save_positions(p2)
                else:
                    new_trail = price * (1 + TRAIL_PCT)
                    if cur_sl == 0 or new_trail < cur_sl:
                        oid = move_sl(pos, new_trail)
                        with _pos_lock:
                            p2 = load_positions()
                            if pkey in p2:
                                p2[pkey]["trail_sl"] = new_trail
                                p2[pkey]["sl_price"] = new_trail
                                p2[pkey]["sl_order_id"] = oid
                                save_positions(p2)
        except Exception as e:
            write_log(f"POS_MGR_ERR | {e}")
        time.sleep(30)

if BYBIT_AVAILABLE:
    threading.Thread(target=_position_manager, daemon=True, name="pos_mgr").start()

# ── Keepalive для Render ──────────────────────────────────────────────────────
def _keepalive():
    time.sleep(60)
    while True:
        if RENDER_URL:
            try:
                r = requests.get(f"{RENDER_URL}/health", timeout=10)
                write_log(f"KEEPALIVE | {r.status_code}")
            except Exception as e:
                write_log(f"KEEPALIVE_ERR | {e}")
        time.sleep(600)

threading.Thread(target=_keepalive, daemon=True, name="keepalive").start()

# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM КОМАНДЫ
# ══════════════════════════════════════════════════════════════════════════════
if bot:
    def _reply(m, text: str):
        kw = {"parse_mode": "HTML", "disable_web_page_preview": True}
        if getattr(m, "message_thread_id", None):
            kw["message_thread_id"] = m.message_thread_id
        try:
            bot.send_message(m.chat.id, text, **kw)
        except Exception as e:
            write_log(f"REPLY_ERR | {e}")

    @bot.message_handler(commands=["start"])
    def cmd_start(m):
        _reply(m, (
            "🚀 <b>Statham Trading Bot</b>\n\n"
            "Принимаю сигналы от TradingView, публикую в группу и исполняю на Bybit.\n\n"
            "👉 /help — список команд"
        ))

    @bot.message_handler(commands=["help"])
    def cmd_help(m):
        _reply(m, (
            "📋 <b>Команды Statham Bot</b>\n\n"
            "<b>📊 Статистика:</b>\n"
            "/stats — общий Win Rate\n"
            "/daily_report — дневной отчёт\n"
            "/weekly_report — недельный отчёт\n"
            "/monthly_report — месячный отчёт\n"
            "/stats_by_ticker BTCUSDT — по тикеру\n"
            "/leaders — топ прибыльных пар\n"
            "/active — открытые сделки\n\n"
            "<b>📋 Сделки:</b>\n"
            "/trades — активные сделки\n\n"
            "<b>🧭 Рынок:</b>\n"
            "/fear_greed — Fear & Greed\n"
            "/sessions — расписание сессий\n"
            "/market — состояние рынка\n\n"
            "<b>🔧 Admin:</b>\n"
            "/balance — баланс биржи\n"
            "/close_all — закрыть все позиции\n"
            "/test_bybit — проверка API\n"
            "/reset_stats — сбросить статистику\n"
            "/cleanup — очистить зависшие сделки\n"
            "/emergency_stop — заблокировать новые сигналы\n"
            "/resume — снять блокировку сигналов\n"
            "/logs — последние строки лога"
        ))

    @bot.message_handler(commands=["stats"])
    def cmd_stats(m):
        s    = load_stats()
        wins = s.get("wins", 0); losses = s.get("losses", 0); total = s.get("total", 0)
        wr   = calc_winrate(wins, total)
        _reply(m, (
            f"📊 <b>Общая статистика</b>\n\n"
            f"Win Rate: <b>{wr}%</b>\n"
            f"✅ TP: {wins}   ❌ SL: {losses}\n"
            f"📈 Всего сделок: {total}"
        ))

    @bot.message_handler(commands=["daily_report"])
    def cmd_daily(m):
        today   = _msk().strftime("%Y-%m-%d")
        history = load_history()
        trades  = [r for r in history if r.get("date_msk") == today]
        if not trades:
            _reply(m, f"📅 Сегодня ({today}) закрытых сделок нет.")
            return
        _reply(m, _build_report(trades, f"📅 <b>Дневной отчёт {today}</b>"))

    @bot.message_handler(commands=["weekly_report"])
    def cmd_weekly(m):
        msk     = _msk()
        wk      = f"{msk.year}-W{msk.isocalendar()[1]:02d}"
        history = load_history()
        trades  = [r for r in history if r.get("week_msk") == wk]
        if not trades:
            _reply(m, f"📅 На этой неделе ({wk}) закрытых сделок нет.")
            return
        _reply(m, _build_report(trades, f"📅 <b>Недельный отчёт {wk}</b>"))

    @bot.message_handler(commands=["monthly_report"])
    def cmd_monthly(m):
        msk     = _msk()
        mo      = f"{msk.year}-{msk.month:02d}"
        history = load_history()
        trades  = [r for r in history if r.get("date_msk","").startswith(mo)]
        if not trades:
            _reply(m, f"📅 В этом месяце ({mo}) закрытых сделок нет.")
            return
        _reply(m, _build_report(trades, f"📅 <b>Месячный отчёт {mo}</b>"))

    @bot.message_handler(commands=["stats_by_ticker"])
    def cmd_stats_by_ticker(m):
        parts = m.text.split()
        if len(parts) < 2:
            _reply(m, "❌ Укажи тикер: /stats_by_ticker BTCUSDT"); return
        ticker  = parts[1].upper().replace(".P","")
        history = load_history()
        trades  = [r for r in history if r.get("ticker","").replace(".P","").upper() == ticker]
        if not trades:
            _reply(m, f"Нет данных по {ticker}"); return
        _reply(m, _build_report(trades, f"📊 <b>Статистика {ticker}</b>"))

    @bot.message_handler(commands=["leaders"])
    def cmd_leaders(m):
        history = load_history()
        if not history:
            _reply(m, "Нет данных."); return
        scores: dict = {}
        for r in history:
            tk = r.get("ticker","").replace(".P","").upper()
            if not tk: continue
            scores[tk] = scores.get(tk, 0) + (1 if r["result"] == "win" else -1)
        top = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:10]
        lines = ["🏆 <b>Лидеры (по очкам TP-SL)</b>\n"]
        for i, (tk, sc) in enumerate(top, 1):
            lines.append(f"{i}. {tk}  {'+' if sc>=0 else ''}{sc}")
        _reply(m, "\n".join(lines))

    @bot.message_handler(commands=["active", "trades"])
    def cmd_active(m):
        trades = load_trades()
        if not trades:
            _reply(m, "📭 Нет активных сделок."); return
        lines = [f"📋 <b>Активных сделок: {len(trades)}</b>\n"]
        for key, t in trades.items():
            arrow = "🟢" if t.get("direction") == "BUY" else "🔴"
            dur   = fmt_duration(int(time.time()) - t.get("created_at", int(time.time())))
            lines.append(f"{arrow} {t.get('ticker',key)} {t.get('direction','')} TF{t.get('timeframe','')}  ⏱{dur}")
        _reply(m, "\n".join(lines))

    @bot.message_handler(commands=["fear_greed"])
    def cmd_fg(m):
        fg = _fetch_fg()
        if fg:
            _reply(m, _build_fg_message(fg, "command"))
        else:
            _reply(m, "❌ Не удалось получить Fear & Greed.")

    @bot.message_handler(commands=["sessions"])
    def cmd_sessions(m):
        _reply(m, _sessions_status())

    @bot.message_handler(commands=["market"])
    def cmd_market(m):
        fg = _fetch_fg()
        fg_line = f"{_fg_emoji(fg['value'])} F&G: {fg['value']} — {fg['label']}\n" if fg else ""
        _reply(m, f"🌐 <b>Состояние рынка</b>\n\n{fg_line}{_sessions_status()}")

    @bot.message_handler(commands=["balance"])
    def cmd_balance(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        _reply(m, get_bybit_balance())

    @bot.message_handler(commands=["test_bybit"])
    def cmd_test_bybit(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        _reply(m, get_bybit_balance())

    @bot.message_handler(commands=["close_all"])
    def cmd_close_all(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        if not BYBIT_AVAILABLE or not BYBIT_API_KEY:
            _reply(m, "❌ Bybit не подключён."); return
        with _pos_lock:
            positions = load_positions()
        closed = []
        for pkey, pos in positions.items():
            ticker    = pos["symbol"]
            remaining = pos.get("remaining_qty", 0)
            if remaining > 0:
                try:
                    cancel_all_orders(ticker)
                    place_market(ticker, pos["opp_side"], remaining, reduce_only=True)
                    closed.append(ticker)
                except Exception as e:
                    write_log(f"CLOSE_ALL_ERR | {ticker} | {e}")
        save_positions({})
        save_trades({})
        _reply(m, f"🚨 <b>Закрыто позиций: {len(closed)}</b>\n{', '.join(closed) or 'нет'}")

    @bot.message_handler(commands=["emergency_stop"])
    def cmd_emergency_stop(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        _emergency_stop.set()
        _reply(m, "🛑 <b>Emergency Stop активирован!</b>\nНовые сигналы заблокированы.\n/resume — снять блокировку")

    @bot.message_handler(commands=["resume"])
    def cmd_resume(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        _emergency_stop.clear()
        _reply(m, "✅ Блокировка снята. Сигналы принимаются.")

    @bot.message_handler(commands=["reset_stats"])
    def cmd_reset_stats(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        save_stats({"wins": 0, "losses": 0, "total": 0})
        save_history([])
        _reply(m, "✅ Статистика сброшена.")

    @bot.message_handler(commands=["cleanup"])
    def cmd_cleanup(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        n = cleanup_old_trades()
        _reply(m, f"🧹 Удалено {n} зависших сделок старше 7 дней.")

    @bot.message_handler(commands=["clean"])
    def cmd_clean(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        save_trades({})
        save_positions({})
        _reply(m, "🧹 Активные сделки и позиции очищены.")

    @bot.message_handler(commands=["logs"])
    def cmd_logs(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        try:
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
            last = "".join(lines[-30:]) if lines else "(пусто)"
            _reply(m, f"<pre>{last[:3500]}</pre>")
        except Exception as e:
            _reply(m, f"❌ {e}")

# ══════════════════════════════════════════════════════════════════════════════
# WEBHOOK — РЕГИСТРАЦИЯ
# ══════════════════════════════════════════════════════════════════════════════
_RENDER_DOMAIN = os.environ.get("RENDER_URL", "").rstrip("/")

def register_webhook():
    if not TG_TOKEN or not _RENDER_DOMAIN:
        write_log("WEBHOOK_SETUP | TG_TOKEN или RENDER_URL не заданы")
        return False
    wh_url = f"{_RENDER_DOMAIN}/webhook/telegram"
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/setWebhook",
            json={
                "url":                  wh_url,
                "drop_pending_updates": False,
                "allowed_updates":      ["message", "edited_message", "callback_query"],
            },
            timeout=10,
        )
        result = r.json()
        write_log(f"WEBHOOK_SETUP | url={wh_url} | result={result}")
        return result.get("ok", False)
    except Exception as e:
        write_log(f"WEBHOOK_SETUP_ERR | {e}")
        return False

def _register_bg():
    time.sleep(5)
    register_webhook()

threading.Thread(target=_register_bg, daemon=True, name="webhook_reg").start()

# ══════════════════════════════════════════════════════════════════════════════
# FLASK ROUTES
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/health")
def health():
    return jsonify({
        "status":        "ok",
        "testnet":       TESTNET,
        "bybit":         BYBIT_AVAILABLE and bool(BYBIT_API_KEY),
        "active_trades": len(load_trades()),
        "positions":     len(load_positions()),
        "emergency":     _emergency_stop.is_set(),
        "time_msk":      _msk().strftime("%Y-%m-%d %H:%M"),
    })


@app.route("/webhook/telegram", methods=["POST"])
def tg_webhook():
    if not bot:
        return "no bot", 400
    try:
        update = telebot.types.Update.de_json(request.get_data().decode("utf-8"))
        bot.process_new_updates([update])
    except Exception as e:
        write_log(f"TG_WEBHOOK_ERR | {e}")
    return "!", 200


@app.route("/webhook/bybit", methods=["POST"])
def bybit_webhook():
    """Принимает сигналы от TradingView / скрипта."""
    # Проверяем secret: в теле JSON или в заголовке
    raw = request.get_data(as_text=True).strip()
    if not raw:
        return jsonify({"status": "ok", "event": "ping"}), 200

    try:
        payload = request.get_json(force=True, silent=True) or json.loads(raw)
    except Exception as e:
        write_log(f"WEBHOOK_PARSE_ERR | {e}")
        return jsonify({"error": "invalid json"}), 400

    if not isinstance(payload, dict):
        return jsonify({"error": "expected json object"}), 400

    # Проверка секрета (в теле или заголовке)
    secret_from_body   = str(payload.get("secret", "") or payload.get("secret_key", ""))
    secret_from_header = request.headers.get("X-Secret", "")
    incoming_secret    = secret_from_body or secret_from_header

    if WEBHOOK_SECRET and incoming_secret != WEBHOOK_SECRET:
        write_log(f"WEBHOOK_FORBIDDEN | got={incoming_secret!r}")
        return jsonify({"error": "forbidden"}), 403

    write_log(f"WEBHOOK | event={payload.get('event','?')} ticker={payload.get('ticker','?')}")
    enqueue_signal(payload)
    return jsonify({"status": "queued", "event": payload.get("event", "?")}), 200


# Обратная совместимость — старый URL /webhook/<secret>
@app.route("/webhook/<secret>", methods=["POST"])
def bybit_webhook_compat(secret: str):
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        return jsonify({"error": "forbidden"}), 403
    raw = request.get_data(as_text=True).strip()
    if not raw:
        return jsonify({"status": "ok", "event": "ping"}), 200
    try:
        payload = request.get_json(force=True, silent=True) or json.loads(raw)
    except Exception:
        return jsonify({"error": "invalid json"}), 400
    write_log(f"WEBHOOK_COMPAT | event={payload.get('event','?')}")
    enqueue_signal(payload)
    return jsonify({"status": "queued"}), 200


@app.route("/setup")
def setup():
    ok = register_webhook()
    return jsonify({"webhook_registered": ok})


@app.route("/debug")
def debug():
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        return "<pre>" + "".join(lines[-100:]) + "</pre>", 200, {"Content-Type": "text/html"}
    except Exception as e:
        return f"<pre>Error: {e}</pre>", 200, {"Content-Type": "text/html"}


@app.route("/trades")
def trades_route():
    return jsonify(load_trades())


@app.route("/stats")
def stats_route():
    s    = load_stats()
    wins = s.get("wins", 0); losses = s.get("losses", 0); total = s.get("total", 0)
    return jsonify({"wins": wins, "losses": losses, "total": total,
                    "winrate": calc_winrate(wins, total)})


@app.route("/history")
def history_route():
    h = load_history()
    return jsonify({"records": len(h), "last_50": h[-50:]})


@app.route("/positions")
def positions_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    return jsonify(load_positions())


@app.route("/close_all", methods=["POST"])
def close_all_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    if not BYBIT_AVAILABLE or not BYBIT_API_KEY:
        return jsonify({"error": "Bybit not configured"}), 400
    with _pos_lock:
        positions = load_positions()
    closed = []
    for pkey, pos in positions.items():
        ticker    = pos["symbol"]
        remaining = pos.get("remaining_qty", 0)
        if remaining > 0:
            try:
                cancel_all_orders(ticker)
                place_market(ticker, pos["opp_side"], remaining, reduce_only=True)
                closed.append(ticker)
            except Exception as e:
                write_log(f"CLOSE_ALL_ERR | {ticker} | {e}")
    save_positions({})
    save_trades({})
    send_signals(f"🚨 <b>АВАРИЙНОЕ ЗАКРЫТИЕ</b>\nЗакрыто: {', '.join(closed) or 'нет'}")
    return jsonify({"closed": closed})


@app.route("/test_bybit")
def test_bybit_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    return jsonify({"result": get_bybit_balance()})


# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
