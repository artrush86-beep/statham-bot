"""
Statham Trading Bot — RENDER (Unified v2.0)
============================================
Два брокера: Bybit  и  BingX — торгуют одновременно.
Для каждой биржи свой набор пар (BYBIT_PAIRS / BINGX_PAIRS).

Эндпоинты:
  POST /webhook/telegram  — Telegram-апдейты
  POST /webhook/bybit     — TradingView-сигналы
  GET  /health            — Healthcheck (публичный)
  GET  /setup             — Переустановить вебхук Telegram
  GET  /debug             — Последние 100 строк лога   [требует ?secret=RENDER_SECRET]
  GET  /trades            — Активные сделки            [требует ?secret=RENDER_SECRET]
  GET  /stats             — Статистика                 [требует ?secret=RENDER_SECRET]
  GET  /history           — История сделок             [требует ?secret=RENDER_SECRET]
  GET  /positions         — Открытые позиции           [требует ?secret=RENDER_SECRET]
  POST /close_all         — Аварийное закрытие         [требует ?secret=RENDER_SECRET]
  GET  /test_bybit        — Проверка Bybit API         [требует ?secret=RENDER_SECRET]
  GET  /test_bingx        — Проверка BingX API         [требует ?secret=RENDER_SECRET]

ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ:
  TG_TOKEN            — токен Telegram-бота
  TG_CHAT             — ID чата (-100...)
  TG_SIGNALS_TOPIC    — ID ветки сигналов
  TG_SESSIONS_TOPIC   — ID ветки сессий/F&G
  WEBHOOK_SECRET      — секрет для /webhook/bybit (ОБЯЗАТЕЛЬНО!)
  RENDER_SECRET       — секрет для admin-эндпоинтов  (ОБЯЗАТЕЛЬНО!)
  RENDER_URL          — свой URL на Render (для keepalive)
  ADMIN_IDS           — через запятую: 123456,789012

  # ── Bybit ────────────────────────────────────────────────────────
  BYBIT_API_KEY       — ключ Bybit
  BYBIT_API_SECRET    — секрет Bybit
  TESTNET             — "false" для реального (ВАЖНО!)
  BYBIT_PAIRS         — пары для Bybit: BTCUSDT,ETHUSDT,...
                        (если не задано — использует ALLOWED_PAIRS)

  # ── BingX ────────────────────────────────────────────────────────
  BINGX_API_KEY       — ключ BingX
  BINGX_API_SECRET    — секрет BingX
  BINGX_DEMO          — "true" для демо-счёта BingX
  BINGX_PAIRS         — пары для BingX: ASTERUSDT,EDGEUSDT,...
                        (если не задано — использует ALLOWED_PAIRS)

  # ── Торговля (общее) ─────────────────────────────────────────────
  ALLOWED_PAIRS       — фолбэк если BYBIT_PAIRS/BINGX_PAIRS не заданы
  DEFAULT_LEVERAGE    — плечо по умолчанию (10)
  DEFAULT_SIZE_USDT   — размер позиции в USDT (1)
  TRAIL_PCT           — трейлинг % (0.5)
  PAIR_SETTINGS_JSON  — '{"BTCUSDT":{"leverage":10,"size_usdt":100}}'
  DATA_DIR            — директория для JSON-файлов (/tmp или /data)
"""
from __future__ import annotations
import calendar, hashlib, hmac as _hmac, json, math, os, re, time
import threading, datetime, logging
from flask import Flask, request, jsonify
import requests
import telebot

# ── Bybit ──────────────────────────────────────────────────────────────────────
try:
    from pybit.unified_trading import HTTP as BybitHTTP
    BYBIT_LIB = True
except ImportError:
    BYBIT_LIB = False

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
RENDER_URL        = os.environ.get("RENDER_URL",        "")

# Секреты
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
RENDER_SECRET  = os.environ.get("RENDER_SECRET",  "")

TESTNET = os.environ.get("TESTNET", "true").lower() == "true"

# Bybit
BYBIT_API_KEY    = os.environ.get("BYBIT_API_KEY",    "")
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", "")

# BingX
BINGX_API_KEY    = os.environ.get("BINGX_API_KEY",    "")
BINGX_API_SECRET = os.environ.get("BINGX_API_SECRET", "")
BINGX_DEMO       = os.environ.get("BINGX_DEMO", "false").lower() == "true"

BYBIT_AVAILABLE = BYBIT_LIB and bool(BYBIT_API_KEY) and bool(BYBIT_API_SECRET)
BINGX_AVAILABLE = bool(BINGX_API_KEY) and bool(BINGX_API_SECRET)

ADMIN_IDS = set(
    int(x.strip()) for x in os.environ.get("ADMIN_IDS", "").split(",")
    if x.strip().lstrip("-").isdigit()
)


def _parse_pairs(env_key: str, fallback_key: str = "ALLOWED_PAIRS",
                 default: str = "ASTERUSDT,EDGEUSDT,STOUSDT") -> set[str]:
    raw = os.environ.get(env_key, "").strip()
    if not raw:
        raw = os.environ.get(fallback_key, default)
    return {p.strip().upper().replace(".P", "") for p in raw.split(",") if p.strip()}


BYBIT_PAIRS   = _parse_pairs("BYBIT_PAIRS")
BINGX_PAIRS   = _parse_pairs("BINGX_PAIRS")
ALLOWED_PAIRS = BYBIT_PAIRS | BINGX_PAIRS

DEFAULT_LEVERAGE  = int(os.environ.get("DEFAULT_LEVERAGE",  "10"))
DEFAULT_SIZE_USDT = float(os.environ.get("DEFAULT_SIZE_USDT", "1"))

try:
    PAIR_SETTINGS: dict = json.loads(os.environ.get("PAIR_SETTINGS_JSON", "{}"))
except Exception:
    PAIR_SETTINGS = {}

TRAIL_PCT    = float(os.environ.get("TRAIL_PCT", "0.5")) / 100.0
TP_CLOSE_PCT = {1: 0.25, 2: 0.20, 3: 0.25, 4: 0.15, 5: 0.10, 6: 0.05}

DATA_DIR = os.environ.get("DATA_DIR", "/tmp")
os.makedirs(DATA_DIR, exist_ok=True)

STATS_FILE      = os.path.join(DATA_DIR, "trade_stats.json")
TRADES_FILE     = os.path.join(DATA_DIR, "active_trades.json")
HISTORY_FILE    = os.path.join(DATA_DIR, "trade_history.json")
POSITIONS_FILE  = os.path.join(DATA_DIR, "positions.json")
FG_STATE_FILE   = os.path.join(DATA_DIR, "fg_state.json")
SENT_FLAGS_FILE = os.path.join(DATA_DIR, "sent_flags.json")
LOG_FILE        = os.path.join(DATA_DIR, "bot.log")

MAX_QUEUE_ATTEMPTS = 3
QUEUE_RETRY_DELAY  = 15
_signal_queue: list[dict] = []
_queue_lock   = threading.Lock()
_pos_lock     = threading.Lock()
_bybit_lock   = threading.Lock()
_emergency_stop = threading.Event()

FG_SCHEDULED_HOURS = {4, 10, 16, 22}

bot = telebot.TeleBot(TG_TOKEN, threaded=False) if TG_TOKEN else None


# ══════════════════════════════════════════════════════════════════════════════
# СТАРТОВЫЕ ПРЕДУПРЕЖДЕНИЯ
# ══════════════════════════════════════════════════════════════════════════════
def _startup_warnings():
    if not RENDER_SECRET:
        log.warning("SECURITY WARNING: RENDER_SECRET не задан! "
                    "Admin-эндпоинты /debug /trades /stats /history /positions /close_all "
                    "доступны без авторизации. Установи RENDER_SECRET в Render Dashboard.")
    if not WEBHOOK_SECRET:
        log.warning("SECURITY WARNING: WEBHOOK_SECRET не задан! "
                    "/webhook/bybit принимает сигналы от кого угодно.")
    if not TESTNET and (BYBIT_AVAILABLE or BINGX_AVAILABLE):
        log.warning("LIVE TRADING ACTIVE! TESTNET=false — торговля реальными деньгами!")
    if BYBIT_AVAILABLE:
        log.info(f"Bybit active | testnet={TESTNET} | pairs={sorted(BYBIT_PAIRS)}")
    if BINGX_AVAILABLE:
        log.info(f"BingX active | demo={BINGX_DEMO} | pairs={sorted(BINGX_PAIRS)}")

_startup_warnings()


# ══════════════════════════════════════════════════════════════════════════════
# БЕЗОПАСНОСТЬ
# ══════════════════════════════════════════════════════════════════════════════
def _http_auth(req) -> bool:
    """Проверяет RENDER_SECRET. На LIVE без секрета — блокируем всё."""
    token = req.headers.get("X-Secret") or req.args.get("secret", "")
    if RENDER_SECRET:
        return token == RENDER_SECRET
    # Если секрет не задан — пускаем только в testnet (предупреждение при старте)
    return TESTNET


_rate_store: dict[str, list[float]] = {}
_rate_lock  = threading.Lock()
RATE_LIMIT  = 30
RATE_WINDOW = 60


def _rate_ok(ip: str) -> bool:
    now = time.time()
    with _rate_lock:
        times = [t for t in _rate_store.get(ip, []) if now - t < RATE_WINDOW]
        if len(times) >= RATE_LIMIT:
            return False
        times.append(now)
        _rate_store[ip] = times
    return True


# ══════════════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ══════════════════════════════════════════════════════════════════════════════
def write_log(entry: str):
    ts   = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
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
# JSON ХЕЛПЕРЫ (с атомарной записью)
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
        tmp = path + ".tmp"
        with _file_lock:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp, path)
    except Exception as e:
        write_log(f"SAVE_JSON_ERR | {path} | {e}")


def load_stats()     : return load_json(STATS_FILE,    {"wins": 0, "losses": 0, "total": 0})
def load_trades()    : return load_json(TRADES_FILE,   {})
def load_history()   : return load_json(HISTORY_FILE,  [])
def load_positions() : return load_json(POSITIONS_FILE,{})
def save_stats(d)    : save_json(STATS_FILE,    d)
def save_trades(d)   : save_json(TRADES_FILE,   d)
def save_history(d)  : save_json(HISTORY_FILE,  d)
def save_positions(d): save_json(POSITIONS_FILE, d)


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
    if ADMIN_IDS and user_id in ADMIN_IDS:
        return True
    if not ADMIN_IDS and bot and TG_CHAT:
        try:
            admins = bot.get_chat_administrators(TG_CHAT)
            return user_id in [a.user.id for a in admins]
        except Exception:
            pass
    return False


def get_exchange_for(ticker: str) -> str:
    """Возвращает 'bybit', 'bingx' или 'none'."""
    t = ticker.upper().replace(".P", "")
    if t in BINGX_PAIRS and BINGX_AVAILABLE:
        return "bingx"
    if t in BYBIT_PAIRS and BYBIT_AVAILABLE:
        return "bybit"
    return "none"


# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM — ОТПРАВКА
# ══════════════════════════════════════════════════════════════════════════════
def send_tg(text: str, thread_id=None, chat_id=None, reply_to=None) -> dict:
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


def send_signals(text: str, reply_to=None) -> dict:
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
    if not BYBIT_LIB:
        raise RuntimeError("pybit не установлен")
    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        raise RuntimeError("BYBIT_API_KEY / BYBIT_API_SECRET не заданы")
    with _bybit_lock:
        if _bybit_session is None:
            _bybit_session = BybitHTTP(
                testnet=TESTNET,
                api_key=BYBIT_API_KEY,
                api_secret=BYBIT_API_SECRET,
            )
            write_log(f"BYBIT | connected | testnet={TESTNET}")
    return _bybit_session


_instrument_cache: dict[str, tuple[dict, float]] = {}
_INSTRUMENT_TTL = 3600


def get_instrument(symbol: str) -> dict:
    now = time.time()
    if symbol in _instrument_cache:
        data, ts = _instrument_cache[symbol]
        if now - ts < _INSTRUMENT_TTL:
            return data
    resp = bybit().get_instruments_info(category="linear", symbol=symbol)
    data = resp["result"]["list"][0]
    _instrument_cache[symbol] = (data, now)
    return data


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


def bybit_get_price(symbol: str) -> float:
    resp = bybit().get_tickers(category="linear", symbol=symbol)
    return float(resp["result"]["list"][0]["lastPrice"])


def bybit_set_leverage(symbol: str, leverage: int):
    try:
        bybit().set_leverage(
            category="linear", symbol=symbol,
            buyLeverage=str(leverage), sellLeverage=str(leverage),
        )
    except Exception as e:
        write_log(f"BYBIT_LEVERAGE_WARN | {symbol} | {e}")


def bybit_place_market(symbol: str, side: str, qty: float, reduce_only: bool = False) -> dict:
    resp = bybit().place_order(
        category="linear", symbol=symbol,
        side=side, orderType="Market",
        qty=round_qty(symbol, qty),
        reduceOnly=reduce_only, timeInForce="IOC",
    )
    write_log(f"BYBIT_MARKET | {symbol} {side} qty={qty} ro={reduce_only} | ret={resp['retCode']}")
    if resp["retCode"] != 0:
        raise Exception(f"Bybit {resp['retCode']}: {resp.get('retMsg','')}")
    return resp


def bybit_place_stop(symbol: str, side: str, qty: float,
                     trigger_price: float, reduce_only: bool = True) -> dict:
    resp = bybit().place_order(
        category="linear", symbol=symbol,
        side=side, orderType="Market",
        qty=round_qty(symbol, qty),
        triggerPrice=round_price(symbol, trigger_price),
        triggerBy="LastPrice", orderFilter="StopOrder",
        reduceOnly=reduce_only, timeInForce="IOC",
    )
    write_log(f"BYBIT_STOP | {symbol} {side} qty={qty} trigger={trigger_price} | ret={resp['retCode']}")
    return resp


def bybit_cancel_all(symbol: str):
    try:
        bybit().cancel_all_orders(category="linear", symbol=symbol)
    except Exception as e:
        write_log(f"BYBIT_CANCEL_ERR | {symbol} | {e}")


def get_bybit_balance() -> str:
    try:
        resp   = bybit().get_wallet_balance(accountType="UNIFIED")
        equity = resp["result"]["list"][0]["totalEquity"]
        avail  = resp["result"]["list"][0]["totalAvailableBalance"]
        mode   = "🧪 TESTNET" if TESTNET else "🔴 LIVE"
        return (f"💰 <b>Bybit</b> {mode}\n"
                f"Equity: <b>{float(equity):.2f} USDT</b>\n"
                f"Доступно: <b>{float(avail):.2f} USDT</b>")
    except Exception as e:
        return f"❌ Ошибка Bybit: {e}"


# ══════════════════════════════════════════════════════════════════════════════
# BINGX КЛИЕНТ
# ══════════════════════════════════════════════════════════════════════════════
BINGX_BASE = "https://open-api.bingx.com"


def _bingx_to_symbol(ticker: str) -> str:
    """BTCUSDT → BTC-USDT"""
    t = ticker.upper().replace(".P", "")
    if t.endswith("USDT") and "-" not in t:
        return t[:-4] + "-USDT"
    return t


def _bingx_sign(params: dict) -> str:
    query = "&".join(f"{k}={params[k]}" for k in sorted(params))
    return _hmac.new(
        BINGX_API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _bingx_req(method: str, path: str, params: dict | None = None) -> dict:
    if params is None:
        params = {}
    params["timestamp"] = str(int(time.time() * 1000))
    params["signature"] = _bingx_sign(params)
    url     = BINGX_BASE + path
    headers = {"X-BX-APIKEY": BINGX_API_KEY}
    if BINGX_DEMO:
        headers["X-BX-DEMO-API"] = "1"
    if method == "GET":
        resp = requests.get(url, params=params, headers=headers, timeout=10)
    elif method == "POST":
        resp = requests.post(url, params=params, headers=headers, timeout=10)
    elif method == "DELETE":
        resp = requests.delete(url, params=params, headers=headers, timeout=10)
    else:
        raise ValueError(f"Unknown method: {method}")
    data = resp.json()
    code = data.get("code", 0)
    if code != 0:
        raise Exception(f"BingX error {code}: {data.get('msg', '')}")
    return data


def bingx_get_price(ticker: str) -> float:
    sym  = _bingx_to_symbol(ticker)
    data = _bingx_req("GET", "/openApi/swap/v2/quote/ticker", {"symbol": sym})
    return float(data["data"]["lastPrice"])


def bingx_set_leverage(ticker: str, leverage: int):
    sym = _bingx_to_symbol(ticker)
    for side in ("LONG", "SHORT"):
        try:
            _bingx_req("POST", "/openApi/swap/v2/trade/leverage",
                       {"symbol": sym, "side": side, "leverage": str(leverage)})
        except Exception as e:
            write_log(f"BINGX_LEVERAGE_WARN | {ticker} {side} | {e}")


def bingx_round_qty(qty: float) -> float:
    return round(math.floor(qty * 1000) / 1000, 4)


def bingx_place_market(ticker: str, side: str, qty: float,
                       reduce_only: bool = False) -> dict:
    """side: 'Buy' / 'Sell' (как Bybit, конвертируем внутри)."""
    sym      = _bingx_to_symbol(ticker)
    bx_side  = side.upper()
    pos_side = ("LONG" if bx_side == "BUY" else "SHORT") if not reduce_only \
               else ("SHORT" if bx_side == "BUY" else "LONG")
    data = _bingx_req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": sym, "side": bx_side, "positionSide": pos_side,
        "type": "MARKET", "quantity": str(bingx_round_qty(qty)),
    })
    write_log(f"BINGX_MARKET | {ticker} {side} pos={pos_side} qty={qty} ro={reduce_only}")
    return data


def bingx_place_stop(ticker: str, side: str, qty: float,
                     trigger_price: float, reduce_only: bool = True) -> dict:
    sym      = _bingx_to_symbol(ticker)
    bx_side  = side.upper()
    pos_side = ("LONG" if bx_side == "BUY" else "SHORT") if not reduce_only \
               else ("SHORT" if bx_side == "BUY" else "LONG")
    data = _bingx_req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": sym, "side": bx_side, "positionSide": pos_side,
        "type": "STOP_MARKET", "quantity": str(bingx_round_qty(qty)),
        "stopPrice": str(round(trigger_price, 8)), "workingType": "CONTRACT_PRICE",
    })
    write_log(f"BINGX_STOP | {ticker} {side} pos={pos_side} qty={qty} trigger={trigger_price}")
    return data


def bingx_cancel_all(ticker: str):
    sym = _bingx_to_symbol(ticker)
    try:
        _bingx_req("DELETE", "/openApi/swap/v2/trade/allOpenOrders", {"symbol": sym})
    except Exception as e:
        write_log(f"BINGX_CANCEL_ERR | {ticker} | {e}")


def get_bingx_balance() -> str:
    try:
        data     = _bingx_req("GET", "/openApi/account/v3/balance")
        balances = data.get("data", {}).get("balance", {})
        equity   = balances.get("equity", "?")
        avail    = balances.get("availableMargin", "?")
        mode     = "🧪 DEMO" if BINGX_DEMO else "🔴 LIVE"
        return (f"💰 <b>BingX</b> {mode}\n"
                f"Equity: <b>{float(equity):.2f} USDT</b>\n"
                f"Доступно: <b>{float(avail):.2f} USDT</b>")
    except Exception as e:
        return f"❌ Ошибка BingX: {e}"


# ══════════════════════════════════════════════════════════════════════════════
# EXCHANGE ADAPTER
# ══════════════════════════════════════════════════════════════════════════════
def ex_get_price(ticker: str, exchange: str) -> float:
    return bingx_get_price(ticker) if exchange == "bingx" else bybit_get_price(ticker)


def ex_set_leverage(ticker: str, leverage: int, exchange: str):
    if exchange == "bingx": bingx_set_leverage(ticker, leverage)
    else:                   bybit_set_leverage(ticker, leverage)


def ex_place_market(ticker: str, side: str, qty: float,
                    reduce_only: bool, exchange: str) -> dict:
    if exchange == "bingx": return bingx_place_market(ticker, side, qty, reduce_only)
    return bybit_place_market(ticker, side, qty, reduce_only)


def ex_place_stop(ticker: str, side: str, qty: float,
                  trigger_price: float, exchange: str) -> dict:
    if exchange == "bingx": return bingx_place_stop(ticker, side, qty, trigger_price)
    return bybit_place_stop(ticker, side, qty, trigger_price)


def ex_cancel_all(ticker: str, exchange: str):
    if exchange == "bingx": bingx_cancel_all(ticker)
    else:                   bybit_cancel_all(ticker)


def ex_calc_qty(ticker: str, size_usdt: float, leverage: int,
                price: float, exchange: str) -> float:
    qty = (size_usdt * leverage) / price
    if exchange == "bybit":
        return max(float(round_qty(ticker, qty)), min_qty(ticker))
    return max(bingx_round_qty(qty), 0.001)


def ex_min_qty(ticker: str, exchange: str) -> float:
    return min_qty(ticker) if exchange == "bybit" else 0.001


# ══════════════════════════════════════════════════════════════════════════════
# ХЕЛПЕРЫ ПОЗИЦИЙ
# ══════════════════════════════════════════════════════════════════════════════
def parse_price(text: str, *prefixes: str):
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


def pos_key(ticker: str, direction: str) -> str:
    return f"{ticker}_{direction}"


def move_sl(pos: dict, new_sl: float) -> str:
    exchange  = pos.get("exchange", "bybit")
    symbol    = pos["symbol"]
    opp_side  = pos["opp_side"]
    remaining = pos.get("remaining_qty", pos["total_qty"])
    if remaining <= 0:
        return ""
    ex_cancel_all(symbol, exchange)
    time.sleep(0.3)
    try:
        resp = ex_place_stop(symbol, opp_side, remaining, new_sl, exchange)
        if exchange == "bybit" and resp.get("retCode") == 0:
            return resp["result"].get("orderId", "")
        elif exchange == "bingx":
            return resp.get("data", {}).get("orderId", "") or "ok"
    except Exception as e:
        write_log(f"SL_MOVE_ERR | {symbol} ({exchange}) | {e}")
    return ""


# ══════════════════════════════════════════════════════════════════════════════
# СТАТИСТИКА И ИСТОРИЯ
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
        "exchange":     payload.get("exchange", ""),
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
        k = f"TP{r['tp_num']}"; tp_counts[k] = tp_counts.get(k, 0) + 1
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
# АКТИВНЫЕ СДЕЛКИ
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
# ОБРАБОТЧИКИ СИГНАЛОВ
# ══════════════════════════════════════════════════════════════════════════════
def handle_entry(payload: dict):
    ticker    = payload.get("ticker", "").upper().replace(".P", "")
    direction = payload.get("direction", "").upper()
    text      = payload.get("text", "").strip()
    trade_id  = str(payload.get("trade_id") or "")
    exchange  = get_exchange_for(ticker)

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
            "exchange":   exchange,
            "is_strong":  payload.get("is_strong", False),
            "created_at": int(time.time()),
        })

    if exchange == "none":
        write_log(f"ENTRY_SKIP | {ticker} | no active exchange")
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

    ex_set_leverage(ticker, leverage, exchange)

    try:
        price = ex_get_price(ticker, exchange)
    except Exception as e:
        write_log(f"ENTRY_ERR | get_price | {ticker} ({exchange}) | {e}")
        send_signals(f"❌ <b>Ошибка входа {ticker}</b>\nЦена недоступна ({exchange}): {e}")
        return

    qty = ex_calc_qty(ticker, size_usdt, leverage, price, exchange)
    write_log(f"ENTRY | {ticker} {direction} [{exchange}] price={price} qty={qty} lev={leverage}x")

    try:
        ex_place_market(ticker, side, qty, False, exchange)
    except Exception as e:
        write_log(f"ENTRY_FAIL | {ticker} ({exchange}) | {e}")
        send_signals(f"❌ <b>Ошибка входа {ticker} {direction}</b> [{exchange}]\n{e}")
        return

    time.sleep(0.5)

    sl_order_id = ""
    if sl_price:
        try:
            resp = ex_place_stop(ticker, opp_side, qty, sl_price, exchange)
            if exchange == "bybit" and resp.get("retCode") == 0:
                sl_order_id = resp["result"].get("orderId", "")
            elif exchange == "bingx":
                sl_order_id = resp.get("data", {}).get("orderId", "") or "ok"
        except Exception as e:
            write_log(f"SL_PLACE_ERR | {ticker} ({exchange}) | {e}")

    pkey = pos_key(ticker, direction)
    with _pos_lock:
        positions = load_positions()
        positions[pkey] = {
            "symbol":        ticker,
            "direction":     direction,
            "side":          side,
            "opp_side":      opp_side,
            "exchange":      exchange,
            "entry_price":   price,
            "total_qty":     qty,
            "remaining_qty": qty,
            "leverage":      leverage,
            "sl_price":      sl_price,
            "sl_order_id":   sl_order_id,
            "tp1_price":     tp1_price,
            "tp2_price":     tp2_price,
            "tp3_price":     tp3_price,
            "tp4_price":     tp4_price,
            "trade_id":      trade_id,
            "created_at":    int(time.time()),
            "trail_active":  False,
            "trail_sl":      None,
        }
        save_positions(positions)

    exch_tag = "Bybit" if exchange == "bybit" else "BingX"
    net_tag  = ("🧪 TEST" if TESTNET else "🔴 LIVE") if exchange == "bybit" \
               else ("🧪 DEMO" if BINGX_DEMO else "🔴 LIVE")
    arrow    = "🟢" if direction == "BUY" else "🔴"
    send_signals(
        f"{arrow} <b>{'LONG' if direction=='BUY' else 'SHORT'} ОТКРЫТ</b>  [{exch_tag}] {net_tag}\n"
        f"#{ticker}  |  {leverage}x  |  {size_usdt} USDT\n"
        f"📍 Вход: <b>{price}</b>  |  ⛔ SL: {sl_price or '—'}\n"
        f"✅ TP1: {tp1_price or '—'}  |  TP2: {tp2_price or '—'}\n"
        f"📦 Объём: {qty} контр."
    )


def handle_tp_hit(payload: dict):
    ticker    = payload.get("ticker", "").upper().replace(".P", "")
    direction = (payload.get("direction") or "").upper()
    tp_num    = int(payload.get("tp_num") or 0)
    text      = payload.get("text", "").strip()
    key       = build_trade_key(payload)

    trade    = get_trade(key) if key else None
    reply_id = trade["message_id"] if trade else None
    send_signals(text or f"✅ TP{tp_num} {ticker}", reply_to=reply_id)
    if tp_num >= 3:
        update_stats(True)
        save_trade_to_history(payload, trade, "win", tp_num)
        if key: remove_trade(key)

    pkey = pos_key(ticker, direction)
    with _pos_lock:
        positions = load_positions()
        pos = positions.get(pkey)
        if not pos:
            return
        if pos.get(f"tp{tp_num}_hit"):
            return  # идемпотентность

        exchange  = pos.get("exchange", "bybit")
        total_qty = pos["total_qty"]
        remaining = pos["remaining_qty"]
        opp_side  = pos["opp_side"]
        close_qty = round(total_qty * TP_CLOSE_PCT.get(tp_num, 0.0), 8)
        close_qty = min(close_qty, remaining)

        min_q = ex_min_qty(ticker, exchange)
        if close_qty < min_q:
            return

        try:
            ex_place_market(ticker, opp_side, close_qty, True, exchange)
        except Exception as e:
            write_log(f"TP_HIT_ERR | {ticker} TP{tp_num} ({exchange}) | {e}")
            return

        new_remaining = max(0.0, remaining - close_qty)
        pos[f"tp{tp_num}_hit"] = True
        pos["remaining_qty"]   = new_remaining

        if tp_num == 2 and pos.get("tp1_price"):
            oid = move_sl(pos, pos["tp1_price"])
            pos["sl_price"]    = pos["tp1_price"]
            pos["sl_order_id"] = oid

        if tp_num >= 3 and not pos["trail_active"]:
            pos["trail_active"] = True
            pos["trail_sl"]     = pos.get("sl_price")

        if new_remaining < min_q or tp_num >= 6:
            ex_cancel_all(ticker, exchange)
            del positions[pkey]
        else:
            positions[pkey] = pos
        save_positions(positions)


def handle_sl_hit(payload: dict):
    ticker    = payload.get("ticker", "").upper().replace(".P", "")
    direction = (payload.get("direction") or "").upper()
    text      = payload.get("text", "").strip()
    key       = build_trade_key(payload)

    trade    = get_trade(key) if key else None
    reply_id = trade["message_id"] if trade else None
    send_signals(text or f"🛑 SL {ticker}", reply_to=reply_id)
    update_stats(False)
    save_trade_to_history(payload, trade, "loss", 0)
    if key: remove_trade(key)

    pkey = pos_key(ticker, direction)
    with _pos_lock:
        positions = load_positions()
        pos       = positions.get(pkey)
        exchange  = pos.get("exchange", "bybit") if pos else "bybit"
        ex_cancel_all(ticker, exchange)
        positions.pop(pkey, None)
        save_positions(positions)


def handle_sl_moved(payload: dict):
    ticker    = payload.get("ticker", "").upper().replace(".P", "")
    direction = (payload.get("direction") or "").upper()
    text      = payload.get("text", "").strip()
    key       = build_trade_key(payload)

    trade    = get_trade(key) if key else None
    reply_id = trade["message_id"] if trade else None
    send_signals(text or f"🔒 SL сдвинут {ticker}", reply_to=reply_id)

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
    if _emergency_stop.is_set():
        write_log("EMERGENCY_STOP active — signal dropped")
        return

    event = payload.get("event", "unknown")
    if event == "enty":  # алиас опечатки из TradingView
        event = "entry"
    write_log(f"PROCESS | event={event} | ticker={payload.get('ticker','?')}")

    text = payload.get("text", "").strip()
    key  = build_trade_key(payload)

    if event in ("entry", "smart_entry"):
        handle_entry(payload)
    elif event == "limit_hit":
        handle_entry(payload)
    elif event == "limit_order":
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


def enqueue_signal(payload: dict):
    with _queue_lock:
        _signal_queue.append({"payload": payload, "attempts": 0, "queued_at": time.time()})


def _queue_worker():
    write_log("QUEUE_WORKER | start")
    while True:
        with _queue_lock:
            item = _signal_queue.pop(0) if _signal_queue else None
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
# FEAR & GREED
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
    v    = fg["value"]; label = fg["label"]
    bar  = _fg_bar(v); msk = _msk().strftime("%d.%m.%Y %H:%M")
    header = "🔔 <b>Fear & Greed — смена зоны!</b>" if trigger == "change" \
             else "📊 <b>Fear & Greed Index</b>"
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
# СЕССИИ
# ══════════════════════════════════════════════════════════════════════════════
SESSIONS = [
    {"name": "🇦🇺 Австралия",    "open": "02:00", "close": "09:00"},
    {"name": "🇯🇵 Азия (Токио)", "open": "03:00", "close": "09:00"},
    {"name": "🇬🇧 Европа",       "open": "10:00", "close": "18:30"},
    {"name": "🇺🇸 Америка",      "open": "16:30", "close": "23:00"},
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
        if c_min < o_min:
            is_open = cur >= o_min or cur < c_min
        else:
            is_open = o_min <= cur < c_min
        status = "🟢 Открыта" if is_open else "🔴 Закрыта"
        lines.append(f"{s['name']}  {s['open']}–{s['close']}  {status}")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# ПЛАНИРОВЩИК
# ══════════════════════════════════════════════════════════════════════════════
def _scheduler():
    write_log("SCHEDULER | start")
    while True:
        try:
            msk   = _msk()
            cur   = msk.hour * 60 + msk.minute
            h, m  = msk.hour, msk.minute
            today = msk.strftime("%Y-%m-%d")

            if h in FG_SCHEDULED_HOURS and 0 <= m <= 1:
                fg_key = f"fg_{today}_{h:02d}"
                if not _was_sent(fg_key):
                    _mark_sent(fg_key)
                    _check_fg(force=True)

            if h == 23 and 55 <= m <= 59:
                key = f"daily_{today}"
                if not _was_sent(key):
                    _mark_sent(key)
                    history    = load_history()
                    day_trades = [r for r in history if r.get("date_msk") == today]
                    if day_trades:
                        send_signals(_build_report(day_trades, f"📅 <b>Дневной отчёт {today}</b>"))

            if msk.weekday() == 6 and h == 23 and 55 <= m <= 59:
                week_key = f"{msk.year}-W{msk.isocalendar()[1]:02d}"
                key = f"weekly_{week_key}"
                if not _was_sent(key):
                    _mark_sent(key)
                    history     = load_history()
                    week_trades = [r for r in history if r.get("week_msk") == week_key]
                    if week_trades:
                        send_signals(_build_report(week_trades, f"📅 <b>Недельный отчёт {week_key}</b>"))

            last_day = calendar.monthrange(msk.year, msk.month)[1]
            if msk.day == last_day and h == 23 and 59 <= m <= 59:
                month_key = f"{msk.year}-{msk.month:02d}"
                key = f"monthly_{month_key}"
                if not _was_sent(key):
                    _mark_sent(key)
                    history      = load_history()
                    month_trades = [r for r in history if r.get("date_msk","").startswith(month_key)]
                    if month_trades:
                        send_signals(_build_report(month_trades, f"📅 <b>Месячный отчёт {month_key}</b>"))

            for s in SESSIONS:
                oh, om = map(int, s["open"].split(":"))
                ch, cm = map(int, s["close"].split(":"))
                open_min  = oh * 60 + om
                close_min = ch * 60 + cm

                if 14 <= open_min - cur <= 16:
                    k = f"sess_open_warn_{today}_{s['open']}"
                    if not _was_sent(k):
                        _mark_sent(k)
                        send_sessions(f"⏰ {s['name']} открывается через 15 мин! ({s['open']} МСК)")

                if -1 <= open_min - cur <= 1:
                    k = f"sess_open_{today}_{s['open']}"
                    if not _was_sent(k):
                        _mark_sent(k)
                        send_sessions(f"🟢 {s['name']} открылась! ({s['open']} МСК)")

                if 14 <= close_min - cur <= 16:
                    k = f"sess_close_warn_{today}_{s['close']}"
                    if not _was_sent(k):
                        _mark_sent(k)
                        send_sessions(f"⏰ {s['name']} закрывается через 15 мин! ({s['close']} МСК)")

                is_midnight = (ch == 0 and cm == 0)
                at_close    = (-1 <= cur <= 1) if is_midnight else (-1 <= close_min - cur <= 1)
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


# ══════════════════════════════════════════════════════════════════════════════
# POSITION MANAGER (Trailing Stop)
# ══════════════════════════════════════════════════════════════════════════════
def _position_manager():
    write_log("POSITION_MANAGER | start")
    while True:
        try:
            with _pos_lock:
                snapshot = dict(load_positions())

            for pkey, pos in snapshot.items():
                if not pos.get("trail_active"):
                    continue
                ticker   = pos["symbol"]
                exchange = pos.get("exchange", "bybit")
                cur_sl   = pos.get("trail_sl") or pos.get("sl_price") or 0
                try:
                    price = ex_get_price(ticker, exchange)
                except Exception:
                    continue

                if pos["direction"] == "BUY":
                    new_trail = price * (1 - TRAIL_PCT)
                    if new_trail <= cur_sl:
                        continue
                else:
                    new_trail = price * (1 + TRAIL_PCT)
                    if cur_sl != 0 and new_trail >= cur_sl:
                        continue

                oid = move_sl(pos, new_trail)
                with _pos_lock:
                    p2 = load_positions()
                    if pkey in p2:
                        p2[pkey]["trail_sl"]    = new_trail
                        p2[pkey]["sl_price"]    = new_trail
                        p2[pkey]["sl_order_id"] = oid
                        save_positions(p2)

        except Exception as e:
            write_log(f"POS_MGR_ERR | {e}")
        time.sleep(30)


if BYBIT_AVAILABLE or BINGX_AVAILABLE:
    threading.Thread(target=_position_manager, daemon=True, name="pos_mgr").start()


# ══════════════════════════════════════════════════════════════════════════════
# KEEPALIVE
# ══════════════════════════════════════════════════════════════════════════════
def _keepalive():
    time.sleep(60)
    while True:
        if RENDER_URL:
            try:
                r = requests.get(f"{RENDER_URL.rstrip('/')}/health", timeout=10)
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
            "🚀 <b>Statham Trading Bot v2</b>\n\n"
            "Принимаю сигналы от TradingView,\n"
            "публикую в группу и исполняю на Bybit / BingX.\n\n"
            "👉 /help — список команд"
        ))

    @bot.message_handler(commands=["help"])
    def cmd_help(m):
        _reply(m, (
            "📋 <b>Команды Statham Bot v2</b>\n\n"
            "<b>📊 Статистика:</b>\n"
            "/stats — Win Rate\n"
            "/daily_report — дневной отчёт\n"
            "/weekly_report — недельный отчёт\n"
            "/monthly_report — месячный отчёт\n"
            "/stats_by_ticker BTCUSDT\n"
            "/leaders — топ пар по WR\n"
            "/active — открытые позиции\n\n"
            "<b>🧭 Рынок:</b>\n"
            "/fear_greed — F&G индекс\n"
            "/sessions — торговые сессии\n"
            "/pairs — разбивка пар по биржам\n\n"
            "<b>🔧 Admin:</b>\n"
            "/balance — баланс Bybit + BingX\n"
            "/close_all — закрыть всё\n"
            "/emergency_stop — стоп сигналов\n"
            "/resume — снять стоп\n"
            "/reset_stats — сбросить статистику\n"
            "/cleanup — удалить зависшие сделки\n"
            "/clean — очистить trades+positions\n"
            "/logs — последние строки лога"
        ))

    @bot.message_handler(commands=["stats"])
    def cmd_stats(m):
        s    = load_stats()
        wins = s.get("wins", 0); losses = s.get("losses", 0); total = s.get("total", 0)
        _reply(m, (f"📊 <b>Общая статистика</b>\n\n"
                   f"Win Rate: <b>{calc_winrate(wins, total)}%</b>\n"
                   f"✅ TP: {wins}   ❌ SL: {losses}\n"
                   f"📈 Всего сделок: {total}"))

    @bot.message_handler(commands=["daily_report"])
    def cmd_daily(m):
        today = _msk().strftime("%Y-%m-%d")
        ts    = [r for r in load_history() if r.get("date_msk") == today]
        if not ts: _reply(m, f"📅 Сегодня ({today}) закрытых сделок нет."); return
        _reply(m, _build_report(ts, f"📅 <b>Дневной отчёт {today}</b>"))

    @bot.message_handler(commands=["weekly_report"])
    def cmd_weekly(m):
        msk = _msk(); wk = f"{msk.year}-W{msk.isocalendar()[1]:02d}"
        ts  = [r for r in load_history() if r.get("week_msk") == wk]
        if not ts: _reply(m, f"📅 На этой неделе ({wk}) закрытых сделок нет."); return
        _reply(m, _build_report(ts, f"📅 <b>Недельный отчёт {wk}</b>"))

    @bot.message_handler(commands=["monthly_report"])
    def cmd_monthly(m):
        msk = _msk(); mo = f"{msk.year}-{msk.month:02d}"
        ts  = [r for r in load_history() if r.get("date_msk","").startswith(mo)]
        if not ts: _reply(m, f"📅 В этом месяце ({mo}) закрытых сделок нет."); return
        _reply(m, _build_report(ts, f"📅 <b>Месячный отчёт {mo}</b>"))

    @bot.message_handler(commands=["stats_by_ticker"])
    def cmd_stats_by_ticker(m):
        parts = m.text.split()
        if len(parts) < 2:
            _reply(m, "❌ Укажи тикер: /stats_by_ticker BTCUSDT"); return
        ticker = parts[1].upper().replace(".P","")
        ts     = [r for r in load_history()
                  if r.get("ticker","").replace(".P","").upper() == ticker]
        if not ts: _reply(m, f"Нет данных по {ticker}"); return
        _reply(m, _build_report(ts, f"📊 <b>Статистика {ticker}</b>"))

    @bot.message_handler(commands=["leaders"])
    def cmd_leaders(m):
        by_ticker: dict = {}
        for r in load_history():
            tk = r.get("ticker","?")
            if tk not in by_ticker: by_ticker[tk] = {"wins":0,"losses":0}
            if r["result"] == "win": by_ticker[tk]["wins"]   += 1
            else:                    by_ticker[tk]["losses"] += 1
        rows = sorted(
            by_ticker.items(),
            key=lambda x: calc_winrate(x[1]["wins"], x[1]["wins"]+x[1]["losses"]),
            reverse=True)[:10]
        if not rows: _reply(m, "Нет данных."); return
        lines = ["🏆 <b>Топ-10 пар по WR</b>\n"]
        for tk, d in rows:
            total = d["wins"] + d["losses"]
            lines.append(f"  {tk}  {calc_winrate(d['wins'],total)}%  ({d['wins']}✅/{d['losses']}❌)")
        _reply(m, "\n".join(lines))

    @bot.message_handler(commands=["active"])
    def cmd_active(m):
        positions = load_positions()
        if not positions: _reply(m, "🔵 Нет открытых позиций."); return
        lines = [f"📌 <b>Открытых позиций: {len(positions)}</b>\n"]
        for pkey, pos in positions.items():
            exch = pos.get("exchange","?")
            lines.append(
                f"• {pos['symbol']} {pos['direction']} [{exch}]\n"
                f"  Вход: {pos.get('entry_price','?')}  |  Осталось: {pos.get('remaining_qty','?')}\n"
                f"  SL: {pos.get('sl_price','—')}  |  Trail: {'✅' if pos.get('trail_active') else '—'}"
            )
        _reply(m, "\n".join(lines))

    @bot.message_handler(commands=["pairs"])
    def cmd_pairs(m):
        mode_bybit = ("🧪 TESTNET" if TESTNET else "🔴 LIVE") if BYBIT_AVAILABLE else "❌ не настроен"
        mode_bingx = ("🧪 DEMO" if BINGX_DEMO else "🔴 LIVE") if BINGX_AVAILABLE else "❌ не настроен"
        _reply(m, (
            f"💱 <b>Биржи и пары</b>\n\n"
            f"<b>Bybit</b> [{mode_bybit}]\n{', '.join(sorted(BYBIT_PAIRS)) or '—'}\n\n"
            f"<b>BingX</b> [{mode_bingx}]\n{', '.join(sorted(BINGX_PAIRS)) or '—'}"
        ))

    @bot.message_handler(commands=["fear_greed"])
    def cmd_fear_greed(m):
        fg = _fetch_fg()
        if fg is None: _reply(m, "❌ Не удалось получить F&G."); return
        _reply(m, _build_fg_message(fg, "manual"))

    @bot.message_handler(commands=["sessions"])
    def cmd_sessions(m):
        _reply(m, _sessions_status())

    @bot.message_handler(commands=["balance"])
    def cmd_balance(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        parts = []
        if BYBIT_AVAILABLE:  parts.append(get_bybit_balance())
        if BINGX_AVAILABLE:  parts.append(get_bingx_balance())
        if not parts:        _reply(m, "❌ Ни одна биржа не настроена."); return
        _reply(m, "\n\n".join(parts))

    @bot.message_handler(commands=["close_all"])
    def cmd_close_all(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        with _pos_lock:
            positions = dict(load_positions())
        closed = []
        for pkey, pos in positions.items():
            ticker    = pos["symbol"]
            remaining = pos.get("remaining_qty", 0)
            exchange  = pos.get("exchange", "bybit")
            if remaining > 0:
                try:
                    ex_cancel_all(ticker, exchange)
                    ex_place_market(ticker, pos["opp_side"], remaining, True, exchange)
                    closed.append(f"{ticker}[{exchange}]")
                except Exception as e:
                    write_log(f"CLOSE_ALL_ERR | {ticker} | {e}")
        save_positions({}); save_trades({})
        _reply(m, f"🚨 <b>Закрыто:</b> {', '.join(closed) or 'нет'}")
        send_signals(f"🚨 <b>АВАРИЙНОЕ ЗАКРЫТИЕ</b>\n{', '.join(closed) or 'нет'}")

    @bot.message_handler(commands=["emergency_stop"])
    def cmd_emergency_stop(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        _emergency_stop.set()
        _reply(m, "🛑 Emergency stop активирован.")

    @bot.message_handler(commands=["resume"])
    def cmd_resume(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        _emergency_stop.clear()
        _reply(m, "✅ Блокировка снята.")

    @bot.message_handler(commands=["reset_stats"])
    def cmd_reset_stats(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        save_stats({"wins": 0, "losses": 0, "total": 0})
        _reply(m, "🔄 Статистика сброшена.")

    @bot.message_handler(commands=["cleanup"])
    def cmd_cleanup(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        _reply(m, f"🧹 Удалено {cleanup_old_trades()} зависших сделок.")

    @bot.message_handler(commands=["clean"])
    def cmd_clean(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        save_trades({}); save_positions({})
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
# WEBHOOK — РЕГИСТРАЦИЯ TELEGRAM
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
        "bybit":         BYBIT_AVAILABLE,
        "bingx":         BINGX_AVAILABLE,
        "bingx_demo":    BINGX_DEMO,
        "active_trades": len(load_trades()),
        "positions":     len(load_positions()),
        "emergency":     _emergency_stop.is_set(),
        "bybit_pairs":   sorted(BYBIT_PAIRS),
        "bingx_pairs":   sorted(BINGX_PAIRS),
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
    client_ip = (request.headers.get("X-Forwarded-For","") or
                 request.remote_addr or "").split(",")[0].strip()
    if not _rate_ok(client_ip):
        write_log(f"WEBHOOK_RATE_LIMIT | ip={client_ip}")
        return jsonify({"error": "rate limit"}), 429

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

    secret_from_body   = str(payload.get("secret","") or payload.get("secret_key",""))
    secret_from_header = request.headers.get("X-Secret", "")
    incoming_secret    = secret_from_body or secret_from_header

    if WEBHOOK_SECRET and incoming_secret != WEBHOOK_SECRET:
        write_log(f"WEBHOOK_FORBIDDEN | ip={client_ip}")
        return jsonify({"error": "forbidden"}), 403

    # Ранняя фильтрация по тикеру
    ticker = payload.get("ticker", "").upper().replace(".P", "")
    event  = payload.get("event", "")
    if event in ("entry", "smart_entry", "limit_hit") and ticker:
        if ALLOWED_PAIRS and ticker not in ALLOWED_PAIRS:
            write_log(f"WEBHOOK_SKIP | {ticker} not in ALLOWED_PAIRS")
            return jsonify({"status": "skipped", "reason": "ticker not allowed"}), 200

    write_log(f"WEBHOOK | event={payload.get('event','?')} ticker={payload.get('ticker','?')}")
    enqueue_signal(payload)
    return jsonify({"status": "queued", "event": payload.get("event","?")}), 200


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
    return jsonify({"webhook_registered": register_webhook()})


@app.route("/debug")
def debug():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        return "<pre>" + "".join(lines[-100:]) + "</pre>", 200, {"Content-Type": "text/html"}
    except Exception as e:
        return f"<pre>Error: {e}</pre>", 200, {"Content-Type": "text/html"}


@app.route("/trades")
def trades_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    return jsonify(load_trades())


@app.route("/stats")
def stats_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    s    = load_stats()
    wins = s.get("wins", 0); losses = s.get("losses", 0); total = s.get("total", 0)
    return jsonify({"wins": wins, "losses": losses, "total": total,
                    "winrate": calc_winrate(wins, total)})


@app.route("/history")
def history_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
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
    with _pos_lock:
        positions = dict(load_positions())
    closed = []
    for pkey, pos in positions.items():
        ticker    = pos["symbol"]
        remaining = pos.get("remaining_qty", 0)
        exchange  = pos.get("exchange", "bybit")
        if remaining > 0:
            try:
                ex_cancel_all(ticker, exchange)
                ex_place_market(ticker, pos["opp_side"], remaining, True, exchange)
                closed.append(f"{ticker}[{exchange}]")
            except Exception as e:
                write_log(f"CLOSE_ALL_ERR | {ticker} | {e}")
    save_positions({}); save_trades({})
    send_signals(f"🚨 <b>АВАРИЙНОЕ ЗАКРЫТИЕ</b>\nЗакрыто: {', '.join(closed) or 'нет'}")
    return jsonify({"closed": closed})


@app.route("/test_bybit")
def test_bybit_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    if not BYBIT_AVAILABLE:
        return jsonify({"error": "Bybit не настроен"}), 400
    return jsonify({"result": get_bybit_balance()})


@app.route("/test_bingx")
def test_bingx_route():
    if not _http_auth(request):
        return jsonify({"error": "forbidden"}), 403
    if not BINGX_AVAILABLE:
        return jsonify({"error": "BingX не настроен"}), 400
    return jsonify({"result": get_bingx_balance()})


# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
