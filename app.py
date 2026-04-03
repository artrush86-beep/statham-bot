"""
Statham Trading Bot — Render.com
Принимает сигналы от PythonAnywhere → Исполняет ордера на Bybit

Версия: 1.0 | Testnet
"""
from __future__ import annotations
import os, json, time, math, threading, logging, re
from flask import Flask, request, jsonify
import requests
from pybit.unified_trading import HTTP

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ (из переменных окружения Render)
# ══════════════════════════════════════════════════════════════════
BYBIT_API_KEY    = os.environ.get("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", "")
TESTNET          = os.environ.get("TESTNET", "true").lower() == "true"
RENDER_SECRET    = os.environ.get("RENDER_SECRET", "statham_bot_secret")
RENDER_URL       = os.environ.get("RENDER_URL", "")   # своя ссылка для keepalive

TG_TOKEN  = os.environ.get("TG_TOKEN", "")
TG_CHAT   = os.environ.get("TG_CHAT", "")
TG_TOPIC  = os.environ.get("TG_TOPIC", "")            # ID темы сигналов (опционально)

# Разрешённые пары (через запятую в env): BTCUSDT,ETHUSDT,SOLUSDT
ALLOWED_PAIRS = set(
    p.strip().upper().replace(".P", "")
    for p in os.environ.get("ALLOWED_PAIRS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")
    if p.strip()
)

# Настройки по умолчанию для всех пар
DEFAULT_LEVERAGE  = int(os.environ.get("DEFAULT_LEVERAGE", "10"))
DEFAULT_SIZE_USDT = float(os.environ.get("DEFAULT_SIZE_USDT", "50"))

# Настройки по парам (JSON в env):
# '{"BTCUSDT":{"leverage":10,"size_usdt":100},"SOLUSDT":{"leverage":5,"size_usdt":30}}'
try:
    PAIR_SETTINGS: dict = json.loads(os.environ.get("PAIR_SETTINGS_JSON", "{}"))
except Exception:
    PAIR_SETTINGS = {}

# TP: доля закрытия позиции при каждом TP (итого 100%)
TP_CLOSE_PCT = {1: 0.25, 2: 0.20, 3: 0.25, 4: 0.15, 5: 0.10, 6: 0.05}

# Trailing: % от текущей цены для трейлинг-стопа
TRAIL_PCT = float(os.environ.get("TRAIL_PCT", "0.5")) / 100.0   # 0.5% по умолчанию

# Файлы состояния (в /tmp — живут пока процесс жив; при рестарте Render — пересоздаются)
STATE_FILE = "/tmp/positions.json"
LOG_FILE   = "/tmp/bot.log"

# ══════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ══════════════════════════════════════════════════════════════════
def write_log(msg: str):
    ts   = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    log.info(msg)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) > 1000:
            with open(LOG_FILE, "w", encoding="utf-8") as f:
                f.writelines(lines[-1000:])
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════
# СОСТОЯНИЕ ПОЗИЦИЙ
# ══════════════════════════════════════════════════════════════════
_pos_lock = threading.Lock()

def load_positions() -> dict:
    with _pos_lock:
        try:
            if os.path.exists(STATE_FILE):
                with open(STATE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

def save_positions(data: dict):
    with _pos_lock:
        try:
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            write_log(f"SAVE_POS_ERR | {e}")

def get_pos(key: str) -> dict | None:
    return load_positions().get(key)

def put_pos(key: str, data: dict):
    positions = load_positions()
    positions[key] = data
    save_positions(positions)

def del_pos(key: str):
    positions = load_positions()
    positions.pop(key, None)
    save_positions(positions)

def pos_key(ticker: str, direction: str) -> str:
    return f"{ticker}_{direction}"

# ══════════════════════════════════════════════════════════════════
# BYBIT КЛИЕНТ
# ══════════════════════════════════════════════════════════════════
_bybit_session: HTTP | None = None

def bybit() -> HTTP:
    global _bybit_session
    if _bybit_session is None:
        _bybit_session = HTTP(
            testnet=TESTNET,
            api_key=BYBIT_API_KEY,
            api_secret=BYBIT_API_SECRET,
        )
        write_log(f"BYBIT | connected | testnet={TESTNET}")
    return _bybit_session

# ── Инструмент ────────────────────────────────────────────────────
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

# ── Цена ─────────────────────────────────────────────────────────
def get_price(symbol: str) -> float:
    resp = bybit().get_tickers(category="linear", symbol=symbol)
    return float(resp["result"]["list"][0]["lastPrice"])

# ── Плечо ────────────────────────────────────────────────────────
def set_leverage(symbol: str, leverage: int):
    try:
        bybit().set_leverage(
            category="linear", symbol=symbol,
            buyLeverage=str(leverage), sellLeverage=str(leverage),
        )
        write_log(f"LEVERAGE | {symbol} = {leverage}x")
    except Exception as e:
        write_log(f"LEVERAGE_WARN | {symbol} | {e}")  # часто ошибка «уже установлено»

# ── Расчёт объёма ────────────────────────────────────────────────
def calc_qty(symbol: str, size_usdt: float, leverage: int, price: float) -> float:
    notional = size_usdt * leverage
    qty = notional / price
    qty_rounded = float(round_qty(symbol, qty))
    return max(qty_rounded, min_qty(symbol))

# ── Ордера ───────────────────────────────────────────────────────
def place_market(symbol: str, side: str, qty: float, reduce_only: bool = False) -> dict:
    resp = bybit().place_order(
        category="linear",
        symbol=symbol,
        side=side,                       # "Buy" / "Sell"
        orderType="Market",
        qty=round_qty(symbol, qty),
        reduceOnly=reduce_only,
        timeInForce="IOC",
    )
    write_log(f"MARKET | {symbol} {side} qty={qty} ro={reduce_only} | ret={resp['retCode']} {resp.get('retMsg','')}")
    return resp

def place_limit(symbol: str, side: str, qty: float, price: float, reduce_only: bool = False) -> dict:
    resp = bybit().place_order(
        category="linear",
        symbol=symbol,
        side=side,
        orderType="Limit",
        qty=round_qty(symbol, qty),
        price=round_price(symbol, price),
        reduceOnly=reduce_only,
        timeInForce="GTC",
    )
    write_log(f"LIMIT  | {symbol} {side} qty={qty} price={price} ro={reduce_only} | ret={resp['retCode']}")
    return resp

def place_stop(symbol: str, side: str, qty: float, trigger_price: float, reduce_only: bool = True) -> dict:
    """Stop Market — для SL."""
    resp = bybit().place_order(
        category="linear",
        symbol=symbol,
        side=side,
        orderType="Market",
        qty=round_qty(symbol, qty),
        triggerPrice=round_price(symbol, trigger_price),
        triggerBy="LastPrice",
        orderFilter="StopOrder",
        reduceOnly=reduce_only,
        timeInForce="IOC",
    )
    write_log(f"STOP   | {symbol} {side} qty={qty} trigger={trigger_price} | ret={resp['retCode']}")
    return resp

def cancel_all(symbol: str):
    try:
        bybit().cancel_all_orders(category="linear", symbol=symbol)
        write_log(f"CANCEL_ALL | {symbol}")
    except Exception as e:
        write_log(f"CANCEL_ALL_ERR | {symbol} | {e}")

# ══════════════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════════════
def tg(text: str):
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        payload: dict = {
            "chat_id": TG_CHAT,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if TG_TOPIC:
            try:
                payload["message_thread_id"] = int(TG_TOPIC)
            except ValueError:
                pass
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json=payload, timeout=8,
        )
    except Exception as e:
        write_log(f"TG_ERR | {e}")

# ══════════════════════════════════════════════════════════════════
# ПАРСИНГ ЦЕНЫ ИЗ ТЕКСТА STATHAM
# ══════════════════════════════════════════════════════════════════
def parse_price(text: str, *prefixes: str) -> float | None:
    """Ищет цену после любого из префиксов в тексте сигнала."""
    for prefix in prefixes:
        idx = text.find(prefix)
        if idx == -1:
            continue
        substr = text[idx + len(prefix):].strip()
        m = re.search(r"[\d]+\.?[\d]*", substr)
        if m:
            try:
                return float(m.group())
            except ValueError:
                pass
    return None

# ══════════════════════════════════════════════════════════════════
# ПЕРЕДВИЖЕНИЕ SL
# ══════════════════════════════════════════════════════════════════
def move_sl(pos: dict, new_sl: float) -> str:
    """Отменяет старый SL, ставит новый. Возвращает новый order_id."""
    symbol    = pos["symbol"]
    opp_side  = pos["opp_side"]
    remaining = pos.get("remaining_qty", pos["total_qty"])

    if remaining <= 0:
        return ""

    cancel_all(symbol)
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

# ══════════════════════════════════════════════════════════════════
# ОБРАБОТЧИКИ СОБЫТИЙ
# ══════════════════════════════════════════════════════════════════
def handle_entry(payload: dict):
    ticker    = payload.get("ticker", "").replace(".P", "").upper()
    direction = payload.get("direction", "").upper()
    trade_id  = str(payload.get("trade_id") or "")
    text      = payload.get("text", "")

    if ticker not in ALLOWED_PAIRS:
        write_log(f"ENTRY_SKIP | {ticker} not in ALLOWED_PAIRS")
        return

    side     = "Buy"  if direction == "BUY"  else "Sell"
    opp_side = "Sell" if side == "Buy" else "Buy"

    # Настройки пары
    cfg      = {**{"leverage": DEFAULT_LEVERAGE, "size_usdt": DEFAULT_SIZE_USDT},
                **PAIR_SETTINGS.get(ticker, {})}
    leverage = int(cfg["leverage"])
    size_usdt = float(cfg["size_usdt"])

    # Парсим SL / TP из текста
    sl_price  = parse_price(text, "SL:", "⛔ SL:")
    tp1_price = parse_price(text, "TP1:", "✅ TP1:")
    tp2_price = parse_price(text, "TP2:", "✅ TP2:")
    tp3_price = parse_price(text, "TP3:", "✅ TP3:")
    tp4_price = parse_price(text, "TP4:", "✅ TP4:")
    tp5_price = parse_price(text, "TP5:", "✅ TP5:")
    tp6_price = parse_price(text, "TP6:", "✅ TP6:")

    # Установка плеча
    set_leverage(ticker, leverage)

    # Текущая цена
    try:
        price = get_price(ticker)
    except Exception as e:
        write_log(f"ENTRY_ERR | get_price | {ticker} | {e}")
        tg(f"❌ <b>Ошибка входа {ticker}</b>\nНе удалось получить цену: {e}")
        return

    # Объём
    qty = calc_qty(ticker, size_usdt, leverage, price)
    write_log(f"ENTRY | {ticker} {direction} | price={price} qty={qty} lev={leverage}x")

    # Маркет-ордер
    try:
        resp = place_market(ticker, side, qty)
        if resp["retCode"] != 0:
            raise Exception(f"{resp['retCode']}: {resp.get('retMsg','')}")
    except Exception as e:
        write_log(f"ENTRY_FAIL | {ticker} | {e}")
        tg(f"❌ <b>Ошибка входа {ticker} {direction}</b>\n{e}")
        return

    time.sleep(0.5)  # ждём исполнения

    # SL stop-ордер (страховка на бирже)
    sl_order_id = ""
    if sl_price:
        try:
            sl_resp = place_stop(ticker, opp_side, qty, sl_price)
            if sl_resp["retCode"] == 0:
                sl_order_id = sl_resp["result"].get("orderId", "")
        except Exception as e:
            write_log(f"SL_PLACE_ERR | {ticker} | {e}")

    # Сохраняем позицию
    pkey = pos_key(ticker, direction)
    put_pos(pkey, {
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
        "tp5_price":    tp5_price,
        "tp6_price":    tp6_price,
        "tp1_hit": False, "tp2_hit": False, "tp3_hit": False,
        "tp4_hit": False, "tp5_hit": False, "tp6_hit": False,
        "trade_id":     trade_id,
        "created_at":   int(time.time()),
        "trail_active": False,
        "trail_sl":     None,
    })

    net_tag = "🧪 TESTNET" if TESTNET else "🔴 LIVE"
    arrow   = "🟢" if direction == "BUY" else "🔴"
    tg(
        f"{arrow} <b>{'LONG' if direction=='BUY' else 'SHORT'} ОТКРЫТ</b>  {net_tag}\n"
        f"━━━━━━━\n"
        f"#{ticker}  |  {leverage}x  |  {size_usdt} USDT\n"
        f"📍 Вход: <b>{price}</b>\n"
        f"⛔ SL: {sl_price or '—'}\n"
        f"✅ TP1: {tp1_price or '—'}\n"
        f"📦 Объём: {qty} контр.\n"
        f"🆔 {trade_id[:12] if trade_id else '—'}"
    )


def handle_tp_hit(payload: dict):
    ticker    = payload.get("ticker", "").replace(".P", "").upper()
    direction = (payload.get("direction") or "").upper()
    tp_num    = int(payload.get("tp_num") or 0)

    if not ticker or not tp_num:
        return

    pkey = pos_key(ticker, direction)
    pos  = get_pos(pkey)
    if not pos:
        write_log(f"TP_HIT | {ticker} TP{tp_num} | нет позиции в state")
        return

    if pos.get(f"tp{tp_num}_hit"):
        write_log(f"TP_HIT | {ticker} TP{tp_num} | уже обработан")
        return

    total_qty = pos["total_qty"]
    remaining = pos["remaining_qty"]
    opp_side  = pos["opp_side"]

    # Сколько закрываем
    close_qty = round(total_qty * TP_CLOSE_PCT.get(tp_num, 0.0), 8)
    close_qty = min(close_qty, remaining)

    if close_qty < min_qty(ticker):
        write_log(f"TP_HIT | {ticker} TP{tp_num} | close_qty={close_qty} < min_qty")
        return

    write_log(f"TP_HIT | {ticker} TP{tp_num} | закрываем {close_qty} из {remaining}")

    try:
        resp = place_market(ticker, opp_side, close_qty, reduce_only=True)
        if resp["retCode"] != 0:
            raise Exception(f"{resp['retCode']}: {resp.get('retMsg','')}")
    except Exception as e:
        write_log(f"TP_HIT_ERR | {ticker} TP{tp_num} | {e}")
        tg(f"❌ <b>Ошибка TP{tp_num} {ticker}</b>\n{e}")
        return

    new_remaining = max(0.0, remaining - close_qty)
    pos[f"tp{tp_num}_hit"] = True
    pos["remaining_qty"]   = new_remaining

    # SL → TP1 после TP2
    if tp_num == 2 and pos.get("tp1_price"):
        new_sl = pos["tp1_price"]
        oid = move_sl(pos, new_sl)
        pos["sl_price"]    = new_sl
        pos["sl_order_id"] = oid
        write_log(f"SL_TO_TP1 | {ticker} SL={new_sl}")

    # Трейлинг после TP3
    if tp_num >= 3 and not pos["trail_active"]:
        pos["trail_active"] = True
        pos["trail_sl"]     = pos.get("sl_price")
        write_log(f"TRAIL_ON | {ticker}")

    # Всё закрыто?
    if new_remaining < min_qty(ticker) or tp_num >= 6:
        cancel_all(ticker)
        del_pos(pkey)
        tg(f"🏆 <b>Позиция закрыта полностью!</b>\n#{ticker}  TP{tp_num}")
        return

    put_pos(pkey, pos)

    try:
        price = get_price(ticker)
    except Exception:
        price = pos["entry_price"]

    entry = pos["entry_price"]
    pnl   = ((price - entry) / entry * 100) if pos["direction"] == "BUY" else \
            ((entry - price) / entry * 100)

    tg(
        f"✅ <b>TP{tp_num} достигнут!</b>\n"
        f"━━━━━━━\n"
        f"#{ticker}  |  закрыто {int(TP_CLOSE_PCT.get(tp_num,0)*100)}%\n"
        f"💰 Цена: {price}\n"
        f"📈 П&Л: {pnl:+.2f}%\n"
        f"📦 Осталось: {round(new_remaining, 4)}"
    )


def handle_sl_moved(payload: dict):
    ticker    = payload.get("ticker", "").replace(".P", "").upper()
    direction = (payload.get("direction") or "").upper()
    text      = payload.get("text", "")

    pkey = pos_key(ticker, direction)
    pos  = get_pos(pkey)
    if not pos:
        return

    # Ищем новый SL в тексте «✅ Стало: 67123.45»
    new_sl = parse_price(text, "✅ Стало:", "Стало:")
    if not new_sl:
        write_log(f"SL_MOVED | {ticker} | не удалось распарсить новый SL из текста")
        return

    oid = move_sl(pos, new_sl)
    pos["sl_price"]    = new_sl
    pos["sl_order_id"] = oid
    put_pos(pkey, pos)

    tg(f"🔒 <b>SL передвинут</b>\n#{ticker} → {new_sl}")


def handle_sl_hit(payload: dict):
    ticker    = payload.get("ticker", "").replace(".P", "").upper()
    direction = (payload.get("direction") or "").upper()

    pkey = pos_key(ticker, direction)
    pos  = get_pos(pkey)

    cancel_all(ticker)
    if pos:
        del_pos(pkey)

    tg(f"🛑 <b>SL сработал</b>\n#{ticker}  {'LONG' if direction=='BUY' else 'SHORT'}")


def handle_limit_order(payload: dict):
    """Лимитка выставлена Statham — показываем инфо, ждём limit_hit."""
    ticker = payload.get("ticker", "").replace(".P", "").upper()
    write_log(f"LIMIT_ORDER_INFO | {ticker} | ждём limit_hit")


def handle_limit_hit(payload: dict):
    """Лимитка исполнена → открываем позицию на бирже."""
    handle_entry(payload)


# ══════════════════════════════════════════════════════════════════
# POSITION MANAGER — фоновый поток (Trailing Stop)
# ══════════════════════════════════════════════════════════════════
def _position_manager():
    write_log("POSITION_MANAGER | старт")
    while True:
        try:
            positions = load_positions()
            for pkey, pos in list(positions.items()):
                if not pos.get("trail_active"):
                    continue

                ticker    = pos["symbol"]
                direction = pos["direction"]
                cur_sl    = pos.get("trail_sl") or pos.get("sl_price") or 0

                try:
                    price = get_price(ticker)
                except Exception:
                    continue

                if direction == "BUY":
                    new_trail = price * (1 - TRAIL_PCT)
                    if new_trail > cur_sl:
                        write_log(f"TRAIL | {ticker} BUY | SL {cur_sl:.4f} → {new_trail:.4f}")
                        oid = move_sl(pos, new_trail)
                        pos["trail_sl"]    = new_trail
                        pos["sl_price"]    = new_trail
                        pos["sl_order_id"] = oid
                        positions[pkey] = pos
                        save_positions(positions)
                else:
                    new_trail = price * (1 + TRAIL_PCT)
                    if cur_sl == 0 or new_trail < cur_sl:
                        write_log(f"TRAIL | {ticker} SELL | SL {cur_sl:.4f} → {new_trail:.4f}")
                        oid = move_sl(pos, new_trail)
                        pos["trail_sl"]    = new_trail
                        pos["sl_price"]    = new_trail
                        pos["sl_order_id"] = oid
                        positions[pkey] = pos
                        save_positions(positions)

        except Exception as e:
            write_log(f"POS_MGR_ERR | {e}")

        time.sleep(30)   # проверяем каждые 30 сек

threading.Thread(target=_position_manager, daemon=True, name="pos_mgr").start()


# ══════════════════════════════════════════════════════════════════
# KEEPALIVE — предотвращает засыпание Render web service
# ══════════════════════════════════════════════════════════════════
def _keepalive():
    time.sleep(60)
    while True:
        try:
            if RENDER_URL:
                r = requests.get(f"{RENDER_URL}/health", timeout=10)
                write_log(f"KEEPALIVE | {r.status_code}")
        except Exception as e:
            write_log(f"KEEPALIVE_ERR | {e}")
        time.sleep(600)   # каждые 10 минут

threading.Thread(target=_keepalive, daemon=True, name="keepalive").start()


# ══════════════════════════════════════════════════════════════════
# FLASK ROUTES
# ══════════════════════════════════════════════════════════════════
def _auth(req) -> bool:
    secret = req.headers.get("X-Secret") or req.args.get("secret", "")
    return not RENDER_SECRET or secret == RENDER_SECRET


@app.route("/health")
def health():
    return jsonify({
        "status":         "ok",
        "testnet":        TESTNET,
        "positions":      len(load_positions()),
        "allowed_pairs":  sorted(ALLOWED_PAIRS),
        "trail_pct":      f"{TRAIL_PCT*100:.2f}%",
        "time_utc":       time.strftime("%Y-%m-%d %H:%M:%S"),
    })


@app.route("/signal", methods=["POST"])
def signal():
    if not _auth(request):
        return jsonify({"error": "forbidden"}), 403

    payload = request.get_json(force=True, silent=True) or {}
    if not payload:
        return jsonify({"status": "empty"}), 200

    event  = payload.get("event", "")
    ticker = payload.get("ticker", "").replace(".P", "").upper()
    write_log(f"SIGNAL | event={event} ticker={ticker} tp={payload.get('tp_num',0)}")

    try:
        if event in ("entry", "smart_entry"):
            handle_entry(payload)
        elif event == "tp_hit":
            handle_tp_hit(payload)
        elif event == "sl_moved":
            handle_sl_moved(payload)
        elif event == "sl_hit":
            handle_sl_hit(payload)
        elif event == "limit_order":
            handle_limit_order(payload)
        elif event == "limit_hit":
            handle_limit_hit(payload)
        else:
            write_log(f"SIGNAL | unknown event={event}")
    except Exception as e:
        write_log(f"SIGNAL_ERR | {event} | {e}")
        tg(f"❌ <b>Ошибка бота</b>\nevent={event}  #{ticker}\n<code>{e}</code>")
        return jsonify({"error": str(e)}), 500

    return jsonify({"status": "ok", "event": event}), 200


@app.route("/positions")
def positions_route():
    if not _auth(request):
        return jsonify({"error": "forbidden"}), 403
    return jsonify(load_positions())


@app.route("/log")
def log_route():
    if not _auth(request):
        return jsonify({"error": "forbidden"}), 403
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        return jsonify({"lines": lines[-100:], "total": len(lines)})
    except Exception:
        return jsonify({"lines": [], "total": 0})


@app.route("/close_all", methods=["POST"])
def close_all():
    """Аварийное закрытие всех позиций."""
    if not _auth(request):
        return jsonify({"error": "forbidden"}), 403

    positions = load_positions()
    closed = []
    for pkey, pos in positions.items():
        ticker    = pos["symbol"]
        remaining = pos.get("remaining_qty", 0)
        if remaining > 0:
            try:
                cancel_all(ticker)
                place_market(ticker, pos["opp_side"], remaining, reduce_only=True)
                closed.append(ticker)
                write_log(f"CLOSE_ALL | {ticker} closed {remaining}")
            except Exception as e:
                write_log(f"CLOSE_ALL_ERR | {ticker} | {e}")

    save_positions({})
    tg(f"🚨 <b>АВАРИЙНОЕ ЗАКРЫТИЕ</b>\nЗакрыто: {', '.join(closed) or 'нет позиций'}")
    return jsonify({"closed": closed})


@app.route("/test_bybit")
def test_bybit():
    """Проверка подключения к Bybit."""
    if not _auth(request):
        return jsonify({"error": "forbidden"}), 403
    try:
        resp = bybit().get_wallet_balance(accountType="UNIFIED")
        balance = resp["result"]["list"][0]["totalEquity"]
        return jsonify({"status": "ok", "testnet": TESTNET, "balance_usdt": balance})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
