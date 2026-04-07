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

# ── Redis (Upstash) ────────────────────────────────────────────────────────────
try:
    import redis as _redis_lib
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

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
TG_CHAT           = os.environ.get("TG_CHAT",           "-1003867089540")
TG_SIGNALS_TOPIC  = os.environ.get("TG_SIGNALS_TOPIC",  "6314")
TG_SESSIONS_TOPIC = os.environ.get("TG_SESSIONS_TOPIC", "1")
RENDER_URL        = os.environ.get("RENDER_URL",        "")

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
                 default: str = "YBUSDT,ASTERUSDT,EDGEUSDT,STOUSDT") -> set[str]:
    raw = os.environ.get(env_key, "").strip()
    if not raw:
        raw = os.environ.get(fallback_key, default)
    return {p.strip().upper().replace(".P", "") for p in raw.split(",") if p.strip()}


BYBIT_PAIRS   = _parse_pairs("BYBIT_PAIRS")
BINGX_PAIRS   = _parse_pairs("BINGX_PAIRS")
ALLOWED_PAIRS = BYBIT_PAIRS | BINGX_PAIRS

DEFAULT_LEVERAGE  = int(os.environ.get("DEFAULT_LEVERAGE",  "10"))
DEFAULT_SIZE_USDT = float(os.environ.get("DEFAULT_SIZE_USDT", "1"))
TRAIL_PCT         = float(os.environ.get("TRAIL_PCT","0.5")) / 100.0

try:
    PAIR_SETTINGS: dict = json.loads(os.environ.get("PAIR_SETTINGS_JSON", "{}"))
except Exception:
    PAIR_SETTINGS = {}

TRAIL_PCT    = float(os.environ.get("TRAIL_PCT", "0.5")) / 100.0
TP_CLOSE_PCT = {1: 0.25, 2: 0.20, 3: 0.25, 4: 0.15, 5: 0.10, 6: 0.05}

DATA_DIR = os.environ.get("DATA_DIR", "/tmp")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Redis connection ───────────────────────────────────────────────────────────
_redis_client = None
_REDIS_PREFIX  = "statham:"

def _get_redis():
    """Возвращает Redis-клиент или None если Redis недоступен."""
    global _redis_client
    if not _REDIS_AVAILABLE:
        return None
    if _redis_client is not None:
        return _redis_client
    url = os.environ.get("REDIS_URL", "").strip()
    if not url:
        return None
    try:
        client = _redis_lib.from_url(
            url, decode_responses=True,
            socket_timeout=5, socket_connect_timeout=5,
        )
        client.ping()
        _redis_client = client
        log.info("Redis | connected OK")
    except Exception as e:
        log.warning(f"Redis | connect failed: {e}")
    return _redis_client

def _rkey(path: str) -> str:
    """Преобразует путь к файлу в ключ Redis: /tmp/active_trades.json → statham:active_trades"""
    return _REDIS_PREFIX + os.path.basename(path).replace(".json", "")

STATS_FILE      = os.path.join(DATA_DIR, "trade_stats.json")
TRADES_FILE     = os.path.join(DATA_DIR, "active_trades.json")
HISTORY_FILE    = os.path.join(DATA_DIR, "trade_history.json")
POSITIONS_FILE  = os.path.join(DATA_DIR, "positions.json")
FG_STATE_FILE   = os.path.join(DATA_DIR, "fg_state.json")
SENT_FLAGS_FILE = os.path.join(DATA_DIR, "sent_flags.json")
CLOSED_TRADES_FILE = os.path.join(DATA_DIR, "closed_trades.json")
LOG_FILE        = os.path.join(DATA_DIR, "bot.log")

MAX_QUEUE_ATTEMPTS = 3
QUEUE_RETRY_DELAY  = 15
_signal_queue: list[dict] = []
_queue_lock   = threading.Lock()
_pos_lock     = threading.Lock()
_bybit_lock   = threading.Lock()
_state_lock   = threading.RLock()
_bg_lock      = threading.Lock()
_emergency_stop = threading.Event()
_bg_started   = False

FG_SCHEDULED_HOURS = {4, 10, 16, 22}

bot = telebot.TeleBot(TG_TOKEN, threaded=False) if TG_TOKEN else None


# ══════════════════════════════════════════════════════════════════════════════
# СТАРТОВЫЕ ПРЕДУПРЕЖДЕНИЯ
# ══════════════════════════════════════════════════════════════════════════════
def _startup_warnings():
    # Секреты не используются
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
    """Авторизация отключена — все эндпоинты открыты."""
    return True


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
    # 1) Пробуем Redis
    r = _get_redis()
    if r is not None:
        try:
            val = r.get(_rkey(path))
            if val is not None:
                return json.loads(val)
        except Exception as e:
            log.warning(f"REDIS_LOAD_ERR | {_rkey(path)} | {e}")
    # 2) Фолбэк: файл
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
    serialized = json.dumps(data, ensure_ascii=False)
    # 1) Сохраняем в Redis
    r = _get_redis()
    if r is not None:
        try:
            r.set(_rkey(path), serialized)
        except Exception as e:
            log.warning(f"REDIS_SAVE_ERR | {_rkey(path)} | {e}")
    # 2) Фолбэк: файл (атомарная запись)
    try:
        tmp = path + ".tmp"
        with _file_lock:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(serialized)
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


def load_closed_trades() -> dict:
    return load_json(CLOSED_TRADES_FILE, {})


def save_closed_trades(d: dict):
    save_json(CLOSED_TRADES_FILE, d)


def _was_sent(key: str) -> bool:
    with _state_lock:
        return load_json(SENT_FLAGS_FILE, {}).get(key, False)


def _mark_sent(key: str):
    with _state_lock:
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
    for attempt in range(3):
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json=payload, timeout=15,
            )
            data = resp.json()
            if not data.get("ok"):
                write_log(f"SEND_TG_FAIL | attempt={attempt} | {data.get('description')}")
                time.sleep(2)
                continue
            write_log(f"SEND_TG_OK | chat={chat_id} thread={thread_id}")
            return data["result"]
        except Exception as e:
            write_log(f"SEND_TG_ERR | attempt={attempt} | {e}")
            time.sleep(2)
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
BINGX_BASE = "https://open-api-vst.bingx.com" if BINGX_DEMO else "https://open-api.bingx.com"


def _bingx_to_symbol(ticker: str) -> str:
    """BTCUSDT → BTC-USDT"""
    t = ticker.upper().replace(".P", "")
    if t.endswith("USDT") and "-" not in t:
        return t[:-4] + "-USDT"
    return t


def _bingx_sign(params: dict) -> str:
    # BingX требует строку без сортировки, в том порядке как параметры добавлялись
    # Но поскольку dict в Python 3.7+ сохраняет порядок, используем items()
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return _hmac.new(
        BINGX_API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _bingx_req(method: str, path: str, params: dict | None = None) -> dict:
    if params is None:
        params = {}
    params["timestamp"] = str(int(time.time() * 1000))
    # Строим строку запроса в том же порядке что и подпись
    query = "&".join(f"{k}={params[k]}" for k in params)
    sig   = _hmac.new(
        BINGX_API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    url     = BINGX_BASE + path + "?" + query + "&signature=" + sig
    headers = {"X-BX-APIKEY": BINGX_API_KEY}
    if method == "GET":
        resp = requests.get(url, headers=headers, timeout=10)
    elif method == "POST":
        resp = requests.post(url, headers=headers, timeout=10)
    elif method == "DELETE":
        resp = requests.delete(url, headers=headers, timeout=10)
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
        if BINGX_DEMO:
            data = _bingx_req("GET", "/openApi/swap/v2/user/balance")
            # balance — это список, берём первый элемент
            balance_data = data.get("data", {}).get("balance", [])
            if isinstance(balance_data, list):
                d = balance_data[0] if balance_data else {}
            else:
                d = balance_data
            equity = d.get("equity", "?")
            avail  = d.get("availableMargin", "?")
        else:
            data     = _bingx_req("GET", "/openApi/account/v3/balance")
            balances = data.get("data", {}).get("balance", {})
            equity   = balances.get("equity", "?")
            avail    = balances.get("availableMargin", "?")

        mode = "🧪 DEMO" if BINGX_DEMO else "🔴 LIVE"
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


def infer_entry_price(text: str):
    return parse_price(text, "🎯 Вход:", "⚡ Вход:", "💰 Цена:", "Цена:")


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
def _trade_instance_id(key: str = "", trade_data: dict | None = None,
                       pos: dict | None = None, payload: dict | None = None) -> str:
    for source in (trade_data, pos):
        if source and source.get("instance_id"):
            return str(source["instance_id"])

    created_at = 0
    for source in (trade_data, pos):
        if source and source.get("created_at"):
            created_at = int(source["created_at"])
            break

    trade_id = ""
    for source in (trade_data, pos, payload or {}):
        if source and source.get("trade_id"):
            trade_id = str(source["trade_id"]).strip()
            break

    ticker = ""
    direction = ""
    timeframe = ""
    for source in (trade_data, pos, payload or {}):
        if source:
            ticker = ticker or str(source.get("ticker") or source.get("symbol") or "").strip()
            direction = direction or str(source.get("direction") or "").strip()
            timeframe = timeframe or str(source.get("timeframe") or "").strip()

    base = trade_id or key or "_".join(x for x in (ticker, direction, timeframe) if x)
    if not base:
        base = "trade"
    if not created_at:
        created_at = int(time.time())
    return f"{base}:{created_at}"


def _highest_tp_hit(pos: dict | None) -> int:
    if not pos:
        return 0
    hits = [n for n in range(1, 7) if pos.get(f"tp{n}_hit")]
    return max(hits, default=0)


def update_stats(is_win: bool):
    with _state_lock:
        s = load_stats()
        s["total"] = s.get("total", 0) + 1
        if is_win:
            s["wins"] = s.get("wins", 0) + 1
        else:
            s["losses"] = s.get("losses", 0) + 1
        save_stats(s)


def save_trade_to_history(payload: dict, trade_data: dict | None, result: str, tp_num: int,
                          close_reason: str = "", pos: dict | None = None):
    trade_data = trade_data or {}
    pos = pos or {}
    now = int(time.time())
    entry_time = (
        trade_data.get("created_at")
        or pos.get("created_at")
        or now
    )
    trade_key = (
        trade_data.get("trade_key")
        or pos.get("trade_key")
        or build_trade_key(payload)
    )
    instance_id = _trade_instance_id(trade_key, trade_data, pos, payload)
    record = {
        "instance_id": instance_id,
        "trade_key": trade_key,
        "trade_id": (
            str(payload.get("trade_id") or trade_data.get("trade_id") or pos.get("trade_id") or "")
        ),
        "ticker": (
            payload.get("ticker")
            or trade_data.get("ticker")
            or pos.get("symbol")
            or ""
        ),
        "direction": (
            payload.get("direction")
            or trade_data.get("direction")
            or pos.get("direction")
            or ""
        ),
        "timeframe": (
            payload.get("timeframe")
            or trade_data.get("timeframe")
            or pos.get("timeframe")
            or ""
        ),
        "exchange": (
            payload.get("exchange")
            or trade_data.get("exchange")
            or pos.get("exchange")
            or ""
        ),
        "trade_mode": (
            trade_data.get("trade_mode")
            or pos.get("trade_mode")
            or ("telegram_only" if (payload.get("exchange") or pos.get("exchange")) == "none" else "trade")
        ),
        "is_strong": bool(
            payload.get("is_strong", trade_data.get("is_strong", pos.get("is_strong", False)))
        ),
        "entry_message_id": trade_data.get("message_id") or pos.get("message_id"),
        "signal_message_id": trade_data.get("signal_message_id") or pos.get("signal_message_id"),
        "entry_price": trade_data.get("entry_price") or pos.get("entry_price"),
        "sl_price": pos.get("sl_price", trade_data.get("sl_price")),
        "total_qty": pos.get("total_qty", trade_data.get("total_qty")),
        "remaining_qty": pos.get("remaining_qty", trade_data.get("remaining_qty")),
        "highest_tp_hit": max(tp_num, _highest_tp_hit(pos)),
        "entry_time": entry_time,
        "close_time": now,
        "duration_sec": max(0, now - entry_time),
        "result": result,
        "tp_num": tp_num,
        "close_reason": close_reason or payload.get("event", ""),
        "date_msk": _msk().strftime("%Y-%m-%d"),
        "week_msk": f"{_msk().year}-W{_msk().isocalendar()[1]:02d}",
    }
    with _state_lock:
        history = load_history()
        history.append(record)
        if len(history) > 5000:
            history = history[-5000:]
        save_history(history)


def _trade_already_closed(instance_id: str) -> bool:
    if not instance_id:
        return False
    with _state_lock:
        return instance_id in load_closed_trades()


def _mark_trade_closed(instance_id: str, result: str, close_reason: str) -> bool:
    if not instance_id:
        return True
    with _state_lock:
        closed = load_closed_trades()
        if instance_id in closed:
            return False
        closed[instance_id] = {
            "closed_at": int(time.time()),
            "result": result,
            "close_reason": close_reason,
        }
        if len(closed) > 5000:
            closed = dict(list(closed.items())[-5000:])
        save_closed_trades(closed)
        return True


def _build_report(trades: list, title: str) -> str:
    wins = [r for r in trades if r["result"] == "win"]
    losses = [r for r in trades if r["result"] == "loss"]
    manuals = [r for r in trades if r["result"] == "manual"]
    scored_total = len(wins) + len(losses)
    total = len(trades)
    wr = calc_winrate(len(wins), scored_total)
    avg_dur = int(sum(r.get("duration_sec", 0) for r in trades) / total) if total else 0
    tp_counts: dict = {}
    for r in wins:
        k = f"TP{r['tp_num']}"
        tp_counts[k] = tp_counts.get(k, 0) + 1
    text = f"{title}\n\n"
    text += f"📊 Win Rate: <b>{wr}%</b>\n"
    text += f"✅ TP: {len(wins)}   ❌ SL: {len(losses)}\n"
    text += f"📈 Всего закрыто: {total}\n"
    if manuals:
        text += f"🧯 Manual close: {len(manuals)}\n"
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


def find_trade_entry(key: str = "", trade_id: str = "", ticker: str = "",
                     direction: str = "") -> tuple[str | None, dict | None]:
    ticker = ticker.upper().replace(".P", "") if ticker else ""
    direction = direction.upper() if direction else ""
    trade_id = str(trade_id or "").strip()
    with _state_lock:
        trades = load_trades()
        if key and key in trades:
            return key, trades[key]
        if trade_id and trade_id in trades:
            return trade_id, trades[trade_id]
        if key and "_" in key:
            parts = key.split("_")
            if len(parts) >= 2:
                ticker = ticker or parts[0]
                direction = direction or parts[1]
        if ticker and direction:
            for stored_key, trade in trades.items():
                if trade.get("ticker", "") == ticker and trade.get("direction", "") == direction:
                    return stored_key, trade
    return None, None


def put_trade(key: str, data: dict):
    with _state_lock:
        t = load_trades()
        t[key] = data
        save_trades(t)


def get_trade(key: str) -> dict | None:
    _, trade = find_trade_entry(key=key)
    return trade


def touch_trade(key: str | None, **fields):
    if not key:
        return
    with _state_lock:
        trades = load_trades()
        trade = trades.get(key)
        if not trade:
            return
        trade.update(fields)
        trades[key] = trade
        save_trades(trades)


def remove_trade(key: str):
    if not key:
        return
    with _state_lock:
        t = load_trades()
        if key in t:
            del t[key]
            save_trades(t)


def dedup_entry(ticker: str, direction: str):
    with _state_lock:
        trades = load_trades()
        keys_to_remove = [
            k for k, v in trades.items()
            if v.get("ticker", "") == ticker and v.get("direction", "") == direction
        ]
        for k in keys_to_remove:
            del trades[k]
        if keys_to_remove:
            save_trades(trades)
            write_log(f"DEDUP | removed {len(keys_to_remove)} old keys for {ticker}_{direction}")


def cleanup_old_trades() -> int:
    with _state_lock:
        trades = load_trades()
        cutoff = int(time.time()) - 7 * 86400
        removed = [k for k, v in trades.items() if v.get("created_at", 0) < cutoff]
        for k in removed:
            del trades[k]
        if removed:
            save_trades(trades)
        return len(removed)


def finalize_trade(payload: dict, trade_key: str | None, trade_data: dict | None,
                   pos: dict | None, result: str, tp_num: int,
                   close_reason: str) -> bool:
    trade_data = trade_data or {}
    pos = pos or {}
    actual_key = trade_key or trade_data.get("trade_key") or pos.get("trade_key") or build_trade_key(payload)
    instance_id = _trade_instance_id(actual_key, trade_data, pos, payload)
    if not _mark_trade_closed(instance_id, result, close_reason):
        write_log(f"FINALIZE_SKIP | {instance_id} already closed")
        return False
    if result in ("win", "loss"):
        update_stats(result == "win")
    save_trade_to_history(payload, trade_data, result, tp_num, close_reason=close_reason, pos=pos)
    if actual_key:
        remove_trade(actual_key)
    return True


# ══════════════════════════════════════════════════════════════════════════════
# ОБРАБОТЧИКИ СИГНАЛОВ
# ══════════════════════════════════════════════════════════════════════════════
def handle_entry(payload: dict):
    ticker = payload.get("ticker", "").upper().replace(".P", "")
    direction = payload.get("direction", "").upper()
    text = payload.get("text", "").strip()
    trade_id = str(payload.get("trade_id") or "")
    timeframe = payload.get("timeframe", "")
    exchange = get_exchange_for(ticker)
    key = build_trade_key(payload)
    pkey = pos_key(ticker, direction)
    trade_mode = "trade" if exchange != "none" else "telegram_only"

    with _pos_lock:
        existing_pos = load_positions().get(pkey)
    if existing_pos:
        write_log(f"ENTRY_DUPLICATE_SKIP | {ticker} {direction} | active position exists")
        return

    source_msg = send_signals(text or f"📥 Вход {ticker} {direction}")
    source_msg_id = source_msg.get("message_id")

    side = "Buy" if direction == "BUY" else "Sell"
    opp_side = "Sell" if side == "Buy" else "Buy"

    # Дефолты по бирже: Bybit = 10x / 1 USDT, BingX = 20x / 30 USDT
    exchange_defaults = {
        "bybit": {"leverage": 10,  "size_usdt": 1.0},
        "bingx": {"leverage": 20,  "size_usdt": 30.0},
    }
    ex_def = exchange_defaults.get(exchange, {"leverage": DEFAULT_LEVERAGE, "size_usdt": DEFAULT_SIZE_USDT})
    cfg = {**ex_def, **PAIR_SETTINGS.get(ticker, {})}
    leverage  = int(cfg["leverage"])
    size_usdt = float(cfg["size_usdt"])

    sl_price  = parse_price(text, "SL:", "⛔ SL:")
    tp1_price = parse_price(text, "TP1:", "✅ TP1:")
    tp2_price = parse_price(text, "TP2:", "✅ TP2:")
    tp3_price = parse_price(text, "TP3:", "✅ TP3:")
    tp4_price = parse_price(text, "TP4:", "✅ TP4:")
    price = infer_entry_price(text)
    qty = 1.0
    sl_order_id = ""

    if trade_mode == "trade":
        ex_set_leverage(ticker, leverage, exchange)

        try:
            price = ex_get_price(ticker, exchange)
        except Exception as e:
            write_log(f"ENTRY_ERR | get_price | {ticker} ({exchange}) | {e}")
            send_signals(
                f"❌ <b>Ошибка входа {ticker}</b>\nЦена недоступна ({exchange}): {e}",
                reply_to=source_msg_id,
            )
            return

        qty = ex_calc_qty(ticker, size_usdt, leverage, price, exchange)
        write_log(f"ENTRY | {ticker} {direction} [{exchange}] price={price} qty={qty} lev={leverage}x")

        try:
            ex_place_market(ticker, side, qty, False, exchange)
        except Exception as e:
            write_log(f"ENTRY_FAIL | {ticker} ({exchange}) | {e}")
            send_signals(
                f"❌ <b>Ошибка входа {ticker} {direction}</b> [{exchange}]\n{e}",
                reply_to=source_msg_id,
            )
            return

        time.sleep(0.5)

        if sl_price:
            try:
                resp = ex_place_stop(ticker, opp_side, qty, sl_price, exchange)
                if exchange == "bybit" and resp.get("retCode") == 0:
                    sl_order_id = resp["result"].get("orderId", "")
                elif exchange == "bingx":
                    sl_order_id = resp.get("data", {}).get("orderId", "") or "ok"
            except Exception as e:
                write_log(f"SL_PLACE_ERR | {ticker} ({exchange}) | {e}")
    else:
        write_log(f"ENTRY_TELEGRAM_ONLY | {ticker} {direction} | no exchange execution")
        if price is None:
            price = 0.0

    created_at = int(time.time())
    instance_id = _trade_instance_id(key, {
        "trade_id": trade_id,
        "ticker": ticker,
        "direction": direction,
        "timeframe": timeframe,
        "created_at": created_at,
    })
    arrow = "🟢" if direction == "BUY" else "🔴"
    side_label = "LONG" if direction == "BUY" else "SHORT"
    if trade_mode == "trade":
        exch_tag = "Bybit" if exchange == "bybit" else "BingX"
        net_tag = ("🧪 TEST" if TESTNET else "🔴 LIVE") if exchange == "bybit" \
                  else ("🧪 DEMO" if BINGX_DEMO else "🔴 LIVE")
        details_line = f"#{ticker}  |  {leverage}x  |  {size_usdt} USDT"
        volume_line = f"📦 Объём: {qty} контр."
    else:
        exch_tag = "Telegram"
        net_tag = "📢 ONLY"
        details_line = f"#{ticker}  |  alert only"
        volume_line = "📦 Исполнение: без биржи"
    entry_price_text = price if price not in (None, 0.0) else "—"
    entry_message = send_signals(
        f"{arrow} <b>{side_label} ОТКРЫТ</b>  [{exch_tag}] {net_tag}\n"
        f"{details_line}\n"
        f"📍 Вход: <b>{entry_price_text}</b>  |  ⛔ SL: {sl_price or '—'}\n"
        f"✅ TP1: {tp1_price or '—'}  |  TP2: {tp2_price or '—'}\n"
        f"{volume_line}",
        reply_to=source_msg_id,
    )
    trade_message_id = entry_message.get("message_id") or source_msg_id

    trade_record = {
        "message_id": trade_message_id,
        "signal_message_id": source_msg_id,
        "chat_id": TG_CHAT,
        "thread_id": int(TG_SIGNALS_TOPIC),
        "event": "entry",
        "ticker": ticker,
        "direction": direction,
        "timeframe": timeframe,
        "exchange": exchange,
        "trade_mode": trade_mode,
        "trade_id": trade_id,
        "trade_key": key,
        "instance_id": instance_id,
        "is_strong": payload.get("is_strong", False),
        "created_at": created_at,
        "entry_price": price,
        "total_qty": qty,
        "remaining_qty": qty,
        "sl_price": sl_price,
    }
    dedup_entry(ticker, direction)
    if key:
        put_trade(key, trade_record)

    pkey = pos_key(ticker, direction)
    with _pos_lock:
        positions = load_positions()
        positions[pkey] = {
            "symbol": ticker,
            "ticker": ticker,
            "direction": direction,
            "side": side,
            "opp_side": opp_side,
            "exchange": exchange,
            "trade_mode": trade_mode,
            "entry_price": price,
            "total_qty": qty,
            "remaining_qty": qty,
            "leverage": leverage,
            "sl_price": sl_price,
            "sl_order_id": sl_order_id,
            "tp1_price": tp1_price,
            "tp2_price": tp2_price,
            "tp3_price": tp3_price,
            "tp4_price": tp4_price,
            "trade_id": trade_id,
            "trade_key": key,
            "instance_id": instance_id,
            "timeframe": timeframe,
            "is_strong": payload.get("is_strong", False),
            "message_id": trade_message_id,
            "signal_message_id": source_msg_id,
            "created_at": created_at,
            "trail_active": False,
            "trail_sl": None,
        }
        save_positions(positions)


def handle_tp_hit(payload: dict):
    ticker = payload.get("ticker", "").upper().replace(".P", "")
    direction = (payload.get("direction") or "").upper()
    tp_num = int(payload.get("tp_num") or 0)
    text = payload.get("text", "").strip()
    key = build_trade_key(payload)
    trade_key, trade = find_trade_entry(
        key=key,
        trade_id=str(payload.get("trade_id") or ""),
        ticker=ticker,
        direction=direction,
    )
    reply_id = (trade or {}).get("message_id")

    pkey = pos_key(ticker, direction)
    final_close = False
    pos_snapshot = None
    highest_tp = tp_num
    with _pos_lock:
        positions = load_positions()
        pos = positions.get(pkey)
        if not pos:
            instance_id = _trade_instance_id(trade_key or key, trade, None, payload)
            if _trade_already_closed(instance_id):
                write_log(f"TP_DUPLICATE_SKIP | {ticker} TP{tp_num} | already closed")
            return
        if pos.get(f"tp{tp_num}_hit"):
            write_log(f"TP_DUPLICATE_SKIP | {ticker} TP{tp_num} | already applied")
            return

        exchange = pos.get("exchange", "bybit")
        total_qty = pos["total_qty"]
        remaining = pos["remaining_qty"]
        opp_side = pos["opp_side"]
        close_qty = round(total_qty * TP_CLOSE_PCT.get(tp_num, 0.0), 8)
        close_qty = min(close_qty, remaining)
        min_q = ex_min_qty(ticker, exchange) if exchange != "none" else 0.0

        if exchange != "none":
            if close_qty < min_q:
                return

            try:
                ex_place_market(ticker, opp_side, close_qty, True, exchange)
            except Exception as e:
                write_log(f"TP_HIT_ERR | {ticker} TP{tp_num} ({exchange}) | {e}")
                return
        elif close_qty <= 0:
            close_qty = 0.0

        new_remaining = max(0.0, remaining - close_qty)
        pos[f"tp{tp_num}_hit"] = True
        pos["remaining_qty"] = new_remaining
        highest_tp = _highest_tp_hit(pos)

        if tp_num == 2 and pos.get("tp1_price"):
            oid = ""
            if exchange != "none":
                oid = move_sl(pos, pos["tp1_price"])
            pos["sl_price"] = pos["tp1_price"]
            pos["sl_order_id"] = oid

        if tp_num >= 3 and not pos["trail_active"]:
            pos["trail_active"] = True
            pos["trail_sl"]     = pos.get("sl_price")

        if new_remaining <= min_q or tp_num >= 6:
            if exchange != "none":
                ex_cancel_all(ticker, exchange)
            positions.pop(pkey, None)
            final_close = True
        else:
            positions[pkey] = pos
        pos_snapshot = dict(pos)
        save_positions(positions)

    if trade_key:
        touch_trade(
            trade_key,
            highest_tp_hit=highest_tp,
            remaining_qty=pos_snapshot.get("remaining_qty"),
            trail_active=pos_snapshot.get("trail_active", False),
        )
    send_signals(text or f"✅ TP{tp_num} {ticker}", reply_to=reply_id or pos_snapshot.get("message_id"))
    if final_close:
        finalize_trade(payload, trade_key, trade, pos_snapshot, "win", highest_tp, close_reason=f"tp_hit_{tp_num}")


def handle_sl_hit(payload: dict):
    ticker = payload.get("ticker", "").upper().replace(".P", "")
    direction = (payload.get("direction") or "").upper()
    text = payload.get("text", "").strip()
    key = build_trade_key(payload)
    trade_key, trade = find_trade_entry(
        key=key,
        trade_id=str(payload.get("trade_id") or ""),
        ticker=ticker,
        direction=direction,
    )

    pkey = pos_key(ticker, direction)
    pos_snapshot = None
    highest_tp = 0
    with _pos_lock:
        positions = load_positions()
        pos = positions.get(pkey)
        if not pos:
            instance_id = _trade_instance_id(trade_key or key, trade, None, payload)
            if _trade_already_closed(instance_id):
                write_log(f"SL_DUPLICATE_SKIP | {ticker} | already closed")
            return
        exchange = pos.get("exchange", "bybit")
        highest_tp = _highest_tp_hit(pos)
        pos_snapshot = dict(pos)
        if exchange != "none":
            ex_cancel_all(ticker, exchange)
        positions.pop(pkey, None)
        save_positions(positions)
    reply_id = (trade or {}).get("message_id") or pos_snapshot.get("message_id")
    send_signals(text or f"🛑 SL {ticker}", reply_to=reply_id)
    result = "win" if highest_tp > 0 else "loss"
    finalize_trade(payload, trade_key, trade, pos_snapshot, result, highest_tp, close_reason="sl_hit")


def handle_sl_moved(payload: dict):
    ticker    = payload.get("ticker", "").upper().replace(".P", "")
    direction = (payload.get("direction") or "").upper()
    text      = payload.get("text", "").strip()
    key       = build_trade_key(payload)

    trade_key, trade = find_trade_entry(
        key=key,
        trade_id=str(payload.get("trade_id") or ""),
        ticker=ticker,
        direction=direction,
    )

    new_sl = parse_price(text, "✅ Стало:", "Стало:")
    if not new_sl:
        return
    pkey = pos_key(ticker, direction)
    with _pos_lock:
        positions = load_positions()
        pos = positions.get(pkey)
        if not pos:
            return
        reply_id = (trade or {}).get("message_id") or pos.get("message_id")
        send_signals(text or f"🔒 SL сдвинут {ticker}", reply_to=reply_id)
        oid = ""
        if pos.get("exchange", "bybit") != "none":
            oid = move_sl(pos, new_sl)
        pos["sl_price"]    = new_sl
        pos["sl_order_id"] = oid
        positions[pkey] = pos
        save_positions(positions)
    if trade_key:
        touch_trade(trade_key, sl_price=new_sl)


def close_position_manually(pos: dict, source: str) -> tuple[bool, str]:
    ticker = pos["symbol"]
    direction = pos["direction"]
    remaining = pos.get("remaining_qty", 0)
    exchange = pos.get("exchange", "bybit")
    trade_key, trade = find_trade_entry(
        key=pos.get("trade_key", ""),
        trade_id=str(pos.get("trade_id") or ""),
        ticker=ticker,
        direction=direction,
    )
    if remaining > 0:
        try:
            if exchange != "none":
                ex_cancel_all(ticker, exchange)
                ex_place_market(ticker, pos["opp_side"], remaining, True, exchange)
        except Exception as e:
            write_log(f"MANUAL_CLOSE_ERR | {ticker} | {e}")
            return False, f"{ticker}[{exchange}]"

    with _pos_lock:
        positions = load_positions()
        positions.pop(pos_key(ticker, direction), None)
        save_positions(positions)

    reply_id = (trade or {}).get("message_id") or pos.get("message_id")
    send_signals(
        f"🧯 <b>Сделка закрыта вручную</b>\n#{ticker} [{exchange}]",
        reply_to=reply_id,
    )
    payload = {
        "ticker": ticker,
        "direction": direction,
        "timeframe": pos.get("timeframe", ""),
        "trade_id": pos.get("trade_id", ""),
        "exchange": exchange,
        "event": "manual_close",
    }
    finalize_trade(
        payload,
        trade_key,
        trade,
        pos,
        "manual",
        _highest_tp_hit(pos),
        close_reason=source,
    )
    return True, f"{ticker}[{exchange}]"


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

            # Сессии — только в будни (пн=0 ... пт=4)
            if msk.weekday() < 5:
                for s in SESSIONS:
                    oh, om = map(int, s["open"].split(":"))
                    ch, cm = map(int, s["close"].split(":"))
                    open_min  = oh * 60 + om
                    close_min = ch * 60 + cm

                    if -1 <= open_min - cur <= 1:
                        k = f"sess_open_{today}_{s['open']}"
                        if not _was_sent(k):
                            _mark_sent(k)
                            send_sessions(f"🟢 {s['name']} открылась! ({s['open']} МСК)")

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
            "/stats_by_ticker BTCUSDT — статистика по паре\n"
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
            if r["result"] == "win":
                by_ticker[tk]["wins"] += 1
            elif r["result"] == "loss":
                by_ticker[tk]["losses"] += 1
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
        for _, pos in positions.items():
            ok, label = close_position_manually(pos, "telegram_close_all")
            if ok:
                closed.append(label)
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


def start_background_threads():
    global _bg_started
    with _bg_lock:
        if _bg_started:
            return
        threading.Thread(target=_queue_worker, daemon=True, name="queue_worker").start()
        threading.Thread(target=_scheduler, daemon=True, name="scheduler").start()
        threading.Thread(target=_keepalive, daemon=True, name="keepalive").start()
        threading.Thread(target=_register_bg, daemon=True, name="webhook_reg").start()
        if BYBIT_AVAILABLE or BINGX_AVAILABLE:
            threading.Thread(target=_position_manager, daemon=True, name="pos_mgr").start()
        _bg_started = True


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

    # Проверка WEBHOOK_SECRET отключена — принимаем все POST запросы

    # Все алерты принимаем: торговые пары торгуются, остальные идут только в Telegram.
    ticker = payload.get("ticker", "").upper().replace(".P", "")
    event  = payload.get("event", "")
    if event in ("entry", "smart_entry", "limit_hit") and ticker:
        if ALLOWED_PAIRS and ticker not in ALLOWED_PAIRS:
            write_log(f"WEBHOOK_TELEGRAM_ONLY | {ticker} not in ALLOWED_PAIRS")

    write_log(f"WEBHOOK | event={payload.get('event','?')} ticker={payload.get('ticker','?')}")
    enqueue_signal(payload)
    return jsonify({"status": "queued", "event": payload.get("event","?")}), 200


@app.route("/webhook/<secret>", methods=["POST"])
def bybit_webhook_compat(secret: str):
    # Проверка секрета отключена
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
    for _, pos in positions.items():
        ok, label = close_position_manually(pos, "http_close_all")
        if ok:
            closed.append(label)
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


@app.route("/redis_status")
def redis_status_route():
    r = _get_redis()
    if r is None:
        return jsonify({"redis": "not configured", "REDIS_URL_set": bool(os.environ.get("REDIS_URL"))}), 200
    try:
        r.ping()
        keys = r.keys(_REDIS_PREFIX + "*")
        sizes = {k: len(r.get(k) or "") for k in keys}
        return jsonify({"redis": "ok", "keys": sizes})
    except Exception as e:
        return jsonify({"redis": "error", "detail": str(e)}), 200


# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    start_background_threads()
    app.run(host="0.0.0.0", port=port, debug=False)
