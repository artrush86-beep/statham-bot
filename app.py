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

TESTNET = os.environ.get("TESTNET", "false").lower() == "true"

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
                 default: str = "YBUSDT,ASTERUSDT,EDGEUSDT,STOUSDT,UNIUSDT,SIRENUSDT,FARTCOINUSDT,WLDUSDT,ZBTUSDT,BLESSUSDT") -> set[str]:
    raw = os.environ.get(env_key, "").strip()
    if not raw:
        raw = os.environ.get(fallback_key, default)
    return {p.strip().upper().replace(".P", "") for p in raw.split(",") if p.strip()}


BYBIT_PAIRS   = _parse_pairs("BYBIT_PAIRS")
BINGX_PAIRS   = _parse_pairs("BINGX_PAIRS")
ALLOWED_PAIRS = BYBIT_PAIRS | BINGX_PAIRS

DEFAULT_LEVERAGE  = int(os.environ.get("DEFAULT_LEVERAGE",  "10"))
DEFAULT_SIZE_USDT = float(os.environ.get("DEFAULT_SIZE_USDT", "1"))
TRAIL_PCT         = float(os.environ.get("TRAIL_PCT", "0.5")) / 100.0

try:
    PAIR_SETTINGS: dict = json.loads(os.environ.get("PAIR_SETTINGS_JSON", "{}"))
except Exception:
    PAIR_SETTINGS = {}

TP_CLOSE_PCT = {1: 0.25, 2: 0.20, 3: 0.25, 4: 0.15, 5: 0.10, 6: 0.05}


def calc_trade_pnl(pos: dict, sl_exit_price: float | None = None) -> dict:
    """
    Рассчитывает реализованный P&L с учётом частичных TP-закрытий и SL-выхода.

    Логика:
      - Для каждого tp{n}_hit: закрыта доля TP_CLOSE_PCT[n] от total_qty по tp{n}_price
      - Остаток (remaining_qty) закрыт по sl_exit_price
      - direction BUY:  profit% = (exit - entry) / entry × leverage
      - direction SELL: profit% = (entry - exit) / entry × leverage

    Возвращает dict: {
        "pnl_pct": float,          # итоговый P&L в % (с учётом плеча)
        "pnl_pct_no_lev": float,   # P&L без плеча (raw move)
        "tp_pnl_pct": float,       # P&L только от TP-частей
        "sl_pnl_pct": float,       # P&L от SL-остатка
        "highest_tp": int,
        "tps_hit": list[int],
        "remaining_pct": float,    # доля позиции, закрытая на SL (0–1)
        "closed_on_tp_pct": float, # доля, закрытая на TP (0–1)
    }
    """
    entry  = float(pos.get("entry_price") or 0)
    lev    = float(pos.get("leverage") or 1)
    dirn   = str(pos.get("direction") or "BUY").upper()
    total  = float(pos.get("total_qty") or 1)

    if entry <= 0:
        return {"pnl_pct": 0.0, "pnl_pct_no_lev": 0.0,
                "tp_pnl_pct": 0.0, "sl_pnl_pct": 0.0,
                "highest_tp": 0, "tps_hit": [],
                "remaining_pct": 0.0, "closed_on_tp_pct": 0.0}

    tps_hit: list[int] = []
    closed_pct   = 0.0   # доля позиции, закрытая на TP (0–1)
    tp_pnl_pct   = 0.0   # накопленный P&L от TP-закрытий (без плеча)

    for n in range(1, 7):
        if not pos.get(f"tp{n}_hit"):
            continue
        tp_price = float(pos.get(f"tp{n}_price") or 0)
        if tp_price <= 0:
            continue
        share = TP_CLOSE_PCT.get(n, 0.0)
        if dirn == "BUY":
            move = (tp_price - entry) / entry
        else:
            move = (entry - tp_price) / entry
        tp_pnl_pct += move * share
        closed_pct  += share
        tps_hit.append(n)

    remaining_pct = max(0.0, 1.0 - closed_pct)
    sl_pnl_pct    = 0.0

    if sl_exit_price and sl_exit_price > 0 and remaining_pct > 0:
        if dirn == "BUY":
            sl_move = (sl_exit_price - entry) / entry
        else:
            sl_move = (entry - sl_exit_price) / entry
        sl_pnl_pct = sl_move * remaining_pct

    total_no_lev = tp_pnl_pct + sl_pnl_pct
    total_with_lev = total_no_lev * lev

    return {
        "pnl_pct":          round(total_with_lev * 100, 2),
        "pnl_pct_no_lev":   round(total_no_lev  * 100, 2),
        "tp_pnl_pct":       round(tp_pnl_pct    * 100, 2),
        "sl_pnl_pct":       round(sl_pnl_pct    * 100, 2),
        "highest_tp":       max(tps_hit, default=0),
        "tps_hit":          tps_hit,
        "remaining_pct":    round(remaining_pct * 100, 1),
        "closed_on_tp_pct": round(closed_pct    * 100, 1),
    }

DATA_DIR = os.environ.get("DATA_DIR", "/tmp")
os.makedirs(DATA_DIR, exist_ok=True)

# ── Redis connection ───────────────────────────────────────────────────────────
_redis_client = None
_REDIS_PREFIX  = "statham:"

def _get_redis():
    """Возвращает Redis-клиент или None если Redis недоступен.
    При таймауте/ошибке инвалидирует клиент и пересоздаёт при следующем вызове."""
    global _redis_client
    if not _REDIS_AVAILABLE:
        return None
    url = os.environ.get("REDIS_URL", "").strip()
    if not url:
        return None
    if _redis_client is not None:
        return _redis_client
    try:
        client = _redis_lib.from_url(
            url, decode_responses=True,
            # Upstash имеет cold-start до 2–3 сек; 10s timeout надёжнее
            socket_timeout=10, socket_connect_timeout=10,
            retry_on_timeout=True,
            health_check_interval=30,
        )
        client.ping()
        _redis_client = client
        log.info("Redis | connected OK")
    except Exception as e:
        log.warning(f"Redis | connect failed: {e}")
    return _redis_client


def _redis_invalidate():
    """Сбрасывает кешированный клиент — следующий вызов _get_redis() пересоздаст."""
    global _redis_client
    _redis_client = None

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
    if not TESTNET and (BYBIT_AVAILABLE or BINGX_AVAILABLE):
        log.warning("LIVE TRADING ACTIVE! TESTNET=false — торговля реальными деньгами!")
    if BYBIT_AVAILABLE:
        log.info(f"Bybit active | testnet={TESTNET} | pairs={sorted(BYBIT_PAIRS)}")
    if BINGX_AVAILABLE:
        log.info(f"BingX active | demo={BINGX_DEMO} | pairs={sorted(BINGX_PAIRS)}")

    # ── Redis — обязательное хранилище ───────────────────────────────────
    redis_url = os.environ.get("REDIS_URL", "").strip()
    if not redis_url:
        log.critical(
            "REDIS_URL не задана! Все данные (сделки, позиции, история) "
            "хранятся только в /tmp и будут ПОТЕРЯНЫ при перезапуске Render. "
            "Задайте REDIS_URL в переменных окружения."
        )
    elif not _REDIS_AVAILABLE:
        log.critical(
            "Пакет redis не установлен. "
            "Данные хранятся только в /tmp — при перезапуске ПОТЕРЯЮТСЯ!"
        )
    else:
        r = _get_redis()
        if r is None:
            log.critical(
                "Redis недоступен (соединение не установлено). "
                "Данные хранятся только в /tmp — при перезапуске ПОТЕРЯЮТСЯ!"
            )
        else:
            log.info("Redis | storage OK — данные персистентны")

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
        # Удаляем IP-адреса без активных запросов (чистим утечку памяти)
        stale = [k for k, v in _rate_store.items() if not v]
        for k in stale:
            del _rate_store[k]
    return True


# ══════════════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ══════════════════════════════════════════════════════════════════════════════
def write_log(entry: str):
    ts   = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {entry}\n"
    log.info(entry)
    try:
        with _file_lock:
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
            # Таймаут/обрыв → сбрасываем клиент; следующий вызов пересоздаст
            _redis_invalidate()
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
            _redis_invalidate()
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
                err_code = data.get("error_code", "?")
                err_desc = data.get("description", "?")
                write_log(f"SEND_TG_FAIL | attempt={attempt} | http={resp.status_code} code={err_code} desc={err_desc}")
                # 429 = flood wait
                if resp.status_code == 429:
                    retry_after = data.get("parameters", {}).get("retry_after", 5)
                    write_log(f"SEND_TG_FLOOD | retry_after={retry_after}s")
                    time.sleep(retry_after)
                else:
                    time.sleep(2)
                continue
            write_log(f"SEND_TG_OK | chat={chat_id} thread={thread_id}")
            return data["result"]
        except Exception as e:
            write_log(f"SEND_TG_ERR | attempt={attempt} | {type(e).__name__}: {e}")
            time.sleep(2)
    write_log(f"SEND_TG_GIVE_UP | chat={chat_id} thread={thread_id} | all {3} attempts failed")
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
    ret_code = resp['retCode']
    ret_msg  = resp.get('retMsg', '')
    order_id = (resp.get('result') or {}).get('orderId', '')
    write_log(f"BYBIT_MARKET | {symbol} {side} qty={qty} ro={reduce_only} | ret={ret_code} msg={ret_msg} order={order_id}")
    if ret_code != 0:
        raise Exception(f"Bybit {ret_code}: {ret_msg}")
    return resp


def bybit_place_stop(symbol: str, side: str, qty: float,
                     trigger_price: float, reduce_only: bool = True) -> dict:
    # triggerDirection: 1 = price rises above triggerPrice (SHORT SL → side=Buy)
    #                   2 = price falls below triggerPrice  (LONG  SL → side=Sell)
    trigger_direction = 1 if side.lower() == "buy" else 2
    resp = bybit().place_order(
        category="linear", symbol=symbol,
        side=side, orderType="Market",
        qty=round_qty(symbol, qty),
        triggerPrice=round_price(symbol, trigger_price),
        triggerBy="LastPrice", triggerDirection=trigger_direction,
        orderFilter="StopOrder",
        reduceOnly=reduce_only, timeInForce="IOC",
    )
    write_log(f"BYBIT_STOP | {symbol} {side} qty={qty} trigger={trigger_price} dir={trigger_direction} | ret={resp['retCode']}")
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
        write_log(f"BINGX_API_ERR | {method} {path} | code={code} msg={data.get('msg','')} http={resp.status_code}")
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
    # BingX fills may be slightly less than ordered qty (exchange rounding).
    # Floor to 1 decimal place for SL to ensure qty ≤ actual filled position.
    safe_qty = math.floor(qty * 10) / 10
    if safe_qty <= 0:
        safe_qty = bingx_round_qty(qty)
    data = _bingx_req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": sym, "side": bx_side, "positionSide": pos_side,
        "type": "STOP_MARKET", "quantity": str(bingx_round_qty(safe_qty)),
        "stopPrice": str(round(trigger_price, 8)), "workingType": "CONTRACT_PRICE",
    })
    write_log(f"BINGX_STOP | {ticker} {side} pos={pos_side} qty={qty}→{safe_qty} trigger={trigger_price}")
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
# TP-ОРДЕРА НА БИРЖЕ
# ══════════════════════════════════════════════════════════════════════════════
def bybit_place_tp_limit(symbol: str, side: str, qty: float, price: float) -> dict:
    """Лимитный TP-ордер на Bybit (reduceOnly, GTC)."""
    resp = bybit().place_order(
        category="linear", symbol=symbol,
        side=side, orderType="Limit",
        price=round_price(symbol, price),
        qty=round_qty(symbol, qty),
        reduceOnly=True, timeInForce="GTC",
    )
    write_log(f"BYBIT_TP_LIMIT | {symbol} {side} qty={qty} price={price} | ret={resp['retCode']}")
    return resp


def bingx_place_tp(ticker: str, side: str, qty: float, trigger_price: float) -> dict:
    """TAKE_PROFIT_MARKET TP-ордер на BingX."""
    sym      = _bingx_to_symbol(ticker)
    bx_side  = side.upper()
    pos_side = "SHORT" if bx_side == "BUY" else "LONG"
    data = _bingx_req("POST", "/openApi/swap/v2/trade/order", {
        "symbol": sym, "side": bx_side, "positionSide": pos_side,
        "type": "TAKE_PROFIT_MARKET", "quantity": str(bingx_round_qty(qty)),
        "stopPrice": str(round(trigger_price, 8)), "workingType": "CONTRACT_PRICE",
    })
    write_log(f"BINGX_TP | {ticker} {side} pos={pos_side} qty={qty} trigger={trigger_price}")
    return data


def place_tp_orders(ticker: str, side: str, opp_side: str, total_qty: float,
                    tp_prices: dict, exchange: str) -> dict:
    """
    Выставляет TP1–TP6 как ордера на бирже.
    tp_prices: {1: price, 2: price, ...}
    Возвращает: {1: order_id, 2: order_id, ...}
    """
    order_ids: dict = {}
    for tp_num, price in sorted(tp_prices.items()):
        pct = TP_CLOSE_PCT.get(tp_num, 0.0)
        if pct <= 0 or not price:
            continue
        close_qty = round(total_qty * pct, 8)
        try:
            if exchange == "bybit":
                min_q = min_qty(ticker)
                if close_qty < min_q:
                    write_log(f"TP_ORDER_SKIP | {ticker} TP{tp_num} | qty {close_qty} < min {min_q}")
                    continue
                resp = bybit_place_tp_limit(ticker, opp_side, close_qty, price)
                if resp.get("retCode") == 0:
                    order_ids[tp_num] = resp["result"].get("orderId", "")
                else:
                    write_log(f"TP_ORDER_FAIL | {ticker} TP{tp_num} | {resp.get('retMsg','')}")
            elif exchange == "bingx":
                bx_qty = bingx_round_qty(close_qty)
                if bx_qty < 0.001:
                    continue
                resp = bingx_place_tp(ticker, opp_side, bx_qty, price)
                order_ids[tp_num] = resp.get("data", {}).get("orderId", "") or "ok"
        except Exception as e:
            write_log(f"TP_ORDER_ERR | {ticker} TP{tp_num} ({exchange}) | {e}")
        time.sleep(0.15)
    write_log(f"TP_ORDERS_PLACED | {ticker} [{exchange}] | {order_ids}")
    return order_ids
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
    # Не трогаем позиции без биржи или с нулевым объёмом
    if exchange == "none" or remaining <= 0:
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


def update_stats(result: str):
    """result: 'win' | 'loss' | 'partial' (partial treated as loss for stats)."""
    with _state_lock:
        s = load_stats()
        s["total"] = s.get("total", 0) + 1
        if result == "win":
            s["wins"] = s.get("wins", 0) + 1
        else:
            s["losses"] = s.get("losses", 0) + 1  # partial counts as loss
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
        "pnl": payload.get("_bot_pnl", {}),
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


def _wr_icon(wr: float) -> str:
    if wr >= 70: return "🟢"
    if wr >= 50: return "🟡"
    return "🔴"


def _build_report(trades: list, title: str, show_last: int = 5,
                  show_top_tickers: bool = False, date_range: str = "") -> str:
    wins     = [r for r in trades if r["result"] == "win"]
    losses   = [r for r in trades if r["result"] == "loss"]
    partials = [r for r in trades if r["result"] == "partial"]
    manuals  = [r for r in trades if r["result"] == "manual"]
    scored   = len(wins) + len(losses) + len(partials)
    total   = len(trades)
    wr      = calc_winrate(len(wins), scored)
    if not trades:
        return f"{title}\n\n<i>Нет данных за период.</i>"
    avg_dur = int(sum(r.get("duration_sec", 0) for r in trades) / total) if total else 0

    # Средний P&L из сохранённых расчётов бота
    pnl_values = [r["pnl"].get("pnl_pct", 0) for r in trades if r.get("pnl")]
    avg_pnl = round(sum(pnl_values) / len(pnl_values), 2) if pnl_values else None

    tp_counts: dict = {}
    for r in wins:
        n = int(r.get("tp_num") or 0)
        if n:
            tp_counts[n] = tp_counts.get(n, 0) + 1

    # ── Заголовок ────────────────────────────────────────────────────
    text = f"{title}\n"
    if date_range:
        text += f"<i>{date_range}</i>\n"
    text += "\n"

    # ── Основные цифры ───────────────────────────────────────────────
    text += f"📊 Win Rate: <b>{_wr_icon(wr)} {wr}%</b>\n"
    text += f"✅ TP: {len(wins)}   ❌ SL: {len(losses)}   🔶 Partial: {len(partials)}" if partials else f"✅ TP: {len(wins)}   ❌ SL: {len(losses)}"
    if manuals:
        text += f"   🧯 Manual: {len(manuals)}"
    text += f"\n📈 Всего закрыто: <b>{total}</b>\n"
    text += f"⏱ Ср. время: {fmt_duration(avg_dur)}\n"
    if avg_pnl is not None:
        _pnl_sign = "+" if avg_pnl >= 0 else ""
        text += f"💰 Ср. П&Л: <b>{_pnl_sign}{avg_pnl}%</b> (с плечом)\n"

    # ── Разбивка по TP ───────────────────────────────────────────────
    if tp_counts:
        text += "\n<b>Разбивка по TP:</b>\n"
        for n, cnt in sorted(tp_counts.items()):
            bar = "█" * cnt
            text += f"  TP{n}: {cnt} ✅  <code>{bar}</code>\n"

    # ── Топ тикеры (для недельного/месячного) ────────────────────────
    if show_top_tickers and total > 0:
        by_tk: dict = {}
        for r in trades:
            tk = r.get("ticker", "?")
            by_tk[tk] = by_tk.get(tk, 0) + 1
        top = sorted(by_tk.items(), key=lambda x: x[1], reverse=True)[:5]
        # Активных дней
        days = len({r.get("date_msk", "") for r in trades if r.get("date_msk")})
        text += f"\n📅 Активных дней: <b>{days}</b>\n"
        text += "\n<b>Топ тикеры:</b>\n"
        for tk, cnt in top:
            text += f"  #{tk.replace('.P','')}: {cnt}\n"

    # ── Последние сделки ─────────────────────────────────────────────
    if show_last and total > 0:
        recent = sorted(trades, key=lambda r: r.get("close_time", 0), reverse=True)[:show_last]
        text += "\n<b>Последние сделки:</b>\n"
        for r in recent:
            tk   = r.get("ticker", "?").replace(".P", "")
            dr   = r.get("direction", "")
            res  = r.get("result", "")
            tp_n = int(r.get("tp_num") or 0)
            dur  = fmt_duration(int(r.get("duration_sec", 0)))
            if res == "win":
                icon = "✅"
                label = f"TP{tp_n}" if tp_n else "TP"
            elif res == "partial":
                icon = "🔶"
                label = f"Partial TP{tp_n}" if tp_n else "Partial"
            elif res == "loss":
                icon = "❌"
                label = "SL"
            else:
                icon = "🧯"
                label = "Manual"
            text += f"{icon} #{tk} {dr} → {label} ({dur})\n"

    return text.rstrip()


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


def dedup_entry(ticker: str, direction: str, current_key: str = ""):
    """Удаляет старые записи о сделке по тому же тикеру/направлению.
    Защита: не трогает текущую сделку (current_key) и записи новее 60 сек."""
    now = int(time.time())
    with _state_lock:
        trades = load_trades()
        keys_to_remove = [
            k for k, v in trades.items()
            if v.get("ticker", "") == ticker
            and v.get("direction", "") == direction
            and k != current_key
            and now - int(v.get("created_at", 0)) > 60
        ]
        for k in keys_to_remove:
            del trades[k]
        if keys_to_remove:
            save_trades(trades)
            write_log(f"DEDUP | removed {len(keys_to_remove)} old keys for {ticker}_{direction}")


def cleanup_old_trades() -> int:
    """Удаляет: (1) сделки старше 7 дней, (2) сделки без соответствующей позиции старше 2 часов."""
    with _state_lock:
        trades = load_trades()
        positions = load_positions()
        now = int(time.time())
        cutoff_old   = now - 7 * 86400   # 7 дней
        cutoff_orphan = now - 2 * 3600   # 2 часа

        pos_keys_set = set()
        for pkey, pos in positions.items():
            # Позиция хранит trade_key
            tk = pos.get("trade_key", "")
            if tk:
                pos_keys_set.add(tk)

        removed = []
        for k, v in list(trades.items()):
            created = v.get("created_at", 0)
            if created < cutoff_old:
                removed.append(k)
                continue
            # Orphan: trade без активной позиции, висит > 2ч
            if k not in pos_keys_set and created < cutoff_orphan:
                removed.append(k)

        for k in removed:
            del trades[k]
        if removed:
            save_trades(trades)
            write_log(f"CLEANUP | removed {len(removed)} stale/orphaned trades: {removed[:10]}")
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
    if result in ("win", "loss", "partial"):
        update_stats(result)
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
        write_log(f"ENTRY_SKIP | {ticker} {direction} | DUPLICATE — active position already exists for {pkey}")
        # BUG 2 FIX: still forward the signal text to Telegram even on duplicate
        # (trader needs to see every signal; exchange execution is skipped)
        _dup_text = text or f"⚠️ Дубль сигнала #{ticker} {direction} — позиция уже открыта"
        send_signals(_dup_text)
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

    # ── Хелпер: безопасный float из payload-поля ──────────────────────
    def _to_float(val) -> float | None:
        """NaN / Inf / null / 'NaN' / пустые строки → None."""
        try:
            if val is None or str(val).strip().lower() in ("", "null", "none", "nan", "undefined", "inf", "-inf"):
                return None
            f = float(val)
            if math.isnan(f) or math.isinf(f):
                return None
            return f
        except Exception:
            return None

    # ── Парсим TP/SL: сначала из текста, затем fallback на числовые поля JSON ──
    sl_price  = parse_price(text, "SL:", "⛔ SL:")   or _to_float(payload.get("sl"))
    tp1_price = parse_price(text, "TP1:", "✅ TP1:") or _to_float(payload.get("tp1"))
    tp2_price = parse_price(text, "TP2:", "✅ TP2:") or _to_float(payload.get("tp2"))
    tp3_price = parse_price(text, "TP3:", "✅ TP3:") or _to_float(payload.get("tp3"))
    tp4_price = parse_price(text, "TP4:", "✅ TP4:") or _to_float(payload.get("tp4"))
    tp5_price = parse_price(text, "TP5:", "✅ TP5:") or _to_float(payload.get("tp5"))
    tp6_price = parse_price(text, "TP6:", "✅ TP6:") or _to_float(payload.get("tp6"))

    # ── Цена входа: из текста, затем из JSON-поля entry_price ────────
    # Pine Script sometimes sends NaN in entry_price JSON field — text has the real value
    price = infer_entry_price(text) or _to_float(payload.get("entry_price"))
    # Additional fallback: try common Pine Script text patterns
    if price is None and text:
        price = (parse_price(text, "📍 Вход:", "Вход:") or
                 parse_price(text, "🎯 Вход:", "Entry:") or
                 parse_price(text, "Price:", "Цена:"))
    # Parse SL/TP from text with additional patterns if JSON was null
    if sl_price is None and text:
        sl_price = parse_price(text, "SL:", "⛔ SL:", "Stop:", "Стоп:")
    if tp1_price is None and text:
        tp1_price = parse_price(text, "TP1:", "TP 1:", "✅ TP1:", "Take 1:")
    if tp2_price is None and text:
        tp2_price = parse_price(text, "TP2:", "TP 2:", "✅ TP2:", "Take 2:")
    if tp3_price is None and text:
        tp3_price = parse_price(text, "TP3:", "TP 3:", "✅ TP3:")
    if tp4_price is None and text:
        tp4_price = parse_price(text, "TP4:", "TP 4:", "✅ TP4:")
    if tp5_price is None and text:
        tp5_price = parse_price(text, "TP5:", "TP 5:", "✅ TP5:")
    if tp6_price is None and text:
        tp6_price = parse_price(text, "TP6:", "TP 6:", "✅ TP6:")

    # ── Диагностика — видно что распарсилось ──────────────────────────
    write_log(f"PARSED | {ticker} entry={price} sl={sl_price} "
              f"tp1={tp1_price} tp2={tp2_price} tp3={tp3_price} "
              f"tp4={tp4_price} tp5={tp5_price} tp6={tp6_price}")
    qty = 1.0
    sl_order_id = ""
    tp_order_ids: dict = {}
    use_exchange_tps = False

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

        # ── Выставляем TP-ордера на биржу ──────────────────────────────────────
        _tp_map = {n: p for n, p in [
            (1, tp1_price), (2, tp2_price), (3, tp3_price),
            (4, tp4_price), (5, tp5_price), (6, tp6_price),
        ] if p}
        if _tp_map:
            time.sleep(0.3)
            tp_order_ids = place_tp_orders(ticker, side, opp_side, qty, _tp_map, exchange)
            use_exchange_tps = bool(tp_order_ids)
    else:
        write_log(f"ENTRY_TELEGRAM_ONLY | {ticker} {direction} | no exchange execution")
        if price is None:
            price = 0.0
        # BUG 4 NOTE: if entry_price is None/NaN, Pine Script sent NaN in the alert.
        # The signal text is forwarded as-is from Pine Script. No fix needed here —
        # raw text (from `text` field) already contains the correct prices as formatted string.

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
        exch_tag = ""
        net_tag = ""
        details_line = f"#{ticker}"
        volume_line = "📦 Исполнение: без биржи"
    entry_price_text = price if price not in (None, 0.0) else "—"
    tag_str = f"  [{exch_tag}] {net_tag}" if exch_tag else ""
    entry_message = send_signals(
        f"{arrow} <b>{side_label} ОТКРЫТ</b>{tag_str}\n"
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
    dedup_entry(ticker, direction, current_key=key)
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
            "tp5_price": tp5_price,
            "tp6_price": tp6_price,
            "tp_order_ids": tp_order_ids,
            "use_exchange_tps": use_exchange_tps,
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
    _found_pos = True  # Bug 2 Fix flag
    with _pos_lock:
        positions = load_positions()
        pos = positions.get(pkey)
        if not pos:
            instance_id = _trade_instance_id(trade_key or key, trade, None, payload)
            if _trade_already_closed(instance_id):
                write_log(f"TP_DUPLICATE_SKIP | {ticker} TP{tp_num} | already closed")
                return  # genuine duplicate — skip silently
            # Bug 2 Fix: position missing but NOT a duplicate → mark and fall through
            _found_pos = False
        elif pos.get(f"tp{tp_num}_hit"):
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
            if pos.get("use_exchange_tps"):
                # TP-ордер уже исполнен биржей как лимитная заявка
                write_log(f"TP_HIT_EXCHANGE | {ticker} TP{tp_num} | limit order filled by exchange")
            else:
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

        if tp_num >= 2 and not pos["trail_active"]:
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

    # Bug 2 Fix: if no position was found, still forward the signal to Telegram
    if not _found_pos:
        send_signals(text or f"✅ TP{tp_num} {ticker}", reply_to=reply_id)
        return

    if trade_key:
        touch_trade(
            trade_key,
            highest_tp_hit=highest_tp,
            remaining_qty=pos_snapshot.get("remaining_qty"),
            trail_active=pos_snapshot.get("trail_active", False),
        )
    if final_close:
        # Position fully closed at TP6 (or remaining ≤ min_qty)
        pnl = calc_trade_pnl(pos_snapshot, None)
        _entry_fc = pos_snapshot.get("entry_price", 0)
        _dirn_fc  = pos_snapshot.get("direction", "?")
        _lev_fc   = pos_snapshot.get("leverage", 1)
        _tps_str  = " → ".join(f"TP{n}" for n in pnl["tps_hit"])
        _pnl_sign = "+" if pnl["pnl_pct"] >= 0 else ""
        tp_close_msg = (
            f"🏆 <b>ПОЗИЦИЯ ЗАКРЫТА ПОЛНОСТЬЮ</b>\n"
            f"━━━━━━━\n"
            f"#{ticker} {_dirn_fc} — TP{tp_num}\n"
            f"📊 Закрыто: {_tps_str}\n"
            f"📈 <b>П&Л (бот): {_pnl_sign}{pnl['pnl_pct']}%</b>"
            f"  <i>(без плеча: {_pnl_sign}{pnl['pnl_pct_no_lev']}%)</i>"
        )
        send_signals(tp_close_msg, reply_to=reply_id or pos_snapshot.get("message_id"))
        payload["_bot_pnl"] = pnl
        finalize_trade(payload, trade_key, trade, pos_snapshot, "win", highest_tp, close_reason=f"tp_hit_{tp_num}")
    else:
        send_signals(text or f"✅ TP{tp_num} {ticker}", reply_to=reply_id or pos_snapshot.get("message_id"))


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
    _found_pos_sl = True  # Bug 2 Fix flag
    with _pos_lock:
        positions = load_positions()
        pos = positions.get(pkey)
        if not pos:
            instance_id = _trade_instance_id(trade_key or key, trade, None, payload)
            if _trade_already_closed(instance_id):
                write_log(f"SL_DUPLICATE_SKIP | {ticker} | already closed")
            _found_pos_sl = False
        else:
            exchange = pos.get("exchange", "bybit")
            highest_tp = _highest_tp_hit(pos)
            pos_snapshot = dict(pos)
            if exchange != "none":
                ex_cancel_all(ticker, exchange)
            positions.pop(pkey, None)
            save_positions(positions)

    # Bug 2 Fix: always forward sl_hit to Telegram, even with no tracked position
    if not _found_pos_sl:
        send_signals(text or f"🛑 SL {ticker}")
        return

    reply_id = (trade or {}).get("message_id") or pos_snapshot.get("message_id")

    # ── Рассчитываем реальный P&L в боте (не доверяем Pine Script +0%) ──────
    # BUG 3 FIX: Pine Script nulls all price fields in sl_hit alert — we use pos_snapshot
    # (loaded from Redis) which always has entry_price, leverage, tp prices etc.
    # sl_exit_price is extracted from Pine Script text, or from payload "exit_price" field.
    sl_exit_price = None
    if text:
        sl_exit_price = (
            parse_price(text, "Цена выхода:", "💰 Цена выхода:")
            or parse_price(text, "Exit:", "exit:")
            or parse_price(text, "Price:", "price:")
        )
    # Also check JSON payload for exit_price field
    if not sl_exit_price:
        try:
            _ep = payload.get("exit_price") or payload.get("close_price")
            if _ep and str(_ep).lower() not in ("null", "none", "nan", ""):
                sl_exit_price = float(_ep)
        except Exception:
            pass
    # If still missing, use sl_price from position as approximation
    if not sl_exit_price and pos_snapshot:
        _sl_approx = pos_snapshot.get("sl_price") or pos_snapshot.get("trail_sl")
        if _sl_approx:
            sl_exit_price = float(_sl_approx)
            write_log(f"SL_EXIT_APPROX | {ticker} | using stored sl_price={sl_exit_price}")
    write_log(f"PNL_CALC | {ticker} | entry={pos_snapshot.get('entry_price')} sl_exit={sl_exit_price} lev={pos_snapshot.get('leverage')}")
    pnl = calc_trade_pnl(pos_snapshot, sl_exit_price)
    _exchange_sl  = pos_snapshot.get("exchange", "bybit")
    _lev          = pos_snapshot.get("leverage", 1)
    _dirn         = pos_snapshot.get("direction", "?")
    _entry        = pos_snapshot.get("entry_price", 0)
    _tps_hit_list = pnl["tps_hit"]

    # Формируем обогащённое сообщение (заменяем текст индикатора нашим)
    if _tps_hit_list:
        _tp_str = " → ".join(f"TP{n}" for n in _tps_hit_list)
        _res_label = f"✅ Частичная прибыль ({_tp_str})"
    else:
        _res_label = "❌ Чистый убыток"

    _pnl_sign = "+" if pnl["pnl_pct"] >= 0 else ""
    _pnl_color = "📈" if pnl["pnl_pct"] >= 0 else "📉"

    # Длительность (из trade_data)
    _dur_str = ""
    _created = (trade or {}).get("created_at") or pos_snapshot.get("created_at")
    if _created:
        _dur_sec = int(time.time()) - int(_created)
        _dur_str = f"\n⏱ Время в сделке: {fmt_duration(_dur_sec)}"

    _exch_label = ""
    if _exchange_sl != "none":
        _demo_flag = "🧪DEMO" if (_exchange_sl == "bingx" and BINGX_DEMO) else ("🧪TEST" if TESTNET else "🔴LIVE")
        _exch_label = f" [{_exchange_sl.upper()} {_demo_flag}]"

    _tp_part_sign = "+" if pnl["tp_pnl_pct"] >= 0 else ""
    _sl_part_sign = "+" if pnl["sl_pnl_pct"] >= 0 else ""
    bot_msg = (
        f"🛑 <b>СТОП ВЫБИТ</b>{_exch_label}\n"
        f"━━━━━━━\n"
        f"#{ticker} {_dirn}"
        f"{_dur_str}\n"
        f"📍 Вход: {_entry}  |  SL-выход: {sl_exit_price or chr(8212)}\n"
        f"📊 {_res_label}\n"
        f"{_pnl_color} <b>П&amp;Л (бот): {_pnl_sign}{pnl['pnl_pct']}%</b>"
        f"  <i>(без плеча: {_pnl_sign}{pnl['pnl_pct_no_lev']}%)</i>\n"
        f"  TP-часть: {_tp_part_sign}{pnl['tp_pnl_pct']}%"
        f"  |  SL-остаток: {_sl_part_sign}{pnl['sl_pnl_pct']}%"
        f"  ({pnl['remaining_pct']}% позиции)"
    )

    # Отправляем НАШЕ сообщение (точный P&L), потом текст из Pine Script если есть
    send_signals(bot_msg, reply_to=reply_id)
    if text and text.strip() != bot_msg.strip():
        # Пересылаем оригинальный текст Pine Script для сравнения
        write_log(f"SL_HIT_PINESCRIPT_TEXT | {ticker} | {text[:120]}")

    # FIX 5: result type
    if highest_tp > 0 and _exchange_sl != "none":
        result = "win"
    elif highest_tp > 0 and _exchange_sl == "none":
        result = "partial"
    else:
        result = "loss"

    # Store pnl in finalize payload
    payload["_bot_pnl"] = pnl
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
    
    # ⏰ Логирование задержки между TradingView и ботом
    alert_time_ms = payload.get("alert_time")
    if alert_time_ms:
        delay_ms = int(time.time() * 1000) - alert_time_ms
        delay_sec = delay_ms / 1000
        write_log(f"PROCESS | event={event} | ticker={payload.get('ticker','?')} | delay={delay_sec:.2f}s")
    else:
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


# Fast-lane events: process immediately in a thread, don't block the queue.
# Entry signals go through the queue (need ordering, dedup). All others → fast lane.
_FAST_LANE_EVENTS = {"tp_hit", "sl_hit", "sl_moved", "limit_order", "scale_in"}


def enqueue_signal(payload: dict):
    event = payload.get("event", "")
    # FIX 8: non-entry events bypass the queue to avoid being blocked by long
    # entry processing (e.g. UNIUSDT open taking 5s delays GPSUSDT tp_hit).
    if event in _FAST_LANE_EVENTS:
        threading.Thread(
            target=_safe_process_signal,
            args=(payload,),
            daemon=True,
            name=f"fast_{event}_{payload.get('ticker','?')}",
        ).start()
        return
    with _queue_lock:
        _signal_queue.append({"payload": payload, "attempts": 0, "queued_at": time.time()})


def _safe_process_signal(payload: dict):
    try:
        process_signal(payload)
    except Exception as e:
        write_log(f"FAST_LANE_ERR | {payload.get('event','?')} {payload.get('ticker','?')} | {e}")


def _queue_worker():
    write_log("QUEUE_WORKER | start")
    while True:
        with _queue_lock:
            item = _signal_queue.pop(0) if _signal_queue else None
        if item:
            payload  = item["payload"]
            attempts = item["attempts"]
            queued_at = item.get("queued_at", 0)
            retry_after = item.get("retry_after", 0)
            
            # ⏰ Если элемент запланирован на будущее - возвращаем в очередь
            now = time.time()
            if retry_after > now:
                with _queue_lock:
                    _signal_queue.append(item)
                time.sleep(0.5)
                continue
            
            # ⏰ Показываем время ожидания в очереди
            wait_time = now - queued_at
            if wait_time > 5:  # Предупреждаем если ждали больше 5 секунд
                write_log(f"QUEUE_WAIT | ticker={payload.get('ticker','?')} | wait={wait_time:.2f}s | attempts={attempts}")
            
            try:
                process_signal(payload)
            except Exception as e:
                write_log(f"QUEUE_ERR | attempt={attempts} | ticker={payload.get('ticker','?')} | {e}")
                if attempts < MAX_QUEUE_ATTEMPTS - 1:
                    item["attempts"] += 1
                    # 🔁 Экспоненциальная задержка: 1s, 2s, 4s вместо фиксированных 15s
                    retry_delay = min(2 ** attempts, 15)
                    item["retry_after"] = time.time() + retry_delay
                    with _queue_lock:
                        # Кладём в КОНЕЦ очереди, не блокируя другие сигналы
                        _signal_queue.append(item)
                    write_log(f"QUEUE_RETRY | ticker={payload.get('ticker','?')} | delay={retry_delay}s | attempt={attempts+1}")
                else:
                    write_log(f"QUEUE_DEAD | ticker={payload.get('ticker','?')} | payload={json.dumps(payload)[:200]}")
        else:
            time.sleep(0.1)  # Быстрее проверяем если очередь пуста


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
    active = []
    upcoming = []
    lines_all = []
    for s in SESSIONS:
        oh, om = map(int, s["open"].split(":"))
        ch, cm = map(int, s["close"].split(":"))
        o_min  = oh * 60 + om
        c_min  = ch * 60 + cm
        if c_min < o_min:
            is_open = cur >= o_min or cur < c_min
        else:
            is_open = o_min <= cur < c_min
        if is_open:
            active.append(s["name"])
        else:
            # Минут до открытия
            diff = o_min - cur
            if diff < 0:
                diff += 1440
            upcoming.append((diff, s["name"], s["open"]))
        status = "🟢 Открыта" if is_open else "🔴 Закрыта"
        lines_all.append(f"{s['name']}  {s['open']}–{s['close']}  {status}")

    text = f"🕐 <b>Время МСК: {msk.strftime('%H:%M')}</b>\n"
    if active:
        text += f"📊 Активные сессии: <b>{', '.join(a.split()[-1] for a in active)}</b>\n"
    else:
        text += "📊 Активные сессии: —\n"

    if upcoming:
        upcoming.sort()
        mins_left, next_name, next_open = upcoming[0]
        h_left, m_left = divmod(mins_left, 60)
        dur_str = f"{h_left}ч {m_left}м" if h_left else f"{m_left}м"
        short = next_name.split()[-1]
        text += f"⏰ Следующее: <b>{short}</b> в {next_open} МСК (через {dur_str})\n"

    text += f"\n📅 <b>Расписание (МСК):</b>\n"
    text += "\n".join(lines_all)
    return text


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

            # Авто-очистка зависших сделок каждые 6 часов
            if h % 6 == 0 and 0 <= m <= 1:
                auto_clean_key = f"auto_cleanup_{today}_{h:02d}"
                if not _was_sent(auto_clean_key):
                    _mark_sent(auto_clean_key)
                    n = cleanup_old_trades()
                    if n > 0:
                        write_log(f"AUTO_CLEANUP | removed {n} orphaned trades")

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
            if msk.day == last_day and h == 23 and 55 <= m <= 59:
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
                exchange = pos.get("exchange", "bybit")
                remaining = pos.get("remaining_qty", 0)

                # Удаляем мёртвые позиции (нулевой объём или без биржи без trail)
                # Bug 3 Fix: only remove positions with zero remaining qty.
                # Previously exchange=="none" positions were removed immediately,
                # causing tp_hit/sl_hit signals to have no position to match → lost stats.
                if remaining <= 0:
                    with _pos_lock:
                        p2 = load_positions()
                        if pkey in p2:
                            p2.pop(pkey)
                            save_positions(p2)
                            write_log(f"POS_MGR_CLEANUP | {pkey} | exchange={exchange} remaining={remaining}")
                    continue

                # Позиции без биржи — только telegram-only, trailing stop не нужен
                if exchange == "none":
                    continue

                if not pos.get("trail_active"):
                    continue
                ticker   = pos["symbol"]
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
        time.sleep(120)


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
        # Bug 1 Fix: use send_tg() (3 retries, requests.post) instead of
        # bot.send_message() which has no retry and fails on RemoteDisconnected.
        thread_id = getattr(m, "message_thread_id", None)
        result = send_tg(text, thread_id=thread_id, chat_id=str(m.chat.id))
        if not result:
            write_log(f"REPLY_ERR | send_tg returned empty for chat={m.chat.id}")

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
            "/market — F&G + сессии одним сообщением\n"
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
            "/logs — последние строки лога\n/diagnostics — проверка всех API\n/redis_fix — переподключить Redis"
        ))

    @bot.message_handler(commands=["stats"])
    def cmd_stats(m):
        s      = load_stats()
        wins   = s.get("wins", 0)
        losses = s.get("losses", 0)
        total  = s.get("total", 0)
        wr     = calc_winrate(wins, wins + losses)
        # TP ≥ 3 из истории
        history = load_history()
        tp3plus = sum(1 for r in history if r.get("result") == "win"
                      and int(r.get("tp_num") or 0) >= 3)
        pnl_vals = [r["pnl"].get("pnl_pct", 0) for r in history if r.get("pnl")]
        avg_pnl_all = round(sum(pnl_vals) / len(pnl_vals), 2) if pnl_vals else None
        pnl_line = ""
        if avg_pnl_all is not None:
            _s = "+" if avg_pnl_all >= 0 else ""
            pnl_line = f"\n💰 Ср. П&Л на сделку: <b>{_s}{avg_pnl_all}%</b>"
        _reply(m, (
            f"📊 <b>Статистика Statham Strategy</b>\n\n"
            f"{_wr_icon(wr)} Win Rate: <b>{wr}%</b>\n"
            f"✅ TP (≥TP3): {tp3plus}   ✅ TP всего: {wins}\n"
            f"❌ SL: {losses}\n"
            f"📈 Всего закрыто: <b>{total}</b>"
            f"{pnl_line}"
        ))

    @bot.message_handler(commands=["daily_report"])
    def cmd_daily(m):
        today = _msk().strftime("%Y-%m-%d")
        ts    = [r for r in load_history() if r.get("date_msk") == today]
        if not ts:
            _reply(m, f"📅 Сегодня ({today}) закрытых сделок нет.")
            return
        _reply(m, _build_report(ts, f"📅 <b>Дневной отчёт {today}</b>",
                                 show_last=5, show_top_tickers=False))

    @bot.message_handler(commands=["weekly_report"])
    def cmd_weekly(m):
        msk  = _msk()
        wk   = f"{msk.year}-W{msk.isocalendar()[1]:02d}"
        # Дата начала/конца недели (пн–вс)
        week_day   = msk.weekday()
        week_start = (msk - datetime.timedelta(days=week_day)).strftime("%d.%m")
        week_end   = (msk + datetime.timedelta(days=6 - week_day)).strftime("%d.%m.%Y")
        ts = [r for r in load_history() if r.get("week_msk") == wk]
        if not ts:
            _reply(m, f"📅 На этой неделе ({wk}) закрытых сделок нет.")
            return
        _reply(m, _build_report(
            ts,
            f"📅 <b>Недельный отчёт</b>",
            date_range=f"с {week_start} по {week_end}",
            show_last=0,
            show_top_tickers=True,
        ))

    @bot.message_handler(commands=["monthly_report"])
    def cmd_monthly(m):
        msk = _msk()
        mo  = f"{msk.year}-{msk.month:02d}"
        import calendar as _cal
        month_name_ru = {1:"Январь",2:"Февраль",3:"Март",4:"Апрель",
                         5:"Май",6:"Июнь",7:"Июль",8:"Август",
                         9:"Сентябрь",10:"Октябрь",11:"Ноябрь",12:"Декабрь"}
        ts = [r for r in load_history() if r.get("date_msk","").startswith(mo)]
        if not ts:
            _reply(m, f"📅 В этом месяце ({mo}) закрытых сделок нет.")
            return
        title = f"📅 <b>Месячный отчёт — {month_name_ru.get(msk.month, mo)} {msk.year}</b>"
        _reply(m, _build_report(ts, title, show_last=0, show_top_tickers=True))

    @bot.message_handler(commands=["stats_by_ticker"])
    def cmd_stats_by_ticker(m):
        parts = m.text.split()
        if len(parts) < 2:
            _reply(m, "❌ Укажи тикер: /stats_by_ticker BTCUSDT")
            return
        ticker = parts[1].upper().replace(".P", "")
        ts = [r for r in load_history()
              if r.get("ticker", "").replace(".P", "").upper() == ticker]
        if not ts:
            _reply(m, f"Нет данных по {ticker}")
            return
        _reply(m, _build_report(ts, f"📊 <b>Статистика #{ticker}</b>",
                                 show_last=5, show_top_tickers=False))

    @bot.message_handler(commands=["leaders"])
    def cmd_leaders(m):
        by_ticker: dict = {}
        for r in load_history():
            tk = r.get("ticker", "?").replace(".P", "")
            if tk not in by_ticker:
                by_ticker[tk] = {"wins": 0, "losses": 0, "tp_counts": {}}
            d      = by_ticker[tk]
            result = r.get("result", "")
            tp_num = int(r.get("tp_num") or 0)
            highest= int(r.get("highest_tp_hit") or 0)
            if result == "win":
                d["wins"] += 1
                best = max(tp_num, highest)
                if best > 0:
                    d["tp_counts"][best] = d["tp_counts"].get(best, 0) + 1
            elif result == "loss":
                d["losses"] += 1

        if not by_ticker:
            _reply(m, "Нет данных.")
            return

        def _wr_val(d):
            t = d["wins"] + d["losses"]
            return calc_winrate(d["wins"], t) if t else -1

        scored = [(tk, d) for tk, d in by_ticker.items()
                  if d["wins"] + d["losses"] > 0]
        scored_by_wr = sorted(scored, key=lambda x: (_wr_val(x[1]), x[1]["wins"]), reverse=True)

        best  = scored_by_wr[:5]
        worst = list(reversed(scored_by_wr[-5:])) if len(scored_by_wr) > 5 else []

        medals_best  = ["🥇","🥈","🥉","4️⃣","5️⃣"]
        medals_worst = ["🔻","🔻","🔻","🔻","🔻"]

        total_wins   = sum(d["wins"]   for _, d in by_ticker.items())
        total_losses = sum(d["losses"] for _, d in by_ticker.items())
        total_all    = total_wins + total_losses
        overall_wr   = calc_winrate(total_wins, total_all)
        tickers_cnt  = len(scored)

        msk = _msk()
        week_day   = msk.weekday()
        week_start = (msk - datetime.timedelta(days=week_day)).strftime("%d.%m")
        week_end   = msk.strftime("%d.%m")

        lines = [f"📊 <b>Топ пар по Win Rate</b>",
                 f"<i>с {week_start} по {week_end}</i>\n"]

        lines.append("🏆 <b>Лучшие тикеры:</b>")
        for i, (tk, d) in enumerate(best):
            wr  = _wr_val(d)
            tot = d["wins"] + d["losses"]
            tp_str = " ".join(f"TP{n}×{c}" for n, c in sorted(d["tp_counts"].items()) if c)
            lines.append(
                f"{medals_best[i]} <b>#{tk}</b> — WR {wr:.1f}% "
                f"({d['wins']}W/{d['losses']}L из {tot})"
                + (f"\n   <i>{tp_str}</i>" if tp_str else "")
            )

        if worst:
            lines.append("\n📉 <b>Худшие тикеры:</b>")
            for i, (tk, d) in enumerate(worst):
                wr  = _wr_val(d)
                tot = d["wins"] + d["losses"]
                lines.append(
                    f"{medals_worst[i]} <b>#{tk}</b> — WR {wr:.1f}% "
                    f"({d['wins']}W/{d['losses']}L из {tot})"
                )

        lines.append(
            f"\nВсего тикеров: <b>{tickers_cnt}</b> | Сделок: <b>{total_all}</b>\n"
            f"📊 Общий WR: <b>{_wr_icon(overall_wr)} {overall_wr}%</b>  "
            f"({total_wins}✅ / {total_losses}❌)"
        )
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

    @bot.message_handler(commands=["market"])
    def cmd_market(m):
        fg = _fetch_fg()
        fg_line = ""
        if fg:
            v = fg["value"]
            label = fg["label"]
            fg_line = f"{_fg_emoji(v)} F&G: <b>{v}</b> — {label}\n"
        else:
            fg_line = "😐 F&G: недоступен\n"
        text = f"🌐 <b>Рынок</b>\n\n{fg_line}\n{_sessions_status()}"
        _reply(m, text)

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
        n = cleanup_old_trades()
        trades_left = len(load_trades())
        pos_left    = len(load_positions())
        _reply(m, f"🧹 Удалено <b>{n}</b> зависших/устаревших сделок.\nОсталось: trades={trades_left}, positions={pos_left}")

    @bot.message_handler(commands=["clean"])
    def cmd_clean(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        save_trades({}); save_positions({})
        _reply(m, "🧹 Активные сделки и позиции очищены.")

    @bot.message_handler(commands=["diagnostics"])
    def cmd_diagnostics(m):
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        lines = ["🔧 <b>Диагностика API</b>\n"]
        # Telegram
        try:
            r = requests.get(f"https://api.telegram.org/bot{TG_TOKEN}/getMe", timeout=8)
            d = r.json()
            bot_name = d.get("result", {}).get("username", "?")
            lines.append(f"{'✅' if d.get('ok') else '❌'} Telegram: @{bot_name}")
        except Exception as e:
            lines.append(f"❌ Telegram: {e}")
        # Redis
        try:
            rc = _get_redis()
            if rc:
                rc.ping()
                keys = rc.keys(_REDIS_PREFIX + "*")
                lines.append(f"✅ Redis: {len(keys)} keys")
            else:
                lines.append("❌ Redis: не подключён")
        except Exception as e:
            _redis_invalidate()
            lines.append(f"❌ Redis: {e}")
        # Bybit
        if BYBIT_AVAILABLE:
            try:
                resp = bybit().get_server_time()
                lines.append(f"{'✅' if resp['retCode']==0 else '❌'} Bybit: server_time={resp.get('result',{}).get('timeSecond','?')}")
            except Exception as e:
                lines.append(f"❌ Bybit: {e}")
        else:
            lines.append("⚪ Bybit: не настроен")
        # BingX
        if BINGX_AVAILABLE:
            try:
                d = _bingx_req("GET", "/openApi/swap/v2/quote/ticker", {"symbol": "BTC-USDT"})
                lines.append(f"✅ BingX: ok, BTC={d.get('data',{}).get('lastPrice','?')}")
            except Exception as e:
                lines.append(f"❌ BingX: {e}")
        else:
            lines.append("⚪ BingX: не настроен")
        # Queue and state
        lines.append(f"\n📊 Позиций: {len(load_positions())} | Сделок: {len(load_trades())}")
        lines.append(f"📬 Очередь: {len(_signal_queue)} | 🛑 Стоп: {_emergency_stop.is_set()}")
        _reply(m, "\n".join(lines))

    @bot.message_handler(commands=["redis_fix"])
    def cmd_redis_fix(m):
        """Принудительно сбрасывает Redis-клиент и переподключается."""
        if not is_admin_user(m.from_user.id):
            _reply(m, "❌ Только для администраторов."); return
        _redis_invalidate()
        rc = _get_redis()
        if rc:
            _reply(m, "✅ Redis переподключён успешно.")
        else:
            _reply(m, "❌ Redis переподключение не удалось — проверь REDIS_URL.")

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
        # Start position manager always — needed for telegram-only positions too
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
            write_log(f"WEBHOOK_TELEGRAM_ONLY | {ticker} not in ALLOWED_PAIRS={sorted(ALLOWED_PAIRS)} — signal → Telegram only, no exchange execution")

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


@app.route("/diagnostics")
def diagnostics_route():
    """Проверяет связь с Telegram, Bybit, BingX, Redis."""
    result = {"time_utc": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}

    # Telegram
    try:
        r = requests.get(f"https://api.telegram.org/bot{TG_TOKEN}/getMe", timeout=8)
        d = r.json()
        result["telegram"] = {"ok": d.get("ok"), "bot": d.get("result", {}).get("username", "?")}
    except Exception as e:
        result["telegram"] = {"ok": False, "error": str(e)}

    # Redis
    try:
        rc = _get_redis()
        if rc:
            rc.ping()
            result["redis"] = {"ok": True, "keys": len(rc.keys(_REDIS_PREFIX + "*"))}
        else:
            result["redis"] = {"ok": False, "error": "not configured or connection failed"}
    except Exception as e:
        result["redis"] = {"ok": False, "error": str(e)}

    # Bybit
    if BYBIT_AVAILABLE:
        try:
            resp = bybit().get_server_time()
            result["bybit"] = {"ok": resp["retCode"] == 0,
                               "server_time": resp.get("result", {}).get("timeSecond", "?")}
        except Exception as e:
            result["bybit"] = {"ok": False, "error": str(e)}
    else:
        result["bybit"] = {"ok": False, "error": "not configured"}

    # BingX
    if BINGX_AVAILABLE:
        try:
            d = _bingx_req("GET", "/openApi/swap/v2/quote/ticker", {"symbol": "BTC-USDT"})
            result["bingx"] = {"ok": True, "price": d.get("data", {}).get("lastPrice", "?")}
        except Exception as e:
            result["bingx"] = {"ok": False, "error": str(e)}
    else:
        result["bingx"] = {"ok": False, "error": "not configured"}

    result["queue_len"]      = len(_signal_queue)
    result["active_trades"]  = len(load_trades())
    result["active_positions"] = len(load_positions())
    result["emergency_stop"] = _emergency_stop.is_set()
    write_log(f"DIAGNOSTICS | tg={result['telegram']['ok']} redis={result['redis']['ok']} bybit={result.get('bybit',{}).get('ok','n/a')} bingx={result.get('bingx',{}).get('ok','n/a')}")
    return jsonify(result)


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
