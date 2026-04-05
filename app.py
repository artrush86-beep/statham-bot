"""
Statham Trading Bot — RENDER (Unified v2.0)
============================================
ИСПРАВЛЕНИЯ v2.0:
  • FIX: direction нормализация — short/long/sell/buy в любом регистре → BUY/SELL
  • FIX: direction fallback из event ("short"/"long" как event → direction)
  • FIX: .P убирается правильно: .upper() сначала, потом .replace(".P","")
  • FIX: TP1/TP2 теперь тоже считаются как win в статистике
  • FIX: _file_lock убран — заменён раздельными lock-ами (нет риска дедлока)
  • FIX: sent.get("message_id") безопасен при пустом ответе TG
  • FIX: sl/tp парсятся и из текста и из полей JSON одновременно
  • НОВОЕ: Поддержка BingX — BYBIT_PAIRS и BINGX_PAIRS раздельно
  • НОВОЕ: normalize_ticker() и normalize_direction() — единая точка очистки
  • НОВОЕ: /balance показывает оба брокера

ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ:
  TG_TOKEN, TG_CHAT
  TG_SIGNALS_TOPIC    — ID ветки сигналов (6314)
  TG_SESSIONS_TOPIC   — ID ветки сессий/F&G (1)
  WEBHOOK_SECRET      — секрет для TradingView → /webhook/bybit
  RENDER_SECRET       — защита admin HTTP-эндпоинтов
  RENDER_URL          — для keepalive и регистрации webhook

  BYBIT_API_KEY, BYBIT_API_SECRET
  BYBIT_TESTNET       — "true"/"false" (по умолчанию false = реальный)
  BYBIT_PAIRS         — YBUSDT,ASTERUSDT,EDGEUSDT,STOUSDT

  BINGX_API_KEY, BINGX_API_SECRET
  BINGX_PAIRS         — BTCUSDT,ETHUSDT  (пусто = BingX не используется)

  ADMIN_IDS           — 123456,789012
  DEFAULT_LEVERAGE    — 10
  DEFAULT_SIZE_USDT   — 1
  PAIR_SETTINGS_JSON  — '{"YBUSDT":{"leverage":10,"size_usdt":5}}'
  TRAIL_PCT           — 0.5
  DATA_DIR            — /tmp (или /data если Render Disk подключён)

ЭНДПОИНТЫ:
  POST /webhook/telegram
  POST /webhook/bybit      ← TradingView сюда шлёт сигналы
  GET  /health
  GET  /setup
  GET  /debug
  GET  /trades, /stats, /history
  GET  /positions          [требует ?secret=RENDER_SECRET]
  POST /close_all          [требует ?secret=RENDER_SECRET]
  GET  /test_bybit         [требует ?secret=RENDER_SECRET]
"""
from __future__ import annotations
import json, os, re, time, math, hmac, hashlib, threading, random, datetime, logging, calendar
from flask import Flask, request, jsonify
import requests
import telebot

try:
    from pybit.unified_trading import HTTP as BybitHTTP
    BYBIT_SDK = True
except ImportError:
    BYBIT_SDK = False

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
RENDER_SECRET     = os.environ.get("RENDER_SECRET",     "")
RENDER_URL        = os.environ.get("RENDER_URL",        "")

BYBIT_API_KEY    = os.environ.get("BYBIT_API_KEY",    "")
BYBIT_API_SECRET = os.environ.get("BYBIT_API_SECRET", "")
BYBIT_TESTNET    = os.environ.get("BYBIT_TESTNET",    "false").lower() == "true"

BINGX_API_KEY    = os.environ.get("BINGX_API_KEY",    "")
BINGX_API_SECRET = os.environ.get("BINGX_API_SECRET", "")
BINGX_BASE_URL   = "https://open-api.bingx.com"

def _parse_pairs(env_key: str, default: str = "") -> set[str]:
    return set(
        p.strip().upper().replace(".P","")
        for p in os.environ.get(env_key, default).split(",")
        if p.strip()
    )

BYBIT_PAIRS   = _parse_pairs("BYBIT_PAIRS", "YBUSDT,ASTERUSDT,EDGEUSDT,STOUSDT")
BINGX_PAIRS   = _parse_pairs("BINGX_PAIRS", "")
ALLOWED_PAIRS = BYBIT_PAIRS | BINGX_PAIRS

ADMIN_IDS: set[int] = set(
    int(x.strip()) for x in os.environ.get("ADMIN_IDS","").split(",")
    if x.strip().lstrip("-").isdigit()
)
DEFAULT_LEVERAGE  = int(os.environ.get("DEFAULT_LEVERAGE",  "10"))
DEFAULT_SIZE_USDT = float(os.environ.get("DEFAULT_SIZE_USDT","1"))
TRAIL_PCT         = float(os.environ.get("TRAIL_PCT","0.5")) / 100.0

try:
    PAIR_SETTINGS: dict = json.loads(os.environ.get("PAIR_SETTINGS_JSON","{}"))
except Exception:
    PAIR_SETTINGS = {}

TP_CLOSE_PCT = {1: 0.25, 2: 0.20, 3: 0.25, 4: 0.15, 5: 0.10, 6: 0.05}

DATA_DIR        = os.environ.get("DATA_DIR", "/tmp")
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
_queue_lock    = threading.Lock()
_pos_lock      = threading.Lock()
_stats_lock    = threading.Lock()
_trades_lock   = threading.Lock()
_history_lock  = threading.Lock()
_flags_lock    = threading.Lock()
_log_lock      = threading.Lock()
_bybit_lock    = threading.Lock()
_emergency_stop = threading.Event()
FG_SCHEDULED_HOURS = {4, 10, 16, 22}

bot = telebot.TeleBot(TG_TOKEN, threaded=False) if TG_TOKEN else None

# ══════════════════════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════════════════════
def _http_auth(req) -> bool:
    if not RENDER_SECRET: return True
    token = req.headers.get("X-Secret") or req.args.get("secret","")
    return token == RENDER_SECRET

# ══════════════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ══════════════════════════════════════════════════════════════════════════════
def write_log(entry: str):
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    log.info(entry)
    try:
        with _log_lock:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"[{ts}] {entry}\n")
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
            if len(lines) > 1000:
                with open(LOG_FILE, "w", encoding="utf-8") as f:
                    f.writelines(lines[-1000:])
    except Exception:
        pass

# ══════════════════════════════════════════════════════════════════════════════
# JSON
# ══════════════════════════════════════════════════════════════════════════════
def load_json(path: str, default):
    if not os.path.exists(path): return default
    try:
        with open(path,"r",encoding="utf-8") as f: return json.load(f)
    except Exception as e:
        write_log(f"LOAD_JSON_ERR | {path} | {e}"); return default

def save_json(path: str, data):
    try:
        with open(path,"w",encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        write_log(f"SAVE_JSON_ERR | {path} | {e}")

def load_stats()     : return load_json(STATS_FILE,     {"wins":0,"losses":0,"total":0})
def load_trades()    : return load_json(TRADES_FILE,    {})
def load_history()   : return load_json(HISTORY_FILE,   [])
def load_positions() : return load_json(POSITIONS_FILE, {})
def save_stats(d)    : save_json(STATS_FILE,     d)
def save_trades(d)   : save_json(TRADES_FILE,    d)
def save_history(d)  : save_json(HISTORY_FILE,   d)
def save_positions(d): save_json(POSITIONS_FILE, d)

def _was_sent(key: str) -> bool:
    return load_json(SENT_FLAGS_FILE,{}).get(key, False)

def _mark_sent(key: str):
    with _flags_lock:
        flags = load_json(SENT_FLAGS_FILE,{})
        flags[key] = True
        save_json(SENT_FLAGS_FILE, flags)

# ══════════════════════════════════════════════════════════════════════════════
# ХЕЛПЕРЫ
# ══════════════════════════════════════════════════════════════════════════════
def _msk() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=3)

def calc_winrate(wins: int, total: int) -> float:
    return round(wins/total*100,1) if total > 0 else 0.0

def fmt_duration(seconds: int) -> str:
    if seconds < 3600:  return f"{seconds//60}м"
    if seconds < 86400: return f"{seconds//3600}ч {(seconds%3600)//60}м"
    return f"{seconds//86400}д {(seconds%86400)//3600}ч"

def normalize_ticker(raw: str) -> str:
    """BINANCE:YBUSDT.P|120 → YBUSDT"""
    s = raw.strip()
    # Убираем префикс биржи (BINANCE:, BYBIT:, ...)
    if ":" in s:
        s = s.split(":")[-1]
    # Убираем суффикс таймфрейма (|120)
    if "|" in s:
        s = s.split("|")[0]
    return s.upper().replace(".P","").strip()

def normalize_direction(raw: str, event: str = "") -> str:
    """Любой формат → BUY или SELL. Пустая строка если не определить."""
    for src in (raw, event):
        d = src.strip().upper()
        if d in ("BUY","LONG"):   return "BUY"
        if d in ("SELL","SHORT"): return "SELL"
    write_log(f"DIRECTION_WARN | raw='{raw}' event='{event}'")
    return ""

def is_admin_user(user_id: int) -> bool:
    if ADMIN_IDS and user_id in ADMIN_IDS: return True
    if not ADMIN_IDS and bot and TG_CHAT:
        try:
            return user_id in [a.user.id for a in bot.get_chat_administrators(TG_CHAT)]
        except Exception: pass
    return False

def pair_cfg(ticker: str) -> dict:
    return {**{"leverage":DEFAULT_LEVERAGE,"size_usdt":DEFAULT_SIZE_USDT},
            **PAIR_SETTINGS.get(ticker,{})}

def _to_f(v) -> float | None:
    try: return float(v) if v is not None else None
    except: return None

# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════
def send_tg(text: str, thread_id=None, chat_id: str|None=None,
            reply_to: int|None=None) -> dict:
    if not TG_TOKEN or not TG_CHAT: return {}
    if chat_id is None: chat_id = TG_CHAT
    payload: dict = {
        "chat_id": chat_id, "text": text[:4000],
        "parse_mode": "HTML", "disable_web_page_preview": True,
    }
    if thread_id is not None:
        try: payload["message_thread_id"] = int(thread_id)
        except: pass
    if reply_to:
        payload["reply_parameters"] = {"message_id": reply_to,
                                       "allow_sending_without_reply": True}
    try:
        resp = requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                             json=payload, timeout=10)
        data = resp.json()
        if not data.get("ok"):
            write_log(f"SEND_TG_FAIL | {data.get('description')} | thread={thread_id}")
            return {}
        return data["result"]
    except Exception as e:
        write_log(f"SEND_TG_ERR | {e}"); return {}

def send_signals(text: str, reply_to: int|None=None) -> dict:
    return send_tg(text, thread_id=TG_SIGNALS_TOPIC, reply_to=reply_to)

def send_sessions(text: str) -> dict:
    return send_tg(text, thread_id=TG_SESSIONS_TOPIC)

def send_fg(text: str) -> dict:
    return send_tg(text, thread_id=TG_SESSIONS_TOPIC)

# ══════════════════════════════════════════════════════════════════════════════
# BYBIT
# ══════════════════════════════════════════════════════════════════════════════
_bybit_session = None
_instr_cache: dict = {}

def _bybit():
    global _bybit_session
    if not BYBIT_SDK: raise RuntimeError("pybit not installed")
    if not BYBIT_API_KEY: raise RuntimeError("BYBIT_API_KEY not set")
    with _bybit_lock:
        if _bybit_session is None:
            _bybit_session = BybitHTTP(testnet=BYBIT_TESTNET,
                                       api_key=BYBIT_API_KEY,
                                       api_secret=BYBIT_API_SECRET)
            write_log(f"BYBIT | connected | testnet={BYBIT_TESTNET}")
    return _bybit_session

def _bybit_instr(symbol: str) -> dict:
    if symbol not in _instr_cache:
        r = _bybit().get_instruments_info(category="linear", symbol=symbol)
        _instr_cache[symbol] = r["result"]["list"][0]
    return _instr_cache[symbol]

def _bybit_rq(symbol: str, qty: float) -> str:
    try:
        step = float(_bybit_instr(symbol)["lotSizeFilter"]["qtyStep"])
        dec  = max(0, round(-math.log10(step)))
        return str(round(math.floor(qty/step)*step, dec))
    except: return str(round(qty,4))

def _bybit_rp(symbol: str, price: float) -> str:
    try:
        tick = float(_bybit_instr(symbol)["priceFilter"]["tickSize"])
        dec  = max(0, round(-math.log10(tick)))
        return str(round(price, dec))
    except: return str(round(price,6))

def _bybit_mq(symbol: str) -> float:
    try: return float(_bybit_instr(symbol)["lotSizeFilter"]["minOrderQty"])
    except: return 0.001

def _bybit_price(symbol: str) -> float:
    r = _bybit().get_tickers(category="linear", symbol=symbol)
    return float(r["result"]["list"][0]["lastPrice"])

def _bybit_set_lev(symbol: str, lev: int):
    try:
        _bybit().set_leverage(category="linear", symbol=symbol,
                              buyLeverage=str(lev), sellLeverage=str(lev))
    except Exception as e:
        write_log(f"BYBIT_LEV_WARN | {symbol} | {e}")

def _bybit_calc_qty(symbol: str, size_usdt: float, lev: int, price: float) -> float:
    qty = (size_usdt*lev)/price
    return max(float(_bybit_rq(symbol,qty)), _bybit_mq(symbol))

def _bybit_market(symbol: str, side: str, qty: float, ro: bool=False) -> dict:
    r = _bybit().place_order(category="linear", symbol=symbol,
                             side=side, orderType="Market",
                             qty=_bybit_rq(symbol,qty),
                             reduceOnly=ro, timeInForce="IOC")
    write_log(f"BYBIT_MARKET | {symbol} {side} qty={qty} ro={ro} | ret={r['retCode']}")
    return r

def _bybit_stop(symbol: str, side: str, qty: float, trigger: float, ro: bool=True) -> dict:
    r = _bybit().place_order(category="linear", symbol=symbol,
                             side=side, orderType="Market",
                             qty=_bybit_rq(symbol,qty),
                             triggerPrice=_bybit_rp(symbol,trigger),
                             triggerBy="LastPrice", orderFilter="StopOrder",
                             reduceOnly=ro, timeInForce="IOC")
    write_log(f"BYBIT_STOP | {symbol} {side} trig={trigger} | ret={r['retCode']}")
    return r

def _bybit_cancel(symbol: str):
    try: _bybit().cancel_all_orders(category="linear", symbol=symbol)
    except Exception as e: write_log(f"BYBIT_CANCEL_ERR | {symbol} | {e}")

def _bybit_move_sl(pos: dict, new_sl: float) -> str:
    sym = pos["symbol"]; opp = pos["opp_side"]
    rem = pos.get("remaining_qty", pos["total_qty"])
    if rem <= 0: return ""
    _bybit_cancel(sym); time.sleep(0.3)
    try:
        r = _bybit_stop(sym, opp, rem, new_sl)
        if r["retCode"]==0:
            oid = r["result"].get("orderId","")
            write_log(f"BYBIT_SL_MOVED | {sym} → {new_sl} | order={oid}")
            return oid
    except Exception as e: write_log(f"BYBIT_SL_MOVE_ERR | {sym} | {e}")
    return ""

def bybit_balance_text() -> str:
    try:
        r     = _bybit().get_wallet_balance(accountType="UNIFIED")
        eq    = float(r["result"]["list"][0]["totalEquity"])
        av    = float(r["result"]["list"][0]["totalAvailableBalance"])
        mode  = "🧪 TESTNET" if BYBIT_TESTNET else "🔴 LIVE"
        return f"Bybit {mode}  Equity: <b>{eq:.2f}</b>  Avail: <b>{av:.2f}</b> USDT"
    except Exception as e: return f"Bybit: ❌ {e}"

# ══════════════════════════════════════════════════════════════════════════════
# BINGX
# ══════════════════════════════════════════════════════════════════════════════
def _bingx_sign(params: dict) -> str:
    qs = "&".join(f"{k}={v}" for k,v in sorted(params.items()))
    return hmac.new(BINGX_API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()

def _bingx_req(method: str, path: str, params: dict) -> dict:
    params = dict(params)
    params["timestamp"] = str(int(time.time()*1000))
    params["signature"] = _bingx_sign(params)
    url     = BINGX_BASE_URL + path
    headers = {"X-BX-APIKEY": BINGX_API_KEY}
    try:
        if method=="GET":
            r = requests.get(url, params=params, headers=headers, timeout=10)
        else:
            r = requests.post(url, json=params, headers=headers, timeout=10)
        return r.json()
    except Exception as e:
        write_log(f"BINGX_REQ_ERR | {path} | {e}")
        return {"code": -1, "msg": str(e)}

def _bingx_price(symbol: str) -> float:
    r = _bingx_req("GET", "/openApi/swap/v2/quote/price", {"symbol": f"{symbol}-USDT"})
    return float(r["data"]["price"])

def _bingx_set_lev(symbol: str, lev: int, pos_side: str):
    try:
        _bingx_req("POST", "/openApi/swap/v2/trade/leverage",
                   {"symbol": f"{symbol}-USDT","side": pos_side,"leverage": str(lev)})
    except Exception as e: write_log(f"BINGX_LEV_ERR | {symbol} | {e}")

def _bingx_market(symbol: str, side: str, pos_side: str, qty: float, ro: bool=False) -> dict:
    params = {"symbol": f"{symbol}-USDT","side": side,"positionSide": pos_side,
              "type": "MARKET","quantity": str(qty),
              "reduceOnly": "true" if ro else "false"}
    r = _bingx_req("POST", "/openApi/swap/v2/trade/order", params)
    write_log(f"BINGX_MARKET | {symbol} {side} {pos_side} qty={qty} ro={ro} | code={r.get('code')}")
    return r

def _bingx_stop(symbol: str, side: str, pos_side: str, qty: float,
                trigger: float, ro: bool=True) -> dict:
    params = {"symbol": f"{symbol}-USDT","side": side,"positionSide": pos_side,
              "type": "STOP_MARKET","quantity": str(qty),
              "stopPrice": str(trigger),"workingType": "MARK_PRICE",
              "reduceOnly": "true" if ro else "false"}
    r = _bingx_req("POST", "/openApi/swap/v2/trade/order", params)
    write_log(f"BINGX_STOP | {symbol} {side} trig={trigger} | code={r.get('code')}")
    return r

def _bingx_cancel(symbol: str):
    try: _bingx_req("DELETE","/openApi/swap/v2/trade/allOpenOrders",
                    {"symbol": f"{symbol}-USDT"})
    except Exception as e: write_log(f"BINGX_CANCEL_ERR | {symbol} | {e}")

def _bingx_move_sl(pos: dict, new_sl: float) -> str:
    sym      = pos["symbol"]
    opp      = pos["opp_side"]
    pos_side = pos.get("pos_side","LONG")
    rem      = pos.get("remaining_qty", pos["total_qty"])
    if rem <= 0: return ""
    _bingx_cancel(sym); time.sleep(0.3)
    try:
        r = _bingx_stop(sym, opp, pos_side, rem, new_sl)
        if r.get("code")==0:
            oid = str(r.get("data",{}).get("order",{}).get("orderId",""))
            write_log(f"BINGX_SL_MOVED | {sym} → {new_sl} | order={oid}")
            return oid
    except Exception as e: write_log(f"BINGX_SL_MOVE_ERR | {sym} | {e}")
    return ""

def bingx_balance_text() -> str:
    try:
        r  = _bingx_req("GET","/openApi/swap/v2/user/balance",{})
        bl = r["data"]["balance"]
        eq = float(bl.get("equity",0))
        av = float(bl.get("availableMargin",0))
        return f"BingX 🟡 LIVE  Equity: <b>{eq:.2f}</b>  Avail: <b>{av:.2f}</b> USDT"
    except Exception as e: return f"BingX: ❌ {e}"

# ══════════════════════════════════════════════════════════════════════════════
# ПАРСИНГ ЦЕНЫ
# ══════════════════════════════════════════════════════════════════════════════
def parse_price(text: str, *prefixes: str) -> float|None:
    for prefix in prefixes:
        idx = text.find(prefix)
        if idx == -1: continue
        m = re.search(r"\d+\.?\d*", text[idx+len(prefix):].strip())
        if m:
            try: return float(m.group())
            except: pass
    return None

# ══════════════════════════════════════════════════════════════════════════════
# СТАТИСТИКА
# ══════════════════════════════════════════════════════════════════════════════
def update_stats(is_win: bool):
    with _stats_lock:
        s = load_stats()
        s["total"]  = s.get("total",0)+1
        if is_win:  s["wins"]   = s.get("wins",0)+1
        else:       s["losses"] = s.get("losses",0)+1
        save_stats(s)

def save_trade_to_history(payload: dict, trade_data: dict|None,
                           result: str, tp_num: int, exchange: str=""):
    now = int(time.time())
    entry_time = trade_data.get("created_at",now) if trade_data else now
    record = {
        "trade_id":     payload.get("trade_id",""),
        "ticker":       normalize_ticker(payload.get("ticker","")),
        "direction":    normalize_direction(payload.get("direction",""),payload.get("event","")),
        "timeframe":    payload.get("timeframe",""),
        "exchange":     exchange,
        "is_strong":    payload.get("is_strong",False),
        "entry_time":   entry_time,
        "close_time":   now,
        "duration_sec": max(0,now-entry_time),
        "result":       result,
        "tp_num":       tp_num,
        "date_msk":     _msk().strftime("%Y-%m-%d"),
        "week_msk":     f"{_msk().year}-W{_msk().isocalendar()[1]:02d}",
    }
    with _history_lock:
        h = load_history(); h.append(record)
        if len(h) > 2000: h = h[-2000:]
        save_history(h)

def _build_report(trades: list, title: str) -> str:
    wins   = [r for r in trades if r["result"]=="win"]
    losses = [r for r in trades if r["result"]=="loss"]
    total  = len(trades)
    wr     = calc_winrate(len(wins),total)
    avg    = int(sum(r.get("duration_sec",0) for r in trades)/total) if total else 0
    tp_counts: dict = {}
    for r in wins:
        k = f"TP{r.get('tp_num',0)}"; tp_counts[k] = tp_counts.get(k,0)+1
    t  = f"{title}\n\n"
    t += f"📊 Win Rate: <b>{wr}%</b>\n"
    t += f"✅ TP: {len(wins)}   ❌ SL: {len(losses)}\n"
    t += f"📈 Всего закрыто: {total}\n"
    t += f"⏱ Ср. время: {fmt_duration(avg)}\n"
    if tp_counts:
        t += "\n<b>Разбивка по TP:</b>\n"
        for tp,cnt in sorted(tp_counts.items()):
            t += f"  {tp}: {cnt} ✅\n"
    return t

# ══════════════════════════════════════════════════════════════════════════════
# АКТИВНЫЕ СДЕЛКИ (для TG reply)
# ══════════════════════════════════════════════════════════════════════════════
def build_trade_key(payload: dict) -> str:
    tid = payload.get("trade_id")
    if tid and str(tid).strip() not in ("","null"): return str(tid).strip()
    ticker    = normalize_ticker(payload.get("ticker",""))
    direction = normalize_direction(payload.get("direction",""),payload.get("event",""))
    tf        = payload.get("timeframe","").strip()
    return f"{ticker}_{direction}_{tf}" if ticker else ""

def put_trade(key: str, data: dict):
    with _trades_lock:
        t = load_trades(); t[key] = data; save_trades(t)

def get_trade(key: str) -> dict|None:
    trades = load_trades()
    if key in trades: return trades[key]
    if key and "_" in key:
        parts = key.split("_")
        if len(parts) >= 2:
            tk,dr = parts[0],parts[1]
            for k,v in trades.items():
                if v.get("ticker","")==tk and v.get("direction","")==dr: return v
    return None

def remove_trade(key: str):
    with _trades_lock:
        t = load_trades()
        if key in t: del t[key]; save_trades(t)

def dedup_entry(ticker: str, direction: str):
    with _trades_lock:
        t = load_trades()
        rm = [k for k,v in t.items() if v.get("ticker","")==ticker and v.get("direction","")==direction]
        for k in rm: del t[k]
        if rm: save_trades(t); write_log(f"DEDUP | removed {len(rm)} for {ticker}_{direction}")

def cleanup_old_trades() -> int:
    with _trades_lock:
        t = load_trades(); cutoff = int(time.time())-7*86400
        rm = [k for k,v in t.items() if v.get("created_at",0)<cutoff]
        for k in rm: del t[k]
        if rm: save_trades(t)
    return len(rm)

# ══════════════════════════════════════════════════════════════════════════════
# РОУТЕР БИРЖ
# ══════════════════════════════════════════════════════════════════════════════
def _get_exchange(ticker: str) -> str:
    if ticker in BYBIT_PAIRS: return "bybit"
    if ticker in BINGX_PAIRS: return "bingx"
    return ""

# ══════════════════════════════════════════════════════════════════════════════
# ОБРАБОТЧИК ENTRY
# ══════════════════════════════════════════════════════════════════════════════
def handle_entry(payload: dict):
    ticker    = normalize_ticker(payload.get("ticker",""))
    direction = normalize_direction(payload.get("direction",""), payload.get("event",""))
    text      = payload.get("text","").strip()
    trade_id  = str(payload.get("trade_id") or "")

    if not ticker:
        write_log("ENTRY_ERR | empty ticker"); return
    if not direction:
        write_log(f"ENTRY_ERR | unknown direction | ticker={ticker}"); return

    exchange = _get_exchange(ticker)
    write_log(f"ENTRY | {ticker} {direction} | exchange={exchange or 'tg-only'}")

    # TG уведомление
    dedup_entry(ticker, direction)
    key  = build_trade_key(payload)
    sent = send_signals(text or f"📥 {'LONG' if direction=='BUY' else 'SHORT'} #{ticker}")
    if key:
        put_trade(key, {
            "message_id": (sent or {}).get("message_id"),
            "chat_id":    TG_CHAT,
            "thread_id":  int(TG_SIGNALS_TOPIC),
            "event":      "entry",
            "ticker":     ticker,
            "direction":  direction,
            "exchange":   exchange,
            "timeframe":  payload.get("timeframe",""),
            "is_strong":  payload.get("is_strong",False),
            "created_at": int(time.time()),
        })

    if not exchange:
        write_log(f"ENTRY_SKIP | {ticker} not in BYBIT_PAIRS or BINGX_PAIRS"); return

    cfg       = pair_cfg(ticker)
    leverage  = int(cfg["leverage"])
    size_usdt = float(cfg["size_usdt"])

    # Парсим SL/TP из текста И из прямых полей JSON
    def _extract(text_keys, json_key):
        v = parse_price(text, *text_keys)
        if v is None: v = _to_f(payload.get(json_key))
        return v

    sl_price  = _extract(("SL:","⛔ SL:","sl:"),  "sl")
    tp1_price = _extract(("TP1:","✅ TP1:"),       "tp1")
    tp2_price = _extract(("TP2:","✅ TP2:"),       "tp2")
    tp3_price = _extract(("TP3:","✅ TP3:"),       "tp3")
    tp4_price = _extract(("TP4:","✅ TP4:"),       "tp4")

    if exchange == "bybit":
        _entry_bybit(ticker, direction, leverage, size_usdt,
                     sl_price, tp1_price, tp2_price, tp3_price, tp4_price, trade_id)
    elif exchange == "bingx":
        _entry_bingx(ticker, direction, leverage, size_usdt,
                     sl_price, tp1_price, tp2_price, tp3_price, tp4_price, trade_id)


def _save_pos(pkey: str, data: dict):
    with _pos_lock:
        positions = load_positions()
        positions[pkey] = data
        save_positions(positions)

def _entry_bybit(ticker, direction, lev, size_usdt,
                 sl, tp1, tp2, tp3, tp4, trade_id):
    if not BYBIT_SDK or not BYBIT_API_KEY:
        write_log(f"BYBIT_SKIP | {ticker} | API not set"); return
    side     = "Buy"  if direction=="BUY" else "Sell"
    opp_side = "Sell" if side=="Buy"      else "Buy"
    _bybit_set_lev(ticker, lev)
    try:
        price = _bybit_price(ticker)
    except Exception as e:
        send_signals(f"❌ Bybit price error {ticker}: {e}"); return
    qty = _bybit_calc_qty(ticker, size_usdt, lev, price)
    write_log(f"BYBIT_ENTRY | {ticker} {direction} price={price} qty={qty} lev={lev}x")
    try:
        r = _bybit_market(ticker, side, qty)
        if r["retCode"] != 0:
            raise Exception(f"{r['retCode']}: {r.get('retMsg','')}")
    except Exception as e:
        send_signals(f"❌ <b>Bybit ошибка входа {ticker}</b>\n<code>{e}</code>"); return
    time.sleep(0.5)
    sl_oid = ""
    if sl:
        try:
            r2 = _bybit_stop(ticker, opp_side, qty, sl)
            if r2["retCode"]==0: sl_oid = r2["result"].get("orderId","")
        except Exception as e: write_log(f"BYBIT_SL_ERR | {ticker} | {e}")
    _save_pos(f"{ticker}_{direction}", {
        "exchange":"bybit","symbol":ticker,"direction":direction,
        "side":side,"opp_side":opp_side,
        "entry_price":price,"total_qty":qty,"remaining_qty":qty,
        "leverage":lev,"sl_price":sl,"sl_order_id":sl_oid,
        "tp1_price":tp1,"tp2_price":tp2,"tp3_price":tp3,"tp4_price":tp4,
        "tp1_hit":False,"tp2_hit":False,"tp3_hit":False,
        "tp4_hit":False,"tp5_hit":False,"tp6_hit":False,
        "trade_id":trade_id,"created_at":int(time.time()),
        "trail_active":False,"trail_sl":None,
    })
    mode  = "🧪 TESTNET" if BYBIT_TESTNET else "🔴 LIVE"
    arrow = "🟢" if direction=="BUY" else "🔴"
    send_signals(
        f"{arrow} <b>{'LONG' if direction=='BUY' else 'SHORT'} ОТКРЫТ</b>  Bybit {mode}\n"
        f"#{ticker}  |  {lev}x  |  {size_usdt} USDT\n"
        f"📍 Вход: <b>{price}</b>  |  ⛔ SL: {sl or '—'}\n"
        f"✅ TP1: {tp1 or '—'}  |  TP2: {tp2 or '—'}\n"
        f"📦 Объём: {qty} контр."
    )


def _entry_bingx(ticker, direction, lev, size_usdt,
                 sl, tp1, tp2, tp3, tp4, trade_id):
    if not BINGX_API_KEY or not BINGX_API_SECRET:
        write_log(f"BINGX_SKIP | {ticker} | API not set"); return
    side     = "BUY"  if direction=="BUY" else "SELL"
    opp_side = "SELL" if side=="BUY"      else "BUY"
    pos_side = "LONG" if direction=="BUY"  else "SHORT"
    _bingx_set_lev(ticker, lev, pos_side)
    try:
        price = _bingx_price(ticker)
    except Exception as e:
        send_signals(f"❌ BingX price error {ticker}: {e}"); return
    qty = round((size_usdt*lev)/price, 4)
    write_log(f"BINGX_ENTRY | {ticker} {direction} price={price} qty={qty} lev={lev}x")
    try:
        r = _bingx_market(ticker, side, pos_side, qty)
        if r.get("code") != 0:
            raise Exception(f"code={r.get('code')} {r.get('msg','')}")
    except Exception as e:
        send_signals(f"❌ <b>BingX ошибка входа {ticker}</b>\n<code>{e}</code>"); return
    time.sleep(0.5)
    sl_oid = ""
    if sl:
        try:
            r2 = _bingx_stop(ticker, opp_side, pos_side, qty, sl)
            if r2.get("code")==0:
                sl_oid = str(r2.get("data",{}).get("order",{}).get("orderId",""))
        except Exception as e: write_log(f"BINGX_SL_ERR | {ticker} | {e}")
    _save_pos(f"{ticker}_{direction}", {
        "exchange":"bingx","symbol":ticker,"direction":direction,
        "side":side,"opp_side":opp_side,"pos_side":pos_side,
        "entry_price":price,"total_qty":qty,"remaining_qty":qty,
        "leverage":lev,"sl_price":sl,"sl_order_id":sl_oid,
        "tp1_price":tp1,"tp2_price":tp2,"tp3_price":tp3,"tp4_price":tp4,
        "tp1_hit":False,"tp2_hit":False,"tp3_hit":False,
        "tp4_hit":False,"tp5_hit":False,"tp6_hit":False,
        "trade_id":trade_id,"created_at":int(time.time()),
        "trail_active":False,"trail_sl":None,
    })
    arrow = "🟢" if direction=="BUY" else "🔴"
    send_signals(
        f"{arrow} <b>{'LONG' if direction=='BUY' else 'SHORT'} ОТКРЫТ</b>  BingX 🟡 LIVE\n"
        f"#{ticker}  |  {lev}x  |  {size_usdt} USDT\n"
        f"📍 Вход: <b>{price}</b>  |  ⛔ SL: {sl or '—'}\n"
        f"✅ TP1: {tp1 or '—'}  |  TP2: {tp2 or '—'}\n"
        f"📦 Объём: {qty} контр."
    )

# ══════════════════════════════════════════════════════════════════════════════
# TP / SL
# ══════════════════════════════════════════════════════════════════════════════
def handle_tp_hit(payload: dict):
    ticker    = normalize_ticker(payload.get("ticker",""))
    direction = normalize_direction(payload.get("direction",""),payload.get("event",""))
    tp_num    = int(payload.get("tp_num") or 0)
    text      = payload.get("text","").strip()
    key       = build_trade_key(payload)
    trade     = get_trade(key) if key else None
    reply_id  = (trade or {}).get("message_id")
    send_signals(text or f"✅ TP{tp_num} #{ticker}", reply_to=reply_id)

    # FIX: ALL TP (не только >=3) = win
    if tp_num >= 1:
        update_stats(True)
        exch = (trade or {}).get("exchange", _get_exchange(ticker))
        save_trade_to_history(payload, trade, "win", tp_num, exch)
        if tp_num >= 3 and key:
            remove_trade(key)

    pkey = f"{ticker}_{direction}"
    with _pos_lock:
        positions = load_positions()
        pos = positions.get(pkey)
        if not pos or pos.get(f"tp{tp_num}_hit"):
            return
        exch      = pos.get("exchange","")
        total_qty = pos["total_qty"]; remaining = pos["remaining_qty"]
        opp_side  = pos["opp_side"]
        close_qty = round(total_qty * TP_CLOSE_PCT.get(tp_num,0.0), 8)
        close_qty = min(close_qty, remaining)
        if close_qty <= 0: return
        try:
            if exch == "bybit":
                if close_qty < _bybit_mq(ticker): return
                r = _bybit_market(ticker, opp_side, close_qty, ro=True)
                if r["retCode"] != 0: raise Exception(f"{r['retCode']}: {r.get('retMsg','')}")
            elif exch == "bingx":
                ps = pos.get("pos_side","LONG")
                r  = _bingx_market(ticker, opp_side, ps, close_qty, ro=True)
                if r.get("code") != 0: raise Exception(f"code={r.get('code')} {r.get('msg','')}")
        except Exception as e:
            write_log(f"TP_HIT_ERR | {ticker} TP{tp_num} | {e}"); return
        new_rem = max(0.0, remaining-close_qty)
        pos[f"tp{tp_num}_hit"] = True
        pos["remaining_qty"]   = new_rem
        if tp_num==2 and pos.get("tp1_price"):
            new_sl = pos["tp1_price"]
            oid = _bybit_move_sl(pos,new_sl) if exch=="bybit" else _bingx_move_sl(pos,new_sl)
            pos["sl_price"]=new_sl; pos["sl_order_id"]=oid
        if tp_num>=3 and not pos.get("trail_active"):
            pos["trail_active"]=True; pos["trail_sl"]=pos.get("sl_price")
        if new_rem <= 0 or tp_num >= 6:
            if exch=="bybit": _bybit_cancel(ticker)
            elif exch=="bingx": _bingx_cancel(ticker)
            del positions[pkey]
        else:
            positions[pkey] = pos
        save_positions(positions)


def handle_sl_hit(payload: dict):
    ticker    = normalize_ticker(payload.get("ticker",""))
    direction = normalize_direction(payload.get("direction",""),payload.get("event",""))
    text      = payload.get("text","").strip()
    key       = build_trade_key(payload)
    trade     = get_trade(key) if key else None
    send_signals(text or f"🛑 SL #{ticker}", reply_to=(trade or {}).get("message_id"))
    update_stats(False)
    exch = (trade or {}).get("exchange", _get_exchange(ticker))
    save_trade_to_history(payload, trade, "loss", 0, exch)
    if key: remove_trade(key)
    pkey = f"{ticker}_{direction}"
    with _pos_lock:
        positions = load_positions()
        pos = positions.pop(pkey, None)
        if pos:
            e = pos.get("exchange","")
            if e=="bybit": _bybit_cancel(ticker)
            elif e=="bingx": _bingx_cancel(ticker)
        save_positions(positions)


def handle_sl_moved(payload: dict):
    ticker    = normalize_ticker(payload.get("ticker",""))
    direction = normalize_direction(payload.get("direction",""),payload.get("event",""))
    text      = payload.get("text","").strip()
    key       = build_trade_key(payload)
    trade     = get_trade(key) if key else None
    send_signals(text or f"🔒 SL передвинут #{ticker}", reply_to=(trade or {}).get("message_id"))
    new_sl = _to_f(parse_price(text,"✅ Стало:","Стало:","SL:") or payload.get("sl"))
    if not new_sl: return
    pkey = f"{ticker}_{direction}"
    with _pos_lock:
        positions = load_positions()
        pos = positions.get(pkey)
        if not pos: return
        e   = pos.get("exchange","")
        oid = _bybit_move_sl(pos,new_sl) if e=="bybit" else \
              _bingx_move_sl(pos,new_sl) if e=="bingx" else ""
        pos["sl_price"]=new_sl; pos["sl_order_id"]=oid
        positions[pkey]=pos; save_positions(positions)


def process_signal(payload: dict):
    if _emergency_stop.is_set():
        write_log("EMERGENCY_STOP — signal dropped"); return
    event  = payload.get("event","unknown").strip().lower()
    if event == "enty": event = "entry"
    ticker = normalize_ticker(payload.get("ticker",""))
    write_log(f"PROCESS | event={event} | ticker={ticker}")

    if event in ("entry","smart_entry","limit_hit"):
        handle_entry(payload)
    elif event in ("long","short","buy","sell"):
        # TradingView может слать event вместо direction
        p2 = dict(payload); p2.setdefault("direction",event); p2["event"]="entry"
        handle_entry(p2)
    elif event == "limit_order":
        send_signals(payload.get("text","") or f"📋 Лимитный ордер #{ticker}")
    elif event == "tp_hit":
        handle_tp_hit(payload)
    elif event == "sl_hit":
        handle_sl_hit(payload)
    elif event == "sl_moved":
        handle_sl_moved(payload)
    elif event == "scale_in":
        key = build_trade_key(payload); trade = get_trade(key) if key else None
        send_signals(payload.get("text","") or f"📈 Scale in #{ticker}",
                     reply_to=(trade or {}).get("message_id"))
    else:
        send_signals(payload.get("text","") or json.dumps(payload,ensure_ascii=False)[:500])


def enqueue_signal(payload: dict):
    with _queue_lock:
        _signal_queue.append({"payload":payload,"attempts":0})

def _queue_worker():
    write_log("QUEUE_WORKER | start")
    while True:
        with _queue_lock:
            item = _signal_queue.pop(0) if _signal_queue else None
        if item:
            try:
                process_signal(item["payload"])
            except Exception as e:
                write_log(f"QUEUE_ERR | attempt={item['attempts']} | {e}")
                if item["attempts"] < MAX_QUEUE_ATTEMPTS-1:
                    item["attempts"] += 1
                    time.sleep(QUEUE_RETRY_DELAY)
                    with _queue_lock: _signal_queue.insert(0,item)
                else:
                    write_log(f"QUEUE_DEAD | {json.dumps(item['payload'])[:200]}")
        else:
            time.sleep(1)

threading.Thread(target=_queue_worker, daemon=True, name="queue_worker").start()

# ══════════════════════════════════════════════════════════════════════════════
# TRAILING STOP
# ══════════════════════════════════════════════════════════════════════════════
def _position_manager():
    write_log("POSITION_MANAGER | start")
    while True:
        try:
            with _pos_lock:
                positions = load_positions()
            for pkey,pos in list(positions.items()):
                if not pos.get("trail_active"): continue
                ticker = pos["symbol"]; exch = pos.get("exchange","")
                cur_sl = pos.get("trail_sl") or pos.get("sl_price") or 0
                try:
                    price = _bybit_price(ticker) if exch=="bybit" else _bingx_price(ticker)
                except: continue
                new_trail = None
                if pos["direction"]=="BUY":
                    c = price*(1-TRAIL_PCT)
                    if c > cur_sl: new_trail = c
                else:
                    c = price*(1+TRAIL_PCT)
                    if cur_sl==0 or c < cur_sl: new_trail = c
                if new_trail:
                    oid = _bybit_move_sl(pos,new_trail) if exch=="bybit" else \
                          _bingx_move_sl(pos,new_trail) if exch=="bingx" else ""
                    with _pos_lock:
                        p2 = load_positions()
                        if pkey in p2:
                            p2[pkey]["trail_sl"]=new_trail
                            p2[pkey]["sl_price"]=new_trail
                            p2[pkey]["sl_order_id"]=oid
                            save_positions(p2)
        except Exception as e: write_log(f"POS_MGR_ERR | {e}")
        time.sleep(30)

threading.Thread(target=_position_manager, daemon=True, name="pos_mgr").start()

# ══════════════════════════════════════════════════════════════════════════════
# FEAR & GREED
# ══════════════════════════════════════════════════════════════════════════════
FEAR_GREED_URL = "https://api.alternative.me/fng/?limit=1"
_FG = [(0,24,"Extreme Fear"),(25,44,"Fear"),(45,55,"Neutral"),(56,74,"Greed"),(75,100,"Extreme Greed")]

def _fg_emoji(v):
    if v<=24: return "😱"
    if v<=44: return "😨"
    if v<=55: return "😐"
    if v<=74: return "🤑"
    return "🚀"

def _fg_label(v):
    for lo,hi,lb in _FG:
        if lo<=v<=hi: return lb
    return "Unknown"

def _fg_bar(v,n=20):
    f = round(v/100*n); return "█"*f+"░"*(n-f)

def _fetch_fg():
    try:
        e = requests.get(FEAR_GREED_URL,timeout=8).json()["data"][0]
        v = int(e["value"])
        return {"value":v,"label":_fg_label(v)}
    except Exception as e:
        write_log(f"FG_FETCH_ERR | {e}"); return None

def _build_fg_message(fg: dict, trigger: str="scheduled") -> str:
    v=fg["value"]; label=fg["label"]; bar=_fg_bar(v); msk=_msk().strftime("%d.%m.%Y %H:%M")
    hdr = "🔔 <b>Fear & Greed — смена зоны!</b>" if trigger=="change" else "📊 <b>Fear & Greed Index</b>"
    t   = f"{hdr}\n\n{_fg_emoji(v)}  <b>{v} / 100</b>  —  {label}\n<code>[{bar}]</code>\n\n"
    if v<=24:   t+="🔻 Рынок в панике. Часто — точка разворота вверх.\n"
    elif v<=44: t+="📉 Преобладает страх. Возможны покупки на откатах.\n"
    elif v<=55: t+="➡️ Рынок нейтрален. Ждём определённости.\n"
    elif v<=74: t+="📈 Жадность растёт. Следим за перекупленностью.\n"
    else:       t+="🔺 Эйфория. Высокий риск разворота вниз.\n"
    return t+f"\n🕐 Обновлено: {msk} МСК"

def _check_fg(force: bool=False) -> bool:
    fg = _fetch_fg()
    if not fg: return False
    state   = load_json(FG_STATE_FILE,{})
    trigger = "change" if (state.get("label") and state["label"]!=fg["label"]) else "scheduled"
    if not force and trigger!="change": return False
    send_fg(_build_fg_message(fg,trigger))
    save_json(FG_STATE_FILE,{"value":fg["value"],"label":fg["label"],
                              "sent_at":int(time.time()),
                              "sent_at_msk":_msk().strftime("%Y-%m-%d %H:%M")})
    return True

# ══════════════════════════════════════════════════════════════════════════════
# СЕССИИ
# ══════════════════════════════════════════════════════════════════════════════
SESSIONS = [
    {"name":"🇦🇺 Австралия",   "open":"02:00","close":"09:00"},
    {"name":"🇯🇵 Азия (Токио)","open":"03:00","close":"09:00"},
    {"name":"🇬🇧 Европа",      "open":"10:00","close":"18:30"},
    {"name":"🇺🇸 Америка",     "open":"16:30","close":"23:00"},
]

def _sessions_status() -> str:
    msk = _msk(); cur = msk.hour*60+msk.minute
    lines = [f"🕐 <b>Торговые сессии</b> ({msk.strftime('%H:%M')} МСК)\n"]
    for s in SESSIONS:
        oh,om = map(int,s["open"].split(":")); ch,cm = map(int,s["close"].split(":"))
        o_min=oh*60+om; c_min=ch*60+cm
        is_open = (cur>=o_min or cur<c_min) if c_min<o_min else (o_min<=cur<c_min)
        lines.append(f"{s['name']}  {s['open']}–{s['close']}  {'🟢 Открыта' if is_open else '🔴 Закрыта'}")
    return "\n".join(lines)

# ══════════════════════════════════════════════════════════════════════════════
# ПЛАНИРОВЩИК
# ══════════════════════════════════════════════════════════════════════════════
def _scheduler():
    write_log("SCHEDULER | start")
    while True:
        try:
            msk = _msk(); cur=msk.hour*60+msk.minute; h,m=msk.hour,msk.minute; today=msk.strftime("%Y-%m-%d")
            if h in FG_SCHEDULED_HOURS and 0<=m<=1:
                k=f"fg_{today}_{h:02d}"
                if not _was_sent(k): _mark_sent(k); _check_fg(force=True)
            if h==23 and 55<=m<=59:
                k=f"daily_{today}"
                if not _was_sent(k):
                    _mark_sent(k)
                    t=[r for r in load_history() if r.get("date_msk")==today]
                    if t: send_signals(_build_report(t,f"📅 <b>Дневной отчёт {today}</b>"))
            if msk.weekday()==6 and h==23 and 55<=m<=59:
                wk=f"{msk.year}-W{msk.isocalendar()[1]:02d}"; k=f"weekly_{wk}"
                if not _was_sent(k):
                    _mark_sent(k)
                    t=[r for r in load_history() if r.get("week_msk")==wk]
                    if t: send_signals(_build_report(t,f"📅 <b>Недельный отчёт {wk}</b>"))
            if msk.day==calendar.monthrange(msk.year,msk.month)[1] and h==23 and m==59:
                mo=f"{msk.year}-{msk.month:02d}"; k=f"monthly_{mo}"
                if not _was_sent(k):
                    _mark_sent(k)
                    t=[r for r in load_history() if r.get("date_msk","").startswith(mo)]
                    if t: send_signals(_build_report(t,f"📅 <b>Месячный отчёт {mo}</b>"))
            for s in SESSIONS:
                oh,om=map(int,s["open"].split(":")); ch,cm=map(int,s["close"].split(":"))
                o_min=oh*60+om; c_min=ch*60+cm
                if 14<=o_min-cur<=16:
                    k=f"sess_open_warn_{today}_{s['open']}"
                    if not _was_sent(k): _mark_sent(k); send_sessions(f"⏰ {s['name']} открывается через 15 мин ({s['open']} МСК)")
                if -1<=o_min-cur<=1:
                    k=f"sess_open_{today}_{s['open']}"
                    if not _was_sent(k): _mark_sent(k); send_sessions(f"🟢 {s['name']} открылась! ({s['open']} МСК)")
                if 14<=c_min-cur<=16:
                    k=f"sess_close_warn_{today}_{s['close']}"
                    if not _was_sent(k): _mark_sent(k); send_sessions(f"⏰ {s['name']} закрывается через 15 мин ({s['close']} МСК)")
                is_mid=(ch==0 and cm==0); at_close=(-1<=cur<=1) if is_mid else (-1<=c_min-cur<=1)
                if at_close:
                    kd=(msk-datetime.timedelta(days=1)).strftime("%Y-%m-%d") if is_mid else today
                    k=f"sess_close_{kd}_{s['close']}"
                    if not _was_sent(k): _mark_sent(k); send_sessions(f"🔴 {s['name']} закрылась! ({s['close']} МСК)")
        except Exception as e: write_log(f"SCHEDULER_ERR | {e}")
        time.sleep(60)

threading.Thread(target=_scheduler, daemon=True, name="scheduler").start()

def _keepalive():
    time.sleep(60)
    while True:
        if RENDER_URL:
            try:
                r = requests.get(f"{RENDER_URL}/health", timeout=10)
                write_log(f"KEEPALIVE | {r.status_code}")
            except Exception as e: write_log(f"KEEPALIVE_ERR | {e}")
        time.sleep(600)

threading.Thread(target=_keepalive, daemon=True, name="keepalive").start()

# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM КОМАНДЫ
# ══════════════════════════════════════════════════════════════════════════════
if bot:
    def _reply(m, text: str):
        kw: dict = {"parse_mode":"HTML","disable_web_page_preview":True}
        if getattr(m,"message_thread_id",None): kw["message_thread_id"]=m.message_thread_id
        try: bot.send_message(m.chat.id, text, **kw)
        except Exception as e: write_log(f"REPLY_ERR | {e}")

    @bot.message_handler(commands=["start"])
    def cmd_start(m): _reply(m,"🚀 <b>Statham Bot v2.0</b>\n\nBybit + BingX + TradingView\n\n👉 /help")

    @bot.message_handler(commands=["help"])
    def cmd_help(m):
        _reply(m,(
            "📋 <b>Команды v2.0</b>\n\n"
            "<b>📊 Статистика:</b>\n/stats /daily_report /weekly_report /monthly_report\n"
            "/stats_by_ticker YBUSDT  /leaders  /active\n\n"
            "<b>🧭 Рынок:</b>\n/fear_greed  /sessions  /market\n\n"
            "<b>🔧 Admin:</b>\n/balance  /close_all  /test_bybit\n"
            "/emergency_stop  /resume\n/reset_stats  /cleanup  /logs"
        ))

    @bot.message_handler(commands=["stats"])
    def cmd_stats(m):
        s=load_stats(); w=s.get("wins",0); l=s.get("losses",0); t=s.get("total",0)
        _reply(m,f"📊 <b>Статистика</b>\n\nWin Rate: <b>{calc_winrate(w,t)}%</b>\n✅ TP: {w}   ❌ SL: {l}\n📈 Всего: {t}")

    @bot.message_handler(commands=["daily_report"])
    def cmd_daily(m):
        today=_msk().strftime("%Y-%m-%d"); t=[r for r in load_history() if r.get("date_msk")==today]
        _reply(m,_build_report(t,f"📅 <b>Дневной {today}</b>") if t else f"📅 Сегодня ({today}) нет сделок.")

    @bot.message_handler(commands=["weekly_report"])
    def cmd_weekly(m):
        msk=_msk(); wk=f"{msk.year}-W{msk.isocalendar()[1]:02d}"
        t=[r for r in load_history() if r.get("week_msk")==wk]
        _reply(m,_build_report(t,f"📅 <b>Неделя {wk}</b>") if t else f"📅 Нет сделок за {wk}.")

    @bot.message_handler(commands=["monthly_report"])
    def cmd_monthly(m):
        msk=_msk(); mo=f"{msk.year}-{msk.month:02d}"
        t=[r for r in load_history() if r.get("date_msk","").startswith(mo)]
        _reply(m,_build_report(t,f"📅 <b>Месяц {mo}</b>") if t else f"📅 Нет сделок за {mo}.")

    @bot.message_handler(commands=["stats_by_ticker"])
    def cmd_tickers(m):
        parts=m.text.split()
        if len(parts)<2: _reply(m,"❌ /stats_by_ticker YBUSDT"); return
        tk=normalize_ticker(parts[1])
        t=[r for r in load_history() if normalize_ticker(r.get("ticker",""))==tk]
        _reply(m,_build_report(t,f"📊 <b>{tk}</b>") if t else f"Нет данных по {tk}")

    @bot.message_handler(commands=["leaders"])
    def cmd_leaders(m):
        h=load_history()
        if not h: _reply(m,"Нет данных."); return
        sc: dict={}
        for r in h:
            tk=normalize_ticker(r.get("ticker",""))
            if tk: sc[tk]=sc.get(tk,0)+(1 if r["result"]=="win" else -1)
        top=sorted(sc.items(),key=lambda x:x[1],reverse=True)[:10]
        _reply(m,"🏆 <b>Лидеры</b>\n\n"+"\n".join(f"{i}. {tk}  {'+' if s>=0 else ''}{s}" for i,(tk,s) in enumerate(top,1)))

    @bot.message_handler(commands=["active","trades"])
    def cmd_active(m):
        t=load_trades()
        if not t: _reply(m,"📭 Нет активных сделок."); return
        lines=[f"📋 <b>Активных: {len(t)}</b>\n"]
        for key,v in t.items():
            arrow="🟢" if v.get("direction")=="BUY" else "🔴"
            exch=v.get("exchange","?")
            dur=fmt_duration(int(time.time())-v.get("created_at",int(time.time())))
            lines.append(f"{arrow} {v.get('ticker',key)} [{exch}] {v.get('direction','')} ⏱{dur}")
        _reply(m,"\n".join(lines))

    @bot.message_handler(commands=["fear_greed"])
    def cmd_fg(m):
        fg=_fetch_fg()
        _reply(m,_build_fg_message(fg,"command") if fg else "❌ Не удалось получить F&G.")

    @bot.message_handler(commands=["sessions"])
    def cmd_sessions(m): _reply(m,_sessions_status())

    @bot.message_handler(commands=["market"])
    def cmd_market(m):
        fg=_fetch_fg()
        fg_line=f"{_fg_emoji(fg['value'])} F&G: {fg['value']} — {fg['label']}\n" if fg else ""
        _reply(m,f"🌐 <b>Рынок</b>\n\n{fg_line}{_sessions_status()}")

    @bot.message_handler(commands=["balance"])
    def cmd_balance(m):
        if not is_admin_user(m.from_user.id): _reply(m,"❌ Только admin"); return
        lines=["💰 <b>Балансы</b>\n"]
        if BYBIT_API_KEY: lines.append(bybit_balance_text())
        if BINGX_API_KEY: lines.append(bingx_balance_text())
        if not lines[1:]: lines.append("Биржи не настроены.")
        _reply(m,"\n".join(lines))

    @bot.message_handler(commands=["test_bybit"])
    def cmd_test(m):
        if not is_admin_user(m.from_user.id): _reply(m,"❌ Только admin"); return
        lines=[]
        if BYBIT_API_KEY: lines.append(bybit_balance_text())
        if BINGX_API_KEY: lines.append(bingx_balance_text())
        _reply(m,"\n".join(lines) or "❌ Биржи не настроены")

    @bot.message_handler(commands=["close_all"])
    def cmd_close_all(m):
        if not is_admin_user(m.from_user.id): _reply(m,"❌ Только admin"); return
        closed=_do_close_all()
        _reply(m,f"🚨 Закрыто: {', '.join(closed) or 'нет позиций'}")

    @bot.message_handler(commands=["emergency_stop"])
    def cmd_emergency(m):
        if not is_admin_user(m.from_user.id): _reply(m,"❌ Только admin"); return
        _emergency_stop.set()
        _reply(m,"🛑 <b>Emergency Stop!</b> Сигналы заблокированы.\n/resume — снять")

    @bot.message_handler(commands=["resume"])
    def cmd_resume(m):
        if not is_admin_user(m.from_user.id): _reply(m,"❌ Только admin"); return
        _emergency_stop.clear(); _reply(m,"✅ Блокировка снята.")

    @bot.message_handler(commands=["reset_stats"])
    def cmd_reset(m):
        if not is_admin_user(m.from_user.id): _reply(m,"❌ Только admin"); return
        save_stats({"wins":0,"losses":0,"total":0}); save_history([])
        _reply(m,"✅ Статистика сброшена.")

    @bot.message_handler(commands=["cleanup"])
    def cmd_cleanup(m):
        if not is_admin_user(m.from_user.id): _reply(m,"❌ Только admin"); return
        _reply(m,f"🧹 Удалено {cleanup_old_trades()} зависших сделок.")

    @bot.message_handler(commands=["clean"])
    def cmd_clean(m):
        if not is_admin_user(m.from_user.id): _reply(m,"❌ Только admin"); return
        save_trades({}); save_positions({}); _reply(m,"🧹 Очищено.")

    @bot.message_handler(commands=["logs"])
    def cmd_logs(m):
        if not is_admin_user(m.from_user.id): _reply(m,"❌ Только admin"); return
        try:
            with open(LOG_FILE,"r",encoding="utf-8") as f: lines=f.readlines()
            _reply(m,f"<pre>{''.join(lines[-30:])[:3500]}</pre>")
        except Exception as e: _reply(m,f"❌ {e}")

# ══════════════════════════════════════════════════════════════════════════════
# CLOSE ALL
# ══════════════════════════════════════════════════════════════════════════════
def _do_close_all() -> list[str]:
    with _pos_lock:
        positions = load_positions()
    closed = []
    for pkey,pos in positions.items():
        ticker=pos["symbol"]; rem=pos.get("remaining_qty",0); exch=pos.get("exchange","")
        if rem > 0:
            try:
                if exch=="bybit":
                    _bybit_cancel(ticker)
                    _bybit_market(ticker, pos["opp_side"], rem, ro=True)
                elif exch=="bingx":
                    _bingx_cancel(ticker)
                    _bingx_market(ticker, pos["opp_side"], pos.get("pos_side","LONG"), rem, ro=True)
                closed.append(f"{ticker}[{exch}]")
            except Exception as e: write_log(f"CLOSE_ALL_ERR | {ticker} | {e}")
    save_positions({}); save_trades({})
    send_signals(f"🚨 <b>АВАРИЙНОЕ ЗАКРЫТИЕ</b>\nЗакрыто: {', '.join(closed) or 'нет позиций'}")
    return closed

# ══════════════════════════════════════════════════════════════════════════════
# WEBHOOK РЕГИСТРАЦИЯ
# ══════════════════════════════════════════════════════════════════════════════
def register_webhook() -> bool:
    if not TG_TOKEN or not RENDER_URL:
        write_log("WEBHOOK_SETUP | TG_TOKEN или RENDER_URL не заданы"); return False
    wh_url = f"{RENDER_URL.rstrip('/')}/webhook/telegram"
    try:
        r = requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/setWebhook",
                          json={"url":wh_url,"drop_pending_updates":False,
                                "allowed_updates":["message","edited_message","callback_query"]},
                          timeout=10)
        result = r.json()
        write_log(f"WEBHOOK_SETUP | url={wh_url} | result={result}")
        return result.get("ok",False)
    except Exception as e:
        write_log(f"WEBHOOK_SETUP_ERR | {e}"); return False

threading.Thread(target=lambda:(time.sleep(5),register_webhook()),
                 daemon=True, name="webhook_reg").start()

# ══════════════════════════════════════════════════════════════════════════════
# FLASK ROUTES
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/health")
def health():
    return jsonify({
        "status":"ok",
        "bybit":bool(BYBIT_API_KEY),"bybit_testnet":BYBIT_TESTNET,
        "bingx":bool(BINGX_API_KEY),
        "bybit_pairs":sorted(BYBIT_PAIRS),"bingx_pairs":sorted(BINGX_PAIRS),
        "active_trades":len(load_trades()),"positions":len(load_positions()),
        "emergency":_emergency_stop.is_set(),"time_msk":_msk().strftime("%Y-%m-%d %H:%M"),
    })

@app.route("/webhook/telegram", methods=["POST"])
def tg_webhook():
    if not bot: return "no bot",400
    try:
        update = telebot.types.Update.de_json(request.get_data().decode("utf-8"))
        bot.process_new_updates([update])
    except Exception as e: write_log(f"TG_WEBHOOK_ERR | {e}")
    return "!",200

@app.route("/webhook/bybit", methods=["POST"])
def bybit_webhook():
    raw = request.get_data(as_text=True).strip()
    if not raw: return jsonify({"status":"ok","event":"ping"}),200
    try:
        payload = request.get_json(force=True,silent=True) or json.loads(raw)
    except Exception as e:
        write_log(f"WEBHOOK_PARSE_ERR | {e}"); return jsonify({"error":"invalid json"}),400
    if not isinstance(payload,dict):
        return jsonify({"error":"expected object"}),400
    secret_in = (str(payload.get("secret","") or payload.get("secret_key",""))
                 or request.headers.get("X-Secret",""))
    if WEBHOOK_SECRET and secret_in != WEBHOOK_SECRET:
        write_log(f"WEBHOOK_FORBIDDEN | got={secret_in!r}"); return jsonify({"error":"forbidden"}),403
    ticker = normalize_ticker(payload.get("ticker",""))
    event  = payload.get("event","?")
    write_log(f"WEBHOOK | event={event} ticker={ticker}")
    enqueue_signal(payload)
    return jsonify({"status":"queued","event":event,"ticker":ticker}),200

# Обратная совместимость
@app.route("/webhook/<path:secret>", methods=["POST"])
def bybit_webhook_compat(secret: str):
    if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
        return jsonify({"error":"forbidden"}),403
    raw = request.get_data(as_text=True).strip()
    if not raw: return jsonify({"status":"ping"}),200
    try:
        payload = request.get_json(force=True,silent=True) or json.loads(raw)
    except: return jsonify({"error":"invalid json"}),400
    enqueue_signal(payload)
    return jsonify({"status":"queued"}),200

@app.route("/setup")
def setup():
    return jsonify({"webhook_registered": register_webhook()})

@app.route("/debug")
def debug():
    try:
        with open(LOG_FILE,"r",encoding="utf-8") as f: lines=f.readlines()
        return "<pre>"+"".join(lines[-100:])+"</pre>",200,{"Content-Type":"text/html"}
    except Exception as e:
        return f"<pre>Error: {e}</pre>",200,{"Content-Type":"text/html"}

@app.route("/trades")
def trades_route(): return jsonify(load_trades())

@app.route("/stats")
def stats_route():
    s=load_stats(); w=s.get("wins",0); l=s.get("losses",0); t=s.get("total",0)
    return jsonify({"wins":w,"losses":l,"total":t,"winrate":calc_winrate(w,t)})

@app.route("/history")
def history_route():
    h=load_history(); return jsonify({"records":len(h),"last_50":h[-50:]})

@app.route("/positions")
def positions_route():
    if not _http_auth(request): return jsonify({"error":"forbidden"}),403
    return jsonify(load_positions())

@app.route("/close_all", methods=["POST"])
def close_all_route():
    if not _http_auth(request): return jsonify({"error":"forbidden"}),403
    return jsonify({"closed":_do_close_all()})

@app.route("/test_bybit")
def test_bybit_route():
    if not _http_auth(request): return jsonify({"error":"forbidden"}),403
    result={}
    if BYBIT_API_KEY: result["bybit"]=bybit_balance_text()
    if BINGX_API_KEY: result["bingx"]=bingx_balance_text()
    if not result: result["error"]="No exchange configured"
    return jsonify(result)

if __name__=="__main__":
    port=int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0",port=port,debug=False)
