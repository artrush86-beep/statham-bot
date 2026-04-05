"""
Statham Moderation Bot — PythonAnywhere (v2.0)
===============================================
ТОЛЬКО модерация чата. Торговля, F&G, сессии — перенесены на Render.

Функции:
  • Антимат + система предупреждений (3 → мут 1 мин)
  • Приветствие новых участников с фото
  • Запись истории пользователей (никнейм, дата входа)
  • /ban /unban /mute /unmute /warn /warns /users_stats /start /help

ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ (задать в wsgi.py):
  BOT_TOKEN    — токен бота-модератора (ОТДЕЛЬНЫЙ от Render-бота!)
  CHAT_ID      — ID чата (-1003867089540)
  ADMIN_IDS    — через запятую: 123456,789012
  PA_DOMAIN    — artrush86.pythonanywhere.com

ПУТЬ: /home/artrush86/mysite/app.py
"""
from __future__ import annotations
import json, os, time, threading, datetime, random
from flask import Flask, request
import requests
import telebot

app = Flask(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ══════════════════════════════════════════════════════════════════════════════
TOKEN     = os.environ.get("BOT_TOKEN",  "")
CHAT_ID   = os.environ.get("CHAT_ID",   "")
PA_DOMAIN = os.environ.get("PA_DOMAIN", "artrush86.pythonanywhere.com")

ADMIN_IDS: set[int] = set(
    int(x.strip()) for x in os.environ.get("ADMIN_IDS", "").split(",")
    if x.strip().lstrip("-").isdigit()
)

BASE_DIR    = "/home/artrush86/mysite"
WARNS_FILE  = os.path.join(BASE_DIR, "warns.json")
USERS_FILE  = os.path.join(BASE_DIR, "users.json")
LOG_FILE    = os.path.join(BASE_DIR, "mod_log.txt")
PHOTO_PATH  = os.path.join(BASE_DIR, "helloboys.png")

bot = telebot.TeleBot(TOKEN, threaded=False)

# ══════════════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ══════════════════════════════════════════════════════════════════════════════
_log_lock = threading.Lock()

def write_log(entry: str):
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    try:
        with _log_lock:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"[{ts}] {entry}\n")
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
            if len(lines) > 500:
                with open(LOG_FILE, "w", encoding="utf-8") as f:
                    f.writelines(lines[-500:])
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
    except Exception:
        return default

def save_json(path: str, data):
    try:
        with _file_lock:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        write_log(f"SAVE_ERR | {path} | {e}")

def load_warns() -> dict: return load_json(WARNS_FILE, {})
def save_warns(d):        save_json(WARNS_FILE, d)
def load_users() -> dict: return load_json(USERS_FILE, {})
def save_users(d):        save_json(USERS_FILE, d)

# ══════════════════════════════════════════════════════════════════════════════
# ХЕЛПЕРЫ
# ══════════════════════════════════════════════════════════════════════════════
def is_admin(chat_id, user_id: int) -> bool:
    """Проверяет ADMIN_IDS и реальный список администраторов чата."""
    if ADMIN_IDS and user_id in ADMIN_IDS:
        return True
    try:
        admins = bot.get_chat_administrators(chat_id)
        return user_id in [a.user.id for a in admins]
    except Exception:
        return False

def _reply(m, text: str):
    kw: dict = {"parse_mode": "HTML"}
    if getattr(m, "message_thread_id", None):
        kw["message_thread_id"] = m.message_thread_id
    try:
        bot.send_message(m.chat.id, text, **kw)
    except Exception as e:
        write_log(f"REPLY_ERR | {e}")

def record_user(user) -> None:
    """Запоминаем пользователя: user_id → {username, имя, дата}."""
    if not user or getattr(user, "is_bot", False):
        return
    uid    = str(user.id)
    now_ts = int(time.time())
    now_dt = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    users  = load_users()
    if uid not in users:
        users[uid] = {
            "user_id":        user.id,
            "first_seen":     now_ts,
            "first_seen_dt":  now_dt,
            "usernames":      [],
        }
    entry = users[uid]
    uname = getattr(user, "username", "") or ""
    if uname and uname not in entry.get("usernames", []):
        entry.setdefault("usernames", []).append(uname)
    entry["first_name"]   = getattr(user, "first_name", "") or ""
    entry["last_seen"]    = now_ts
    entry["last_seen_dt"] = now_dt
    users[uid] = entry
    save_users(users)

def _mute(chat_id, user_id: int, minutes: int):
    perms = telebot.types.ChatPermissions(
        can_send_messages=False, can_send_media_messages=False,
        can_send_other_messages=False, can_add_web_page_previews=False,
    )
    bot.restrict_chat_member(chat_id, user_id,
                             until_date=int(time.time() + minutes * 60),
                             permissions=perms)

def _unmute(chat_id, user_id: int):
    perms = telebot.types.ChatPermissions(
        can_send_messages=True, can_send_media_messages=True,
        can_send_other_messages=True, can_add_web_page_previews=True,
    )
    bot.restrict_chat_member(chat_id, user_id, permissions=perms)

# ══════════════════════════════════════════════════════════════════════════════
# ФИЛЬТРЫ
# ══════════════════════════════════════════════════════════════════════════════
BAD_WORDS = [
    "бля","блять","блядь","хуй","хуйня","охуел","похуй","пизда","пиздец",
    "ебать","ебаный","заебал","мудак","гондон","шлюха","сука","тварь","мразь",
    "урод","дебил","чмо","пидор","гнида","жопа","залупа","падла","хуйло",
    "пиздобол","хуесос","ублюдок",
]

GREETINGS = [
    "привет","здравствуй","здравствуйте","добрый","хай",
    "hello","hi","hey","салам","ку","йо",
]

REPLIES = [
    "Салют, {name}! 🚀",
    "Привет, {name}! 👋",
    "Йоу, {name}! 🤙",
    "Здорово, {name}! 💪",
]

# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM КОМАНДЫ
# ══════════════════════════════════════════════════════════════════════════════
@bot.message_handler(commands=["start"])
def cmd_start(m):
    _reply(m, (
        "👮 <b>Statham Moderation Bot</b>\n\n"
        "Слежу за порядком в чате.\n\n"
        "/help — список команд"
    ))

@bot.message_handler(commands=["help"])
def cmd_help(m):
    _reply(m, (
        "👮 <b>Команды модерации</b>\n\n"
        "<b>Для всех:</b>\n"
        "/start — приветствие\n"
        "/help — эта справка\n\n"
        "<b>Для Admin (ответом на сообщение пользователя):</b>\n"
        "/ban — забанить навсегда\n"
        "/unban — разбанить\n"
        "/mute [мин] — замутить (по умолч. 60 мин)\n"
        "/unmute — размутить\n"
        "/warn — выдать предупреждение (3 = мут 1 мин)\n"
        "/warns — текущие предупреждения\n"
        "/users_stats — статистика участников"
    ))

@bot.message_handler(commands=["ban"])
def cmd_ban(m):
    if not is_admin(m.chat.id, m.from_user.id):
        _reply(m, "❌ Только для администраторов."); return
    if not m.reply_to_message:
        _reply(m, "❌ Ответьте командой /ban на сообщение пользователя."); return
    target = m.reply_to_message.from_user
    if is_admin(m.chat.id, target.id):
        _reply(m, "❌ Нельзя забанить администратора."); return
    try:
        bot.ban_chat_member(m.chat.id, target.id)
        write_log(f"BAN | {target.id} @{target.username} | by {m.from_user.id}")
        _reply(m, f"🚫 <b>{target.first_name}</b> забанен.")
    except Exception as e:
        write_log(f"BAN_ERR | {e}"); _reply(m, f"❌ {e}")

@bot.message_handler(commands=["unban"])
def cmd_unban(m):
    if not is_admin(m.chat.id, m.from_user.id):
        _reply(m, "❌ Только для администраторов."); return
    if not m.reply_to_message:
        _reply(m, "❌ Ответьте командой /unban на сообщение пользователя."); return
    target = m.reply_to_message.from_user
    try:
        bot.unban_chat_member(m.chat.id, target.id, only_if_banned=True)
        write_log(f"UNBAN | {target.id} @{target.username} | by {m.from_user.id}")
        _reply(m, f"✅ <b>{target.first_name}</b> разбанен.")
    except Exception as e:
        write_log(f"UNBAN_ERR | {e}"); _reply(m, f"❌ {e}")

@bot.message_handler(commands=["mute"])
def cmd_mute(m):
    if not is_admin(m.chat.id, m.from_user.id):
        _reply(m, "❌ Только для администраторов."); return
    if not m.reply_to_message:
        _reply(m, "❌ Ответьте /mute [минуты] на сообщение пользователя."); return
    target = m.reply_to_message.from_user
    if is_admin(m.chat.id, target.id):
        _reply(m, "❌ Нельзя замутить администратора."); return
    args = m.text.split()
    mins = 60
    if len(args) > 1:
        try: mins = max(1, int(args[1]))
        except ValueError: pass
    try:
        _mute(m.chat.id, target.id, mins)
        write_log(f"MUTE | {target.id} @{target.username} | {mins}мин | by {m.from_user.id}")
        _reply(m, f"🔇 <b>{target.first_name}</b> замучен на {mins} мин.")
    except Exception as e:
        write_log(f"MUTE_ERR | {e}"); _reply(m, f"❌ {e}")

@bot.message_handler(commands=["unmute"])
def cmd_unmute(m):
    if not is_admin(m.chat.id, m.from_user.id):
        _reply(m, "❌ Только для администраторов."); return
    if not m.reply_to_message:
        _reply(m, "❌ Ответьте командой /unmute на сообщение пользователя."); return
    target = m.reply_to_message.from_user
    try:
        _unmute(m.chat.id, target.id)
        write_log(f"UNMUTE | {target.id} @{target.username} | by {m.from_user.id}")
        _reply(m, f"🔊 <b>{target.first_name}</b> размучен.")
    except Exception as e:
        write_log(f"UNMUTE_ERR | {e}"); _reply(m, f"❌ {e}")

@bot.message_handler(commands=["warn"])
def cmd_warn(m):
    if not is_admin(m.chat.id, m.from_user.id):
        _reply(m, "❌ Только для администраторов."); return
    if not m.reply_to_message:
        _reply(m, "❌ Ответьте командой /warn на сообщение пользователя."); return
    target = m.reply_to_message.from_user
    if is_admin(m.chat.id, target.id):
        _reply(m, "❌ Нельзя выдать предупреждение администратору."); return
    uid   = str(target.id)
    warns = load_warns()
    warns[uid] = warns.get(uid, 0) + 1
    cnt   = warns[uid]
    save_warns(warns)
    write_log(f"WARN | {target.id} | cnt={cnt} | by {m.from_user.id}")
    if cnt >= 3:
        try:
            _mute(m.chat.id, target.id, 1)
            warns[uid] = 0; save_warns(warns)
            _reply(m, f"🔇 <b>{target.first_name}</b> замучен на 1 мин (3/3 предупреждений).")
        except Exception as e:
            _reply(m, f"⚠️ Нет прав на мут: {e}")
    else:
        _reply(m, f"⚠️ <b>{target.first_name}</b>, предупреждение {cnt}/3.")

@bot.message_handler(commands=["warns"])
def cmd_warns(m):
    if not is_admin(m.chat.id, m.from_user.id):
        _reply(m, "❌ Только для администраторов."); return
    warns = load_warns()
    # Если ответили на сообщение — показываем предупреждения конкретного пользователя
    if m.reply_to_message:
        uid = str(m.reply_to_message.from_user.id)
        cnt = warns.get(uid, 0)
        _reply(m, f"⚠️ {m.reply_to_message.from_user.first_name}: {cnt}/3 предупреждений.")
        return
    active = {k: v for k, v in warns.items() if v > 0}
    if not active:
        _reply(m, "✅ Активных предупреждений нет."); return
    lines = ["⚠️ <b>Активные предупреждения:</b>\n"]
    for uid, cnt in sorted(active.items(), key=lambda x: -x[1]):
        lines.append(f"• ID {uid}: {cnt}/3")
    _reply(m, "\n".join(lines))

@bot.message_handler(commands=["users_stats"])
def cmd_users_stats(m):
    if not is_admin(m.chat.id, m.from_user.id):
        _reply(m, "❌ Только для администраторов."); return
    users  = load_users()
    total  = len(users)
    recent = sorted(users.values(), key=lambda u: u.get("first_seen", 0), reverse=True)[:10]
    lines  = [f"👥 <b>Участников в базе: {total}</b>\n\n<b>Последние 10 вошедших:</b>"]
    for u in recent:
        unames = "@" + ", @".join(u.get("usernames", [])) if u.get("usernames") else "—"
        lines.append(f"• <b>{u.get('first_name','?')}</b>  {unames}  ({u.get('first_seen_dt','')})")
    _reply(m, "\n".join(lines))

# ══════════════════════════════════════════════════════════════════════════════
# НОВЫЕ УЧАСТНИКИ
# ══════════════════════════════════════════════════════════════════════════════
@bot.message_handler(content_types=["new_chat_members"])
def welcome(message):
    for user in message.new_chat_members:
        if getattr(user, "is_bot", False):
            continue
        record_user(user)
        write_log(f"JOIN | {user.id} @{getattr(user,'username','')} {user.first_name}")
        text = f"👋 Привет, <b>{user.first_name}</b>! Добро пожаловать в <b>Statham Elite</b> 🚀"
        try:
            if os.path.exists(PHOTO_PATH):
                with open(PHOTO_PATH, "rb") as p:
                    bot.send_photo(message.chat.id, p, caption=text, parse_mode="HTML")
            else:
                bot.send_message(message.chat.id, text, parse_mode="HTML")
        except Exception as e:
            write_log(f"WELCOME_ERR | {e}")

@bot.message_handler(content_types=["left_chat_member"])
def farewell(message):
    user = message.left_chat_member
    if getattr(user, "is_bot", False): return
    write_log(f"LEFT | {user.id} @{getattr(user,'username','')} {user.first_name}")

# ══════════════════════════════════════════════════════════════════════════════
# МОДЕРАЦИЯ СООБЩЕНИЙ
# ══════════════════════════════════════════════════════════════════════════════
@bot.message_handler(func=lambda m: True, content_types=["text"])
def handle_message(message):
    # Игнорируем старые сообщения
    if time.time() - message.date > 60:
        return

    user = message.from_user
    record_user(user)

    # Администраторы не модерируются
    if is_admin(message.chat.id, user.id):
        t = message.text.lower()
        if any(w in t for w in GREETINGS):
            bot.reply_to(message, random.choice(REPLIES).format(name=user.first_name))
        return

    t   = message.text.lower()
    uid = str(user.id)

    # Проверка мата
    for word in BAD_WORDS:
        if word in t:
            try: bot.delete_message(message.chat.id, message.message_id)
            except Exception: pass
            warns = load_warns()
            warns[uid] = warns.get(uid, 0) + 1
            cnt = warns[uid]
            write_log(f"BADWORD | {user.id} @{getattr(user,'username','')} | word={word} | warn={cnt}")
            if cnt >= 3:
                try:
                    _mute(message.chat.id, user.id, 1)
                    bot.send_message(message.chat.id,
                        f"🔇 <b>{user.first_name}</b> замучен на 1 мин (3/3 предупреждений).",
                        parse_mode="HTML")
                    warns[uid] = 0
                except Exception as e:
                    write_log(f"MUTE_AUTO_ERR | {e}")
                    bot.send_message(message.chat.id, f"⚠️ Нет прав на мут: {e}")
            else:
                bot.send_message(message.chat.id,
                    f"⚠️ <b>{user.first_name}</b>, мат запрещён! Предупреждение {cnt}/3.",
                    parse_mode="HTML")
            save_warns(warns)
            return

    # Приветствие
    if any(w in t for w in GREETINGS):
        bot.reply_to(message, random.choice(REPLIES).format(name=user.first_name))

# ══════════════════════════════════════════════════════════════════════════════
# WEBHOOK
# ══════════════════════════════════════════════════════════════════════════════
_webhook_ok = False

def _do_register_webhook():
    global _webhook_ok
    if not TOKEN or not PA_DOMAIN:
        write_log("WEBHOOK_SKIP | TOKEN или PA_DOMAIN не заданы"); return
    try:
        wh_url = f"https://{PA_DOMAIN}/{TOKEN}"
        r = requests.post(
            f"https://api.telegram.org/bot{TOKEN}/setWebhook",
            json={"url": wh_url, "drop_pending_updates": False,
                  "allowed_updates": ["message","callback_query"]},
            timeout=10,
        )
        result = r.json()
        write_log(f"WEBHOOK_SETUP | url={wh_url} | result={result}")
        _webhook_ok = result.get("ok", False)
    except Exception as e:
        write_log(f"WEBHOOK_SETUP_ERR | {e}")

threading.Thread(target=lambda: (time.sleep(5), _do_register_webhook()),
                 daemon=True).start()


@app.route("/" + (TOKEN or "TOKEN_NOT_SET"), methods=["POST"])
def tg_webhook():
    try:
        update = telebot.types.Update.de_json(request.get_data().decode("utf-8"))
        bot.process_new_updates([update])
    except Exception as e:
        write_log(f"TG_WEBHOOK_ERR | {e}")
    return "!", 200


@app.route("/setup")
def setup():
    _do_register_webhook()
    return f"Webhook {'OK ✅' if _webhook_ok else 'FAIL ❌'}"


@app.route("/debug")
def debug():
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        return "<pre>" + "".join(lines[-50:]) + "</pre>", 200, {"Content-Type": "text/html"}
    except Exception as e:
        return f"<pre>Error: {e}</pre>", 200, {"Content-Type": "text/html"}


@app.route("/health")
def health():
    return "OK", 200


# WSGI entrypoint для PythonAnywhere
application = app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
