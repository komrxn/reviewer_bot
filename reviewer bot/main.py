import requests
import asyncio
from aiogram import Bot, types
import html
import time

TG_TOKEN = "8134806060:AAGzdzqWBL8lzhceYmsdEuPX-yimZvqfsx4"
TG_CHAT_IDS = ["2040216796", "238088316"]

BOT_IPS = {
    "http://149.102.137.52:8017": "Agreement handler",
    "http://149.102.137.52:8018": "Specialist bot",
    "http://149.102.137.52:8019": "Long term goal",
    "http://149.102.137.52:8020": "M1 kitchen",
    "http://149.102.137.52:8021": "M2 kitchen",
    "http://149.102.137.52:8022": "Team QA"
}
ENDPOINTS = [f"{ip}/redis-alt.json" for ip in BOT_IPS]

MAX_LEN = 3500
FINISH_TIMEOUT = 60 * 10  # 30 минут разрыв

chat_sessions = {}  # (base_url, chat_id) -> list of sessions [{messages, start, end, sent}]
time_started = int(time.time())

def get_user_id_from_chat(chat_id):
    if chat_id.endswith("app"):
        try:
            return int(chat_id[:-3])
        except:
            return None
    return None

async def probe_user_info(user_id, chat_id, messages, bot):
    username = None
    first_name = None
    last_name = None
    # Пробуем найти в HUMAN-сообщениях
    for m in messages:
        data = m.get('data', {})
        if isinstance(data, dict):
            if 'username' in data and data['username']:
                username = '@' + str(data['username']).lstrip('@')
            if 'user' in data and isinstance(data['user'], dict):
                if 'username' in data['user'] and data['user']['username']:
                    username = '@' + str(data['user']['username']).lstrip('@')
                if 'first_name' in data['user']:
                    first_name = data['user']['first_name']
                if 'last_name' in data['user']:
                    last_name = data['user']['last_name']
            if 'first_name' in data:
                first_name = data['first_name']
            if 'last_name' in data:
                last_name = data['last_name']
        if username:
            break
    # Если не нашли — пробуем Telegram API
    if not username:
        try:
            user = await bot.get_chat(user_id)
            if user.username:
                username = '@' + user.username
            if user.first_name:
                first_name = user.first_name
            if user.last_name:
                last_name = user.last_name
        except Exception:
            pass
    if not username:
        username = f"ID:{user_id}"
    name = (first_name or "") + (" " + last_name if last_name else "")
    name = name.strip()
    return username, name

async def monitor():
    bot = Bot(token=TG_TOKEN)
    print("Мониторинг только новых диалогов после запуска...")

    while True:
        now = int(time.time())
        for url in ENDPOINTS:
            base_url = url.replace('/redis-alt.json', '')
            bot_name = BOT_IPS[base_url]
            try:
                resp = requests.get(url, timeout=4)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                chats = {}
                for key, msg in data.items():
                    sid = msg.get('session_id', 'unknown')
                    chats.setdefault(sid, []).append(msg)
                for chat_id, msgs in chats.items():
                    uniq_id = (base_url, chat_id)
                    # Сортируем сообщения по времени
                    msgs_sorted = sorted(msgs, key=lambda m: m['timestamp'])
                    # Пересобираем все сессии каждый раз с нуля
                    sessions = []
                    session = []
                    prev_ts = None
                    for m in msgs_sorted:
                        if not session:
                            session = [m]
                        else:
                            if m['timestamp'] - prev_ts >= FINISH_TIMEOUT:
                                sessions.append({
                                    "messages": session,
                                    "start": session[0]['timestamp'],
                                    "end": session[-1]['timestamp'],
                                    "sent": False
                                })
                                session = [m]
                            else:
                                session.append(m)
                        prev_ts = m['timestamp']
                    if session:
                        sessions.append({
                            "messages": session,
                            "start": session[0]['timestamp'],
                            "end": session[-1]['timestamp'],
                            "sent": False
                        })

                    # Фикс: помечаем старые сессии (до запуска) sent=True
                    for s in sessions:
                        if s["start"] < time_started:
                            s["sent"] = True

                    # Переносим статус sent из предыдущей итерации
                    prev_sessions = {(s["start"], s["end"]): s["sent"] for s in chat_sessions.get(uniq_id, [])}
                    for s in sessions:
                        s["sent"] = prev_sessions.get((s["start"], s["end"]), s["sent"])

                    chat_sessions[uniq_id] = sessions

                    # Только сессии, которые начались ПОСЛЕ time_started и не были отправлены
                    for s in chat_sessions[uniq_id]:
                        if s["start"] < time_started or s["sent"]:
                            continue
                        # Считаем завершённой только если прошло timeout c конца сессии
                        if now - s["end"] >= FINISH_TIMEOUT:
                            user_id = get_user_id_from_chat(chat_id)
                            username, name = await probe_user_info(user_id, chat_id, s["messages"], bot)
                            text = (
                                f"<b>🆕 Диалог из бота: {html.escape(bot_name)}</b>\n"
                                f"<b>Пользователь:</b> <code>{username} {name}</code>\n"
                                f"<b>chat_id:</b> <code>{chat_id}</code>\n"
                            )
                            for m in s["messages"]:
                                role = html.escape(m['data']['type'])
                                ts_human = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(m['timestamp']))
                                content = html.escape(m['data'].get('content', ''))
                                text += f"\n<b>{role.upper()}</b> | <code>{ts_human}</code>\n<pre>{content}</pre>\n"
                            for i in range(0, len(text), MAX_LEN):
                                for tg_id in TG_CHAT_IDS:
                                    await bot.send_message(
                                        tg_id,
                                        text[i:i+MAX_LEN],
                                        parse_mode='HTML'
                                    )
                            s["sent"] = True
            except Exception as e:
                print(f"FAIL {url}: {e}")
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(monitor())
