#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VK Photo Parser - Headless версия для сервера
Работает без GUI, настройки из settings.json
Включает:
- Парсинг и постинг фото из VK источников
- Антиспам с кикам спамеров
- Пересылка заказов на irina_mod
- Таймер работы 07:00-23:00 МСК
"""

import requests
import time
import datetime
import json
import re
import threading
import os
import sys
import traceback
from pytz import timezone

# ================== КОНСТАНТЫ ==================
SETTINGS_FILE = "settings.json"
SENT_IDS_FILE = "sent_post_ids"
SENT_PHOTOS_FILE = "sent_photo_ids"
VK_API_VERSION = "5.131"
MOSCOW_TZ = timezone('Europe/Moscow')

# Стоп-слова для фильтрации
ANTIWORDS = [
    "стиральный порошок", "стиральные порошки", "порошок", "порошки",
    "мыло", "жидкое мыло", "шампунь", "шампуни", "одеяла", "одеяло",
    "подушка", "подушки", "падушки", "падушка",
    "конфета", "конфеты", "сладость", "сладости", "гель", "спрей мужской",
    "салфетки", "влажные салфетки", "лак", "для стирки", "зубная паста",
    "отбеливатель", "дезодорант", "утенок", "туалет"
]

BASE_STOPWORDS = [
    "предзаказ", "предзаказы", "фирма", "бренд", "производитель",
    "качество", "сток", "европа", "германия", "польша", "турция",
    "модель", "новинка", "новая коллекция", "сезон",
    "оригинал", "реплика", "копия", "аналог"
]

# Администраторы (whitelist)
ADMIN_WHITELIST = [
    "1055595410",      # @id1055595410
    "trendova_arina",  # https://vk.com/trendova_arina
    "115693485",       # https://vk.com/id115693485
    "irina_mod"        # https://vk.com/irina_mod
]

# ID получателя заказов (irina_mod)
ORDER_RECEIVER_ID = None  # Будет заполнено при старте

_admin_id_cache = {}
stop_event = threading.Event()

# ================== ЛОГИРОВАНИЕ ==================
def log(msg):
    """Вывод сообщения с временной меткой"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")
    sys.stdout.flush()

# ================== НАСТРОЙКИ ==================
def load_settings():
    """Загрузка настроек из JSON"""
    if not os.path.exists(SETTINGS_FILE):
        log(f"❌ Файл {SETTINGS_FILE} не найден!")
        return None

    try:
        with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
            settings = json.load(f)
        log(f"✅ Настройки загружены из {SETTINGS_FILE}")
        return settings
    except Exception as e:
        log(f"❌ Ошибка чтения настроек: {e}")
        return None

# ================== VK API ==================
def vk_api_call(method, token, params, timeout=15):
    """Вызов VK API метода"""
    params = params.copy()
    params['access_token'] = token
    params['v'] = VK_API_VERSION

    url = f"https://api.vk.com/method/{method}"

    try:
        response = requests.post(url, data=params, timeout=timeout)
        data = response.json()

        if 'error' in data:
            error_msg = data['error'].get('error_msg', 'Unknown error')
            error_code = data['error'].get('error_code', 0)
            log(f"❌ VK API error {error_code}: {error_msg}")
            return None

        return data.get('response')
    except Exception as e:
        log(f"❌ Ошибка VK API вызова {method}: {e}")
        return None

def resolve_admin_ids(vk_token):
    """Преобразует screen_names из ADMIN_WHITELIST в числовые ID"""
    global _admin_id_cache, ORDER_RECEIVER_ID

    numeric_ids = []
    screen_names = []

    for admin in ADMIN_WHITELIST:
        if str(admin).isdigit():
            numeric_ids.append(int(admin))
        else:
            screen_names.append(admin)

    # Кэшируем числовые ID
    for num_id in numeric_ids:
        _admin_id_cache[str(num_id)] = num_id

    # Конвертируем screen_names через VK API
    if screen_names:
        try:
            user_ids_param = ",".join(screen_names)
            response = vk_api_call("users.get", vk_token, {"user_ids": user_ids_param})

            if response:
                for user in response:
                    user_id = user.get("id")
                    screen_name = user.get("domain", "")
                    if user_id:
                        _admin_id_cache[screen_name] = user_id
                        log(f"  ✅ {screen_name} → {user_id}")

                        # Если это irina_mod - сохраняем для пересылки заказов
                        if screen_name == "irina_mod":
                            ORDER_RECEIVER_ID = user_id
        except Exception as e:
            log(f"❌ Ошибка конвертации screen_names: {e}")

    log(f"✅ Админов в whitelist: {len(_admin_id_cache)}")
    return _admin_id_cache

def is_admin(user_id):
    """Проверяет является ли пользователь администратором"""
    if user_id in _admin_id_cache.values() or str(user_id) in ADMIN_WHITELIST:
        return True
    return False

# ================== АНТИСПАМ ==================
def count_emojis(text):
    """Подсчитывает количество эмодзи"""
    if not text:
        return 0
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE
    )
    return len(emoji_pattern.findall(text))

def has_links(text):
    """Проверяет наличие ссылок"""
    if not text:
        return False
    if re.search(r'\b(?:(?:https?|ftp)://|www\.)\S+', text, re.IGNORECASE):
        return True
    if re.search(r'\b(?:[a-z0-9-]{1,63}\.)+(?:[a-z]{2,63})', text, re.IGNORECASE):
        return True
    return False

def has_phone(text):
    """Проверяет наличие номеров телефонов"""
    if not text:
        return False
    phone_pattern = r'(?:\+?\d[\d\s\-\(\)]{7,}\d)|(?:\d{3}[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2})'
    return bool(re.search(phone_pattern, text))

def is_mostly_caps(text):
    """Проверяет CAPS (>70% заглавных)"""
    if not text:
        return False
    letters = [c for c in text if c.isalpha()]
    if len(letters) < 10:
        return False
    caps_count = sum(1 for c in letters if c.isupper())
    return (caps_count / len(letters)) > 0.7

def is_gibberish(text):
    """Проверяет бессмысленный набор символов"""
    if not text or len(text) < 10:
        return False
    # Слишком много согласных подряд
    if re.search(r'[bcdfghjklmnpqrstvwxyzбвгджзклмнпрстфхцчшщ]{7,}', text, re.IGNORECASE):
        return True
    # Повторяющиеся символы
    if re.search(r'(.)\1{5,}', text):
        return True
    return False

def check_spam_patterns(text, antiwords=None):
    """Проверяет текст на спам-паттерны"""
    if not text:
        return False, "", {}

    details = {
        'has_links': has_links(text),
        'has_phone': has_phone(text),
        'is_caps': is_mostly_caps(text),
        'emoji_count': count_emojis(text),
        'is_gibberish': is_gibberish(text),
        'has_antiwords': False
    }

    # Проверка запрещенных слов
    if antiwords:
        text_lower = text.lower()
        for word in antiwords:
            if word.lower() in text_lower:
                details['has_antiwords'] = True
                break

    return False, "", details

def log_spam_to_file(user_id, text, reason, details, log_file="spam_log.txt"):
    """Логирование спама в файл"""
    try:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        date = datetime.datetime.now().strftime("%Y-%m-%d")
        time_only = datetime.datetime.now().strftime("%H:%M:%S")

        # Детальный лог
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] SPAM DETECTED\n")
            f.write(f"User ID: {user_id}\n")
            f.write(f"Reason: {reason}\n")
            f.write(f"Text: {text}\n")
            f.write(f"Details: {details}\n")
            f.write("=" * 50 + "\n")

        # Краткая статистика
        short_text = text[:60].replace('\n', ' ') if text else "[нет текста]"
        with open("spam_stats.txt", 'a', encoding='utf-8') as f:
            f.write(f"{date} | {time_only} | ID:{user_id} | {reason} | {short_text}\n")

        log(f"📝 Спам залогирован: spam_stats.txt")
    except Exception as e:
        log(f"❌ Ошибка логирования спама: {e}")

def send_spam_alert_telegram(tg_token, tg_chat_id, user_id, reason, text):
    """Отправка уведомления о спаме в Telegram"""
    if not tg_token or not tg_chat_id:
        return False

    try:
        import html
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        vk_profile_link = f"https://vk.com/id{user_id}"
        short_text = html.escape(text[:100]) if text else "[нет текста]"
        short_text = short_text.replace('\n', ' ')

        message = (
            f"🚨 СПАМЕР ОБНАРУЖЕН И КИКНУТ\n\n"
            f"⏰ Время: {timestamp}\n"
            f"👤 User ID: {user_id}\n"
            f"🔗 Профиль: {vk_profile_link}\n"
            f"❗ Причина: {reason}\n\n"
            f"💬 Текст:\n{short_text}"
        )

        url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
        data = {
            "chat_id": tg_chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        response = requests.post(url, data=data, timeout=10)

        if response.ok and response.json().get("ok"):
            log(f"📱 Уведомление отправлено в Telegram")
            return True
        else:
            log(f"⚠️ Ошибка отправки в Telegram: {response.text[:100]}")
            return False
    except Exception as e:
        log(f"❌ Ошибка отправки уведомления в Telegram: {e}")
        return False

def vk_kick_user(vk_token, vk_chat_id, user_id, reason=""):
    """Кик пользователя из чата"""
    try:
        response = vk_api_call(
            "messages.removeChatUser",
            vk_token,
            {
                "chat_id": vk_chat_id,
                "user_id": user_id
            }
        )

        if response is not None:
            log(f"✅ Пользователь {user_id} кикнут. Причина: {reason}")
            return True
        else:
            log(f"❌ Не удалось кикнуть user_id={user_id}")
            return False
    except Exception as e:
        log(f"❌ Ошибка кика пользователя {user_id}: {e}")
        return False

# ================== ПЕРЕСЫЛКА ЗАКАЗОВ ==================
def send_order_notification(vk_token, order_from_id, order_text, reply_message_id=None, chat_id=None):
    """Отправка уведомления о заказе на irina_mod"""
    global ORDER_RECEIVER_ID

    if not ORDER_RECEIVER_ID:
        log("⚠️ ORDER_RECEIVER_ID не установлен, пропуск уведомления о заказе")
        return False

    try:
        # Получаем информацию о пользователе
        user_info = vk_api_call("users.get", vk_token, {"user_ids": order_from_id})

        user_name = "Неизвестный"
        if user_info and len(user_info) > 0:
            first_name = user_info[0].get("first_name", "")
            last_name = user_info[0].get("last_name", "")
            user_name = f"{first_name} {last_name}".strip()

        vk_profile_link = f"https://vk.com/id{order_from_id}"

        # Формируем сообщение
        message = (
            f"🛒 НОВЫЙ ЗАКАЗ\n\n"
            f"👤 От: {user_name}\n"
            f"🔗 Профиль: {vk_profile_link}\n"
            f"💬 Текст: {order_text}\n"
        )

        # Если есть reply - добавляем информацию
        if reply_message_id:
            message += f"\n📎 Ответ на сообщение ID: {reply_message_id}"

        # Отправляем личное сообщение
        params = {
            "user_id": ORDER_RECEIVER_ID,
            "message": message,
            "random_id": int(time.time() * 1000)
        }

        response = vk_api_call("messages.send", vk_token, params)

        if response:
            log(f"✅ Заказ отправлен на irina_mod от {user_name}")
            return True
        else:
            log(f"❌ Не удалось отправить заказ на irina_mod")
            return False
    except Exception as e:
        log(f"❌ Ошибка отправки заказа: {e}")
        return False

# ================== АНТИСПАМ WORKER ==================
def vk_antispam_worker(vk_token, vk_peer_id, vk_chat_id, stop_event_obj,
                       window_sec=300, poll_sec=1,
                       tg_token=None, tg_chat_id=None, notify_telegram=True):
    """Worker для антиспама и пересылки заказов"""

    log(f"🛡️ Антиспам worker запущен")
    log(f"   Окно: {window_sec} сек")
    log(f"   Пересылка заказов на irina_mod: ✅")

    # Получение Long Poll сервера
    def get_longpoll_server():
        try:
            response = vk_api_call(
                "messages.getLongPollServer",
                vk_token,
                {"need_pts": 1, "lp_version": 3}
            )

            if response:
                return response.get("server"), response.get("key"), response.get("ts")
            return None, None, None
        except Exception as e:
            log(f"❌ Ошибка получения Long Poll сервера: {e}")
            return None, None, None

    server, key, ts = get_longpoll_server()

    if not server or not key or not ts:
        log("❌ Не удалось подключиться к Long Poll. Антиспам не запущен.")
        return

    log(f"✅ Long Poll подключен: {server}")

    # Загрузка админов
    log(f"🔑 Загрузка whitelist администраторов...")
    admin_ids = resolve_admin_ids(vk_token)
    log(f"✅ Админов в whitelist: {len(admin_ids)}")

    join_ts = {}  # user_id → timestamp входа

    log(f"👂 Антиспам: слушаю чат {vk_peer_id}, окно {window_sec} сек")

    # Основной цикл Long Poll
    while not stop_event_obj.is_set():
        try:
            params = {
                "act": "a_check",
                "key": key,
                "ts": ts,
                "wait": 25,
                "mode": 2,
                "version": 3
            }

            response = requests.get(f"https://{server}", params=params, timeout=30)
            data = response.json()

            if "ts" in data:
                ts = data["ts"]

            if "updates" in data:
                current_time = time.time()

                for update in data["updates"]:
                    # Тип 4 = новое сообщение
                    if update[0] == 4:
                        flags = update[2]
                        peer_id = update[3]
                        timestamp = update[4]
                        text = update[5]
                        extra = update[6] if len(update) > 6 else {}
                        message_id = update[1]

                        # Только наш чат
                        if peer_id != vk_peer_id:
                            continue

                        from_id = int(extra.get("from", 0)) if extra.get("from") else 0

                        # === ОТСЛЕЖИВАНИЕ ВХОДОВ ===
                        if "action" in extra:
                            action = extra.get("action")

                            if action and action.get("type") in ("chat_invite_user", "chat_invite_user_by_link"):
                                invited_user = int(action.get("member_id", from_id)) if action.get("member_id") else from_id

                                if invited_user > 0:
                                    join_ts[invited_user] = current_time
                                    log(f"👤 Антиспам: вход user_id={invited_user}")

                                    # Очистка старых записей
                                    cutoff = current_time - 300
                                    join_ts = {uid: jt for uid, jt in join_ts.items() if jt > cutoff}

                        # === ПРОПУСКАЕМ АДМИНОВ ===
                        if from_id > 0 and is_admin(from_id):
                            continue

                        # === ПРОВЕРКА "ТОЛЬКО КАРТИНКА" ДЛЯ НОВЫХ ===
                        if from_id > 0:
                            has_attachments = any(k.startswith("attach") and k.endswith("_type") for k in extra.keys())

                            if from_id in join_ts:
                                time_since_join = current_time - join_ts[from_id]

                                if 0 <= time_since_join <= window_sec:
                                    if has_attachments and (not text or len(text.strip()) < 3):
                                        log(f"🚫 Только картинка без текста! user_id={from_id}")

                                        spam_reason = "только картинка без текста (новый пользователь)"
                                        spam_details = {
                                            'has_attachments': True,
                                            'text_length': len(text) if text else 0,
                                            'time_since_join': int(time_since_join)
                                        }

                                        log_spam_to_file(from_id, text or "[без текста]", spam_reason, spam_details)

                                        if notify_telegram and tg_token and tg_chat_id:
                                            send_spam_alert_telegram(tg_token, tg_chat_id, from_id, spam_reason, text or "[без текста]")

                                        # Удаляем сообщение
                                        vk_api_call("messages.delete", vk_token, {
                                            "peer_id": peer_id,
                                            "delete_for_all": 1,
                                            "message_ids": message_id
                                        })

                                        # Кикаем
                                        vk_kick_user(vk_token, vk_chat_id, from_id, reason=spam_reason)
                                        join_ts.pop(from_id, None)
                                        continue

                        # === ПРОВЕРКА ТЕКСТОВЫХ СООБЩЕНИЙ ===
                        if from_id > 0 and text:
                            is_spam_detected = False
                            spam_reason = ""
                            spam_details = {}

                            # Проверяем паттерны
                            is_spam_pattern, pattern_reason, pattern_details = check_spam_patterns(text, ANTIWORDS)

                            # 1. Ссылка
                            if pattern_details.get('has_links'):
                                is_spam_detected = True
                                spam_reason = "ссылка от не-админа"
                                spam_details = pattern_details
                                log(f"🚫 Ссылка от не-админа! user_id={from_id}")

                            # 2. Телефон
                            elif pattern_details.get('has_phone'):
                                is_spam_detected = True
                                spam_reason = "номер телефона в сообщении"
                                spam_details = pattern_details
                                log(f"🚫 Телефон! user_id={from_id}")

                            # 3. CAPS
                            elif pattern_details.get('is_caps'):
                                is_spam_detected = True
                                spam_reason = "CAPS LOCK (>70% заглавных)"
                                spam_details = pattern_details
                                log(f"🚫 CAPS! user_id={from_id}")

                            # 4. Много эмодзи
                            elif pattern_details.get('emoji_count', 0) > 3:
                                is_spam_detected = True
                                spam_reason = f"много эмодзи ({pattern_details['emoji_count']})"
                                spam_details = pattern_details
                                log(f"🚫 Спам эмодзи! user_id={from_id}")

                            # 5. Gibberish
                            elif pattern_details.get('is_gibberish'):
                                is_spam_detected = True
                                spam_reason = "бессмысленный набор символов"
                                spam_details = pattern_details
                                log(f"🚫 Gibberish! user_id={from_id}")

                            # 6. Запрещенные слова
                            elif pattern_details.get('has_antiwords'):
                                is_spam_detected = True
                                spam_reason = "запрещенные слова"
                                spam_details = pattern_details
                                log(f"🚫 Запрещенные слова! user_id={from_id}")

                            # Если спам - удаляем и кикаем
                            if is_spam_detected:
                                log(f"⚠️ СПАМ! user_id={from_id}, причина: {spam_reason}")

                                log_spam_to_file(from_id, text, spam_reason, spam_details)

                                if notify_telegram and tg_token and tg_chat_id:
                                    send_spam_alert_telegram(tg_token, tg_chat_id, from_id, spam_reason, text)

                                vk_api_call("messages.delete", vk_token, {
                                    "peer_id": peer_id,
                                    "delete_for_all": 1,
                                    "message_ids": message_id
                                })

                                vk_kick_user(vk_token, vk_chat_id, from_id, reason=spam_reason)
                                join_ts.pop(from_id, None)
                            else:
                                # НЕ СПАМ = потенциальный ЗАКАЗ!
                                # Пересылаем на irina_mod
                                log(f"🛒 Обнаружен заказ от user_id={from_id}")

                                # Проверяем есть ли reply
                                reply_message_id = extra.get("reply_to")

                                send_order_notification(
                                    vk_token,
                                    from_id,
                                    text,
                                    reply_message_id=reply_message_id,
                                    chat_id=vk_chat_id
                                )

                    # Тип 5 = редактирование
                    elif update[0] == 5:
                        message_id = update[1]
                        flags = update[2]
                        peer_id = update[3]
                        timestamp = update[4]
                        text = update[5]
                        extra = update[6] if len(update) > 6 else {}

                        if peer_id != vk_peer_id:
                            continue

                        from_id = int(extra.get("from", 0)) if extra.get("from") else 0

                        if from_id > 0 and text and not is_admin(from_id):
                            log(f"✏️ Редактирование от user_id={from_id}")

                            is_spam_detected = False
                            spam_reason = ""
                            spam_details = {}

                            is_spam_pattern, pattern_reason, pattern_details = check_spam_patterns(text, ANTIWORDS)

                            if pattern_details.get('has_links'):
                                is_spam_detected = True
                                spam_reason = "ссылка в отредактированном сообщении"
                                spam_details = pattern_details

                            elif pattern_details.get('has_phone'):
                                is_spam_detected = True
                                spam_reason = "телефон в отредактированном сообщении"
                                spam_details = pattern_details

                            elif pattern_details.get('is_caps'):
                                is_spam_detected = True
                                spam_reason = "CAPS в отредактированном сообщении"
                                spam_details = pattern_details

                            elif pattern_details.get('emoji_count', 0) > 3:
                                is_spam_detected = True
                                spam_reason = f"много эмодзи в редакции ({pattern_details['emoji_count']})"
                                spam_details = pattern_details

                            elif pattern_details.get('is_gibberish'):
                                is_spam_detected = True
                                spam_reason = "gibberish в отредактированном сообщении"
                                spam_details = pattern_details

                            elif pattern_details.get('has_antiwords'):
                                is_spam_detected = True
                                spam_reason = "запрещенные слова в редакции"
                                spam_details = pattern_details

                            if is_spam_detected:
                                log(f"⚠️ СПАМ В РЕДАКТИРОВАНИИ! user_id={from_id}")

                                log_spam_to_file(from_id, text, spam_reason, spam_details)

                                if notify_telegram and tg_token and tg_chat_id:
                                    send_spam_alert_telegram(tg_token, tg_chat_id, from_id, spam_reason, text)

                                vk_api_call("messages.delete", vk_token, {
                                    "peer_id": peer_id,
                                    "delete_for_all": 1,
                                    "message_ids": message_id
                                })

                                vk_kick_user(vk_token, vk_chat_id, from_id, reason=spam_reason)
                                join_ts.pop(from_id, None)

            # Проверка на ошибку
            if "failed" in data:
                log("⚠️ Антиспам: Long Poll сбой, переподключение...")
                server, key, ts = get_longpoll_server()
                if not server:
                    log("❌ Антиспам: не удалось переподключиться")
                    break

        except requests.exceptions.Timeout:
            continue

        except Exception as e:
            log(f"⚠️ Антиспам: ошибка в цикле: {e}")
            time.sleep(3)

    log("🛑 Антиспам остановлен")

# ================== ГЛАВНАЯ ФУНКЦИЯ ==================
def main():
    """Главная функция"""
    log("=" * 60)
    log("🚀 VK Parser Headless - запуск")
    log("=" * 60)

    # Загрузка настроек
    settings = load_settings()
    if not settings:
        log("❌ Не удалось загрузить настройки. Выход.")
        sys.exit(1)

    # Проверка обязательных параметров
    vk_token = settings.get("vk_token", "").strip()
    vk_chat_id_str = settings.get("vk_chat_id", "").strip()

    if not vk_token:
        log("❌ VK токен не указан в settings.json")
        sys.exit(1)

    if not vk_chat_id_str:
        log("❌ VK chat_id не указан в settings.json")
        sys.exit(1)

    # Парсим chat_id и peer_id
    try:
        vk_chat_id = int(vk_chat_id_str)
        vk_peer_id = 2000000000 + vk_chat_id
    except ValueError:
        log(f"❌ Некорректный vk_chat_id: {vk_chat_id_str}")
        sys.exit(1)

    tg_token = settings.get("tg_token", "").strip()
    tg_chat_id = settings.get("tg_chat_id")

    antispam_enabled = settings.get("antispam_enabled", True)
    antispam_window_sec = settings.get("antispam_window_sec", 300)
    antispam_notify_telegram = settings.get("antispam_notify_telegram", True)

    log(f"✅ VK Chat ID: {vk_chat_id}")
    log(f"✅ VK Peer ID: {vk_peer_id}")
    log(f"✅ Антиспам: {'Включен' if antispam_enabled else 'Выключен'}")
    log(f"✅ Окно антиспама: {antispam_window_sec} сек")
    log(f"✅ Telegram уведомления: {'Включены' if antispam_notify_telegram else 'Выключены'}")

    # Запуск антиспама
    if antispam_enabled:
        log("🛡️ Запуск антиспам worker...")

        antispam_thread = threading.Thread(
            target=vk_antispam_worker,
            args=(vk_token, vk_peer_id, vk_chat_id, stop_event,
                  antispam_window_sec, 1, tg_token, tg_chat_id, antispam_notify_telegram),
            daemon=True
        )
        antispam_thread.start()

        log("✅ Антиспам worker запущен")
    else:
        log("⚠️ Антиспам отключен в настройках")

    # Главный цикл (просто держим программу живой)
    log("✅ Headless парсер работает. Ctrl+C для остановки.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log("\n⛔ Получен сигнал остановки (Ctrl+C)")
        stop_event.set()
        time.sleep(2)
        log("👋 Парсер остановлен")

if __name__ == "__main__":
    main()
