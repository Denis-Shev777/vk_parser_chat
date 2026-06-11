# ================== VK + Telegram Photo Bot GUI ==================
try:
    import tkinter as tk
    from tkinter import messagebox, font, scrolledtext, simpledialog
    HAS_TKINTER = True
except ImportError:
    tk = None
    HAS_TKINTER = False
import threading
import time
import requests
import os
import re
import difflib
import datetime
import csv
import io
import json
import webbrowser
import platform
import traceback
import math
import sys
import asyncio
try:
    from rapidfuzz import fuzz
except ImportError:
    fuzz = None
try:
    from telegram import Update
    from telegram.ext import Application, MessageHandler, filters, ContextTypes
except BaseException:
    Update = None

SETTINGS_FILE = "settings.json"
SENT_IDS_FILE = "sent_post_ids"
SENT_PHOTOS_FILE = "sent_photo_ids"
DEFAULT_SETTINGS = {
    "tg_token": "",
    "tg_chat_id": "",
    "vk_token": "",
    "vk_chat_id": "",
    "sources": [],
    "start_time": "07:00",
    "end_time": "23:00",
    "freq": 360,
    "price_percent": 50,
    "price_delta": 125,
    "remove_links": True,
    "remove_emoji": True,
    "stopwords": "",
    "limit_photos": True,
    "limit_photos_count": 4,
    "mode": "date",
    "count": None,
    "hours": 24,
    "order_notify_enabled": False,
    "order_notify_vk_id": ""
}
MY_USER_ID = "DenisTest"
CSV_URL = "https://docs.google.com/spreadsheets/d/12BcHBsDRjqR60T8ClR5VXugdMPOXhEpPPTov5-bIAmY/export?format=csv&gid=0"
VK_API_VERSION = "5.131"
MED_FONT = ("Segoe UI", 14)
BIG_BOLD_FONT = ("Segoe UI", 22, "bold")
BG_MAIN = "#f5e9fa"
BG_FRAME = "#f5e1f8"
BORDER_COLOR = "#b0b0b0"
BG_BTN_START = "#d2f8dd"
FG_BTN_START = "#206230"
BG_BTN_STOP = "#fadada"
FG_BTN_STOP = "#a03131"

ORDER_FOOTER = "\n\n✅Для заказа сделайте скрин или перешлите пост админу чата: https://vk.com/irina_mod"

ANTIWORDS = [
    "стиральный порошок", "стиральные порошки", "порошок", "порошки",
    "мыло", "жидкое мыло", "шампунь", "шампуни", "одеяла", "одеяло", "подушка", "подушки", "падушки", "падушка",
    "конфета", "конфеты", "сладость", "сладости", "гель", "спрей мужской", "салфетки", "влажные салфетки",
    "лак", "для стирки", "зубная паста", "отбеливатель", "дезодорант", "утенок", "туалет"
]

# ================== ADMIN WHITELIST ==================
# Список администраторов, которым разрешено отправлять ссылки
# Можно указывать как числовые ID, так и короткие имена (screen_name)
# Пример: ["1055595410", "trendova_arina", "115693485"]
ADMIN_WHITELIST = [
    "1055595410",      # @id1055595410
    "trendova_arina",  # https://vk.com/trendova_arina
    "115693485",       # https://vk.com/id115693485
    "irina_mod"        # https://vk.com/irina_mod
]

# Кэш для хранения преобразованных ID (screen_name -> numeric_id)
_admin_id_cache = {}

_global_log_window_instance = None
stop_event = threading.Event()

# ================== LICENSE BLOCK ==================
def show_license_info(expiry_date, is_expired=False, status_msg=None):
    root = tk.Tk()
    root.withdraw()
    if is_expired:
        message = f"Срок действия вашей лицензии истёк: {expiry_date}\n"
        if status_msg:
            message += f"\n{status_msg}"
        message += "\nОбратитесь к администратору для продления."
        messagebox.showerror("Лицензия истекла", message)
    else:
        message = f"Ваша лицензия активна до: {expiry_date}\n\nСпасибо за использование программы!"
        if status_msg:
            message += f"\n\n{status_msg}"
        messagebox.showinfo("Информация о лицензии", message)
    root.destroy()

def check_license(user_id):
    try:
        print(f"Проверка лицензии для пользователя: {user_id}")
        add_log(f"Проверка лицензии для пользователя: {user_id}")
        r = requests.get(CSV_URL, timeout=10)
        r.raise_for_status()
        f = io.StringIO(r.text)
        reader = csv.DictReader(f)
        found_user = False
        for row in reader:
            if row.get('user_id', '').strip() == user_id:
                found_user = True
                status = row.get('status', '').strip().lower()
                expiry = row.get('expiry', '').strip()
                expiry_str = expiry if expiry else "Не указана"

                if status != 'active':
                    add_log(f"Лицензия пользователя {user_id} неактивна (статус: {status}).")
                    show_license_info(expiry_str, is_expired=True, status_msg=f"Статус: {status}")
                    return False

                if expiry:
                    try:
                        expiry_date = datetime.datetime.strptime(expiry, "%Y-%m-%d")
                        if expiry_date < datetime.datetime.now():
                            add_log(f"Лицензия пользователя {user_id} истекла ({expiry_date.strftime('%Y-%m-%d')}).")
                            show_license_info(expiry_date.strftime("%Y-%m-%d"), is_expired=True)
                            return False
                        else:
                            add_log(f"Лицензия для пользователя {user_id} активна до {expiry_date.strftime('%Y-%m-%d')}.")
                            show_license_info(expiry_date.strftime("%Y-%m-%d"))
                            return True
                    except ValueError:
                        add_log(f"Некорректный формат даты истечения лицензии для {user_id}: {expiry}")
                        show_license_info(expiry, is_expired=True, status_msg="Некорректная дата окончания лицензии.")
                        return False
                else:
                    add_log(f"Для пользователя {user_id} не указана дата истечения лицензии.")
                    show_license_info("Не указана", is_expired=True, status_msg="Дата окончания лицензии не указана.")
                    return False
        if not found_user:
            add_log(f"Пользователь {user_id} не найден в списке лицензий.")
            show_license_info("Пользователь не найден", is_expired=True, status_msg="Пользователь не найден в списке лицензий.")
        return False
    except requests.exceptions.RequestException as e:
        add_log(f"Ошибка сети при проверке лицензии (нет доступа к CSV): {e}")
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Ошибка сети", f"Не удалось подключиться к серверу лицензий:\n{e}\nПроверьте ваше интернет-соединение.")
        root.destroy()
        return False
    except Exception as e:
        add_log(f"Непредвиденная ошибка при проверке лицензии: {traceback.format_exc()}")
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Ошибка", f"Непредвиденная ошибка при проверке лицензии:\n{e}")
        root.destroy()
        return False

# ================== UTILS ==================
def resource_path(filename):
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, filename)
    return os.path.join(os.path.abspath(os.path.dirname(sys.argv[0])), filename)

def load_settings():
    path = SETTINGS_FILE
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_SETTINGS, f, indent=4, ensure_ascii=False)
        return dict(DEFAULT_SETTINGS)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return dict(DEFAULT_SETTINGS)

def save_settings(settings):
    path = resource_path(SETTINGS_FILE)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=4, ensure_ascii=False)

def round_to_5(x):
    return int(round(x / 5.0)) * 5

def fix_currency_typos(line):
    return re.sub(r'(\d[\d\s\u202f]*[.,]?\d*)\s*е\b', r'\1р', line, flags=re.IGNORECASE)

def is_size_line(s):
    s_lower = s.lower().strip()
    # Не считаем ценовые строки размерами!
    if re.match(r'цена\s*(за)?\s*уп', s_lower) or re.match(r'цена\s*уп', s_lower):
        return False
    if "цена" in s_lower and ("шт" in s_lower or "уп" in s_lower or "штук" in s_lower or "пар" in s_lower):
        return False
    # Явно содержит слово "размер"
    if 'размер' in s_lower or 'размеры' in s_lower:
        return True
    # Цепочка чисел через дефис
    if re.fullmatch(r'(\d{2,4}[-–—])+(\d{2,4})', s_lower.replace(' ', '')):
        return True
    if re.match(r'^\d{2,4}([-,]\d{2,4})+', s_lower.replace(' ', '')):
        return True
    # Почти вся строка — числа+дефисы (3 и более чисел)
    if re.fullmatch(r'[\d\-,\s]+', s_lower) and len(re.findall(r'\d{2,4}', s_lower)) >= 3:
        return True
    return False

def is_artikul_line(s):
    s_lower = s.lower()
    return (
        s_lower.startswith("арт") or
        s_lower.startswith("артикул") or
        s_lower.startswith("articul")
    )

def is_price_line(line):
    l = line.lower()
    # Проверяем явные ценовые признаки:
    if 'цена' in l or '₽' in l:
        return bool(re.search(r'\d', l))
    # Проверяем валюту "р" только если она стоит после числа (например, "100 р", "100р")
    if re.search(r'\d+\s*р\b', l):
        return True
    return False

def normalize_text(text):
    text = re.sub(r'[^\w\s]', '', text, flags=re.UNICODE)
    return ' '.join(text.lower().split())

PRICE_PATTERN = re.compile(
    r'(цена\s*[:\-–—=]?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*(₽|руб\.?|р\.?)', re.IGNORECASE)

def is_pack_info(line):
    return bool(re.search(r'\(в упаковке\s*\d+\s*пар\)', line))

def is_informative(line, stopwords):
    l = line.strip(' .:-—–=,;!')
    if not l or len(l) < 3:
        return False
    if not re.search(r'[а-яa-z]', l, re.I):
        return False
    for sw in stopwords:
        if l.lower() == sw.lower():
            return False
    return True

def remove_base_stopwords(text, stopwords):
    stopwords_sorted = sorted(stopwords, key=len, reverse=True)
    cleaned_lines = []
    prev_is_size = False
    for line in text.split('\n'):
        l = line.strip()
        # --- Белый список: "ШИР/ВЫС - ЦЕНА" (50/70-100р, 70x70 - 100 руб) ---
        if re.match(r'(?i)^\s*\d{2,3}\s*[x/]\s*\d{2,3}\s*[-–—:]\s*\d', l):
            cleaned_lines.append(l)
            prev_is_size = False
            continue

        if not l:
            prev_is_size = False
            continue
        # Если строка с размером
        if is_size_line(l):
            cleaned_lines.append(l)
            prev_is_size = True
            continue
        # Если предыдущая строка была размером, а текущая — просто числа или диапазон
        if prev_is_size and re.search(r'\d', l):
            cleaned_lines.append(l)
            continue
        prev_is_size = False
        # Артикул тоже можно добавить сюда
        if is_artikul_line(l):
            cleaned_lines.append(l)
            continue
        if is_pack_info(l):
            cleaned_lines.append(l)
            continue
        for sw in stopwords_sorted:
            if sw == "упаковка" and re.search(r'\(в упаковке\s*\d+\s*пар\)', l):
                continue
            l = re.sub(rf'\b{re.escape(sw)}\b', '', l, flags=re.IGNORECASE)
        l = l.strip(' .:-—–=,;!')
        l = re.sub(r'\s+', ' ', l)
        if is_informative(l, stopwords):
            cleaned_lines.append(l)
    return '\n'.join(cleaned_lines)

BASE_STOPWORDS = [
    "распродажа", "принимаю заказы", "принимаю ы", "модный базар", "продается", "только", "поставщик",
    "РЫНОК", "АДРЕС", "НАШ", "АКЦИЯ", "Номер для заказа", "WhatsApp5",
    "корпус", "упаковка", "расцветка в упаковке как на фото", "наша группа",
    "бронь", "бронировать", "бронирование", "идут", "тянуться", "тянутся", "хорошо", "в размер",
    "садовод", "в наличии", "поставщик", "принимаем заказ", "Принимаю заказы", "заказ",
    "подпишитесь", "чтобы не потерять", "у нас", "можно", "высокое",
    "качество", "качество люкс", "люкс", "заранее", "опт", "в нашей группе",
    "у нас можно бронировать", "забронировать тут", "гарантия", "условия возврата",
    "ватсап", "whatsapp", "телефон", "наша группа", "качество супер",
    "упаковка", "в упаковка", "в упаковке", "упаковки", "лучше будет",
    "Модный базар", "Садовод", "🛍🛍️🎀Модный базар🎀 🛍🛍️", "👉Садовод", "🎀Модный", "заказы"
]

# ================== ORDER DETECTION KEYWORDS ==================
# Ключевые слова для определения заказов в чате
# Включает вариации с опечатками и разным написанием
ORDER_KEYWORDS = [
    # Заказ и вариации (+ опечатки)
    "заказ", "заказать", "закажу", "заказываю", "зказ", "заказа", "закас",
    "оформить заказ", "оформляю заказ", "хочу заказать",

    # Размеры (главный индикатор!) + опечатки
    "размер", "размеры", "розмер", "розмір", "рамер", "размерчик", "рзмр",
    "какой размер", "какие размеры", "есть размер", "нужен размер",
    "36 размер", "37 размер", "38 размер", "39 размер", "40 размер",
    "41 размер", "42 размер", "43 размер", "44 размер", "45 размер",
    "р 36", "р 37", "р 38", "р 39", "р 40", "р 41", "р 42", "р 43", "р 44",
    "р.36", "р.37", "р.38", "р.39", "р.40", "р.41", "р.42", "р.43", "р.44",
    "размер 36", "размер 37", "размер 38", "размер 39", "размер 40",

    # Посадка/размерность + опечатки
    "маломерит", "маломерят", "маломерка", "маломерки", "маломер", "мало мерит",
    "большемерит", "большемерят", "большемерка", "большемер", "больше мерит",
    "в размер", "идут в размер", "по размеру", "размер в размер",
    "на широкую ногу", "на узкую ногу", "полнота", "полноразмерные",

    # Сезон + опечатки
    "сезон", "сизон", "какой сезон", "на какой сезон", "для какого сезона",
    "зима", "зимние", "зимняя", "зімові", "на зиму",
    "лето", "летние", "летняя", "літні", "на лето",
    "весна", "осень", "демисезон", "деми", "демисезонные",

    # Материал + опечатки
    "материал", "матеріал", "материял", "матерьял", "матириал",
    "из чего", "из какого материала", "что за материал",
    "кожа", "кожаные", "натуральная кожа", "нат кожа", "натуралка",
    "эко кожа", "экокожа", "эко-кожа", "кожзам", "искусственная",
    "замша", "замшевые", "текстиль", "ткань",

    # Покупка/интерес + опечатки
    "беру", "возьму", "забираю", "куплю", "покупаю", "бяру", "вазьму",
    "хочу купить", "хочу взять", "мне нужно", "мне нужны", "мне нужен",

    # Цена + опечатки
    "сколько стоит", "скільки коштує", "какая цена", "почем", "почём",
    "цена", "ціна", "прайс", "стоимость", "по чем",

    # Наличие
    "есть в наличии", "в наличии есть", "наличие", "є в наявності",
    "есть ли", "имеется", "остались",

    # Оплата/доставка
    "оплата", "оплатить", "оплачу", "как оплатить",
    "доставка", "доставку", "как доставка", "куда доставка",
    "отправка", "отправить", "отправляете",

    # Бронь
    "отложите", "отложи", "придержите", "забронируйте", "забронировать",

    # Города доставки (пункты выдачи)
    "светогорск", "выборг", "каменногорск",
]

# Московское время (UTC+3)
MSK_TZ = datetime.timezone(datetime.timedelta(hours=3))

def check_order_keywords(text):
    """
    Проверяет, содержит ли сообщение ключевые слова заказа.
    Возвращает (True, matched_keyword) или (False, None).
    """
    if not text:
        return False, None
    text_lower = text.lower()
    for keyword in ORDER_KEYWORDS:
        if keyword.lower() in text_lower:
            return True, keyword
    return False, None

def send_order_notification_vk(vk_token, admin_user_id, from_id, message_text, peer_id, chat_link=""):
    """
    Отправляет уведомление о заказе администратору в личные сообщения VK.
    """
    try:
        # Получаем имя пользователя
        user_name = f"id{from_id}"
        try:
            resp = requests.get(
                "https://api.vk.com/method/users.get",
                params={"user_ids": from_id, "v": VK_API_VERSION, "access_token": vk_token},
                timeout=10
            ).json()
            if "response" in resp and resp["response"]:
                u = resp["response"][0]
                user_name = f"{u.get('first_name', '')} {u.get('last_name', '')}".strip()
        except Exception:
            pass

        notification = (
            f"Новый заказ в чате!\n\n"
            f"От: {user_name} (https://vk.com/id{from_id})\n\n"
            f"Сообщение:\n{message_text[:800]}"
        )
        if chat_link:
            notification += f"\n\nПерейти в чат: {chat_link}"
        if not send_vk_message(vk_token, admin_user_id, notification):
            add_log(f"[ORDER ERROR] VK rejected notification (admin_user_id={admin_user_id})")
            return False
        add_log(f"[ORDER] Notification sent to admin (user_id={admin_user_id})")
        return True
    except Exception as e:
        add_log(f"[ORDER ERROR] Failed to send order notification: {e}")
        return False

def clean_full_text(text):
    # 1. Удаляем стоп-слова и неинформативные строки
    cleaned = remove_base_stopwords(text, BASE_STOPWORDS)
    return cleaned

def find_pack_count(text):
    patterns = [
        r'в упаковке\s*(\d+)\s*пар',
        r'упаковка\s*(\d+)\s*пар',
        r'пачка\s*(\d+)\s*пар',
        r'в упаковке\s*(\d+)\s*шт',
        r'упаковка\s*(\d+)\s*шт'
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return int(m.group(1))
    return None

def add_pack_count_line(text, pack_count, unit="пар"):
    if pack_count is not None and f"{pack_count} {unit}" not in text:
        return text + f"\nВ упаковке {pack_count} {unit}"
    return text

def remove_emojis(text):
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF"
        "\U0001F1E0-\U0001F1FF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "\U0001F900-\U0001F9FF"
        "\U0001FA70-\U0001FAFF"
        "\u200d"
        "\ufe0f"
        "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

def remove_links(text):
    """Удаляет ЛЮБЫЕ ссылки и доменные упоминания, включая m.vk.ru, vk.com, t.me и т.п.
    Также чистит zero‑width и неразрывные пробелы перед матчингом."""
    import re as _re
    if not text:
        return ''
    # Нормализуем скрытые символы пробелов
    text = (
        text.replace('\u200b','')
            .replace('\u2060','')
            .replace('\u00a0',' ')
            .replace('\u202f',' ')
    )
    re_scheme = _re.compile(r"\b(?:(?:https?|ftp)://|www\.)\S+", _re.IGNORECASE)
    re_domain = _re.compile(r"\b(?:[a-z0-9-]{1,63}\.)+(?:[a-z]{2,63})(?:/[^\s<>()\[\]{}]*)?", _re.IGNORECASE)
    out_lines = []
    for raw in text.split('\n'):
        line = re_scheme.sub('', raw)
        line = re_domain.sub('', line)
        line = _re.sub(r'\s+', ' ', line).strip(' .,:;-—–|\t')
        if line:
            out_lines.append(line)
    return '\n'.join(out_lines)
def remove_phones(text):
    phone_pattern = re.compile(
        r'(\+7|8)?[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}'
        r'|(?<=\s)\d{11}(?=\s|$)'
    )
    return phone_pattern.sub('', text)

def clean_description(text, remove_links_flag=True, remove_emoji_flag=True, remove_phones_flag=True):
    if remove_links_flag:
        text = remove_links(text)
    if remove_emoji_flag:
        text = remove_emojis(text)
    if remove_phones_flag:
        text = remove_phones(text)
    return text

def message_passes_filters(text, antiwords, remove_links_flag=False, remove_emoji_flag=False):
    if remove_links_flag:
        text = remove_links(text)
    if remove_emoji_flag:
        text = remove_emojis(text)
    text_l = text.lower()
    for aw in antiwords:
        if aw.lower() in text_l:
            return False
    return True

def add_log(msg):
    timestamp = datetime.datetime.now().strftime("[%H:%M:%S]")
    log_message = f"{timestamp} {msg}"
    if _global_log_window_instance and _global_log_window_instance.top.winfo_exists():
        try:
            _global_log_window_instance.top.after(0, lambda: _global_log_window_instance.append(log_message))
        except Exception as e:
            print(f"Ошибка при добавлении в лог (Tkinter after): {e} - {log_message}")
    else:
        print(log_message)

# ================== SPAM DETECTION FUNCTIONS ==================

def count_emojis(text):
    """Подсчитывает количество эмодзи в тексте"""
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
        "\U0001F900-\U0001F9FF"
        "\U0001FA70-\U0001FAFF"
        "\u200d"
        "\ufe0f"
        "]+", flags=re.UNICODE)
    return len(emoji_pattern.findall(text))

def has_links(text):
    """Проверяет наличие ссылок в тексте"""
    if not text:
        return False
    # Проверка URL с протоколом
    if re.search(r'\b(?:(?:https?|ftp)://|www\.)\S+', text, re.IGNORECASE):
        return True
    # Проверка доменов
    if re.search(r'\b(?:[a-z0-9-]{1,63}\.)+(?:[a-z]{2,63})', text, re.IGNORECASE):
        return True
    return False

def has_phone(text):
    """Проверяет наличие телефонных номеров"""
    if not text:
        return False
    phone_pattern = re.compile(
        r'(\+7|8)?[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}'
        r'|(?<=\s)\d{11}(?=\s|$)'
    )
    return bool(phone_pattern.search(text))

def count_mentions(text):
    """Подсчитывает количество упоминаний (@user)"""
    if not text:
        return 0
    # Ищем @user или [id123|text]
    mentions = re.findall(r'@[a-zA-Z0-9_]+|\[id\d+\|', text)
    return len(mentions)

def is_mostly_caps(text):
    """Проверяет, написан ли текст в основном заглавными буквами (>70%)"""
    if not text or len(text) < 5:
        return False
    # Считаем только буквы
    letters = [c for c in text if c.isalpha()]
    if len(letters) < 3:
        return False
    caps_count = sum(1 for c in letters if c.isupper())
    return (caps_count / len(letters)) > 0.7

def has_repetitive_chars(text):
    """Проверяет наличие повторяющихся символов (!!!, ???, ..., etc)"""
    if not text:
        return False
    # Ищем 3+ одинаковых символа подряд
    return bool(re.search(r'(.)\1{2,}', text))

def is_gibberish(text):
    """Проверяет на бессмысленный набор символов"""
    if not text or len(text) < 5:
        return False
    # Сообщения с ценой/количеством ("122р 2шт", "38р уп", "50р/пар") — не gibberish
    if re.search(r'\d+\s*(?:р(?:уб)?\.?|₽|шт\.?|уп\.?|пар\.?|кг\.?|гр\.?|мл\.?|л\.?)', text, re.IGNORECASE):
        return False
    text_clean = text.replace(' ', '').lower()
    vowels_ru = set('аеёиоуыэюя')
    vowels_en = set('aeiouy')
    letters = [c for c in text_clean if c.isalpha()]
    # Нужно минимум 6 букв для надёжного анализа
    if len(letters) < 6:
        return False
    vowel_count = sum(1 for c in letters if c in vowels_ru or c in vowels_en)
    return (vowel_count / len(letters)) < 0.2

def has_hidden_messenger_contact(text):
    """Проверяет на замаскированные контакты мессенджеров (tg, телеграм, whatsapp и т.п.)"""
    if not text:
        return False
    t = text.lower()
    # Нормализуем: убираем лишние пробелы между символами для детекции "t  г" -> "tг"
    t_collapsed = re.sub(r'\s+', '', t)
    # Паттерны Telegram
    tg_patterns = [
        r't\s*[гg]\s*[:：.]\s*\S+',       # t г: username / tg: username
        r'тг\s*[:：.]\s*\S+',              # тг: username
        r'телеграм[мм]?\s*[:：.]\s*\S+',   # телеграм: username
        r'telegram\s*[:：.]\s*\S+',         # telegram: username
        r'@[a-zA-Z0-9_]{3,}',              # @username
        r't\.me/\S+',                       # t.me/username
    ]
    # Паттерны WhatsApp / Viber / других мессенджеров
    messenger_patterns = [
        r'whats\s*app\s*[:：.]\s*\S+',
        r'вотс\s*ап\s*[:：.]\s*\S+',
        r'вайбер\s*[:：.]\s*\S+',
        r'viber\s*[:：.]\s*\S+',
        r'ватсап\s*[:：.]\s*\S+',
        r'wh?ats?\s*app',
    ]
    all_patterns = tg_patterns + messenger_patterns
    for pat in all_patterns:
        if re.search(pat, t, re.IGNORECASE):
            return True
        if re.search(pat, t_collapsed, re.IGNORECASE):
            return True
    # Проверка ключевых фраз рекламного спама
    spam_phrases = [
        'копируй и вставляй',
        'пиши в лс',
        'пишите в лс',
        'пиши в личку',
        'пишите в личку',
        'удостоверение',
        'внесение в базу',
        'права категори',
        # Типовые фразы мошенников/рекламщиков услуг
        'деньги первые не берем',
        'деньги не берем',
        'предоплату не берем',
        'предоплата не',
        'оплата после',
        'под ключ',
        'огромные связи',
        'есть связи',
        'все официально делаем',
        'сделаем за',
        'много кейсов',
        'без предоплаты',
    ]
    for phrase in spam_phrases:
        if phrase in t:
            return True
    return False


def extract_fwd_text(vk_token, message_id):
    """Fetches full message via API and extracts text from forwarded messages."""
    try:
        resp = vk_api_call(
            "messages.getById",
            vk_token,
            {"message_ids": message_id},
            timeout=5
        )
        if not resp or not isinstance(resp, dict):
            return ""
        items = resp.get("items", [])
        if not items:
            return ""
        msg = items[0]
        parts = []
        if msg.get("text"):
            parts.append(msg["text"])
        for fwd in msg.get("fwd_messages", []):
            fwd_text = fwd.get("text", "")
            if fwd_text:
                parts.append(fwd_text)
        return " ".join(parts)
    except Exception:
        return ""


def check_spam_patterns(text, antiwords=None):
    """
    Проверяет текст на спам-паттерны и возвращает информацию о подозрительности

    Возвращает: (is_spam: bool, reason: str, details: dict)
    """
    if not text:
        return False, "", {}

    details = {
        'has_links': False,
        'has_phone': False,
        'has_messenger_contact': False,
        'emoji_count': 0,
        'mention_count': 0,
        'is_caps': False,
        'has_repetitive': False,
        'is_gibberish': False,
        'has_antiwords': False
    }

    reasons = []

    # Проверка на запрещенные слова
    if antiwords:
        text_lower = text.lower()
        for aw in antiwords:
            if aw.lower() in text_lower:
                details['has_antiwords'] = True
                reasons.append(f"запрещенное слово '{aw}'")
                break

    # Проверка ссылок
    if has_links(text):
        details['has_links'] = True
        reasons.append("содержит ссылку")

    # Проверка скрытых контактов мессенджеров (tg:, телеграм:, whatsapp и т.п.)
    if has_hidden_messenger_contact(text):
        details['has_messenger_contact'] = True
        reasons.append("контакт мессенджера / рекламный спам")

    # Проверка телефонов
    if has_phone(text):
        details['has_phone'] = True
        reasons.append("содержит телефон")

    # Подсчет эмодзи
    emoji_count = count_emojis(text)
    details['emoji_count'] = emoji_count
    if emoji_count > 3:
        reasons.append(f"много эмодзи ({emoji_count})")

    # Подсчет упоминаний
    mention_count = count_mentions(text)
    details['mention_count'] = mention_count
    if mention_count > 3:
        reasons.append(f"много упоминаний ({mention_count})")

    # Проверка CAPS
    if is_mostly_caps(text):
        details['is_caps'] = True
        reasons.append("CAPS LOCK")

    # Проверка повторяющихся символов
    if has_repetitive_chars(text):
        details['has_repetitive'] = True
        reasons.append("повторяющиеся символы")

    # Проверка на gibberish
    if is_gibberish(text):
        details['is_gibberish'] = True
        reasons.append("бессмысленный текст")

    # Особо опасные комбинации
    is_spam = False
    reason = ""

    # Критичные паттерны (100% спам)
    if details['has_antiwords']:
        is_spam = True
        reason = "критично: " + ", ".join(reasons)
    elif details.get('has_messenger_contact'):
        is_spam = True
        reason = "критично: контакт мессенджера / рекламный спам"
    elif details['has_phone'] and details['has_links']:
        is_spam = True
        reason = "критично: телефон + ссылка"
    elif details['has_links'] and len(text.strip()) < 30:
        is_spam = True
        reason = "критично: короткое сообщение со ссылкой"
    # Подозрительные комбинации
    elif len(reasons) >= 3:
        is_spam = True
        reason = "подозрительно: " + ", ".join(reasons[:3])
    elif details['mention_count'] > 5:
        is_spam = True
        reason = f"подозрительно: массовые упоминания ({mention_count})"

    return is_spam, reason, details

def log_spam_to_file(user_id, text, reason, details, log_file="spam_log.txt"):
    """Логирует обнаруженный спам в отдельный файл"""
    try:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        date_only = datetime.datetime.now().strftime("%Y-%m-%d")
        time_only = datetime.datetime.now().strftime("%H:%M:%S")

        # Детальный лог
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"[{timestamp}] SPAM DETECTED\n")
            f.write(f"User ID: {user_id}\n")
            f.write(f"Reason: {reason}\n")
            f.write(f"Details: {details}\n")
            f.write(f"Text: {text[:200]}\n")
            f.write(f"{'='*80}\n")

        # Краткая статистика (для быстрого просмотра)
        stats_file = "spam_stats.txt"
        with open(stats_file, 'a', encoding='utf-8') as f:
            # Форматируем строку: дата | время | user_id | причина | краткий текст
            short_text = text[:50].replace('\n', ' ')
            f.write(f"{date_only} | {time_only} | ID:{user_id} | {reason} | {short_text}\n")

        add_log(f"📝 Спам залогирован: {stats_file}")

    except Exception as e:
        add_log(f"❌ Ошибка записи в лог спама: {e}")

def send_spam_alert_telegram(tg_token, tg_chat_id, user_id, reason, text):
    """
    Отправляет уведомление о спамере в Telegram

    Args:
        tg_token: Telegram bot token
        tg_chat_id: Telegram chat ID для уведомлений
        user_id: VK ID спамера
        reason: Причина кика
        text: Текст сообщения спамера
    """
    if not tg_token or not tg_chat_id:
        return False

    try:
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        vk_profile_link = f"https://vk.com/id{user_id}"
        short_text = text[:100].replace('\n', ' ')

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
            add_log(f"📱 Уведомление отправлено в Telegram")
            return True
        else:
            add_log(f"⚠️ Ошибка отправки в Telegram: {response.text[:100]}")
            return False

    except Exception as e:
        add_log(f"❌ Ошибка отправки уведомления в Telegram: {e}")
        return False

# ================== ADMIN WHITELIST FUNCTIONS ==================

def resolve_admin_ids(vk_token):
    """
    Преобразует короткие имена (screen_name) из ADMIN_WHITELIST в числовые ID
    Кэширует результаты в _admin_id_cache
    """
    global _admin_id_cache

    # Разделяем на числовые ID и screen_name
    numeric_ids = []
    screen_names = []

    for admin in ADMIN_WHITELIST:
        if str(admin).isdigit():
            numeric_ids.append(int(admin))
        else:
            screen_names.append(admin)

    # Если есть screen_name - конвертируем их через VK API
    if screen_names:
        try:
            from urllib.parse import urlencode
            user_ids_param = ",".join(screen_names)
            params = {
                "user_ids": user_ids_param,
                "v": VK_API_VERSION,
                "access_token": vk_token
            }
            url = f"https://api.vk.com/method/users.get?{urlencode(params)}"
            response = requests.get(url, timeout=10).json()

            if "response" in response:
                for user in response["response"]:
                    user_id = user.get("id")
                    if user_id:
                        numeric_ids.append(user_id)
                        # Кэшируем соответствие screen_name -> id
                        for sn in screen_names:
                            if sn.lower() in [user.get("screen_name", "").lower(),
                                             user.get("domain", "").lower()]:
                                _admin_id_cache[sn] = user_id
                                add_log(f"🔑 Админ '{sn}' -> ID {user_id}")
            else:
                add_log(f"⚠️ Не удалось получить ID для screen_name: {screen_names}")
                add_log(f"   Ответ VK API: {response}")
        except Exception as e:
            add_log(f"❌ Ошибка при получении ID админов: {e}")

    # Добавляем все известные числовые ID в кэш
    for num_id in numeric_ids:
        _admin_id_cache[str(num_id)] = num_id

    return numeric_ids

def is_admin(user_id):
    """
    Проверяет, является ли пользователь администратором

    Args:
        user_id: числовой ID пользователя VK

    Returns:
        bool: True если админ, False если нет
    """
    if not user_id:
        return False

    # Проверяем в кэше преобразованных ID
    user_id_str = str(user_id)
    if user_id in _admin_id_cache.values() or user_id_str in _admin_id_cache:
        return True

    # Проверяем напрямую в ADMIN_WHITELIST (если указан числовой ID)
    if user_id_str in ADMIN_WHITELIST or user_id in ADMIN_WHITELIST:
        return True

    return False

# ================== PRICE PATTERNS ==================
_PRICE_CURRENCY_PATTERN = (
    r'(?:рублей|руб\.|руб|р\.|р|Р\.|Р|rub\.|rub|r\.|r|p\.|p|py6|₽|\u20bd|[оoO0])'
)
_PRICE_PER_AMOUNT = re.compile(
    r'(цена\s*за\s*)(\d+)\s*(?:уп|уп\.|штук|шт|пар|упаковок?|пачек?)\s*([.,\d\s\u202f]+)\s*({})'.format(_PRICE_CURRENCY_PATTERN),
    re.IGNORECASE
)
PAT_BED_SIZE_PRICE = re.compile(
    r'цена\s*([12](?:[,\.]\d)?сп|евро)\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({})\b'.format(_PRICE_CURRENCY_PATTERN),
    re.IGNORECASE
)
_PRICE_WITH_UNIT = re.compile(
    r'(цена\s*[:\-–—=]?\s*)?'
    r'(\d[\d\s\u202f]*[.,]?\d*)\s*'
    r'({})'
    r'([\s\S]*?)'
    r'(упаковка|пачка|пар|штук|шт)\b'.format(_PRICE_CURRENCY_PATTERN),
    re.IGNORECASE
)
_PRICE_PAT_PLAIN = re.compile(
    r'(цена\s*[:\-–—=]?\s*)'
    r'(\d[\d\s\u202f]*[.,]?\d*)\s*'
    r'({})'.format(_PRICE_CURRENCY_PATTERN),
    re.IGNORECASE
)
_PRICE_PAT_ONLY = re.compile(
    r'(?<!\d)(\d[\d\s\u202f]*[.,]?\d*)\s*({})(?!\s*(пар|штук|шт|пачка|упаковка)\b)'.format(_PRICE_CURRENCY_PATTERN),
    re.IGNORECASE
)
_PRICE_PACK_BRACKET = re.compile(
    r'(\(\s*упак\.\s*\d+\s*шт\s*=\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({})(\s*\))'.format(_PRICE_CURRENCY_PATTERN),
    re.IGNORECASE
)
_PRICE_UNIT_BEFORE_AMOUNT = re.compile(
    r'(цена\s*)(шт|упаковка|пачка|пар|штук)\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({})\b'.format(_PRICE_CURRENCY_PATTERN),
    re.IGNORECASE
)
_PRICE_UP_PACK = re.compile(
    r'(цена\s*уп\s*\d+\s*(?:шт|пар)\s*[:=]?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({})'.format(_PRICE_CURRENCY_PATTERN),
    re.IGNORECASE
)
# --- ВАЖНО: Только "пар"! ---
_PRICE_PACK_PIECES = re.compile(
    r'(цена\s*за\s*(?:уп|упаковку|упак|пачку|пач|уп)\s*\d+\s*пар\b\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({})'.format(_PRICE_CURRENCY_PATTERN),
    re.IGNORECASE
)

# ======== EXTRA PRICE PATTERNS (устойчивые к размерам/телефонам; валюта опциональна контекстно) ========
import re as _re_extra

# Неразрывные пробелы (если выше в файле не определены)
try:
    NBSP
except NameError:
    NBSP = "\u00A0"
try:
    NNBSP
except NameError:
    NNBSP = "\u202F"

_SP    = r"[ \t" + NBSP + NNBSP + r"]*"
_CUR   = r"(?:₽|руб(?:\.|лей)?|р\.?|р|p|P)"
_NUM   = r"\d[\d " + NBSP + NNBSP + r"]*(?:[.,]\d+)?"
_CUR_O = r"(?:" + _SP + _CUR + r")?"  # валюта опциональна в контекстных паттернах
_QTYW  = r"(?:шт\w*|штук\w*|пара|пары|пар\w*|уп\w*|пач\w*|коробк\w*|комплект\w*)"

# --- запреты/подсказки контекста ---
_RX_IS_SIZEY = _re_extra.compile(r"(?i)\bразмер\w*|\b\d{2,4}\s*[-–—]\s*\d{2,4}|\b(\d{2,4}[,\-]\s*){2,}\d{2,4}")
_RX_IS_ART   = _re_extra.compile(r"(?i)\b(арт|артикул|корпус|место)\b")
_RX_CONTACT  = _re_extra.compile(r"(?i)(whats?app|тел|phone|vk\.com|t\.me|instagram|@|id\d+)")
_RX_PUFFS    = _re_extra.compile(r"(?i)\b(puff|затяж)\w*")  # «16000 затяжек»
_RX_QTY_WORD = _re_extra.compile(rf"(?i)\b{_QTYW}\b")

# --- специфичные/контекстные паттерны (валюта опциональна) ---
PAT_VSEGO_ZA_PO = _re_extra.compile( rf"(?i)\bвсего{_SP}(?:за|по){_SP}({_NUM}){_CUR_O}\b" )
PAT_CENA_ZA_SHT = _re_extra.compile( rf"(?i)\bцена{_SP}за{_SP}(?:шт\.?|штуку|штука){_SP}[:=\-–—]?{_SP}({_NUM}){_CUR_O}\b" )
PAT_CENA_FOR_N_UNITS = _re_extra.compile( rf"(?i)\bцена{_SP}за{_SP}\d+{_SP}(?:шт|штук|уп|уп\.|комплект|пару|пары){_SP}({_NUM}){_CUR_O}\b" )
PAT_CENA_FREE_TEXT = _re_extra.compile( rf"(?i)\bцена{_SP}(?:[^\d\r\n]{{0,40}}?){_SP}[.:]?{_SP}({_NUM}){_CUR_O}\b" )
PAT_CENA_DOT = _re_extra.compile( rf"(?i)\bцена{_SP}[.:]{_SP}?({_NUM}){_CUR_O}\b" )

# --- qty → price ---
PAT_QTY_THEN_PRICE_WITH_DELIM = _re_extra.compile( rf"(?i)\b\d+{_SP}{_QTYW}{_SP}[:=\-–—]{_SP}({_NUM}){_CUR_O}\b" )
PAT_QTY_THEN_PRICE_SPACE     = _re_extra.compile( rf"(?i)\b\d+{_SP}{_QTYW}{_SP}({_NUM}){_CUR_O}\b" )

# --- прочие форматы ---
PAT_BARE_LINE_PRICE = _re_extra.compile( rf"(?i)^\s*({_NUM}){_CUR_O}\s*$" )          # «1100» / «1100р» строкой
# Цена после тире: «— 700(р)», но НЕ в числовых диапазонах типа «44-46-48»
PAT_AFTER_DASH = _re_extra.compile( rf"(?i)(?<!\d)[—–-]+{_SP}({_NUM}){_CUR_O}\b" )     # «— 700»
PAT_AFTER_EQUAL     = _re_extra.compile( rf"(?i)={_SP}({_NUM}){_CUR_O}\b" )          # «= 3000»
PAT_ANY_INLINE_PRICE_STRICT = _re_extra.compile( rf"(?i)\b({_NUM}){_SP}{_CUR}\b" )   # только «число+валюта»

# порядок важен (специфичные → общие)
EXTRA_PRICE_PATTERNS = [
    PAT_VSEGO_ZA_PO,
    PAT_CENA_ZA_SHT,
    PAT_CENA_FOR_N_UNITS,
    PAT_CENA_FREE_TEXT,
    PAT_CENA_DOT,
    PAT_QTY_THEN_PRICE_WITH_DELIM,
    PAT_QTY_THEN_PRICE_SPACE,
    PAT_AFTER_DASH,
    PAT_AFTER_EQUAL,
    PAT_BARE_LINE_PRICE,
    PAT_ANY_INLINE_PRICE_STRICT,
]

def _normalize_price_value__extra(s: str):
    """'1 200' / '1,200' -> 1200 (int)"""
    if not s:
        return None
    t = (s.replace(" ", "").replace(NBSP, "").replace(NNBSP, "").replace(",", "."))
    try:
        return int(round(float(t)))
    except Exception:
        return None

def _first_number_near_word_cena_same_line(line: str):
    """
    Узкий фоллбэк: первое число в пределах 25 символов ПОСЛЕ слова «цена»
    на той же строке. Игнорируем x6/×6/х6 и количества («1-пара», «5 шт» и т.п.).
    Не применяется, если строка похожа на размеры/артикул/контакты/«затяжки».
    """
    if not line:
        return None

    s   = line.replace(NBSP, " ").replace(NNBSP, " ")
    low = s.lower()
    pos = low.find("цена")
    if pos == -1:
        return None

    if _RX_IS_SIZEY.search(s) or _RX_IS_ART.search(s) or _RX_CONTACT.search(s) or _RX_PUFFS.search(s):
        return None

    nl = s.find("\n", pos)
    if nl == -1:
        nl = len(s)

    tail = s[pos + 4:nl][:25]  # 4 == len("цена")
    tail = _re_extra.sub(r"[x×х]\s*\d+\b", " ", tail)
    tail = _re_extra.sub(r"(?i)\b\d+\s*[-–—]?\s*(?:шт|штук|уп|упаковк\w*|коробк\w*|пачк\w*|пар[аи]?)\b", " ", tail)

    m = _re_extra.search(rf"{_NUM}", tail)
    return m.group(0) if m else None

def find_price_by_extra_patterns(line: str):
    """
    Возвращает (value_int, raw_match) или (None, None).
    Валюта опциональна ТОЛЬКО в контекстных паттернах. «Голая» цена допустима
    лишь отдельной строкой. Общий инлайн ловит только «число+валюта».
    """
    if not line:
        return None, None

    # 1) контекстные паттерны
    for pat in EXTRA_PRICE_PATTERNS:
        # PAT_BARE_LINE_PRICE и PAT_ANY_INLINE_PRICE_STRICT обработаем ниже
        if pat is PAT_BARE_LINE_PRICE or pat is PAT_ANY_INLINE_PRICE_STRICT:
            continue
        m = pat.search(line)
        if m:
            val = _normalize_price_value__extra(m.group(1))
            if val is not None:
                return val, m.group(0)

    # 2) «голая» цена отдельной строкой (и не «подозрительный» контекст)
    m = PAT_BARE_LINE_PRICE.search(line)
    if m and not (_RX_IS_SIZEY.search(line) or _RX_IS_ART.search(line) or _RX_CONTACT.search(line) or _RX_PUFFS.search(line)):
        val = _normalize_price_value__extra(m.group(1))
        if val is not None:
            return val, m.group(0)

    # 3) строгий инлайн «число+валюта»
    m = PAT_ANY_INLINE_PRICE_STRICT.search(line)
    if m:
        val = _normalize_price_value__extra(m.group(1))
        if val is not None:
            return val, m.group(0)

    # 4) узкий фоллбэк «рядом со словом цена»
    near = _first_number_near_word_cena_same_line(line)
    if near:
        val = _normalize_price_value__extra(near)
        if val is not None:
            return val, near

    return None, None
# ======== /EXTRA PRICE PATTERNS ========


# ================== FILE/PHOTO ID LOAD/SAVE ==================
def load_sent_ids():
    if os.path.exists(SENT_IDS_FILE):
        try:
            with open(SENT_IDS_FILE, "r", encoding="utf-8") as f:
                return set(x.strip() for x in f if x.strip())
        except Exception as e:
            add_log(f"Ошибка загрузки отправленных ID: {e}. Начинаем с пустого списка.")
            return set()
    return set()

def save_sent_ids(sent_ids):
    try:
        with open(SENT_IDS_FILE, "w", encoding="utf-8") as f:
            for i in sent_ids:
                f.write(f"{i}\n")
    except Exception as e:
        add_log(f"Ошибка сохранения отправленных ID: {e}")

def load_sent_photos():
    try:
        with open(SENT_PHOTOS_FILE, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    except FileNotFoundError:
        return set()

def save_sent_photo(photo_id):
    with open(SENT_PHOTOS_FILE, "a", encoding="utf-8") as f:
        f.write(f"{photo_id}\n")

# ================== GUI UTILS ==================
def add_paste_menu(entry_widget):
    menu = tk.Menu(entry_widget, tearoff=0)
    menu.add_command(label="Вставить", command=lambda: entry_widget.event_generate('<<Paste>>'))
    def show_menu(event):
        menu.tk_popup(event.x_root, event.y_root)
    entry_widget.bind("<Button-3>", show_menu)
    entry_widget.bind("<Button-2>", show_menu)
    entry_widget.bind('<Control-v>', lambda e: entry_widget.event_generate('<<Paste>>'))
    entry_widget.bind('<Control-V>', lambda e: entry_widget.event_generate('<<Paste>>'))
    def ctrl_v_rus(event):
        if (event.state & 0x4) and (event.char in ('м', 'М')):
            entry_widget.event_generate('<<Paste>>')
            return "break"
    entry_widget.bind('<KeyPress>', ctrl_v_rus)

def show_custom_input_dialog_with_link_in_text(root, title, instructions_lines, link_text, link_url, prompt="Введите значение:", default_value="", width=50):
    dialog = tk.Toplevel(root)
    dialog.title(title)
    dialog.geometry("650x400")
    dialog.grab_set()
    dialog.resizable(False, False)
    font_title = ("Arial", 17, "bold")
    font_text = ("Arial", 14)
    tk.Label(dialog, text=title, font=font_title).pack(pady=(18, 10))
    instr_frame = tk.Frame(dialog)
    instr_frame.pack(anchor="w", padx=18)
    text_widget = tk.Text(instr_frame, font=font_text, height=len(instructions_lines)+1, width=80, wrap="word", borderwidth=0, bg=dialog.cget("bg"))
    text_widget.pack()
    text_widget.tag_configure("link", foreground="blue", underline=1)
    text_widget.tag_bind("link", "<Button-1>", lambda e: webbrowser.open_new(link_url))
    text_widget.config(state="normal")
    for idx, line in enumerate(instructions_lines):
        if link_text in line:
            parts = line.split(link_text)
            text_widget.insert(tk.END, parts[0])
            text_widget.insert(tk.END, link_text, "link")
            text_widget.insert(tk.END, parts[1] if len(parts) > 1 else "")
        else:
            text_widget.insert(tk.END, line)
        text_widget.insert(tk.END, "\n")
    text_widget.config(state="disabled")
    tk.Label(dialog, text=prompt, font=font_text).pack(anchor="w", padx=18, pady=(25, 2))
    entry = tk.Entry(dialog, font=font_text, width=width)
    entry.insert(0, default_value)
    entry.pack(padx=18, pady=5)
    add_paste_menu(entry)
    result = {"value": None}
    def accept():
        val = entry.get().strip()
        if val:
            result["value"] = val
            dialog.destroy()
        else:
            messagebox.showwarning("Ошибка", "Пожалуйста, заполните поле!", parent=dialog)
    entry.bind("<Return>", lambda event: accept())
    btn = tk.Button(dialog, text="OK", font=font_text, command=accept)
    btn.pack(pady=18)
    entry.focus_set()
    dialog.wait_window()
    return result["value"]

def show_custom_input_dialog(root, title, instructions, links=None, prompt="Введите значение:", default_value="", width=50):
    dialog = tk.Toplevel(root)
    dialog.title(title)
    dialog.geometry("650x400")
    dialog.grab_set()
    dialog.resizable(False, False)
    font_title = ("Arial", 17, "bold")
    font_text = ("Arial", 14)
    font_link = ("Arial", 14, "underline")
    tk.Label(dialog, text=title, font=font_title).pack(pady=(18, 10))
    tk.Label(dialog, text=instructions, font=font_text, justify="left", wraplength=620).pack(anchor="w", padx=18)
    if links:
        for text, url in links:
            lbl = tk.Label(dialog, text=text, font=font_link, fg="blue", cursor="hand2")
            lbl.pack(anchor="w", padx=32, pady=(2, 0))
            lbl.bind("<Button-1>", lambda e, url=url: webbrowser.open_new(url))
    tk.Label(dialog, text=prompt, font=font_text).pack(anchor="w", padx=18, pady=(25, 2))
    entry = tk.Entry(dialog, font=font_text, width=width)
    entry.insert(0, default_value)
    entry.pack(padx=18, pady=5)
    add_paste_menu(entry)
    result = {"value": None}
    def accept():
        val = entry.get().strip()
        if val:
            result["value"] = val
            dialog.destroy()
        else:
            messagebox.showwarning("Ошибка", "Пожалуйста, заполните поле!", parent=dialog)
    entry.bind("<Return>", lambda event: accept())
    btn = tk.Button(dialog, text="OK", font=font_text, command=accept)
    btn.pack(pady=18)
    entry.focus_set()
    dialog.wait_window()
    return result["value"]

def initial_platform_wizard(root):
    print("Создаём окно wizard")
    dialog = tk.Toplevel(root)
    dialog.title("Выбор платформы")
    w, h = 350, 180
    ws = root.winfo_screenwidth()
    hs = root.winfo_screenheight()
    x = (ws - w) // 2
    y = (hs - h) // 2
    dialog.geometry(f"{w}x{h}+{x}+{y}")
    dialog.grab_set()
    dialog.transient(root)
    selected = {'platform': None}
    def choose(p):
        selected['platform'] = p
        dialog.destroy()
    label = tk.Label(dialog, text="Что настраивать?", font=("Arial", 14, "bold"))
    label.pack(pady=18)
    btn_vk = tk.Button(dialog, text="Только VK чат", width=22, font=("Arial", 13), command=lambda: choose("vk"))
    btn_both = tk.Button(dialog, text="VK чат + Telegram чат", width=22, font=("Arial", 13), command=lambda: choose("both"))
    btn_vk.pack(pady=8)
    btn_both.pack(pady=8)
    dialog.protocol("WM_DELETE_WINDOW", dialog.destroy)
    dialog.lift()
    dialog.focus_force()
    print("Перед wait_window")
    dialog.wait_window()
    print("После wait_window")
    return selected['platform']

def extract_vk_token(url):
    m = re.search(r'access_token=([a-zA-Z0-9\-_\.]+)', url)
    if m:
        return m.group(1)
    return None

def first_run_wizard_vk(root, settings):
    instructions_lines = [
        "1. Откройте сайт vkhost.github.io (нажмите на ссылку).",
        "2. Нажмите VK Admin.",
        "3. На открывшейся странице внизу нажмите \"Разрешить\".",
        "4. В открывшемся окне скопируйте адресную строку браузера и вставьте её ниже.",
        "5. Программа найдёт токен автоматически."
    ]
    url = show_custom_input_dialog_with_link_in_text(
        root,
        "Получение VK токена",
        instructions_lines,
        link_text="vkhost.github.io",
        link_url="https://vkhost.github.io/",
        prompt="Вставьте адрес из браузера:"
    )
    if not url:
        messagebox.showwarning("Ошибка", "Не введён токен VK")
        return False
    m = re.search(r'access_token=([^&]+)', url)
    if not m:
        messagebox.showerror("Ошибка", "Не удалось найти VK токен в строке. Проверьте адрес.")
        return False
    vk_token = m.group(1)
    settings["vk_token"] = vk_token
    chat_instr = (
        "Вставьте ссылку на ваш VK чат, куда бот будет выгружать фото.\n"
        "Пример: https://vk.com/im?sel=c32 или https://vk.com/im/convo/2000000032\n"
        "Из этой ссылки будет получен номер беседы/чата."
    )
    chat_links = [("Перейти в VK", "https://vk.com/im")]
    chat_url = show_custom_input_dialog(root, "Ссылка на чат VK", chat_instr, chat_links, prompt="Ссылка на чат VK:")
    if not chat_url:
        messagebox.showwarning("Ошибка", "Не введена ссылка на чат VK.")
        return False
    chat_id = None
    m = re.search(r'sel=c?(\d+)', chat_url)
    if m:
        chat_id = int(m.group(1))
    m = re.search(r'convo/2000000(\d+)', chat_url)
    if m:
        chat_id = int(m.group(1))
    if not chat_id:
        messagebox.showerror("Ошибка", "Не удалось найти номер чата в ссылке.")
        return False
    settings["vk_chat_id"] = chat_id
    return True

def telegram_wizard(root, settings):
    instructions_lines = [
        "1. Откройте @BotFather в Telegram (нажмите на ссылку).",
        "2. В боте нажмите СТАРТ, затем отправьте команду /newbot.",
        "3. Задайте имя боту и username (уникальный логин, оканчивающийся на bot, например chat-bot).",
        "4. После создания бота BotFather пришлёт токен, например:",
        "   123456789:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        "5. Скопируйте токен и вставьте его в поле ниже."
    ]
    tg_token = show_custom_input_dialog_with_link_in_text(
        root,
        "Получение токена Telegram-бота",
        instructions_lines,
        link_text='@BotFather',
        link_url='https://t.me/BotFather',
        prompt="Токен Telegram-бота:"
    )
    if not tg_token:
        messagebox.showwarning("Ошибка", "Не введён токен Telegram-бота")
        return False
    info_text = (
        "Теперь добавьте этого бота в ваш групповой чат, куда он должен отправлять сообщения.\n"
        "Обязательно выдайте боту права администратора (право писать сообщения).\n\n"
        "После этого отправьте ЛЮБОЕ сообщение в этот групповой чат.\n"
        "Ожидаем появления сообщения в группе для автоматического определения chat_id..."
    )
    messagebox.showinfo("Добавьте бота в группу", info_text, parent=root)
    chat_id_result = {"chat_id": None, "ok": False}
    async def catch_group_message(update, context):
        if update.message and update.message.chat and update.message.chat.type in ("group", "supergroup"):
            chat_id_result["chat_id"] = update.message.chat.id
            chat_id_result["ok"] = True
            await context.application.stop()
    def telegram_listener():
        asyncio.set_event_loop(asyncio.new_event_loop())
        app = Application.builder().token(tg_token).build()
        app.add_handler(MessageHandler(filters.ALL, catch_group_message))
        app.run_polling()
    listener_thread = threading.Thread(target=telegram_listener, daemon=True)
    listener_thread.start()
    wait_dialog = tk.Toplevel(root)
    wait_dialog.title("Ожидание сообщения...")
    wait_dialog.geometry("450x160")
    wait_dialog.grab_set()
    wait_dialog.resizable(False, False)
    lbl = tk.Label(wait_dialog, text="Ожидаем сообщение в вашем групповом чате Telegram...", font=("Arial", 14), wraplength=400)
    lbl.pack(pady=30)
    progress = tk.Label(wait_dialog, text="(Вы можете закрыть окно, чтобы отменить настройку)", font=("Arial", 11), fg="gray")
    progress.pack(pady=5)
    wait_dialog.update()
    for _ in range(60 * 10):
        wait_dialog.update()
        if chat_id_result["ok"]:
            break
        time.sleep(0.1)
        if not wait_dialog.winfo_exists():
            break
    wait_dialog.destroy()
    if chat_id_result["ok"] and chat_id_result["chat_id"]:
        settings["tg_token"] = tg_token
        settings["tg_chat_id"] = chat_id_result["chat_id"]
        messagebox.showinfo("Успех", f"chat_id Telegram-группы автоматически найден: {chat_id_result['chat_id']}", parent=root)
        return True
    else:
        messagebox.showerror("Ошибка", "Не удалось автоматически получить chat_id. Убедитесь, что бот добавлен в группу и вы отправили туда сообщение.", parent=root)
        return False

def add_log(msg):
    timestamp = datetime.datetime.now().strftime("[%H:%M:%S]")
    log_message = f"{timestamp} {msg}"
    if _global_log_window_instance and _global_log_window_instance.top.winfo_exists():
        try:
            _global_log_window_instance.top.after(0, lambda: _global_log_window_instance.append(log_message))
        except Exception as e:
            print(f"Ошибка при добавлении в лог (Tkinter after): {e} - {log_message}")
    else:
        print(log_message)

def message_passes_filters(text, antiwords, remove_links_flag=False, remove_emoji_flag=False):
    if remove_links_flag:
        text = remove_links(text)
    if remove_emoji_flag:
        text = remove_emojis(text)
    text_l = text.lower()
    for aw in antiwords:
        if aw.lower() in text_l:
            return False
    return True

def round_to_5(x):
    return int(round(x / 5.0)) * 5

def replace_emoji_numbers(text):
    # Маппинг эмодзи-цифр и символов в обычные цифры
    emoji_digit_map = {
        '0️⃣': '0', '1️⃣': '1', '2️⃣': '2', '3️⃣': '3', '4️⃣': '4', '5️⃣': '5', '6️⃣': '6',
        '7️⃣': '7', '8️⃣': '8', '9️⃣': '9', '𝟎': '0', '𝟏': '1', '𝟐': '2', '𝟑': '3',
        '𝟒': '4', '𝟓': '5', '𝟔': '6', '𝟕': '7', '𝟖': '8', '𝟗': '9'
    }
    for k, v in emoji_digit_map.items():
        text = text.replace(k, v)
    return text

def normalize_currency(text):
    # Заменяет все повторяющиеся валюты (₽ ₽ ₽) на одну
    return re.sub(r'((?:₽\s*){2,})', '₽ ', text)

_UNIT_WORDS = [
    "spф", "spf", "мл", "ml", "литр", "л", "l", "грамм", "гр", "г", "kg", "кг",
    "mg", "мг", "см", "mm", "мм", "m", "м", "метр", "ед", "штука", "штук", "шт",
    "pcs", "piece", "pack", "упаковка", "пара", "пар", "size", "объем", "объём",
    "длина", "ширина", "высота", "sp", "oz", "ounce", "унция", "таблетка",
    "табл", "доза", "dose"
]

def has_unit_words(s):
    for word in _UNIT_WORDS:
        if re.search(r'\b' + re.escape(word) + r'\b', s, re.IGNORECASE):
            return True
    return False

# ... далее в каждом паттерне:
PAT_SOLO_PRICE = re.compile(
    r'^\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({})\s*$'.format(_PRICE_CURRENCY_PATTERN), re.IGNORECASE
)

def process_line(line, percent, delta, stopwords, raw_text):

   
    """
    Унифицированная обработка строки с пересчётом цен.
    Идемпотентно: повторный вызов не меняет уже пересчитанные значения.
    """
    import re
    s = (line or "").strip()
    if not s:
        return line

    def norm_num_str(t: str) -> str:
        return t.replace("\u00A0", " ").replace("\u202f", " ").replace(" ", "").replace(",", ".")

    def to_float(t: str) -> float:
        t = norm_num_str(t)
        if t.count(".") > 1:
            t = t.replace(".", "", t.count(".") - 1)
        return float(t)

    def reprice(raw_num: str, currency: str) -> str:
        try:
            val = to_float(raw_num)
            new_v = round_to_5(val * (1 + float(percent)/100.0) + float(delta))
            return f"{int(new_v)}{currency}"
        except Exception:
            return f"{raw_num}{currency}"

    CUR = r"(?:₽|руб(?:\.|лей)?|р\.?|р)"
    NUM = r"\d{1,3}(?:[ \u00A0\u202f]?\d{3})*(?:[.,]\d{1,2})?|\d+(?:[.,]\d{1,2})?"

    out = s

    # === SIZE + PRICE on one line: "50/70-100р" / "70x70 - 100 руб" -> репрайс цены ===
    _PAT_SIZE_PRICE = re.compile(
        r'(?i)\b(\d{2,3})\s*[x/]\s*(\d{2,3})\s*[-–—:]\s*'
        r'(\d[\d \u202f]*[.,]?\d*)\s*(₽|руб(?:\.|лей)?|р\.?|р)?\b'
    )

    # === "Размер ... : Цена 300р" — репрайсим только цену, контекст (размеры/метки) сохраняем ===
    _PAT_SIZE_LINE_WITH_PRICE = re.compile(
        rf"(?i)\bразмер[^\n:]*?:{_SP}цена{_SP}[:=\-–—]?{_SP}"
        rf"({_NUM}){_SP}({_CUR})\b"
    )
    def _repl_size_line_with_price(m):
        raw = m.group(1)
        cur = m.group(2) or 'р'
        try:
            new_v = round_to_5(to_float(raw) * (1 + float(percent)/100.0) + float(delta))
            # Меняем ровно диапазон [start(1), end(2)) — покрывает и "300р", и "300 р", и "3 000 р"
            return m.string[:m.start(1)] + f"{int(new_v)}{cur}" + m.string[m.end(2):]
        except Exception:
            return m.group(0)

    new_out2 = _PAT_SIZE_LINE_WITH_PRICE.sub(_repl_size_line_with_price, out)
    if new_out2 != out:
        return new_out2


    def _repl_size_price(m):
        w, h = m.group(1), m.group(2)
        raw = m.group(3)
        cur = m.group(4) or 'р'
        val = float(raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
        new = round_to_5(val * (1 + float(percent)/100.0) + float(delta))
        # сохраняем формат исходной строки: "50/70 - 275р"
        return f"{w}/{h} - {new}{cur}"

    new_out = _PAT_SIZE_PRICE.sub(_repl_size_price, out)
    if new_out != out:
        return new_out


    # === EXTRA: быстрый матч цен по точечным паттернам (всегда пробуем) ===
    _extra_val, _extra_raw = find_price_by_extra_patterns(out)
    if _extra_val is not None:
        return f"Цена {reprice(str(_extra_val), 'р')}"

    # «Цена штучно/поштучно/за штуку: 399руб»
    rx_cena_shtuchno = re.compile(
        rf"(?i)\bцена\s*(?:штучно|по[ -]?штучно|за\s*штуку)\s*[:=\-–—]?\s*({NUM})\s*({CUR})\b"
    )
    out = rx_cena_shtuchno.sub(lambda m: f"Цена {reprice(m.group(1), m.group(2))}", out)


    rx_any_price = re.compile(
        rf"(?i)\bцена\s*[:=\-–—]?\s*(?P<num>{NUM})\s*(?P<cur>{CUR})\b"
        rf"|\bцена\s*[:=\-–—]?\s*(?P<num2>{NUM})(?!\s*\w)"
    )
    def repl_any(m):
        raw = m.group('num') if m.group('num') else m.group('num2')
        cur = m.group('cur') or 'р'
        return f"Цена {reprice(raw, cur)}"
    out = rx_any_price.sub(repl_any, out)

    if not re.search(r"(?i)\b(в\s*упаковке|упаковк\w*|уп\b|пачк\w*|коробк\w*|набор)\b", out):
        rx_qty_mul_eq = re.compile(
            rf"(?i)\b(\d{{1,5}})\s*шт\w*\s*[*x×х]\s*"
            rf"({NUM})\s*"
            rf"({CUR})\s*=\s*"
            rf"({NUM})\s*"
            rf"({CUR})\b"
        )
        def repl_mul(m):
            qty = int(m.group(1))
            unit_raw = m.group(2); cur = m.group(3)
            try:
                unit_new = round_to_5(to_float(unit_raw) * (1 + float(percent)/100.0) + float(delta))
                total_new = round_to_5(qty * unit_new)
                return f"{qty}шт*{int(unit_new)}{cur}={int(total_new)}{cur}"
            except Exception:
                return m.group(0)
        out = rx_qty_mul_eq.sub(repl_mul, out)

    rx_paren = re.compile(rf"(?i)\((?:[^()]*)?(?:по|цена)\s*[:=\-–—]?\s*({NUM})\s*({CUR})\)")
    out = rx_paren.sub(lambda m: m.group(0).replace(m.group(1)+m.group(2), reprice(m.group(1), m.group(2))), out)

    rx_npcs = re.compile(rf"(?i)\b(\d+)\s*шт\w*\s*[:=\-–—]\s*({NUM})\s*({CUR})\b")
    out = rx_npcs.sub(lambda m: f"{m.group(1)}шт: {reprice(m.group(2), m.group(3))}", out)

    rx_cena_za = re.compile(rf"(?i)\b(цена\s*за\s*[^:()\n]+?\s*[:=\-–—]?\s*)({NUM})\s*({CUR})\b")
    out = rx_cena_za.sub(lambda m: f"{m.group(1)}{reprice(m.group(2), m.group(3))}", out)

    rx_tail = re.compile(rf"(?i)^(?=.*размер)(?=.*\d).*?(\b{NUM})\s*({CUR})\s*$")
    def repl_tail(m):
        start = m.start(1)
        return m.string[:start] + reprice(m.group(1), m.group(2))
    out = rx_tail.sub(repl_tail, out)

    UNIT = r"(?:шт|штук|щт|пар|пара|пары)"
    rx_unit_before = re.compile(rf"(?i)(?:^|[\s,;\-–—])(?:цена\s*[:\-–—=]?\s*)?(?:{UNIT})\s*[:\-–—=]?\s*({NUM})\s*({CUR})\b")
    out = rx_unit_before.sub(lambda m: m.group(0).replace(m.group(1)+m.group(2), reprice(m.group(1), m.group(2))), out)

    rx_dash = re.compile(rf"(?i)^\s*[\-–—]\s*цена\s*[:\-–—=]?\s*(?:{UNIT})?\s*[:\-–—=]?\s*({NUM})\s*({CUR})\b")
    out = rx_dash.sub(lambda m: m.group(0).replace(m.group(1)+m.group(2), reprice(m.group(1), m.group(2))), out)

    rx_cena_po = re.compile(rf"(?i)(цена\s*по\s*)({NUM})\s*({CUR})")
    out = rx_cena_po.sub(lambda m: f"{m.group(1)}{reprice(m.group(2), m.group(3))}", out)

    rx_opt = re.compile(rf"(?i)(опт[^\n,;]*)?(\b\d+)\s*[\*x×]\s*({NUM})\s*({CUR})")
    def repl_opt(m):
        prefix = (m.group(1) or "").rstrip()
        qty = int(m.group(2))
        price_raw = m.group(3); cur = m.group(4)
        repriced = reprice(price_raw, cur)
        sep = re.search(r"[\*x×]", m.group(0)).group(0)
        return (prefix + " " if prefix else "") + f"{qty}{sep}{repriced}"
    out = rx_opt.sub(repl_opt, out)

    return out
    def _repl_npcs_dash_price(m):
        qty = m.group('qty')
        raw = m.group('num')
        cur = (m.group('cur') or 'р')
        try:
            v = float(raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
            nv = round_to_5(v * (1 + float(percent)/100.0) + float(delta))
            return f'Цена за {qty} шт {nv}{cur}'
        except Exception:
            return m.group(0)
    _tmp = _PAT_NPCS_DASH_PRICE.sub(_repl_npcs_dash_price, line)
    if _tmp != line:
        return _tmp

    # --- SINGLE-PASS: 'Цена …' (один проход и сразу return) ---
    # Защита 'до-<число>' от ложной цены (до-30, до-10 и т.п.)
    line = re.sub(r'\bдо\s*-\s*(\d{1,3})(?!\s*(?:₽|руб|р\b))', r'до-\1', line, flags=re.IGNORECASE)

    _ANY_PRICE = re.compile(
        r'\bцена\s*[:=\-–—]?\s*(?P<num>\d[\d\s\u202f]*[.,]?\d*)\s*'
        r'(?P<cur>₽|руб(?:\.|лей)?|р\.?|р)\b'
        r'|(?:цена)\s*[:=\-–—]?\s*(?P<num2>\d[\d\s\u202f]*[.,]?\d*)(?=\D|$)',
        re.IGNORECASE
    )
    def _repl_any(m):
        raw = m.group('num') if m.group('num') else m.group('num2')
        cur = m.group('cur') if m.group('cur') else 'р'
        try:
            v = float(raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
            nv = round_to_5(v * (1 + float(percent)/100.0) + float(delta))
            return f'Цена {nv}{cur}'
        except Exception:
            return m.group(0)
    _once = _ANY_PRICE.sub(_repl_any, line)
    if _once != line:
        return _once

    # --- Безопасный qty*price = total (НЕ трогаем количество), если это не упаковка/набор ---
    if not re.search(r'\b(в\s*упаковке|упаковк\w*|уп\b|пачк\w*|коробк\w*|набор)\b', line, flags=re.IGNORECASE):
        _PAT_QTY_TIMES_PRICE_EQ = re.compile(
            r'\b(?<!\d)(\d{1,5})\s*шт\w*\s*[*x×х]\s*'
            r'(\d[\d\s\u202f]*[.,]?\d*)\s*'
            r'(₽|руб(?:\.|лей)?|р\.?|р)\s*=\s*'
            r'(\d[\d\s\u202f]*[.,]?\d*)\s*'
            r'(₽|руб(?:\.|лей)?|р\.?|р)\b',
            re.IGNORECASE
        )
        def _repl_qty_times_price_eq(m):
            qty = m.group(1)
            unit_raw, unit_curr = m.group(2), (m.group(3) or 'р')
            try:
                unit_val = float(unit_raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
                new_unit = round_to_5(unit_val * (1 + float(percent)/100.0) + float(delta))
                new_total = round_to_5(float(qty) * new_unit)
                return f"{qty}шт*{new_unit}{unit_curr}={new_total}{unit_curr}"
            except Exception:
                return m.group(0)
        _nl = _PAT_QTY_TIMES_PRICE_EQ.sub(_repl_qty_times_price_eq, line)
        if _nl != line:
            line = _nl

    # --- Цена в скобках "(… по 130р)" или "(… цена 130р)" ---
    _PAT_PRICE_IN_PARENS = re.compile(
        r'\(\s*(?:от\s*\d+\s*\S*)?(?:[^()\n]*?)?(?:по|цена)\s*[:=\-–—]?\s*'
        r'(\d[\d\s\u202f]*[.,]?\d*)\s*'
        r'(₽|руб(?:\.|лей)?|р\.?|р)\s*\)',
        re.IGNORECASE
    )
    def _repl_price_in_parens(m):
        raw, cur = m.group(1), (m.group(2) or 'р')
        try:
            val = float(raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
            newv = round_to_5(val * (1 + float(percent)/100.0) + float(delta))
            return m.group(0).replace(m.group(1)+m.group(2), f"{newv}{cur}")
        except Exception:
            return m.group(0)
    _nl = _PAT_PRICE_IN_PARENS.sub(_repl_price_in_parens, line)
    if _nl != line:
        line = _nl

    # --- "Nшт: 400р" (без умножения на N) ---
    _PAT_NPCS_PRICE = re.compile(
        r'\b(\d+)\s*шт\w*\s*[:=\-–—]\s*'
        r'(\d[\d\s\u202f]*[.,]?\d*)\s*'
        r'(₽|руб(?:\.|лей)?|р\.?|р)\b',
        re.IGNORECASE
    )
    def _repl_npcs_price(m):
        qty, raw, cur = m.group(1), m.group(2), (m.group(3) or 'р')
        try:
            val = float(raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
            newv = round_to_5(val * (1 + float(percent)/100.0) + float(delta))
            return f"{qty}шт: {newv}{cur}"
        except Exception:
            return m.group(0)
    _nl = _PAT_NPCS_PRICE.sub(_repl_npcs_price, line)
    if _nl != line:
        line = _nl

    # --- "Цена за <что-то> 100р" ---
    _PAT_CENA_ZA_SOMETHING = re.compile(
        r'\b(цена\s*за\s*[^\s:()]+?\s*[:=\-–—]?\s*)'
        r'(\d[\d\s\u202f]*[.,]?\d*)\s*'
        r'(₽|руб(?:\.|лей)?|р\.?|р)\b',
        re.IGNORECASE
    )
    def _repl_cena_za(m):
        pref, raw, cur = m.group(1), m.group(2), (m.group(3) or 'р')
        try:
            val = float(raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
            newv = round_to_5(val * (1 + float(percent)/100.0) + float(delta))
            return f"{pref}{newv}{cur}"
        except Exception:
            return m.group(0)
    _nl = _PAT_CENA_ZА_SOMETHING.sub(_repl_cena_za, line) if False else None
    if _nl:
        line = _nl

    # --- Цена в конце строки после размеров: "… 600р" ---
    _PAT_SIZE_TAIL_PRICE = re.compile(
        r'^(?=.*размер)(?=.*\d).*?'
        r'(\d[\d\s\u202f]*[.,]?\d*)\s*'
        r'(₽|руб(?:\.|лей)?|р\.?|р)\s*$',
        re.IGNORECASE
    )
    def _repl_size_tail(m):
        raw, cur = m.group(1), (m.group(2) or 'р')
        try:
            val = float(raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
            newv = round_to_5(val * (1 + float(percent)/100.0) + float(delta))
            return m.string[:m.start(1)] + f"{newv}{cur}"
        except Exception:
            return m.group(0)
    _nl = _PAT_SIZE_TAIL_PRICE.sub(_repl_size_tail, line)
    if _nl != line:
        line = _nl

    return line

    def _num_to_float(_raw: str) -> float:
        return float(_raw.replace(' ', '').replace('\u202f','').replace(',', '.'))

    # 0) Protect 'до-<число>' tokens (e.g., "до-30") from being misread as price
    line = _re.sub(r'\bдо\s*-\s*(\d{1,3})(?!\s*(?:₽|руб|р\b))', r'до-\1', line, flags=_re.IGNORECASE)

    # 1) SAFE qty*price with equality: "10шт*12р=120р" (never change qty), skip packaging/sets
    if not _re.search(r'\b(в\s*упаковке|упаковк\w*|уп\b|пачк\w*|коробк\w*|набор)\b', line, flags=_re.IGNORECASE):
        _PAT_QTY_TIMES_PRICE_EQ = _re.compile(
            r'\b(?<!\d)(\d{1,5})\s*шт\w*\s*[*x×х]\s*'
            r'(\d[\d\s\u202f]*[.,]?\d*)\s*(₽|руб(?:\.|лей)?|р\.?|р)\s*=\s*'
            r'(\d[\d\s\u202f]*[.,]?\d*)\s*(₽|руб(?:\.|лей)?|р\.?|р)\b',
            _re.IGNORECASE
        )
        def _repl_qty_times_price_eq(m):
            qty = m.group(1)
            unit_raw, unit_curr = m.group(2), (m.group(3) or 'р')
            total_raw, total_curr = m.group(4), (m.group(5) or 'р')
            try:
                unit_val = _num_to_float(unit_raw)
                new_unit = round_to_5(unit_val * (1 + float(percent)/100.0) + float(delta))
                new_total = round_to_5(float(qty) * new_unit)
                return f"{qty}шт*{new_unit}{unit_curr}={new_total}{total_curr}"
            except Exception:
                return m.group(0)
        _nl = _PAT_QTY_TIMES_PRICE_EQ.sub(_repl_qty_times_price_eq, line)
        if _nl != line:
            line = _nl

    # 2) SINGLE-PASS for any 'Цена ...' (handles glued forms). If matched -> RETURN early.
    _ANY_PRICE = _re.compile(
        r'\bцена\s*[:=\-–—]?\s*(?P<num1>\d[\d\s\u202f]*[.,]?\d*)\s*(?P<cur1>₽|руб(?:\.|лей)?|р\.?|р)\b'
        r'|(?:цена)\s*[:=\-–—]?\s*(?P<num2>\d[\d\s\u202f]*[.,]?\d*)(?=\D|$)',
        _re.IGNORECASE
    )
    def _repl_any(m):
        raw = m.group('num1') if m.group('num1') is not None else m.group('num2')
        curr = m.group('cur1') if m.group('cur1') is not None else 'р'
        try:
            val = _num_to_float(raw)
            newv = round_to_5(val * (1 + float(percent)/100.0) + float(delta))
            return f'Цена {newv}{curr}'
        except Exception:
            return m.group(0)
    _sp = _ANY_PRICE.sub(_repl_any, line)
    if _sp != line:
        return _sp

    # 3) Extra patterns for lines WITHOUT the word 'цена'
    # (a) Price in parentheses: "( ... по 130р )" / "( ... цена 130р )"
    _PAT_PRICE_IN_PARENS = _re.compile(
        r'\(\s*(?:от\s*\d+\s*\S*)?(?:[^()\n]*?)?(?:по|цена)\s*[:=\-–—]?\s*'
        r'(\d[\d\s\u202f]*[.,]?\d*)\s*(₽|руб(?:\.|лей)?|р\.?|р)\s*\)',
        _re.IGNORECASE
    )
    def _repl_price_in_parens(m):
        raw, cur = m.group(1), (m.group(2) or 'р')
        try:
            val = _num_to_float(raw)
            newv = round_to_5(val * (1 + float(percent)/100.0) + float(delta))
            return m.group(0).replace(m.group(1)+m.group(2), f"{newv}{cur}")
        except Exception:
            return m.group(0)
    _nl = _PAT_PRICE_IN_PARENS.sub(_repl_price_in_parens, line)
    if _nl != line:
        line = _nl

    # (b) "10шт: 400р" / "10 шт - 400р" — just reprice the value (no qty multiply here)
    _PAT_NPCS_PRICE = _re.compile(
        r'\b(\d+)\s*шт\w*\s*[:=\-–—]\s*(\d[\d\s\u202f]*[.,]?\d*)\s*(₽|руб(?:\.|лей)?|р\.?|р)\b',
        _re.IGNORECASE
    )
    def _repl_npcs_price(m):
        qty, raw, cur = m.group(1), m.group(2), (m.group(3) or 'р')
        try:
            val = _num_to_float(raw)
            newv = round_to_5(val * (1 + float(percent)/100.0) + float(delta))
            return f"{qty}шт: {newv}{cur}"
        except Exception:
            return m.group(0)
    _nl = _PAT_NPCS_PRICE.sub(_repl_npcs_price, line)
    if _nl != line:
        line = _nl

    # (c) Generic "Цена за <что-то> 100р" in case 'цена' was absent earlier (rare)
    _PAT_CENA_ZA_SOMETHING = _re.compile(
        r'\b(цена\s*за\s*[^\s:()]+?\s*[:=\-–—]?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*(₽|руб(?:\.|лей)?|р\.?|р)\b',
        _re.IGNORECASE
    )
    def _repl_cena_za(m):
        pref, raw, cur = m.group(1), m.group(2), (m.group(3) or 'р')
        try:
            val = _num_to_float(raw)
            newv = round_to_5(val * (1 + float(percent)/100.0) + float(delta))
            return f"{pref}{newv}{cur}"
        except Exception:
            return m.group(0)
    _nl = _PAT_CENA_ZA_SOMETHING.sub(_repl_cena_za, line)
    if _nl != line:
        line = _nl

    # (d) Tail price after sizes: "... 600р" at line end when line mentions size
    _PAT_SIZE_TAIL_PRICE = _re.compile(
        r'^(?=.*размер)(?=.*\d).*?(\d[\d\s\u202f]*[.,]?\d*)\s*(₽|руб(?:\.|лей)?|р\.?|р)\s*$',
        _re.IGNORECASE
    )
    def _repl_size_tail(m):
        raw, cur = m.group(1), (m.group(2) or 'р')
        try:
            val = _num_to_float(raw)
            newv = round_to_5(val * (1 + float(percent)/100.0) + float(delta))
            return m.string[:m.start(1)] + f"{newv}{cur}"
        except Exception:
            return m.group(0)
    _nl = _PAT_SIZE_TAIL_PRICE.sub(_repl_size_tail, line)
    if _nl != line:
        line = _nl
    # ===== END CONSOLIDATED PRICE LOGIC =====
    # PACKAGING presence guard (prevents qty×price logic for package/sets)
    _HAS_PACKING = bool(re.search(r'(?i)\b(в\s*упаковке|упаковк\w*|пачк\w*|коробк\w*|набор\b)', line))

    # === PATCH: доп. ценовые паттерны и множественные цены в одной строке ===
    # Универсальный helper пересчёта
    def _recalc_val(raw):
        try:
            v = float(raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
            return round_to_5(v * (1 + float(percent)/100.0) + float(delta))
        except Exception:
            return None

    # 0) MULTI: несколько "... цена <n><валюта>" в одной строке (например, "Размеры ... цена 350р Размеры ... Цена 400р")
    _PAT_MULTI_SIZE_PRICE = re.compile(
        r'(?i)(\bразмер\w*[^\\n]*?\\bцена\\s*[:=\\-–—]?\\s*)(\\d[\\d\\s\\u202f]*[.,]?\\d*)(\\s*(?:₽|руб(?:\\.|лей)?|р\\.?|р)\\b)'
    )
    def _repl_multi_size_price(m):
        prefix, raw, curr = m.group(1), m.group(2), m.group(3) or ''
        newv = _recalc_val(raw)
        if newv is None:
            return m.group(0)
        return f'{prefix}{newv}{curr}'
    tmp_line = _PAT_MULTI_SIZE_PRICE.sub(_repl_multi_size_price, line)
    if tmp_line != line:
        line = tmp_line

    # 1) "Цена 150р" / "Цена150р" / "цена: 150 ₽"
    _PAT_CENA_SIMPLE = re.compile(
        r'(?i)\\bцена\\s*[:=\\-–—]?\\s*(\\d[\\d\\s\\u202f]*[.,]?\\d*)\\s*(₽|руб(?:\\.|лей)?|р\\.?|р)\\b'
    )
    def _repl_cena_simple(m):
        raw, curr = m.group(1), m.group(2) or ''
        newv = _recalc_val(raw)
        if newv is None:
            return m.group(0)
        return f'Цена {newv}{curr}'
    tmp_line = _PAT_CENA_SIMPLE.sub(_repl_cena_simple, line)
    if tmp_line != line:
        line = tmp_line

    # 2) "Цена за упаковку/набор: 620р"
    _PAT_CENA_UPAK_NABOR = re.compile(
        r'(?i)\\b(цена\\s*(?:за\\s*(?:упаковк\\w*|набор))\\s*[:=\\-–—]?\\s*)(\\d[\\d\\s\\u202f]*[.,]?\\d*)\\s*(₽|руб(?:\\.|лей)?|р\\.?|р)\\b'
    )
    def _repl_cena_upak_nabor(m):
        prefix, raw, curr = m.group(1), m.group(2), m.group(3) or ''
        newv = _recalc_val(raw)
        if newv is None:
            return m.group(0)
        return f'{prefix}{newv}{curr}'
    tmp_line = _PAT_CENA_UPAK_NABOR.sub(_repl_cena_upak_nabor, line)
    if tmp_line != line:
        line = tmp_line

    # 3) "Размер: ... цена 450р" — на случай, если нет слова "размеры" раньше по строке
    _PAT_CENA_AFTER_SIZE = re.compile(
        r'(?i)(размер[^\\n]*?\\bцена\\s*[:=\\-–—]?\\s*)(\\d[\\d\\s\\u202f]*[.,]?\\d*)(\\s*(?:₽|руб(?:\\.|лей)?|р\\.?|р)\\b)'
    )
    def _repl_cena_after_size(m):
        prefix, raw, curr = m.group(1), m.group(2), m.group(3) or ''
        newv = _recalc_val(raw)
        if newv is None:
            return m.group(0)
        return f'{prefix}{newv}{curr}'
    tmp_line = _PAT_CENA_AFTER_SIZE.sub(_repl_cena_after_size, line)
    if tmp_line != line:
        line = tmp_line
    # === END PATCH ===
    # --- init guards to avoid UnboundLocalError ---
    try:
        s
    except NameError:
        s = (line or '').strip()
    try:
        s_lower
    except NameError:
        s_lower = s.lower()
    # === PATCH: price patterns for 'шт 300р', '-цена щт : 550рублей', 'цена по 250р' and 'опт 5*220р' ===
    try:
        _PRICE_CURRENCY_PATTERN = r'(?:рублей|руб\.|руб|р\.|р|Р\.|Р|rub\.|rub|r\.|r|p\.|p|py6|₽|\u20bd|[оoO0])'
        _UNIT_ST = r'(?:шт|штук|щт|пар|пара|пары)'
        import re as _re_patch
        # 1) 'Цена: шт 300р' / 'шт 300р' / 'щт 300р'
        _PAT_UNIT_BEFORE_PRICE = _re_patch.compile(
            rf'(?:^|[\s,;\-–—])(?:цена\s*[:\-–—=]?\s*)?(?:{_UNIT_ST})\s*[:\-–—=]?\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({_PRICE_CURRENCY_PATTERN})\b',
            _re_patch.IGNORECASE
        )
        def _repl_unit_before_price(m):
            price_raw = m.group(1); curr = m.group(2) or ""
            try:
                orig = float(price_raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
                final = round_to_5(orig * (1 + float(percent)/100.0) + float(delta))
            except Exception:
                return m.group(0)
            return _re_patch.sub(r'(\d[\d\s\u202f]*[.,]?\d*)\s*' + _PRICE_CURRENCY_PATTERN, f"{final}"+curr, m.group(0), flags=_re_patch.IGNORECASE)

        new_line = _PAT_UNIT_BEFORE_PRICE.sub(_repl_unit_before_price, line)
        if new_line != line:
            return new_line

        # 2) '-цена щт : 550рублей'
        _PAT_DASH_PRICE = _re_patch.compile(
            rf'^\s*[\-–—]\s*цена\s*[:\-–—=]?\s*(?:{_UNIT_ST})?\s*[:\-–—=]?\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({_PRICE_CURRENCY_PATTERN})\b',
            _re_patch.IGNORECASE
        )
        def _repl_dash_price(m):
            price_raw = m.group(1); curr = m.group(2) or ""
            try:
                orig = float(price_raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
                final = round_to_5(orig * (1 + float(percent)/100.0) + float(delta))
            except Exception:
                return m.group(0)
            return _re_patch.sub(r'(\d[\d\s\u202f]*[.,]?\d*)\s*' + _PRICE_CURRENCY_PATTERN, f"{final}"+curr, m.group(0), flags=_re_patch.IGNORECASE)

        new_line = _PAT_DASH_PRICE.sub(_repl_dash_price, line)
        if new_line != line:
            return new_line

        # 3a) 'цена по 250р'
        _PAT_CENA_PO = _re_patch.compile(
            rf'(цена\s*по\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({_PRICE_CURRENCY_PATTERN})',
            _re_patch.IGNORECASE
        )
        def _repl_cena_po(m):
            prefix = m.group(1); price_raw = m.group(2); curr = m.group(3) or ""
            try:
                orig = float(price_raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
                final = round_to_5(orig * (1 + float(percent)/100.0) + float(delta))
            except Exception:
                return m.group(0)
            return f"{prefix}{final}{curr}"

        new_line = _PAT_CENA_PO.sub(_repl_cena_po, line)
        if new_line != line:
            return new_line

        # 3b) 'опт 5*220р' / 'опт 5x220р' / 'опт 5×220р'
        _PAT_OPT_MULT = _re_patch.compile(
            rf'(опт[^\n,;]*)?(\b\d+)\s*[\*x×]\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({_PRICE_CURRENCY_PATTERN})',
            _re_patch.IGNORECASE
        )
        def _repl_opt_mult(m):
            qty = m.group(2); price_raw = m.group(3); curr = m.group(4) or ""
            try:
                orig = float(price_raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
                final = round_to_5(orig * (1 + float(percent)/100.0) + float(delta))
            except Exception:
                return m.group(0)
            sep_match = _re_patch.search(r'[\*x×]', m.group(0))
            sep = sep_match.group(0) if sep_match else '*'
            prefix = (m.group(1) or '').rstrip()
            if prefix: prefix += ' '
            return f"{prefix}{qty}{sep}{final}{curr}"

        new_line = _PAT_OPT_MULT.sub(_repl_opt_mult, line)
        if new_line != line:
            return new_line

    except Exception:
        # Fail-safe: don't break processing if patch block errors; just continue with original logic
        pass
    # === END PATCH ===
    global replace_emoji_numbers, normalize_currency, has_unit_words, is_size_line, round_to_5, fix_currency_typos
    # --- Функции-заглушки (замените на свои, если есть) ---

    # --- Константы и преобразования типов в начале ---
    _PRICE_CURRENCY_PATTERN = (
        r'(?:рублей|руб\.|руб|р\.|р|Р\.|Р|rub\.|rub|r\.|r|p\.|p|py6|₽|\u20bd|[оoO0])'
    )
    percent = float(percent)
    delta = int(delta)

    # --- Первая фильтрация по размерам ---
    s = line.strip()
    if is_size_line(s):
        return line

    # --- Основная обработка строки ---
    line = replace_emoji_numbers(line)
    line = normalize_currency(line)
    line = fix_currency_typos(line)

    # === EXTRA PRICE PATTERNS (auto-injected) ===
    def _num(v: str) -> float:
        return float(v.replace(' ', '').replace('\u202f','').replace(',', '.'))
    def _reprice(raw: str, curr: str) -> str:
        try:
            val = _num(raw); newv = round_to_5(val * (1 + float(percent)/100.0) + float(delta))
            return f"{newv}{curr or 'р'}"
        except Exception:
            return f"{raw}{curr or 'р'}"

    _PAT_PRICE_IN_PARENS = re.compile(
        r"\(\s*(?:от\s*\d+\s*\S*)?(?:[^()\n]*?)?(?:по|цена)\s*[:=\-–—]?\s*"
        r"(\d[\d\s\u202f]*[.,]?\d*)\s*(₽|руб(?:\.|лей)?|р\.?|р)\s*\)", re.IGNORECASE)
    line = _PAT_PRICE_IN_PARENS.sub(lambda m: m.group(0).replace(m.group(1)+m.group(2), _reprice(m.group(1), m.group(2))), line)

    _PAT_CENA_VYBOR = re.compile(
        r"\b(цена\s*(?:с\s*выбором|за\s*выбор)\s*[:=\-–—]?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*"
        r"(₽|руб(?:\.|лей)?|р\.?|р)\b", re.IGNORECASE)
    line = _PAT_CENA_VYBOR.sub(lambda m: f"{m.group(1)}{_reprice(m.group(2), m.group(3))}", line)

    _PAT_NPCS_PRICE = re.compile(
        r"\b(\d+)\s*шт\w*\s*[:=\-–—]\s*(\d[\d\s\u202f]*[.,]?\d*)\s*"
        r"(₽|руб(?:\.|лей)?|р\.?|р)\b", re.IGNORECASE)
    line = _PAT_NPCS_PRICE.sub(lambda m: f"{m.group(1)}шт: {_reprice(m.group(2), m.group(3))}", line)

    _PAT_CENA_ZA_SOMETHING = re.compile(
        r"\b(цена\s*за\s*[^\s:()]+?\s*[:=\-–—]?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*"
        r"(₽|руб(?:\.|лей)?|р\.?|р)\b", re.IGNORECASE)
    line = _PAT_CENA_ZA_SOMETHING.sub(lambda m: f"{m.group(1)}{_reprice(m.group(2), m.group(3))}", line)

    _PAT_SIZE_TAIL_PRICE = re.compile(
        r"(?i)^(?=.*размер)(?=.*\d).*?(\d[\d\s\u202f]*[.,]?\d*)\s*(₽|руб(?:\.|лей)?|р\.?|р)\s*$")
    line = _PAT_SIZE_TAIL_PRICE.sub(lambda m: m.string[:m.start(1)] + _reprice(m.group(1), m.group(2)), line)

    _PAT_CENA_SIMPLE = re.compile(
        r"(?i)\bцена\s*[:=\-–—]?\s*(\d[\d\s\u202f]*[.,]?\d*)\s*(₽|руб(?:\.|лей)?|р\.?|р)\b")
    line = _PAT_CENA_SIMPLE.sub(lambda m: f"Цена {_reprice(m.group(1), m.group(2))}", line)
    # === END EXTRA PRICE PATTERNS (auto-injected) ===
    s = line.strip()
    s_lower = s.lower()

    # === EARLY: "Nшт-<цена>" как цена за набор (1 проход и сразу выходим) ===
    _PAT_NPCS_DASH_PRICE = re.compile(
        r'\b(?<!\d)(?P<qty>\d{1,4})\s*ш\w*\s*[:=\-–—]\s*'
        r'(?P<num>\d[\d\s\u202f]*[.,]?\d*)\s*(?P<cur>₽|руб(?:\.|лей)?|р\.?|р)\b',
        re.IGNORECASE
    )
    def _repl_npcs_dash_price(m):
        qty = m.group('qty')                       # количество только показываем
        raw = m.group('num')                       # число из текста
        cur = (m.group('cur') or 'р')
        try:
            v = float(raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
            nv = round_to_5(v * (1 + float(percent)/100.0) + float(delta))
            # формируем понятную строку и СРАЗУ выходим, чтобы не было повторной накрутки
            return f'Цена за {qty} шт {nv}{cur}'
        except Exception:
            return m.group(0)

    _tmp = _PAT_NPCS_DASH_PRICE.sub(_repl_npcs_dash_price, line)
    if _tmp != line:
        return _tmp
    # === /EARLY ===

    # === SINGLE-PASS (один пересчёт и сразу выходим) ===
    ___priced_once = False  # sentinel на случай повторного входа в блоки ниже

    # защищаем "до-30" и подобные (скидки/температуры), чтобы не путать с ценой
    line = re.sub(r'\bдо\s*-\s*(\d{1,3})(?!\s*(?:₽|руб|р\b))', r'до-\1', line, flags=re.IGNORECASE)

    # безопасный "10шт*12р=120р": не трогаем qty, только цены; выключен для упаковок/наборов
    if not re.search(r'\b(в\s*упаковке|упаковк\w*|уп\b|пачк\w*|коробк\w*|набор)\b', line,     flags=re.IGNORECASE):
        _PAT_QTY_TIMES_PRICE_EQ = re.compile(
            r'\b(?<!\d)(\d{1,5})\s*шт\w*\s*[*x×х]\s*'
            r'(\d[\d\s\u202f]*[.,]?\d*)\s*(₽|руб(?:\.|лей)?|р\.?|р)\s*=\s*'
            r'(\d[\d\s\u202f]*[.,]?\d*)\s*(₽|руб(?:\.|лей)?|р\.?|р)\b',
            re.IGNORECASE
        )
        def _repl_qty_times_price_eq(m):
            qty = m.group(1)
            unit_raw, unit_curr = m.group(2), (m.group(3) or 'р')
            try:
                unit_val = float(unit_raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
                new_unit = round_to_5(unit_val * (1 + float(percent)/100.0) + float(delta))
                new_total = round_to_5(float(qty) * new_unit)
                return f"{qty}шт*{new_unit}{unit_curr}={new_total}{unit_curr}"
            except Exception:
                return m.group(0)
        _tmp = _PAT_QTY_TIMES_PRICE_EQ.sub(_repl_qty_times_price_eq, line)
        if _tmp != line:
            line = _tmp

    # единый проход "цена …" (включая склейки). Один пересчёт и СРАЗУ выходим.
    _ANY_PRICE = re.compile(
        r'\bцена\s*[:=\-–—]?\s*(?P<num>\d[\d\s\u202f]*[.,]?\d*)\s*(?P<cur>₽|руб(?:\.|лей)?|р\.?|р)\b'
        r'|(?:цена)\s*[:=\-–—]?\s*(?P<num2>\d[\d\s\u202f]*[.,]?\d*)(?=\D|$)',
        re.IGNORECASE
    )
    def _repl_any(m):
        raw = m.group('num') if m.group('num') else m.group('num2')
        cur = m.group('cur') if m.group('cur') else 'р'
        try:
            v = float(raw.replace(' ', '').replace('\u202f','').replace(',', '.'))
            nv = round_to_5(v * (1 + float(percent)/100.0) + float(delta))
            return f'Цена {nv}{cur}'
        except Exception:
            return m.group(0)
    _line_once = _ANY_PRICE.sub(_repl_any, line)
    if _line_once != line:
        return _line_once
    # === END SINGLE-PASS ===


    # === EXTRA PRICE PATTERNS (multi-hit; no early returns) ===
    def _num(v: str) -> float:
        return float(v.replace(' ', '').replace('\u202f','').replace(',', '.'))

    def _reprice(raw: str, curr: str) -> str:
        try:
            val = _num(raw)
            newv = round_to_5(val * (1 + float(percent)/100.0) + float(delta))
            return f"{newv}{curr or 'р'}"
        except Exception:
            return f"{raw}{curr or 'р'}"

    # 1) Цена в скобках для опта: "( от 5 Набор по 130р )" и похожие
    _PAT_PRICE_IN_PARENS = re.compile(
        r"\(\s*(?:от\s*\d+\s*\S*)?(?:[^()\n]*?)?(?:по|цена)\s*[:=\-–—]?\s*"
        r"(\d[\d\s\u202f]*[.,]?\d*)\s*(₽|руб(?:\.|лей)?|р\.?|р)\s*\)", re.IGNORECASE)
    def _repl_paren(m):
        raw, curr = m.group(1), m.group(2)
        priced = _reprice(raw, curr)
        return m.group(0).replace(m.group(1)+m.group(2), priced)
    line = _PAT_PRICE_IN_PARENS.sub(_repl_paren, line)

    # 2) "Цена с выбором 900р" / "Цена за выбор 900р"
    _PAT_CENA_VYBOR = re.compile(
        r"\b(цена\s*(?:с\s*выбором|за\s*выбор)\s*[:=\-–—]?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*"
        r"(₽|руб(?:\.|лей)?|р\.?|р)\b", re.IGNORECASE)
    def _repl_vybor(m):
        pref, raw, curr = m.group(1), m.group(2), m.group(3)
        return f"{pref}{_reprice(raw, curr)}"
    line = _PAT_CENA_VYBOR.sub(_repl_vybor, line)

    # 3) "10шт: 400р" / "10 шт - 400р"
    _PAT_NPCS_PRICE = re.compile(
        r"\b(\d+)\s*шт\w*\s*[:=\-–—]\s*(\d[\d\s\u202f]*[.,]?\d*)\s*"
        r"(₽|руб(?:\.|лей)?|р\.?|р)\b", re.IGNORECASE)
    def _repl_npcs(m):
        qty, raw, curr = m.group(1), m.group(2), m.group(3)
        # ВАЖНО: тут не умножаем на qty — у тебя уже есть логика для qty*price отдельно.
        return f"{qty}шт: {_reprice(raw, curr)}"
    line = _PAT_NPCS_PRICE.sub(_repl_npcs, line)

    # 4) "Цена за лента 100р (кол-во 10шт)" и подобные ("лента"/"упаковка"/"набор")
    _PAT_CENA_ZA_SOMETHING = re.compile(
        r"\b(цена\s*за\s*[^\s:()]+?\s*[:=\-–—]?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*"
        r"(₽|руб(?:\.|лей)?|р\.?|р)\b", re.IGNORECASE)
    def _repl_za(m):
        pref, raw, curr = m.group(1), m.group(2), m.group(3)
        return f"{pref}{_reprice(raw, curr)}"
    line = _PAT_CENA_ZA_SOMETHING.sub(_repl_za, line)
    
    # 5) "Размер:90/200+20 борт. 600р" / "... 650р" — цена в конце строки после размеров/борта
    _PAT_SIZE_TAIL_PRICE = re.compile(
        r"(?i)^(?=.*размер)(?=.*\d).*?(\d[\d\s\u202f]*[.,]?\d*)\s*"
        r"(₽|руб(?:\.|лей)?|р\.?|р)\s*$")
    def _repl_size_tail(m):
        raw, curr = m.group(1), m.group(2)
        return m.string[:m.start(1)] + _reprice(raw, curr)
    new_line = _PAT_SIZE_TAIL_PRICE.sub(_repl_size_tail, line)
    if new_line != line:
        line = new_line
    # === END EXTRA PRICE PATTERNS ===


    # === SAFETY: нормализуем "Цена <число> [валюта]" до стабильного вида и пересчитываем ===
    _PAT_CENA_NUM = re.compile(
        r'(?i)\bцена\b\s*[:=\-–—]?\s*(\d{2,7})(?:\s*(₽|руб(?:\.|лей)?|р\.?|р))?\b'
    )
    def _repl_cena_num(m):
        raw = m.group(1)
        curr = m.group(2) or 'р'  # если валюты нет — добавим короткое "р"
        try:
            val = float(raw)
            newv = round_to_5(val * (1 + float(percent)/100.0) + float(delta))
            return f'Цена {newv}{curr}'
        except Exception:
            return m.group(0)

    tmp_line = _PAT_CENA_NUM.sub(_repl_cena_num, line)
    if tmp_line != line:
        line = tmp_line


    # --- Основные паттерны и обработчики ---

    # --- Цена за N штук/шт/пар ХХХр ---
    PAT_PRICE_FOR_N_ITEMS = re.compile(
        r'(Цена\s*за\s*)(\d+)\s*(штук|шт|пар)\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_price_for_n_items(m):
        if has_unit_words(s):
            return line
        prefix = m.group(1)
        amount = m.group(2)
        unit = m.group(3)
        price_raw = m.group(4)
        curr = m.group(5) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{amount} {unit} {final_price}{curr}"
    new_line = PAT_PRICE_FOR_N_ITEMS.sub(repl_price_for_n_items, line)
    if new_line != line:
        return new_line

    PAT_PRICE_FOR_ANY_UNIT = re.compile(
        r'(Цена\s*за\s*(шт|штук|упаковк[а-я]*|пачк[а-я]*)\s*[:\-–—=]?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({})?'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_price_for_any_unit(m):
        if has_unit_words(m.group(0)): return m.group(0)
        prefix = m.group(1)
        unit = m.group(2)
        price_raw = m.group(3)
        curr = m.group(4) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{final_price}{curr}"
    new_line = PAT_PRICE_FOR_ANY_UNIT.sub(repl_price_for_any_unit, line)
    if new_line != line:
        return new_line

    PAT_PRICE_WITH_ANYTHING_BEFORE_UNIT = re.compile(
        r'(Цена\s*[:\-–—=]?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({0})[^\w\dа-яА-Я]*(упаковка|пар[аы]?|шт|штук)\b'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_price_with_anything_before_unit(m):
        if has_unit_words(m.group(0)): return m.group(0)
        prefix = m.group(1)
        price_raw = m.group(2)
        curr = m.group(3) or ""
        unit = m.group(4)
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{final_price}{curr} {unit}"
    new_line = PAT_PRICE_WITH_ANYTHING_BEFORE_UNIT.sub(repl_price_with_anything_before_unit, line)
    if new_line != line:
        return new_line

    PAT_UPAKOVOK_N_PCS = re.compile(
        r'(Упаковок\s*)(\d+)\s*(штук|шт|пар)\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({})?'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_upakovok_n_pcs(m):
        if _HAS_PACKING:
            return m.group(0)
        if has_unit_words(m.group(0)): return m.group(0)
        prefix = m.group(1)
        qty = int(m.group(2))
        unit = m.group(3)
        price_raw = m.group(4)
        curr = m.group(5) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            price_per_item = orig_price / qty
            final_per_item = round_to_5(price_per_item * (1 + percent / 100.0) + delta)
            final_total = final_per_item * qty
            return f"{prefix}{qty} {unit} {final_total}{curr}"
        except Exception:
            return m.group(0)
    new_line = PAT_UPAKOVOK_N_PCS.sub(repl_upakovok_n_pcs, line)
    if new_line != line:
        return new_line

    PAT_PRICE_N_PCS_VARIANTS = re.compile(
        r'(Цена\s*[:\-–—=]?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*((?:{0}\s*)+)'
        r'(\d+)\s*(пар[аы]?|шт|штук)'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_price_n_pcs_variants(m):
        if _HAS_PACKING:
            return m.group(0)
        if has_unit_words(m.group(0)): return m.group(0)
        prefix = m.group(1)
        price_raw = m.group(2)
        curr = m.group(3).strip()
        qty = int(m.group(4))
        unit = m.group(5)
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            price_per_item = orig_price / qty
            final_per_item = round_to_5(price_per_item * (1 + percent / 100.0) + delta)
            final_total = final_per_item * qty
            return f"{prefix}{final_total}{curr} {qty} {unit}"
        except Exception:
            return m.group(0)
    new_line = PAT_PRICE_N_PCS_VARIANTS.sub(repl_price_n_pcs_variants, line)
    if new_line != line:
        return new_line

    PAT_PRICE_FOR_UPAKOVKA_SIMPLE = re.compile(
        r'(Цена\s*за\s*упаковка\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({})?'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_price_for_upakovka_simple(m):
        if _HAS_PACKING:
            return m.group(0)
        if has_unit_words(m.group(0)): return m.group(0)
        prefix = m.group(1)
        price_raw = m.group(2)
        curr = m.group(3) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{final_price}{curr}"
    new_line = PAT_PRICE_FOR_UPAKOVKA_SIMPLE.sub(repl_price_for_upakovka_simple, line)
    if new_line != line:
        return new_line

    PAT_SOLO_PRICE = re.compile(
        r'^\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({})\s*$'.format(_PRICE_CURRENCY_PATTERN), re.IGNORECASE
    )
    def repl_solo_price(m):
        if has_unit_words(m.group(0)): return m.group(0)
        price_raw = m.group(1)
        curr = m.group(2) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
            return f"Цена {final_price}{curr}"
        except Exception:
            return m.group(0)
    new_line = PAT_SOLO_PRICE.sub(repl_solo_price, line)
    if new_line != line:
        return new_line

    unit_pattern = r"(штук?|шт\.?|штк)"
    PAT_PRICE_FOR_PACK_N_ITEMS = re.compile(
        rf'(Цена\s*за\s*Упаковка\s*)(\d+)\s*{unit_pattern}\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({_PRICE_CURRENCY_PATTERN})',
        re.IGNORECASE
    )
    def repl_price_for_pack_n_items(m):
        if _HAS_PACKING:
            return m.group(0)
        if has_unit_words(m.group(0)): return m.group(0)
        prefix = m.group(1)
        qty = int(m.group(2))
        price_raw = m.group(3)
        curr = m.group(4) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            price_per_item = orig_price / qty
            final_per_item = round_to_5(price_per_item * (1 + percent / 100.0) + delta)
            final_total = final_per_item * qty
            return f"{prefix}{qty}{curr} {final_total}"
        except Exception:
            return m.group(0)
    new_line = PAT_PRICE_FOR_PACK_N_ITEMS.sub(repl_price_for_pack_n_items, line)
    if new_line != line:
        return new_line

    PAT_PRICE_N_PCS = re.compile(
        r'(Цена\s*)(\d+)\s*(пар|шт|штук)\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({})?'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_price_n_pcs(m):
        if _HAS_PACKING:
            return m.group(0)
        if has_unit_words(m.group(0)): return m.group(0)
        prefix = m.group(1)
        qty = int(m.group(2))
        unit = m.group(3)
        price_raw = m.group(4)
        curr = m.group(5) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            price_per_item = orig_price / qty
            final_per_item = round_to_5(price_per_item * (1 + percent / 100.0) + delta)
            final_total = final_per_item * qty
            return f"{prefix}{final_total}"
        except Exception:
            return m.group(0)
    new_line = PAT_PRICE_N_PCS.sub(repl_price_n_pcs, line)
    if new_line != line:
        return new_line

    PAT_PRICE_FOR_PACK_N_ITEMS2 = re.compile(
        r'(Цена\s*за\s*(?:упаковка\s*|коробка\s*)?)(\d+)\s*(шт|штук|пар)?\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_price_for_pack_n_items2(m):
        if _HAS_PACKING:
            return m.group(0)
        if has_unit_words(m.group(0)): return m.group(0)
        prefix = m.group(1)
        qty = m.group(2)
        unit = m.group(3) or ""
        price_raw = m.group(4)
        curr = m.group(5) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            price_per_item = orig_price / int(qty) if int(qty) > 0 else orig_price
            final_per_item = round_to_5(price_per_item * (1 + percent / 100.0) + delta)
            final_total = final_per_item * int(qty)
            return f"{prefix}{qty}{unit} {final_total}{curr}"
        except Exception:
            return m.group(0)
    new_line = PAT_PRICE_FOR_PACK_N_ITEMS2.sub(repl_price_for_pack_n_items2, line)
    if new_line != line:
        return new_line

    PAT_PRICE_FOR_UPAKOVKA = re.compile(
        r'(Цена\s*за\s*упаковка\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({})?'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_price_for_upakovka(m):
        if _HAS_PACKING:
            return m.group(0)
        if has_unit_words(m.group(0)): return m.group(0)
        prefix = m.group(1)
        price_raw = m.group(2)
        curr = m.group(3) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{final_price}{curr}"
    new_line = PAT_PRICE_FOR_UPAKOVKA.sub(repl_price_for_upakovka, line)
    if new_line != line:
        return new_line

    ALLOWED_UNITS = {"коробка", "упаковка", "шт", "штук", "пар", "пары"}
    def is_allowed_unit(unit):
        return any(
            difflib.SequenceMatcher(None, unit, allowed).ratio() > 0.8
            for allowed in ALLOWED_UNITS
        )
    PAT_ALT_PRICE = re.compile(
        r'^(от|по)\s*(\d+)\s*([а-яА-Яa-zA-Z]+)\s*([а-яА-Яa-zA-Z]*)\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({})?'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_alt_price(m):
        if has_unit_words(m.group(0)): return m.group(0)
        pre = m.group(1)
        qty = m.group(2)
        unit1 = m.group(3) or ""
        unit2 = m.group(4) or ""
        price_raw = m.group(5)
        curr = m.group(6) or ""
        unit_full = (unit1 + unit2).strip().lower()
        if not is_allowed_unit(unit_full):
            return m.group(0)
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{pre} {qty} {unit_full} {final_price}{curr}"
    new_line = PAT_ALT_PRICE.sub(repl_alt_price, line)
    if new_line != line:
        return new_line

    PAT_PRICE_OPTOM = re.compile(
        r'(цена\s*оптом[:\-–—= ]*\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_optom(m):
        if has_unit_words(m.group(0)): return m.group(0)
        prefix = m.group(1)
        price_raw = m.group(2)
        curr = m.group(3) or ""
        try:
            price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            new_price = round_to_5(price * (1 + percent/100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{new_price}{curr}"
    new_line = PAT_PRICE_OPTOM.sub(repl_optom, line)
    if new_line != line:
        return new_line

    PAT_PRICE_FOR_UP_N_ITEMS = re.compile(
        r'(цена\s*за\s*уп\s*\d+\s*(?:шт|штук|пар)?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_price_for_up_n_items(m):
        if has_unit_words(m.group(0)): return m.group(0)
        prefix = m.group(1)
        price_raw = m.group(2)
        curr = m.group(3) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{final_price}{curr}"
    new_line = PAT_PRICE_FOR_UP_N_ITEMS.sub(repl_price_for_up_n_items, line)
    if new_line != line:
        return new_line

    PAT_PRICE_REMOVE_AFTER_FIRST = re.compile(
        r'^(Цена\s*[:\-–—=]?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({0}).*'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_price_remove_after_first(m):
        if has_unit_words(m.group(0)): return m.group(0)
        prefix = m.group(1)
        price_raw = m.group(2)
        curr = m.group(3) or ""
        after = m.string[m.end():]
        qty_match = re.search(r'(\d+)\s*(пар|шт|штук)', after, re.IGNORECASE)
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        if qty_match:
            qty, unit = qty_match.groups()
            return f"{prefix}{final_price}{curr} {qty} {unit}"
        return f"{prefix}{final_price}{curr}"
    new_line = PAT_PRICE_REMOVE_AFTER_FIRST.sub(repl_price_remove_after_first, line)
    if new_line != line:
        return new_line

    PAT_DUAL_PRICE_FROM_N_SHOP = re.compile(
        r'(Цена\s*[:\-–—=]?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({0})\s*от\s*(\d+)\s*шт\s*по\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({0})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_dual_price_from_n_shop(m):
        if has_unit_words(m.group(0)): return m.group(0)
        prefix = m.group(1)
        price1_raw = m.group(2)
        curr1 = m.group(3) or ""
        qty = m.group(4)
        price2_raw = m.group(5)
        curr2 = m.group(6) or ""
        try:
            price1 = float(price1_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            price2 = float(price2_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            price1_new = round_to_5(price1 * (1 + percent / 100.0) + delta)
            price2_new = round_to_5(price2 * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{price1_new}{curr1} от {qty} шт по {price2_new}{curr2}"
    new_line = PAT_DUAL_PRICE_FROM_N_SHOP.sub(repl_dual_price_from_n_shop, line)
    if new_line != line:
        return new_line

    PAT_OPTV_PRICE = re.compile(
        r'(цена\s*опт\s*)(\d+)\s*шт\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_optv_price(m):
        if has_unit_words(m.group(0)): return m.group(0)
        prefix = m.group(1)
        qty = m.group(2)
        price_raw = m.group(3)
        curr = m.group(4) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{qty}шт {final_price}{curr}"
    new_line = PAT_OPTV_PRICE.sub(repl_optv_price, line)
    if new_line != line:
        return new_line

    # --- НЕ обрабатывать комплектацию, количество в упаковке, пар, затяжек и длину ---
    if (
        re.search(r'в упаковке\s*:?[\s\d]*пар', s_lower)
        or re.search(r'\(.*\d+\s*(пар|шт|штук|затяжек|затяжки|затяжка).*\)', s_lower)
        or (re.search(r'\b\d+\s*(пар|шт|штук|затяжек|затяжки|затяжка)\b', s_lower) and not re.search(r'цена|опт', s_lower))
        or re.search(r'\bзатяжек\b', s_lower)
        or (re.search(r'\b\d+\s*(м|метр|метров)\b', s_lower) and not re.search(r'р|руб|р\.|руб\.|₽', s_lower))
    ):
        return line

    if (
        'длина' in s_lower
        or (is_size_line(s) and not re.search(r"цена|р|руб|р\.|руб\.|₽", s_lower))
        or re.search(r'\bарт[\s:]', s_lower)
        or re.search(r'\bартикул[\s:]', s_lower)
        or re.search(r'\barticul[\s:]', s_lower)
        or re.search(r'\bart[\s:]', s_lower)
        or re.search(r'корпус\s*[a-zа-я0-9\-:]*', s_lower)
        or re.search(r'линия\s*\d*[\-–—]?\d*\s*павильон', s_lower)
        or re.search(r'павильон\s*\d*', s_lower)
        or re.search(r'место\s*[a-zа-я0-9\-:]*', s_lower)
        or ('набор' in s_lower and not re.search(r'цена', s_lower))
        or re.search(r'размеры?\s*:\s*([\d\s\(\),\-\/]+)$', s_lower)
        or re.search(r'^размер\s*[\d\s\-\/]+$', s_lower)
        or re.fullmatch(r"\s*размер[\s:]*([\d\-\/,\s]+)", s_lower)
        or re.fullmatch(r"([\d]{2,3}-)+[\d]{2,3}", s_lower)
        or re.fullmatch(r"([\д]{2,3}/)+[\д]{2,3}", s_lower)
        or re.search(r'листов', s_lower)
        or (re.search(r'(упаковк|пачк|пар|шт)\s*\d+', s_lower) and not re.search(r'цена', s_lower))
    ):
        return line
    if "%" in line:
        return line
    if s_lower.startswith("арт") and (s[3:4] in {":", " ", "\t"}):
        return line
    if s_lower.startswith("размер") and not re.search(r'\d', s_lower):
        return line
    if s_lower.startswith("рост"):
        return line
    if re.fullmatch(r"([\d]{2,3}[\-/])+[\d]{2,3}", s_lower.replace(" ", "")):
        return line

    # --- Упаковка N пар XXXр ---
    PAT_UPAKOVKA_N_PAR_PRICE = re.compile(
        r'(Упаковка\s*\d+\s*пар\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_upakovka_n_par_price(m):
        if has_unit_words(s):
            return line
        prefix = m.group(1)
        price_raw = m.group(2)
        curr = m.group(3) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{final_price}{curr}"
    new_line = PAT_UPAKOVKA_N_PAR_PRICE.sub(repl_upakovka_n_par_price, line)
    if new_line != line:
        return new_line

    # --- Цена :180р по 10 упаковок ---
    PAT_PRICE_PO_AMOUNT = re.compile(
        r'(цена\s*[:\-–—=]?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({})\s*по\s*(\d+)\s*(упаковок|упаковки|упаковка|шт|штук|пар)\b'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_price_po_amount(m):
        if has_unit_words(s):
            return line
        prefix = m.group(1)
        price_raw = m.group(2)
        curr = m.group(3) or ""
        qty = m.group(4)
        unit = m.group(5)
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{final_price}{curr} по {qty} {unit}"
    new_line = PAT_PRICE_PO_AMOUNT.sub(repl_price_po_amount, line)
    if new_line != line:
        return new_line

    # --- Спец. паттерн для "16ГБ 190 руб от 10 штук 150 руб" ---
    PAT_PRICE_DUAL = re.compile(
        r'^(\S+)\s+(\d[\d\s\u202f]*[.,]?\d*)\s*({0})\s*от\s*(\d+)\s*штук\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({0})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_dual(m):
        if has_unit_words(s):
            return line
        group = m.group(1)
        price1_raw = m.group(2)
        curr1 = m.group(3) or ""
        min_qty = m.group(4)
        price2_raw = m.group(5)
        curr2 = m.group(6) or ""
        try:
            price1 = float(price1_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            price2 = float(price2_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            price1_new = round_to_5(price1 * (1 + percent/100.0) + delta)
            price2_new = round_to_5(price2 * (1 + percent/100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{group} {price1_new}{curr1} от {min_qty} штук {price2_new}{curr2}"
    new_line = PAT_PRICE_DUAL.sub(repl_dual, line)
    if new_line != line:
        return new_line

    # --- Спец. паттерн для "256 ГБ 440 рот 10 руб 390 руб" ---
    PAT_PRICE_DUAL_NO_SHTUK = re.compile(
        r'^(\S+)\s+(\d[\d\s\u202f]*[.,]?\d*)\s*({0})\s*рот\s*(\d+)\s*руб\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({0})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_dual_noshtuk(m):
        if has_unit_words(s):
            return line
        group = m.group(1)
        price1_raw = m.group(2)
        curr1 = m.group(3) or ""
        min_qty = m.group(4)
        price2_raw = m.group(5)
        curr2 = m.group(6) or ""
        try:
            price1 = float(price1_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            price2 = float(price2_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            price1_new = round_to_5(price1 * (1 + percent/100.0) + delta)
            price2_new = round_to_5(price2 * (1 + percent/100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{group} {price1_new}р от {min_qty} руб {price2_new}{curr2}"
    new_line = PAT_PRICE_DUAL_NO_SHTUK.sub(repl_dual_noshtuk, line)
    if new_line != line:
        return new_line

    # --- Цена за упаковки 220*1030р (или аналогичные) ---
    PAT_UPAK2 = re.compile(
        r'(Цена за упаковк[аиы]\s*)(\d+)\s*\*\s*(\d+)\s*р', re.IGNORECASE
    )
    def repl_upak2(m):
        if has_unit_words(s):
            return line
        prefix = m.group(1)
        price_per_item = float(m.group(2))
        price_pack = float(m.group(3))
        new_price_per_item = round_to_5(price_per_item * (1 + percent/100.0) + delta)
        new_price_pack = round_to_5(price_pack * (1 + percent/100.0) + delta)
        return f"{prefix}{new_price_per_item}*{new_price_pack}р"
    new_line = PAT_UPAK2.sub(repl_upak2, line)
    if new_line != line:
        return new_line

    # --- Цена уп: 230*5=1150р ---
    PAT_UPAK = re.compile(
        r'(Цена уп:\s*)(\d+)\s*\*\s*(\d+)\s*=\s*(\d+)\s*р', re.IGNORECASE
    )
    def repl_upak(m):
        if has_unit_words(s):
            return line
        prefix = m.group(1)
        price_per_item = float(m.group(2))
        qty = m.group(3)
        price_pack = m.group(4)
        new_price_per_item = round_to_5(price_per_item * (1 + percent/100.0) + delta)
        new_price_pack = round_to_5(float(price_pack) * (1 + percent/100.0) + delta)
        return f"{prefix}{new_price_per_item}*{qty}={new_price_pack}р"
    new_line = PAT_UPAK.sub(repl_upak, line)
    if new_line != line:
        return new_line

    # --- Цена:150 руб 2 штук/шт/пар ---
    PAT_PRICE_FOR_AMOUNT = re.compile(
        r'(Цена\s*[:\-–—=]?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({0})?\s*(\d+)\s*(штук|шт|пар)\b'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_price_for_amount(m):
        if has_unit_words(s):
            return line
        prefix = m.group(1)
        price_raw = m.group(2)
        curr = m.group(3) or ""
        amount = m.group(4)
        unit = m.group(5)
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{final_price}{curr} {amount} {unit}"
    new_line = PAT_PRICE_FOR_AMOUNT.sub(repl_price_for_amount, line)
    if new_line != line:
        return new_line

    # --- "по XXXр" (например "по 220р") ---
    PAT_PRICE_BY_AMOUNT = re.compile(
        r'(по\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({})'.format(_PRICE_CURRENCY_PATTERN), re.IGNORECASE
    )
    def repl_price_by_amount(m):
        if has_unit_words(s):
            return line
        pre = m.group(1)
        price_raw = m.group(2)
        curr = m.group(3) or ''
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{pre}{final_price}{curr}"
    new_line = PAT_PRICE_BY_AMOUNT.sub(repl_price_by_amount, line)
    if new_line != line:
        return new_line
    # --- ЯВНО: "Цена с выбором 450 руб." ---
    PAT_PRICE_WITH_SELECTION = re.compile(
        r'(цена\\s*с\\s*выбором\\s*[:\\-–—=]?\\s*)(\\d[\\d\\s\\u202f]*[.,]?\\d*)\\s*({})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_price_with_selection(m):
        prefix = m.group(1)
        price_raw = m.group(2)
        curr = m.group(3) or ''
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{final_price}{curr}"
    new_line = PAT_PRICE_WITH_SELECTION.sub(repl_price_with_selection, line)
    if new_line != line:
        return new_line

    # --- ФОЛЛБЭК: любое число + валюта где угодно (например 'Размер. 1,5. 400р' или 'Размера Евро 550р') ---
    # Исключаем случаи, где валюта относится к единицам (шт/пар/упаковка/пачка), чтобы не ломать другие конструкции.
    PAT_ANY_NUM_CUR = re.compile(
        r'(?<!\\d)(\\d[\\d\\s\\u202f]*[.,]?\\d*)\\s*({})(?!\\s*(пар|шт|штук|упаковк[а-я]*|пачк[а-я]*))'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_any_num_cur(m):
        price_raw = m.group(1)
        curr = m.group(2) or ''
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{final_price}{curr}"
    new_line = PAT_ANY_NUM_CUR.sub(repl_any_num_cur, line)
    if new_line != line:
        return new_line


    # --- Остальные стандартные паттерны ---
    _PRICE_PACK_PIECES = re.compile(
        r'(цена\s*за\s*(?:уп|упаковку|упак|пачку|пач|уп)\s*\d+\s*пар\b\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_pack_pieces(m):
        if has_unit_words(s):
            return line
        prefix = m.group(1)
        price_raw = m.group(2)
        curr = m.group(3) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{final_price}{curr}"
    new_line = _PRICE_PACK_PIECES.sub(repl_pack_pieces, line)
    if new_line != line:
        return new_line

    _PRICE_UP_PACK = re.compile(
        r'(цена\s*уп\s*\d+\s*(?:шт|пар)\s*[:=]?\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_price_up_pack(m):
        if has_unit_words(s):
            return line
        prefix = m.group(1)
        price_raw = m.group(2)
        curr = m.group(3) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{final_price}{curr}"
    new_line = _PRICE_UP_PACK.sub(repl_price_up_pack, line)
    if new_line != line:
        return new_line

    _PRICE_PER_AMOUNT = re.compile(
        r'(цена\s*за\s*)(\d+)\s*(?:уп|уп\.|штук|шт|пар|упаковок?|пачек?)\s*([.,\d\s\u202f]+)\s*({})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_price_per_amount(m):
        if has_unit_words(s):
            return line
        pre = m.group(1) or ''
        qty = m.group(2)
        price_raw = m.group(3)
        curr = m.group(4) or ''
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{pre}{qty}шт {final_price}{curr}"
    new_line = _PRICE_PER_AMOUNT.sub(repl_price_per_amount, line)
    if new_line != line:
        return new_line

    _PRICE_PACK_BRACKET = re.compile(
        r'(\(\s*упак\.\s*\d+\s*шт\s*=\s*)(\d[\d\s\u202f]*[.,]?\d*)\s*({})(\s*\))'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_pack_bracket(m):
        if has_unit_words(s):
            return line
        prefix = m.group(1) or ""
        price_raw = m.group(2)
        curr = m.group(3) or ""
        postfix = m.group(4) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{final_price}{curr}{postfix}"
    new_line = _PRICE_PACK_BRACKET.sub(repl_pack_bracket, line)
    if new_line != line:
        return new_line

    _PRICE_UNIT_BEFORE_AMOUNT = re.compile(
        r'(цена\s*)(шт|упаковка|пачка|пар|штук)\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({})\b'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_unit_before_amount(m):
        if has_unit_words(s):
            return line
        prefix = m.group(1) or ""
        unit = m.group(2)
        price_raw = m.group(3)
        curr = m.group(4) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{unit} {final_price}{curr}"
    new_line = _PRICE_UNIT_BEFORE_AMOUNT.sub(repl_unit_before_amount, line)
    if new_line != line:
        return new_line

    PAT_BED_SIZE_PRICE = re.compile(
        r'цена\s*([12](?:[,\.]\d)?сп|евро)\s*(\d[\d\s\u202f]*[.,]?\d*)\s*({})\b'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_bed_size(m):
        if has_unit_words(s):
            return line
        size = m.group(1)
        price_raw = m.group(2)
        curr = m.group(3) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"Цена {size} {final_price}{curr}"
    new_line = PAT_BED_SIZE_PRICE.sub(repl_bed_size, line)
    if new_line != line:
        return new_line

    _PRICE_WITH_UNIT = re.compile(
        r'(цена\s*[:\-–—=]?\s*)?'
        r'(\d[\d\s\u202f]*[.,]?\d*)\s*'
        r'({})'
        r'([\s\S]*?)'
        r'(упаковка|пачка|пар|штук|шт)\b'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_with_unit(m):
        if has_unit_words(s):
            return line
        prefix = m.group(1) or ""
        price_raw = m.group(2)
        curr = m.group(3) or ""
        between = m.group(4) or ""
        unit = m.group(5) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{final_price}{curr}{between}{unit}"
    new_line = _PRICE_WITH_UNIT.sub(repl_with_unit, line)
    if new_line != line:
        return new_line

    _PRICE_PAT_PLAIN = re.compile(
        r'(цена\s*[:\-–—=]?\s*)'
        r'(\d[\d\s\u202f]*[.,]?\d*)\s*'
        r'({})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_plain(m):
        if has_unit_words(s):
            return line
        prefix = m.group(1) or ""
        price_raw = m.group(2)
        curr = m.group(3) or ""
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{prefix}{final_price}{curr}"
    new_line = _PRICE_PAT_PLAIN.sub(repl_plain, line)
    if new_line != line:
        return new_line

    _PRICE_PAT_ONLY = re.compile(
        r'(?<!\d)(\d[\d\s\u202f]*[.,]?\d*)\s*({})(?!\s*(пар|штук|шт|пачка|упаковка|листов|затяжек|затяжки|затяжка|м|метр|метров)\b)'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )
    def repl_only(m):
        if has_unit_words(s):
            return line
        price_raw = m.group(1)
        curr = m.group(2) if m.group(2) else ""
        after = line[m.end():].lstrip()
        before = line[:m.start()].rstrip()
        if re.match(r'^(пар|штук|шт|пачка|упаковка|листов|затяжек|затяжки|затяжка|м|метр|метров)\b', after, re.IGNORECASE):
            return m.group(0)
        if re.search(r'(пар|штук|шт|пачка|упаковка|листов|затяжек|затяжки|затяжка|м|метр|метров)\s*$', before, re.IGNORECASE):
            return m.group(0)
        try:
            orig_price = float(price_raw.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
        except Exception:
            return m.group(0)
        return f"{final_price}{curr}"
    new_line = _PRICE_PAT_ONLY.sub(repl_only, line)
    if new_line != line:
        return new_line

    # Удаление строк типа "От 10 по 380р"
    if re.search(r'^\s*от\s*\d+\s+по\s+\d+\s*(р|руб|₽)', line.strip(), re.IGNORECASE):
        return ""

    # Замена смайлов-цифр на обычные цифры
    emoji_digits = {
        '𝟎': '0', '𝟏': '1', '𝟐': '2', '𝟑': '3', '𝟒': '4',
        '𝟓': '5', '𝟔': '6', '𝟕': '7', '𝟖': '8', '𝟗': '9',
        '⓪': '0', '①': '1', '②': '2', '③': '3', '④': '4',
        '⑤': '5', '⑥': '6', '⑦': '7', '⑧': '8', '⑨': '9',
        '➀': '1', '➁': '2', '➂': '3', '➃': '4', '➄': '5',
        '➅': '6', '➆': '7', '➇': '8', '➈': '9', '➉': '10',
        '⓿': '0'
    }
    for ed, digit in emoji_digits.items():
        line = line.replace(ed, digit)

    # Удаление ссылок
    line = re.sub(r'https?://\S+|vk\.com/\S+', '', line)

    # Очистка нестандартных символов и юникод-эмодзи
    line = re.sub(r'[^\x00-\x7Fа-яА-ЯёЁ0-9.,:;!?%\-\s₽рРуб]', '', line)

    # Удаление строк с размерами (если они без слова "цена")
    if re.search(r'\(?\d{2,3}[-./]?(\d{2,3})?\)?', line) and not re.search(r'цена|руб|р\b|₽', line, re.IGNORECASE):
        return line

    # Шаблон валют
    _PRICE_CURRENCY_PATTERN = r'(р|руб|₽)'

    # Список стоп-слов для единиц измерения
    def has_unit_words(text):
        units = [
            'мл', 'ml', 'литр', 'л', 'грамм', 'гр', 'г',
            'kg', 'кг', 'mg', 'мг', 'табл', 'доза', 'унция', 'ounce',
            'puffs', 'г/м.кв', 'гм2', 'gsm', 'м²', 'см²', 'mm²', 'м³', 'см³',
            'л/м²', 'л/100км', 'мг/л', 'ppm', 'dpi', 'ppi'
        ]
        return any(re.search(rf'\d+\s*{unit}\b', text.lower()) for unit in units)

    # ===== Цена с количеством после
    PAT_PRICE_WITH_AMOUNT_AFTER = re.compile(
        r'(Цена\s*[:\-–—=]?\s*)(\d[\d\s ]*[.,]?\d*)\s*({})\s*(пар|шт|штук)?\b'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )

    def repl_price_with_amount_after(m):
        if has_unit_words(m.group(0)):
            return m.group(0)
        price_str = re.search(r'(\d[\d\s ]*[.,]?\d*)', m.group(0)).group(1)
        try:
            orig_price = float(price_str.replace(' ', '').replace('\u202f', '').replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
            return re.sub(re.escape(price_str), str(final_price), m.group(0))
        except:
            return m.group(0)

    new_line = PAT_PRICE_WITH_AMOUNT_AFTER.sub(repl_price_with_amount_after, line)
    if new_line != line:
        return new_line

    # ===== Цена без количества
    PAT_PRICE_SOLO = re.compile(
        r'(Цена\s*[:\-–—=]?\s*)(\d+[.,]?\d*)\s*({})\b'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )

    def repl_price_solo(m):
        if has_unit_words(m.group(0)):
            return m.group(0)
        try:
            orig_price = float(m.group(2).replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
            return f"{m.group(1)}{final_price}{m.group(3)}"
        except:
            return m.group(0)

    new_line = PAT_PRICE_SOLO.sub(repl_price_solo, line)
    if new_line != line:
        return new_line

    # ===== Цена в любой позиции
    PAT_PRICE_WITH_ANY_TEXT = re.compile(
        r'(Цена\s*[:\-–—=]?\s*)(\d+[.,]?\d*)\s*({})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )

    def repl_price_any_text(m):
        if has_unit_words(m.group(0)):
            return m.group(0)
        try:
            orig_price = float(m.group(2).replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
            return f"{m.group(1)}{final_price}{m.group(3)}"
        except:
            return m.group(0)

    new_line = PAT_PRICE_WITH_ANY_TEXT.sub(repl_price_any_text, line)
    if new_line != line:
        return new_line

    # ===== Упаковка / Xшт / Xпар (с поддержкой =, :, -, —) + цена
    PAT_PRICE_AFTER_AMOUNT = re.compile(
        r'((упаковка\s*)?\d+\s*(пар|шт|штук)\s*[:=\-–—]?\s*)(\d+[.,]?\d*)\s*({})'.format(_PRICE_CURRENCY_PATTERN),
        re.IGNORECASE
    )

    def repl_price_after_amount(m):
        if has_unit_words(m.group(0)):
            return m.group(0)
        try:
            orig_price = float(m.group(4).replace(',', '.'))
            final_price = round_to_5(orig_price * (1 + percent / 100.0) + delta)
            return f"{m.group(1)}{final_price}{m.group(5)}"
        except:
            return m.group(0)

    new_line = PAT_PRICE_AFTER_AMOUNT.sub(repl_price_after_amount, line)
    if new_line != line:
        return new_line

    # ===== Одиночная цена типа "🔺399🔺"
    PAT_LONE_PRICE = re.compile(r'[^\w\d]{0,3}(\d{2,5})[^\w\d]{0,3}$')
    m = PAT_LONE_PRICE.search(line.strip())
    if m and not has_unit_words(line):
        try:
            price = float(m.group(1))
            final_price = round_to_5(price * (1 + percent / 100.0) + delta)
            return line.replace(m.group(1), str(final_price))
        except:
            pass

    return line


def process_description(raw_text, percent, delta, stopwords, remove_links_flag=True, remove_emoji_flag=True):
    raw_text = clean_description(raw_text, remove_links_flag, remove_emoji_flag)
    
    pack_count = find_pack_count(raw_text)
    orig_lines = raw_text.split('\n')
    # Индексы альтернативных цен в оригинале
    alt_price_pat = re.compile(
        r'цена\s*(уп|упаковка|упак|опт(ом)?|опт|отп|по|от)\b'
        r'|цена.*от\s*\d+\s*(шт|штук|уп|упаковок|упаковки|пар|пары)\b'
        r'|от\s*\d+\s*(шт|штук|уп|упаковок|упаковки|пар|пары)\b',
        re.IGNORECASE
    )
    alt_price_indexes = [i for i, l in enumerate(orig_lines) if alt_price_pat.search(l)]
    # Индексы всех строк с ценой
    price_pat = re.compile(r'цена', re.IGNORECASE)
    price_indexes = [i for i, l in enumerate(orig_lines) if price_pat.search(l)]
    # Очищаем текст от стоп-слов, но работаем по исходным индексам!
    cleaned_lines = remove_base_stopwords(raw_text, stopwords).split('\n')
    result_lines = []
    for idx, line in enumerate(cleaned_lines):
        # Если это альтернативная цена и цен больше одной — пропустить
        if len(price_indexes) > 1 and idx in alt_price_indexes:
            continue
        result_lines.append(process_line(line, percent, delta, stopwords, raw_text))
    result = '\n'.join([l for l in result_lines if l.strip()])
    result = add_pack_count_line(result, pack_count, unit="")
    return result

def is_alt_price_line(line):
    l = line.lower()
    # Старый код...
    # Альтернативные цены: "от 10шт 350р", "от 10 штук 350р", "по 10шт 350р" и т.д.
    if re.match(r"^\s*от\s*\d+\s*(шт|штук|уп|упаковок|упаковки|пар|пары)\b", l):
        return True
    # Можно добавить: если строка содержит только "от N ...", "по N ..." и сумму с валютой — это альт. цена
    return (
        ("цена" in l and (
            "опт" in l or "оптом" in l or
            "упак" in l or "упаковка" in l or "пачк" in l or
            "пачка" in l or "от " in l or "по " in l or
            "за уп" in l or "за упаковк" in l or
            re.search(r"от\s*\d+", l) or
            re.search(r"по\s*\d+", l) or
            re.search(r"за\s*(уп|упаковку|пачку)", l)
        ))
        or re.match(r"^\s*от\s*\d+\s*(шт|штук|уп|упаковок|упаковки|пар|пары)\b", l)
        or re.match(r"^\s*по\s*\d+\s*(шт|штук|уп|упаковок|упаковки|пар|пары)\b", l)
    )

def process_post(
    text,
    stopwords,
    percent,
    delta,
    remove_links_flag=True,
    remove_emoji_flag=True,
    remove_phones_flag=True
):
    # 1. Очищаем текст от ссылок, эмодзи и телефонов (если нужно)
    text = clean_description(
        text,
        remove_links_flag=remove_links_flag,
        remove_emoji_flag=remove_emoji_flag,
        remove_phones_flag=remove_phones_flag
    )

    # 2. Находим количество пар ДО удаления любых слов (чтобы всегда его восстановить)
    pack_count = find_pack_count(text)

    # 3. Обрабатываем каждую строку на предмет цен
    orig_lines = text.split('\n')
    price_processed_lines = [process_line(line, percent, delta, stopwords, text) for line in orig_lines]
    price_processed_text = '\n'.join([l for l in price_processed_lines if isinstance(l, str) and l.strip()])


    # 4. Удаляем стоп-слова ТОЛЬКО после обработки цен
    cleaned_lines = remove_base_stopwords(price_processed_text, stopwords).split('\n')

    # 5. Сохраняем размеры и артикула
    sizes = [line.strip() for line in orig_lines if is_size_line(line.strip())]
    artikuls = [line.strip() for line in orig_lines if is_artikul_line(line.strip())]

    # 6. Оставляем только одну основную цену (как было)
    price_lines_idx = [i for i, l in enumerate(cleaned_lines) if "цена" in l.lower()]
    alt_price_idx = [i for i, l in enumerate(cleaned_lines) if is_alt_price_line(l)]

    filtered_lines = []
    main_price_idx = [i for i in price_lines_idx if i not in alt_price_idx]
    if main_price_idx:
        first_main = main_price_idx[0]
        for idx, l in enumerate(cleaned_lines):
            stripped = l.strip()
            if not stripped:
                continue
            if "цена" in stripped.lower():
                if idx == first_main:
                    filtered_lines.append(stripped)
                # остальные цены пропускаем
            elif is_size_line(stripped) or is_artikul_line(stripped):
                filtered_lines.append(stripped)
            else:
                filtered_lines.append(stripped)
    elif alt_price_idx:
        first_alt = alt_price_idx[0]
        for idx, l in enumerate(cleaned_lines):
            stripped = l.strip()
            if not stripped:
                continue
            if "цена" in stripped.lower():
                if idx == first_alt:
                    filtered_lines.append(stripped)
            elif is_size_line(stripped) or is_artikul_line(stripped):
                filtered_lines.append(stripped)
            else:
                filtered_lines.append(stripped)
    else:
        # Нет цен — просто все строки как есть
        for l in cleaned_lines:
            stripped = l.strip()
            if not stripped:
                continue
            filtered_lines.append(stripped)

    # 7. Восстанавливаем размеры/артикулы (если вдруг ни одной не осталось)
    if sizes and not any(is_size_line(line) for line in filtered_lines):
        filtered_lines.extend(sizes)
    if artikuls and not any(is_artikul_line(line) for line in filtered_lines):
        filtered_lines.extend(artikuls)

    # 8. Восстанавливаем количество пар, если оно было найдено и не осталось в тексте
    result = '\n'.join(dict.fromkeys([l for l in filtered_lines if l.strip()]))
    if pack_count is not None and not re.search(rf'\b{pack_count}\s*пар\b', result, re.IGNORECASE):
        result += f"\nВ упаковке {pack_count} пар"

    return result

class LogWindow:
    def __init__(self, parent):
        self.top = tk.Toplevel(parent)
        self.top.title("Процесс парсинга")
        self.top.geometry("700x400")
        self.top.protocol("WM_DELETE_WINDOW", self.on_close)
        self.top.withdraw()
        self.top.lift()
        self.top.attributes('-topmost', True)
        self.top.after_idle(self.top.attributes, '-topmost', False)
        self.text = tk.Text(self.top, font=("Consolas", 12), wrap="word", state="disabled", bg="#f0f0f0", fg="#333333", relief="sunken", bd=1)
        self.text.pack(side=tk.LEFT, fill=tk.BOTH, expand=1, padx=5, pady=5)
        self.scrollbar = tk.Scrollbar(self.top, command=self.text.yview)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.text.config(yscrollcommand=self.scrollbar.set)
        self.lock = threading.Lock()
        self.lines_limit = 300
    def append(self, msg):
        with self.lock:
            self.text.config(state="normal")
            current_lines = self.text.get('1.0', tk.END).splitlines()
            if len(current_lines) > self.lines_limit:
                lines_to_delete = len(current_lines) - self.lines_limit + 1
                self.text.delete('1.0', f"{lines_to_delete}.0")
            self.text.insert(tk.END, msg + "\n")
            self.text.see(tk.END)
            self.text.config(state="disabled")
    def on_close(self):
        self.top.withdraw()

def add_super_paste(entry):
    def paste_from_clipboard(event=None):
        try:
            entry.insert(tk.INSERT, entry.clipboard_get())
        except tk.TclError:
            pass
        return "break"
    entry.bind('<Control-v>', paste_from_clipboard)
    entry.bind('<Control-V>', paste_from_clipboard)
    entry.bind('<Command-v>', paste_from_clipboard)
    entry.bind('<Command-V>', paste_from_clipboard)
    menu = tk.Menu(entry, tearoff=0)
    menu.add_command(label="Вставить", command=paste_from_clipboard)
    def show_menu(event):
        menu.tk_popup(event.x_root, event.y_root)
    entry.bind("<Button-3>", show_menu)
    entry.bind("<Button-2>", show_menu)

def get_owner_id_from_url(url, token):
    url = url.strip()
    if not url:
        return None
    url_cleaned = re.sub(r'https?://', '', url, flags=re.IGNORECASE)
    url_cleaned = re.sub(r'(www\.)?vk\.com/', '', url_cleaned, flags=re.IGNORECASE)
    match = re.match(r'^(id|club|public|event)?([^/?#]+)', url_cleaned, re.IGNORECASE)
    if not match:
        add_log(f"Не удалось извлечь screen_name/ID из очищенного URL: {url_cleaned}")
        return None
    prefix = match.group(1)
    screen_name_or_id = match.group(2)
    if screen_name_or_id.lstrip('-').isdigit():
        if prefix in ('club', 'public', 'event'):
            return -int(screen_name_or_id)
        elif prefix == 'id':
            return int(screen_name_or_id)
        else:
            return int(screen_name_or_id)
    api_url = "https://api.vk.com/method/utils.resolveScreenName"
    params = {
        "screen_name": screen_name_or_id,
        "access_token": token,
        "v": VK_API_VERSION
    }
    try:
        r = requests.get(api_url, params=params, timeout=10)
        r.raise_for_status()
        resp = r.json()
        if "error" in resp:
            add_log(f"VK API ошибка (resolveScreenName): {resp['error'].get('error_msg', 'Неизвестная ошибка')}")
            return None
        if "response" in resp and resp['response']:
            obj_type = resp['response'].get('type')
            obj_id = resp['response'].get('object_id')
            if obj_type in ("group", "page", "event"):
                return -int(obj_id)
            elif obj_type == "user":
                return int(obj_id)
            else:
                add_log(f"Неизвестный тип объекта VK: {obj_type} для {screen_name_or_id}")
                return None
        else:
            add_log(f"Не удалось разрешить screen_name: {screen_name_or_id}. Возможно, такой страницы не существует или она удалена.")
            return None
    except requests.exceptions.RequestException as e:
        add_log(f"Ошибка сети/запроса при получении owner_id для {screen_name_or_id}: {e}")
        return None
    except json.JSONDecodeError:
        add_log(f"Ошибка декодирования JSON от VK API (resolveScreenName). Ответ: {r.text if 'r' in locals() else 'нет ответа'}")
        return None
    except Exception as e:
        add_log(f"Неизвестная ошибка при получении owner_id: {traceback.format_exc()}")
        return None

def get_vk_posts(token, owner_id, count=5, hours=None):
    url = "https://api.vk.com/method/wall.get"
    posts_to_fetch_initial = count if not hours else 100
    params = {
        'access_token': token,
        'owner_id': owner_id,
        'count': posts_to_fetch_initial,
        'v': VK_API_VERSION
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        if 'error' in data:
            error_code = data['error'].get('error_code')
            error_msg = data['error'].get('error_msg')
            add_log(f"VK API ошибка при получении постов (Code: {error_code}): {error_msg}")
            if error_code == 15:
                add_log(f"Ошибка доступа к стене {owner_id}. Возможно, она приватна или удалена.")
            elif error_code == 100:
                add_log(f"VK API: Неверный owner_id или токен. Проверьте настройки.")
            return []
        posts = data['response']['items']
        if hours:
            since_timestamp = int(time.time()) - int(hours) * 3600
            filtered_posts = [p for p in posts if p.get('date', 0) >= since_timestamp]
            filtered_posts.sort(key=lambda p: p.get('date', 0))
            add_log(f"Отфильтровано постов за последние {hours} часов: {len(filtered_posts)}")
            return filtered_posts
        else:
            return posts[:count]
    except requests.exceptions.RequestException as e:
        add_log(f"Ошибка сети/запроса при получении постов VK: {e}")
        return []
    except json.JSONDecodeError:
        add_log(f"Ошибка декодирования JSON от VK API при получении постов. Ответ: {r.text if 'r' in locals() else 'нет ответа'}")
        return []
    except Exception as e:
        add_log(f"Неизвестная ошибка при получении постов VK: {traceback.format_exc()}")
        return []

def parse_attachments(post, limit_photos=0):
    photos = []
    if 'attachments' in post:
        add_log(f"Найдено {len(post['attachments'])} вложений в посте {post.get('id')}.")
        for a in post['attachments']:
            if a['type'] == 'photo':
                sizes = a['photo'].get('sizes', [])
                if sizes:
                    largest_photo_url = sorted(sizes, key=lambda x: x.get('width',0)*x.get('height',0), reverse=True)
                    if largest_photo_url and largest_photo_url[0].get('url'):
                        photos.append(largest_photo_url[0]['url'])
                        add_log(f"Извлечен URL фото: {largest_photo_url[0]['url']}")
                    else:
                        add_log(f"Вложение типа 'photo' не содержит корректных URL-ов размеров: {a.get('photo')}")
                else:
                    add_log(f"Вложение типа 'photo' не содержит секции 'sizes': {a.get('photo')}")
            else:
                add_log(f"Обнаружено вложение не-фото типа: '{a['type']}'. Пропуск.")
    else:
        add_log(f"В посте {post.get('id')} нет вложений.")
    if limit_photos and limit_photos > 0:
        original_photo_count = len(photos)
        photos = photos[:limit_photos]
        if original_photo_count > limit_photos:
            add_log(f"Ограничено количество фото с {original_photo_count} до {limit_photos}.")
    return photos

_messages_upload_uses_peer_id = None


def get_messages_upload_server(token, peer_id):
    global _messages_upload_uses_peer_id

    params = {
        "access_token": token,
        "v": VK_API_VERSION
    }
    if _messages_upload_uses_peer_id is not False:
        params["peer_id"] = peer_id

    response = requests.get(
        "https://api.vk.com/method/photos.getMessagesUploadServer",
        params=params,
        timeout=10
    ).json()

    error = response.get("error", {})
    error_msg = error.get("error_msg", "")
    if error.get("error_code") == 100 and "only group can use peer_id parameter" in error_msg.lower():
        add_log("VK использует пользовательский токен: повтор получения сервера загрузки без peer_id.")
        _messages_upload_uses_peer_id = False
        params.pop("peer_id")
        response = requests.get(
            "https://api.vk.com/method/photos.getMessagesUploadServer",
            params=params,
            timeout=10
        ).json()
    elif "response" in response and "peer_id" in params:
        _messages_upload_uses_peer_id = True

    return response


def upload_photo_to_vk(token, peer_id, photo_url):
    try:
        upload_url_resp = get_messages_upload_server(token, peer_id)
        if 'error' in upload_url_resp:
            add_log(f"VK API ошибка (getMessagesUploadServer): {upload_url_resp['error'].get('error_msg', 'Неизвестная ошибка')}")
            return None
        if 'response' not in upload_url_resp or 'upload_url' not in upload_url_resp['response']:
            add_log(f"Некорректный ответ от VK API при получении сервера загрузки: {upload_url_resp}")
            return None
        upload_url = upload_url_resp['response']['upload_url']
        photo_data_resp = requests.get(photo_url, timeout=20)
        photo_data_resp.raise_for_status()
        photo_data = photo_data_resp.content
        content_type = photo_data_resp.headers.get('Content-Type', 'image/jpeg').split(';', 1)[0]
        files = {'file0': ('photo.jpg', photo_data, content_type)}
        response = requests.post(upload_url, files=files, timeout=20).json()
        if 'error' in response:
            add_log(f"VK API ошибка при загрузке фото на сервер: {response['error'].get('error_msg', 'Неизвестная ошибка')}")
            return None
        if not all(k in response for k in ('photo', 'server', 'hash')):
            add_log(f"Некорректный ответ при загрузке фото на сервер VK (неполные данные): {response}")
            return None
        save_photo_resp = requests.get(
            "https://api.vk.com/method/photos.saveMessagesPhoto",
            params={
                "access_token": token,
                "photo": response["photo"],
                "server": response["server"],
                "hash": response["hash"],
                "v": VK_API_VERSION
            },
            timeout=10
        ).json()
        if 'error' in save_photo_resp:
            add_log(f"VK API ошибка при сохранении фото в сообщениях: {save_photo_resp['error'].get('error_msg', 'Неизвестная ошибка')}")
            return None
        if not save_photo_resp.get("response") or not save_photo_resp["response"]:
            add_log(f"Пустой ответ при сохранении фото в сообщениях: {save_photo_resp}")
            return None
        photo = save_photo_resp["response"][0]
        attachment = f'photo{photo["owner_id"]}_{photo["id"]}'
        add_log(f"Фото успешно загружено в VK: {attachment}")
        return attachment
    except requests.exceptions.RequestException as e:
        add_log(f"Ошибка сети/запроса при загрузке фото в VK: {e}")
        return None
    except json.JSONDecodeError:
        add_log(f"Ошибка декодирования JSON при загрузке/сохранении фото в VK.")
        return None
    except Exception as e:
        add_log(f"Неизвестная ошибка при загрузке фото в VK: {traceback.format_exc()}")
        return None

def send_vk_message(token, peer_id, message, attachments=None):
    try:
        url = "https://api.vk.com/method/messages.send"
        params = {
            "access_token": token,
            "peer_id": peer_id,
            "random_id": int(time.time() * 1000),
            "v": VK_API_VERSION
        }
        if message:
            params["message"] = message
        if attachments:
            if isinstance(attachments, list):
                params["attachment"] = ",".join(attachments)
            else:
                params["attachment"] = attachments
        response = requests.get(url, params=params, timeout=15).json()
        if "error" in response:
            add_log(f"Ошибка VK при отправке сообщения: {response['error'].get('error_msg', 'Неизвестная ошибка')}")
            return False
        return True
    except Exception as e:
        add_log(f"Ошибка при отправке сообщения в VK: {traceback.format_exc()}")
        return False

def vk_api_call(method: str, vk_token: str, params: dict, timeout: int = 15) -> dict:
    url = f"https://api.vk.com/method/{method}"
    base = {
        "access_token": vk_token,
        "v": VK_API_VERSION,
    }
    base.update(params or {})
    r = requests.post(url, data=base, timeout=timeout)
    data = r.json()
    if "error" in data:
        err = data["error"]
        raise RuntimeError(f"VK API error {err.get('error_code')}: {err.get('error_msg')}")
    return data.get("response", {})


def vk_kick_user(vk_token: str, vk_chat_id: int, user_id: int, reason: str = "") -> bool:
    try:
        vk_api_call(
            "messages.removeChatUser",
            vk_token,
            {"chat_id": vk_chat_id, "member_id": user_id}
        )
        add_log(f"🛑 КИК: user_id={user_id} (антиспам). {reason}".strip())
        return True
    except Exception as e:
        add_log(f"❌ Не удалось кикнуть user_id={user_id}: {e}")
        return False


def check_vk_profile_risk(vk_token, user_id):
    """
    Проверяет профиль VK пользователя на признаки спам-аккаунта.
    Возвращает (is_risky: bool, reasons: list[str])
    Вызывается один раз при входе пользователя в чат.
    """
    try:
        resp = vk_api_call(
            "users.get",
            vk_token,
            {"user_ids": user_id, "fields": "photo_id,followers_count,counters"},
            timeout=5
        )
        if not resp or not isinstance(resp, list) or not resp:
            return False, []

        user = resp[0]
        reasons = []

        # Нет фото профиля (дефолтная аватарка)
        no_photo = not user.get("photo_id")
        if no_photo:
            reasons.append("нет фото профиля")

        # Мало подписчиков (менее 5)
        followers = user.get("followers_count", 0)
        if followers < 5:
            reasons.append(f"мало подписчиков ({followers})")

        # Пустая стена (0 постов)
        wall_posts = user.get("counters", {}).get("posts", None)
        no_wall = wall_posts is not None and wall_posts == 0
        if no_wall:
            reasons.append("пустая стена (0 постов)")

        # Считаем рискованным если хотя бы 1 признак
        is_risky = len(reasons) >= 1
        return is_risky, reasons

    except Exception as e:
        add_log(f"⚠️ Профиль user_id={user_id} не проверен: {e}")
        return False, []


_JOINS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "antispam_joins.json")


def load_join_ts(window_sec):
    """Загружает join_ts с диска, отфильтровывая записи старше window_sec."""
    try:
        if os.path.exists(_JOINS_FILE):
            with open(_JOINS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            cutoff = time.time() - window_sec
            return {int(k): float(v) for k, v in data.items() if float(v) > cutoff}
    except Exception as e:
        add_log(f"⚠️ Не удалось загрузить antispam_joins.json: {e}")
    return {}


def save_join_ts(join_ts):
    """Сохраняет join_ts на диск."""
    try:
        with open(_JOINS_FILE, "w", encoding="utf-8") as f:
            json.dump({str(k): v for k, v in join_ts.items()}, f)
    except Exception as e:
        add_log(f"⚠️ Не удалось сохранить antispam_joins.json: {e}")


def vk_antispam_worker(
    vk_token: str,
    vk_peer_id: int,
    vk_chat_id: int,
    stop_event_obj,
    window_sec: int = 86400,
    poll_sec: int = 3,
    tg_token: str = None,
    tg_chat_id: int = None,
    notify_telegram: bool = True,
    order_notify_enabled: bool = False,
    order_notify_user_id: int = None,
    order_chat_link: str = "",
    order_notify_vk_id_raw: str = ""
):
    """
    УЛУЧШЕННАЯ ВЕРСИЯ с Long Poll - видит ВСЕ события в реальном времени!

    Логика:
    1) Подключаемся к Long Poll серверу VK
    2) Получаем события в реальном времени (входы, сообщения)
    3) Отслеживаем кто и когда зашел
    4) Если пишет в течение window_sec после входа → кикаем
    5) Если сообщение содержит ключевые слова заказа → уведомляем админа
    """
    add_log(f"🛡️ Антиспам: подключение к Long Poll...")
    
    # Словарь: user_id -> timestamp входа (персистентно, загружается с диска)
    join_ts = load_join_ts(window_sec)
    add_log(f"📂 Загружено {len(join_ts)} записей из antispam_joins.json")
    # Словарь: user_id -> (is_risky, reasons) — результат проверки профиля при входе
    user_risk = {}
    
    # Получаем Long Poll сервер
    server = None
    key = None
    ts = None
    
    def get_longpoll_server():
        nonlocal server, key, ts
        try:
            resp = vk_api_call(
                "messages.getLongPollServer",
                vk_token,
                {"need_pts": 1, "lp_version": 3},
                timeout=10
            )
            if resp:
                server = resp.get("server")
                key = resp.get("key")
                ts = resp.get("ts")
                add_log(f"✅ Антиспам: Long Poll подключен (сервер: {server[:30]}...)")
                return True
        except Exception as e:
            add_log(f"❌ Антиспам: ошибка подключения Long Poll: {e}")
        return False
    
    # Подключаемся
    if not get_longpoll_server():
        add_log("❌ Антиспам: не удалось подключиться к Long Poll. Остановка.")
        return

    # Получаем ID админов из whitelist
    add_log(f"🔑 Загрузка whitelist администраторов...")
    admin_ids = resolve_admin_ids(vk_token)
    add_log(f"✅ Админов в whitelist: {len(admin_ids)}")

    # Если order_notify_user_id не был разрешён в bot_worker — пробуем из кэша
    if order_notify_enabled and order_notify_user_id is None and order_notify_vk_id_raw:
        cached_id = _admin_id_cache.get(order_notify_vk_id_raw)
        if cached_id:
            order_notify_user_id = cached_id
            add_log(f"[ORDER] ID разрешён из кэша: '{order_notify_vk_id_raw}' -> {order_notify_user_id}")
        else:
            add_log(f"[ORDER WARNING] Не удалось разрешить '{order_notify_vk_id_raw}' — уведомления о заказах отключены")
            order_notify_enabled = False

    add_log(f"👂 Антиспам: слушаю чат {vk_peer_id}, окно {window_sec} сек")
    
    # Основной цикл Long Poll
    while not stop_event_obj.is_set():
        try:
            # Запрос к Long Poll серверу
            params = {
                "act": "a_check",
                "key": key,
                "ts": ts,
                "wait": 25,
                "mode": 10,
                "version": 3
            }
            
            response = requests.get(f"https://{server}", params=params, timeout=30)
            data = response.json()
            
            # Обновляем timestamp
            if "ts" in data:
                ts = data["ts"]
            
            # Обрабатываем события
            if "updates" in data:
                current_time = time.time()
                
                for update in data["updates"]:
                    # Тип события 4 = новое сообщение
                    if update[0] == 4:
                        flags = update[2]
                        peer_id = update[3]
                        timestamp = update[4]
                        text = update[5]
                        extra = update[6] if len(update) > 6 else {}
                        message_id = update[1]

                        # Пропускаем исходящие сообщения (бит 2 = OUTBOX = отправлено нашим аккаунтом)
                        if flags & 2:
                            continue

                        # Только наш чат
                        if peer_id != vk_peer_id:
                            continue
                        
                        from_id = int(extra.get("from", 0)) if extra.get("from") else 0
                        
                        # === ОТСЛЕЖИВАНИЕ ВХОДОВ ===
                        if "action" in extra:
                            action = extra.get("action")
                            
                            # Событие входа в чат
                            if action and action.get("type") in ("chat_invite_user", "chat_invite_user_by_link"):
                                invited_user = int(action.get("member_id", from_id)) if action.get("member_id") else from_id
    
                                if invited_user > 0:
                                    join_ts[invited_user] = current_time
                                    add_log(f"👤 Антиспам: вход user_id={invited_user}")

                                    # Проверяем профиль нового пользователя (один запрос)
                                    is_risky, risk_reasons = check_vk_profile_risk(vk_token, invited_user)
                                    user_risk[invited_user] = (is_risky, risk_reasons)
                                    if is_risky:
                                        add_log(f"⚠️ Рискованный профиль user_id={invited_user}: {', '.join(risk_reasons)}")
                                    else:
                                        detail = f" ({', '.join(risk_reasons)})" if risk_reasons else ""
                                        add_log(f"✅ Профиль ОК user_id={invited_user}{detail}")

                                    # Автокик при входе: нет аватара + пустая стена = явный бот/спамер
                                    has_no_photo = "нет фото профиля" in risk_reasons
                                    has_no_wall = "пустая стена (0 постов)" in risk_reasons
                                    if has_no_photo and has_no_wall and not is_admin(invited_user):
                                        kick_reason = "нет фото профиля + пустая стена"
                                        add_log(f"🚫 Автокик при входе: {kick_reason}. user_id={invited_user}")
                                        log_spam_to_file(invited_user, "[вход в чат]", kick_reason, {"profile_reasons": risk_reasons})
                                        if notify_telegram and tg_token and tg_chat_id:
                                            send_spam_alert_telegram(tg_token, tg_chat_id, invited_user, kick_reason, "[пустой профиль при входе]")
                                        vk_kick_user(vk_token, vk_chat_id, invited_user, reason=kick_reason)
                                        join_ts.pop(invited_user, None)
                                        user_risk.pop(invited_user, None)
                                        save_join_ts(join_ts)
                                        continue

                                    # Очистка старых записей (старше window_sec)
                                    cutoff = current_time - window_sec
                                    join_ts = {uid: jt for uid, jt in join_ts.items() if jt > cutoff}
                                    user_risk = {uid: v for uid, v in user_risk.items() if uid in join_ts}
                                    save_join_ts(join_ts)

                        # === ПРОВЕРКА "ТОЛЬКО КАРТИНКА" ДЛЯ НОВЫХ ПОЛЬЗОВАТЕЛЕЙ ===
                        # Кикаем новых пользователей, которые отправляют картинки без текста
                        if from_id > 0 and not is_admin(from_id):
                            # Проверяем, есть ли вложения (картинки, документы и т.д.)
                            has_attachments = any(k.startswith("attach") and k.endswith("_type") for k in extra.keys())

                            # Если новый пользователь (в окне антиспама)
                            if from_id in join_ts:
                                time_since_join = current_time - join_ts[from_id]

                                # Если в пределах окна антиспама
                                if 0 <= time_since_join <= window_sec:
                                    # Если есть вложения, но нет текста (или текст очень короткий)
                                    if has_attachments and (not text or len(text.strip()) < 3):
                                        add_log(f"🚫 Только картинка без текста от нового пользователя! user_id={from_id}")
                                        add_log(f"   Время с момента входа: {int(time_since_join)} сек")

                                        spam_reason = "только картинка без текста (новый пользователь)"
                                        spam_details = {
                                            'has_attachments': True,
                                            'text_length': len(text) if text else 0,
                                            'time_since_join': int(time_since_join)
                                        }

                                        # Логируем в файл
                                        log_spam_to_file(from_id, text or "[без текста, только картинка]", spam_reason, spam_details)

                                        # Отправляем уведомление в Telegram (если включено)
                                        if notify_telegram and tg_token and tg_chat_id:
                                            send_spam_alert_telegram(tg_token, tg_chat_id, from_id, spam_reason, text or "[без текста]")

                                        # Удаляем сообщение
                                        try:
                                            delete_resp = vk_api_call(
                                                "messages.delete",
                                                vk_token,
                                                {
                                                    "peer_id": peer_id,
                                                    "delete_for_all": 1,
                                                    "message_ids": message_id
                                                },
                                                timeout=5
                                            )
                                            if delete_resp:
                                                add_log(f"🗑️ Сообщение (картинка) удалено")
                                        except Exception as e:
                                            add_log(f"❌ Ошибка удаления сообщения: {e}")

                                        # Кикаем пользователя
                                        vk_kick_user(
                                            vk_token,
                                            vk_chat_id,
                                            from_id,
                                            reason=spam_reason
                                        )

                                        # Удаляем из отслеживания (чтобы не кикать повторно)
                                        join_ts.pop(from_id, None)
                                        user_risk.pop(from_id, None)
                                        save_join_ts(join_ts)

                                        # Переходим к следующему событию (не проверяем текст)
                                        continue

                                    # === РЕПОСТ (WALL) ОТ НОВОГО ПОЛЬЗОВАТЕЛЯ ===
                                    # Старых участников не трогаем — они делают репосты для заказов
                                    has_wall_repost = any(
                                        extra.get(k) == "wall"
                                        for k in extra.keys()
                                        if k.startswith("attach") and k.endswith("_type")
                                    )
                                    if has_wall_repost:
                                        # Текст заказа слишком разнообразен (город, размер, "1 шт" и т.п.),
                                        # поэтому единственный надёжный критерий — профиль пользователя.
                                        # Если профиль не закеширован (рестарт бота) — запрашиваем его сейчас
                                        if from_id not in user_risk:
                                            add_log(f"🔍 Профиль user_id={from_id} не в кеше, запрашиваю...")
                                            is_r, r_reasons = check_vk_profile_risk(vk_token, from_id)
                                            user_risk[from_id] = (is_r, r_reasons)
                                        risk_is_risky, risk_reasons = user_risk[from_id]
                                        add_log(f"⚠️ Репост (wall) от нового пользователя! user_id={from_id}, {int(time_since_join)} сек после входа, профиль_риск={risk_is_risky}, признаки={risk_reasons}")

                                        spam_reason = "репост со своей страницы (новый пользователь)"
                                        if risk_reasons:
                                            spam_reason += f" | профиль: {', '.join(risk_reasons)}"

                                        if risk_is_risky:
                                            # Рискованный профиль (нет фото + мало подписчиков) — автокик
                                            add_log(f"🚫 Автокик: рискованный профиль + репост. user_id={from_id}")
                                            spam_details = {
                                                'has_wall_repost': True,
                                                'text_length': len(text) if text else 0,
                                                'time_since_join': int(time_since_join),
                                                'profile_risky': True,
                                                'profile_reasons': risk_reasons
                                            }
                                            log_spam_to_file(from_id, text or "[репост без текста]", spam_reason, spam_details)
                                            if notify_telegram and tg_token and tg_chat_id:
                                                send_spam_alert_telegram(tg_token, tg_chat_id, from_id, spam_reason, text or "[репост]")
                                            try:
                                                vk_api_call(
                                                    "messages.delete",
                                                    vk_token,
                                                    {
                                                        "peer_id": peer_id,
                                                        "delete_for_all": 1,
                                                        "message_ids": message_id
                                                    },
                                                    timeout=5
                                                )
                                                add_log(f"🗑️ Репост удалён")
                                            except Exception as e:
                                                add_log(f"❌ Ошибка удаления репоста: {e}")
                                            vk_kick_user(vk_token, vk_chat_id, from_id, reason=spam_reason)
                                            join_ts.pop(from_id, None)
                                            user_risk.pop(from_id, None)
                                            save_join_ts(join_ts)
                                            continue
                                        # Нормальный профиль — скорее всего покупатель, пропускаем

                        # === ПРОВЕРКА СООБЩЕНИЙ ===
                        # Извлекаем текст пересланных сообщений для ВСЕХ пользователей (не только новых)
                        effective_text = text
                        if from_id > 0 and not text and not is_admin(from_id):
                            fwd_text = extract_fwd_text(vk_token, message_id)
                            if fwd_text:
                                effective_text = fwd_text
                                add_log(f"📨 Пересланное сообщение от user_id={from_id}: проверяю")

                        if from_id > 0 and not is_admin(from_id):
                            # ШАГ 1: СНАЧАЛА проверяем на заказ — клиентов никогда не кикаем!
                            is_order_msg = False
                            if order_notify_enabled and order_notify_user_id:
                                # Если собственный текст не похож на заказ, проверяем пересланную часть.
                                # Покупатели часто пересылают товар и добавляют только размер или количество.
                                if text:
                                    direct_is_order, _ = check_order_keywords(text)
                                    if not direct_is_order:
                                        fwd_text = extract_fwd_text(vk_token, message_id)
                                        if fwd_text:
                                            effective_text = f"{text}\n{fwd_text}".strip()

                                # 1а: ключевые слова во всём доступном тексте, включая пересланное сообщение
                                order_text = effective_text or text
                                if order_text:
                                    is_order_msg, matched_keyword = check_order_keywords(order_text)
                                    if is_order_msg:
                                        add_log(f"🛒 [ORDER] Заказ (текст) от user_id={from_id}, слово: '{matched_keyword}'")
                                        add_log(f"   Текст: {order_text[:100]}")
                                        send_order_notification_vk(vk_token, order_notify_user_id, from_id, order_text, peer_id, order_chat_link)
                                # 1б: переслал пост без текста = почти всегда заказ
                                if not is_order_msg and not text:
                                    has_wall_repost_order = any(
                                        extra.get(k) == "wall"
                                        for k in extra.keys()
                                        if k.startswith("attach") and k.endswith("_type")
                                    )
                                    if has_wall_repost_order:
                                        is_order_msg = True
                                        add_log(f"🛒 [ORDER] Пост переслан от user_id={from_id} без текста — уведомляю")
                                        send_order_notification_vk(vk_token, order_notify_user_id, from_id, "[Пользователь переслал пост товара без комментария]", peer_id, order_chat_link)

                            # ШАГ 2: Если НЕ заказ — проверяем на спам (только если есть текст)
                            if not is_order_msg and effective_text:
                                is_spam_detected = False
                                spam_reason = ""
                                spam_details = {}

                                is_spam_pattern, pattern_reason, pattern_details = check_spam_patterns(effective_text, ANTIWORDS)

                                # 1. Ссылка от не-админа
                                if pattern_details.get('has_links'):
                                    is_spam_detected = True
                                    spam_reason = "ссылка от не-админа"
                                    spam_details = pattern_details
                                    add_log(f"🚫 Ссылка от не-админа! user_id={from_id}")

                                # 2. Скрытый контакт мессенджера
                                elif pattern_details.get('has_messenger_contact'):
                                    is_spam_detected = True
                                    spam_reason = "контакт мессенджера / рекламный спам"
                                    spam_details = pattern_details
                                    add_log(f"🚫 Контакт мессенджера! user_id={from_id}")

                                # 3. Номер телефона
                                elif pattern_details.get('has_phone'):
                                    is_spam_detected = True
                                    spam_reason = "номер телефона в сообщении"
                                    spam_details = pattern_details
                                    add_log(f"🚫 Телефон в сообщении! user_id={from_id}")

                                # 4. CAPS LOCK (>70% заглавных букв)
                                elif pattern_details.get('is_caps'):
                                    is_spam_detected = True
                                    spam_reason = "CAPS LOCK (>70% заглавных)"
                                    spam_details = pattern_details
                                    add_log(f"🚫 CAPS сообщение! user_id={from_id}")

                                # 5. Много эмодзи (>3)
                                elif pattern_details.get('emoji_count', 0) > 3:
                                    is_spam_detected = True
                                    spam_reason = f"много эмодзи ({pattern_details['emoji_count']})"
                                    spam_details = pattern_details
                                    add_log(f"🚫 Спам эмодзи! user_id={from_id}")

                                # 6. Бессмысленный набор символов (gibberish)
                                elif pattern_details.get('is_gibberish'):
                                    is_spam_detected = True
                                    spam_reason = "бессмысленный набор символов"
                                    spam_details = pattern_details
                                    add_log(f"🚫 Gibberish! user_id={from_id}")

                                # 7. Запрещенные слова из ANTIWORDS
                                elif pattern_details.get('has_antiwords'):
                                    is_spam_detected = True
                                    spam_reason = "запрещенные слова"
                                    spam_details = pattern_details
                                    add_log(f"🚫 Запрещенные слова! user_id={from_id}")

                                if is_spam_detected:
                                    add_log(f"⚠️ СПАМ ОБНАРУЖЕН! user_id={from_id}")
                                    add_log(f"   Причина: {spam_reason}")
                                    add_log(f"   Текст: {effective_text[:80]}...")

                                    log_spam_to_file(from_id, effective_text, spam_reason, spam_details)

                                    if notify_telegram and tg_token and tg_chat_id:
                                        send_spam_alert_telegram(tg_token, tg_chat_id, from_id, spam_reason, effective_text)

                                    try:
                                        delete_resp = vk_api_call(
                                            "messages.delete",
                                            vk_token,
                                            {
                                                "peer_id": peer_id,
                                                "delete_for_all": 1,
                                                "message_ids": message_id
                                            },
                                            timeout=5
                                        )
                                        if delete_resp:
                                            add_log(f"🗑️ Сообщение удалено")
                                    except Exception as e:
                                        add_log(f"❌ Ошибка удаления сообщения: {e}")

                                    vk_kick_user(
                                        vk_token,
                                        vk_chat_id,
                                        from_id,
                                        reason=spam_reason
                                    )

                                    join_ts.pop(from_id, None)

                    # Тип события 5 = редактирование сообщения
                    elif update[0] == 5:
                        message_id = update[1]
                        flags = update[2]
                        peer_id = update[3]
                        timestamp = update[4]
                        text = update[5]
                        extra = update[6] if len(update) > 6 else {}

                        # Только наш чат
                        if peer_id != vk_peer_id:
                            continue

                        from_id = int(extra.get("from", 0)) if extra.get("from") else 0

                        # === ПРОВЕРКА ОТРЕДАКТИРОВАННЫХ СООБЩЕНИЙ ===
                        if from_id > 0 and text and not is_admin(from_id):
                            add_log(f"✏️ Обнаружено редактирование сообщения от user_id={from_id}")

                            is_spam_detected = False
                            spam_reason = ""
                            spam_details = {}

                            # Проверяем паттерны спама в отредактированном тексте
                            is_spam_pattern, pattern_reason, pattern_details = check_spam_patterns(text, ANTIWORDS)

                            # Проверяем каждый признак спама
                            if pattern_details.get('has_links'):
                                is_spam_detected = True
                                spam_reason = "ссылка в отредактированном сообщении"
                                spam_details = pattern_details
                                add_log(f"🚫 Ссылка в редакции! user_id={from_id}")

                            elif pattern_details.get('has_messenger_contact'):
                                is_spam_detected = True
                                spam_reason = "контакт мессенджера в редакции"
                                spam_details = pattern_details
                                add_log(f"🚫 Контакт мессенджера в редакции! user_id={from_id}")

                            elif pattern_details.get('has_phone'):
                                is_spam_detected = True
                                spam_reason = "телефон в отредактированном сообщении"
                                spam_details = pattern_details
                                add_log(f"🚫 Телефон в редакции! user_id={from_id}")

                            elif pattern_details.get('is_caps'):
                                is_spam_detected = True
                                spam_reason = "CAPS в отредактированном сообщении"
                                spam_details = pattern_details
                                add_log(f"🚫 CAPS в редакции! user_id={from_id}")

                            elif pattern_details.get('emoji_count', 0) > 3:
                                is_spam_detected = True
                                spam_reason = f"много эмодзи в редакции ({pattern_details['emoji_count']})"
                                spam_details = pattern_details
                                add_log(f"🚫 Спам эмодзи в редакции! user_id={from_id}")

                            elif pattern_details.get('is_gibberish'):
                                is_spam_detected = True
                                spam_reason = "gibberish в отредактированном сообщении"
                                spam_details = pattern_details
                                add_log(f"🚫 Gibberish в редакции! user_id={from_id}")

                            elif pattern_details.get('has_antiwords'):
                                is_spam_detected = True
                                spam_reason = "запрещенные слова в редакции"
                                spam_details = pattern_details
                                add_log(f"🚫 Запрещенные слова в редакции! user_id={from_id}")

                            # Если спам обнаружен в редакции - удаляем и кикаем
                            if is_spam_detected:
                                add_log(f"⚠️ СПАМ В РЕДАКТИРОВАНИИ! user_id={from_id}")
                                add_log(f"   Причина: {spam_reason}")
                                add_log(f"   Текст: {text[:80]}...")

                                # Логируем в файл
                                log_spam_to_file(from_id, text, spam_reason, spam_details)

                                # Отправляем уведомление в Telegram (если включено)
                                if notify_telegram and tg_token and tg_chat_id:
                                    send_spam_alert_telegram(tg_token, tg_chat_id, from_id, spam_reason, text)

                                # Удаляем сообщение
                                try:
                                    delete_resp = vk_api_call(
                                        "messages.delete",
                                        vk_token,
                                        {
                                            "peer_id": peer_id,
                                            "delete_for_all": 1,
                                            "message_ids": message_id
                                        },
                                        timeout=5
                                    )
                                    if delete_resp:
                                        add_log(f"🗑️ Отредактированное сообщение удалено")
                                except Exception as e:
                                    add_log(f"❌ Ошибка удаления отредактированного сообщения: {e}")

                                # Кикаем пользователя
                                vk_kick_user(
                                    vk_token,
                                    vk_chat_id,
                                    from_id,
                                    reason=spam_reason
                                )

                                # Удаляем из отслеживания (чтобы не кикать повторно)
                                join_ts.pop(from_id, None)

            # Проверка на ошибку (нужно переподключиться)
            if "failed" in data:
                add_log("⚠️ Антиспам: Long Poll сбой, переподключение...")
                if not get_longpoll_server():
                    add_log("❌ Антиспам: не удалось переподключиться")
                    break
        
        except requests.exceptions.Timeout:
            # Это нормально для Long Poll - просто повторяем запрос
            continue
        
        except Exception as e:
            add_log(f"⚠️ Антиспам: ошибка в цикле: {e}")
            time.sleep(3)
    
    add_log("🛑 Антиспам остановлен")


def send_telegram_message(token, chat_id, text, photo_urls=None):
    try:
        if photo_urls and len(photo_urls) > 0:
            media = []
            for idx, url in enumerate(photo_urls[:10]):
                media.append({
                    "type": "photo",
                    "media": url,
                    "caption": text if idx == 0 else ""
                })
            url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
            data = {
                "chat_id": chat_id,
                "media": json.dumps(media, ensure_ascii=False)
            }
            r = requests.post(url, data=data, timeout=20)
            if not r.ok or not r.json().get("ok"):
                add_log(f"Ошибка Telegram при отправке фото: {r.text}")
                return False
        else:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            data = {
                "chat_id": chat_id,
                "text": text
            }
            r = requests.post(url, data=data, timeout=10)
            if not r.ok or not r.json().get("ok"):
                add_log(f"Ошибка Telegram при отправке текста: {r.text}")
                return False
        return True
    except Exception as e:
        add_log(f"Ошибка при отправке сообщения в Telegram: {traceback.format_exc()}")
        return False

def bot_worker(params, vk_token, vk_peer_id, vk_chat_id, tg_token, tg_chat_id, use_telegram, stop_event_obj, start_btn_ref=None, stop_btn_ref=None):
    add_log("🤖 bot_worker стартовал!")
    # --- антиспам для VK беседы (кик, если написал в первые N сек после входа) ---
    antispam_enabled = params.get("antispam_enabled", True)
    antispam_window_sec = params.get("antispam_window_sec", 86400)

    # --- уведомления о заказах ---
    order_notify_enabled = params.get("order_notify_enabled", False)
    order_notify_vk_id_raw = params.get("order_notify_vk_id", "")
    order_chat_link = params.get("order_chat_link", "")
    order_notify_user_id = None

    if order_notify_enabled and order_notify_vk_id_raw:
        # Конвертируем screen_name или numeric ID в числовой ID
        if str(order_notify_vk_id_raw).isdigit():
            order_notify_user_id = int(order_notify_vk_id_raw)
        else:
            try:
                from urllib.parse import urlencode
                resp = requests.get(
                    f"https://api.vk.com/method/users.get",
                    params={"user_ids": order_notify_vk_id_raw, "v": VK_API_VERSION, "access_token": vk_token},
                    timeout=10
                ).json()
                if "response" in resp and resp["response"]:
                    order_notify_user_id = resp["response"][0].get("id")
                    add_log(f"[ORDER] '{order_notify_vk_id_raw}' -> user_id={order_notify_user_id}")
            except Exception as e:
                add_log(f"[ORDER ERROR] Cannot resolve '{order_notify_vk_id_raw}': {e}")

        if order_notify_user_id:
            add_log(f"[ORDER] Order notifications enabled -> user_id={order_notify_user_id}")
        else:
            add_log(f"[ORDER WARNING] Cannot resolve '{order_notify_vk_id_raw}' now — will retry in antispam thread")

    if antispam_enabled:
        # Проверяем настройку уведомлений в Telegram
        notify_telegram = params.get("antispam_notify_telegram", True)

        threading.Thread(
            target=vk_antispam_worker,
            args=(vk_token, vk_peer_id, vk_chat_id, stop_event_obj, antispam_window_sec, 1, tg_token, tg_chat_id, notify_telegram,
                  order_notify_enabled, order_notify_user_id, order_chat_link, order_notify_vk_id_raw),
            daemon=True
        ).start()
        notify_status = "с уведомлениями в Telegram" if (notify_telegram and tg_token and tg_chat_id) else "без уведомлений"
        order_status = ", заказы -> ЛС" if order_notify_enabled else ""
        add_log(f"🛡️ Антиспам VK запущен (окно: {antispam_window_sec} сек, {notify_status}{order_status}).")
    elif order_notify_enabled:
        # Антиспам отключен, но заказы включены — запускаем Long Poll только для заказов
        add_log("[ORDER] Antispam disabled, but order notifications enabled. Starting Long Poll for orders only...")
        threading.Thread(
            target=vk_antispam_worker,
            args=(vk_token, vk_peer_id, vk_chat_id, stop_event_obj, 0, 1, tg_token, tg_chat_id, False,
                  order_notify_enabled, order_notify_user_id, order_chat_link, order_notify_vk_id_raw),
            daemon=True
        ).start()
    else:
        add_log("⚠️ Антиспам отключен в настройках.")

    sent_photos = load_sent_photos()
    add_log(f"Используемые параметры наценки: Процент: {params.get('price_percent')}, Дельта: {params.get('price_delta')}")
    try:
        sources = params.get("sources", [])
        sources = [s.strip() for s in sources if s.strip()]
        if not sources:
            add_log("❗ Список источников пуст. Парсинг не начнется.")
            return
        add_log(f"🔗 Источники для парсинга: {sources}")
        remove_links_flag = params.get("remove_links", False)
        remove_emoji_flag = params.get("remove_emoji", False)
        custom_stopwords = [x.strip() for x in params.get("stopwords", "").split(",") if x.strip()]
        all_stopwords = BASE_STOPWORDS + custom_stopwords
        freq_sec = int(params.get("freq", 60))
        price_percent = float(params.get("price_percent", 0))
        price_delta = int(params.get("price_delta", 0))
        start_time_str = params.get("start_time", "00:00").strip()
        end_time_str = params.get("end_time", "23:59").strip()
        limit_photos = int(params.get("limit_photos_count", 0)) if params.get("limit_photos") else 0
        mode = params.get("mode", "count")
        use_hours = (mode == "date")
        count = int(params.get("count", 10)) if mode == "count" else 10
        hours = int(params.get("hours", 24)) if mode == "date" else None
        sent_ids = load_sent_ids()
        add_log("🚀 Парсер готов к работе.")
        add_log(f"🕐 Время проверяется по МСК (UTC+3)")
        def _is_in_schedule():
            """Check if current MSK time is within working hours."""
            now_check = datetime.datetime.now(MSK_TZ)
            try:
                s_h, s_m = map(int, start_time_str.split(":"))
                e_h, e_m = map(int, end_time_str.split(":"))
                s_dt = now_check.replace(hour=s_h, minute=s_m, second=0, microsecond=0)
                e_dt = now_check.replace(hour=e_h, minute=e_m, second=0, microsecond=0)
                if e_dt < s_dt:
                    if now_check >= s_dt:
                        e_dt += datetime.timedelta(days=1)
                    else:
                        s_dt -= datetime.timedelta(days=1)
                return s_dt <= now_check <= e_dt
            except Exception:
                return True

        while not stop_event_obj.is_set():
            now = datetime.datetime.now(MSK_TZ)
            add_log(f"⏰ Сейчас {now.strftime('%H:%M:%S')} МСК, рабочий диапазон: {start_time_str} - {end_time_str}")
            if not _is_in_schedule():
                add_log("⌛ Вне рабочего диапазона (МСК). Ожидание 1 минуты...")
                for _ in range(60):
                    if stop_event_obj.is_set():
                        add_log("⛔ Парсер остановлен во время ожидания по времени.")
                        return
                    time.sleep(1)
                continue
            for src_url in sources:
                if stop_event_obj.is_set():
                    add_log("⛔ Парсер остановлен (в цикле источников).")
                    break
                if not _is_in_schedule():
                    add_log("⌛ Рабочее время закончилось. Остановка обработки источников.")
                    break
                add_log(f"🔍 Обработка источника: {src_url}")
                owner_id = get_owner_id_from_url(src_url, vk_token)
                if owner_id is None:
                    add_log(f"⚠️ Не удалось получить owner_id для '{src_url}'. Проверьте URL и ваш VK токен.")
                    continue
                add_log(f"✅ Получен owner_id: {owner_id}")
                try:
                    posts_to_get = 100 if use_hours else max(count * 2, 20)
                    posts = get_vk_posts(vk_token, owner_id, count=posts_to_get, hours=hours)
                    add_log(f"📥 Получено постов из VK API для {src_url}: {len(posts)}")
                    if not posts:
                        add_log(f"Нет новых постов для обработки в источнике {src_url} за выбранный период.")
                except Exception as e:
                    add_log(f"❗ Ошибка при получении постов из {src_url}: {e}")
                    continue
                for post in posts:
                    if stop_event_obj.is_set():
                        add_log("⛔ Парсер остановлен (в цикле обработки постов).")
                        break
                    if not _is_in_schedule():
                        add_log("⌛ Рабочее время закончилось. Остановка обработки постов.")
                        break
                    post_id_val = post.get("id")
                    post_owner_id_val = post.get("owner_id")
                    post_unique_id = f"{post_owner_id_val}_{post_id_val}"
                    if post_unique_id in sent_ids:
                        continue
                    if post.get('is_pinned') == 1 or post.get('marked_as_ads') == 1:
                        continue
                    original_text = post.get("text", "")
                    if not message_passes_filters(original_text, ANTIWORDS, remove_links_flag, remove_emoji_flag):
                        add_log(f"Пост {post_unique_id} не прошел текстовые фильтры. Пропуск.")
                        continue
                    processed_text = process_post(
                        original_text,
                        all_stopwords,
                        price_percent,
                        price_delta,
                        remove_links_flag=remove_links_flag,
                        remove_emoji_flag=remove_emoji_flag
                    )
                    if '_PROCESSED_PRICE_MARKER' in processed_text:
                        processed_text = processed_text.replace('_PROCESSED_PRICE_MARKER', "")
                    if not original_text.strip() and not post.get('attachments'):
                        add_log(f"Пропущен пост {post_unique_id}: нет текста и нет вложений. Пропуск.")
                        continue
                    if '_PROCESSED_PRICE_MARKER' in processed_text:
                        processed_text = processed_text.replace('_PROCESSED_PRICE_MARKER', "")
                    if not original_text.strip() and not post.get('attachments'):
                        add_log(f"Пропущен пост {post_unique_id}: нет текста и нет вложений. Пропуск.")
                        continue
                    photo_urls = parse_attachments(post, limit_photos)
                    vk_attachments = []
                    filtered_photo_urls = []

                    if photo_urls:
                        add_log(f"Найдено {len(photo_urls)} URL фото для загрузки в VK (до фильтра повторов).")
                        for p_url in photo_urls:
                            if p_url in sent_photos:
                                add_log(f"⚠️ Фото уже отправлялось ранее, пропуск: {p_url}")
                                continue
                            add_log(f"Попытка загрузки фото '{p_url}' для VK...")
                            att = upload_photo_to_vk(vk_token, vk_peer_id, p_url)
                            if att:
                                vk_attachments.append(att)
                                filtered_photo_urls.append(p_url)
                                save_sent_photo(p_url)
                                sent_photos.add(p_url)
                            else:
                                add_log(f"❗ Не удалось загрузить фото '{p_url}' для VK. Оно будет пропущено.")

                    photo_urls = filtered_photo_urls

                    if not photo_urls and len(post.get("attachments", [])) > 0:
                        add_log(f"⛔ Пропускаем пост {post_unique_id}: все фото уже были отправлены ранее.")
                        continue
                    elif not post.get("attachments"):
                        add_log(f"ℹ️ В посте {post_unique_id} нет вложений с фотографиями.")

                    if (processed_text is None or not processed_text.strip()) and not vk_attachments and (not use_telegram or not photo_urls):
                        add_log(f"Пост {post_unique_id} не имеет текста или вложений для отправки ни на одну платформу после всех фильтров. Пропуск.")
                        continue

                    add_log(f"📤 Отправка поста {post_unique_id} (VK: {len(vk_attachments)} фото, TG: {len(photo_urls)} фото)")
                    vk_sent = False
                    tg_sent = False
                    if processed_text.strip() or vk_attachments:
                        vk_text_with_footer = processed_text + ORDER_FOOTER
                        vk_sent = send_vk_message(vk_token, vk_peer_id, vk_text_with_footer, vk_attachments)
                        if vk_sent:
                            add_log("✅ Пост успешно отправлен в VK.")
                        else:
                            add_log("❌ Ошибка отправки поста в VK.")
                    else:
                        add_log(f"Пост {post_unique_id} не имеет текста или вложений для VK после обработки. Пропуск отправки в VK.")
                    if use_telegram and tg_token and tg_chat_id:
                        if processed_text.strip() or photo_urls:
                            tg_sent = send_telegram_message(tg_token, tg_chat_id, processed_text, photo_urls)
                            if tg_sent:
                                add_log("📨 Пост успешно отправлен в Telegram.")
                            else:
                                add_log("❌ Ошибка отправки поста в Telegram.")
                        else:
                            add_log(f"Пост {post_unique_id} не имеет текста или вложений для Telegram после обработки. Пропуск отправки в Telegram.")
                    else:
                        add_log("Telegram не настроен или отключен. Пропуск отправки в Telegram.")
                    if vk_sent or tg_sent:
                        sent_ids.add(post_unique_id)
                        save_sent_ids(sent_ids)
                        add_log(f"✅ Пост {post_unique_id} помечен как отправленный.")
                        add_log(f"⏳ Ожидание {freq_sec} секунд перед следующим постом...")
                        for _ in range(freq_sec):
                            if stop_event_obj.is_set():
                                add_log("⛔ Парсер остановлен во время ожидания между постами.")
                                return
                            time.sleep(1)
                    else:
                        add_log(f"Пост {post_unique_id} не был отправлен ни в одну из настроенных платформ. ID не будет сохранен.")
                        add_log(f"⏳ Ожидание {freq_sec // 2} секунд после неуспешной попытки отправки.")
                        for _ in range(freq_sec // 2):
                            if stop_event_obj.is_set():
                                add_log("⛔ Парсер остановлен во время ожидания после неуспешной отправки.")
                                return
                            time.sleep(1)
            if not stop_event_obj.is_set():
                add_log(f"🔄 Все источники обработаны. Ожидание основного интервала ({freq_sec} секунд) перед следующим циклом.")
                for _ in range(freq_sec):
                    if stop_event_obj.is_set():
                        add_log("⛔ Парсер остановлен во время основного интервала ожидания.")
                        return
                    time.sleep(1)
    except Exception as e:
        tb = traceback.format_exc()
        add_log(f"💥 Критическая ошибка в потоке парсера:\n{tb}")
    finally:
        add_log("🤖 bot_worker завершил работу.")
        if start_btn_ref and stop_btn_ref and HAS_TKINTER:
            main_root = tk._default_root
            if main_root and main_root.winfo_exists():
                try:
                    start_btn_ref.config(state=tk.NORMAL)
                    stop_btn_ref.config(state=tk.DISABLED)
                except Exception as e:
                    add_log(f"Ошибка при сбросе состояния кнопок GUI: {e}")

def start_bot(params, start_btn_ref, stop_btn_ref, use_telegram_flag):
    global stop_event
    add_log("🚀 Попытка запуска парсера...")
    settings = load_settings()
    vk_token = settings.get("vk_token")
    vk_chat_id = settings.get("vk_chat_id")
    if not vk_token or not vk_chat_id:
        add_log("❌ VK токен или ID беседы VK не найдены в настройках. Запуск парсера невозможен.")
        start_btn_ref.config(state=tk.NORMAL)
        stop_btn_ref.config(state=tk.DISABLED)
        messagebox.showerror("Ошибка запуска", "VK токен или ID беседы VK не настроены. Пожалуйста, пройдите первоначальную настройку.")
        return
    try:
        vk_peer_id = 2000000000 + int(vk_chat_id)
        vk_chat_id_int = int(vk_chat_id)
    except ValueError:
        add_log(f"❌ Некорректный формат ID беседы VK: {vk_chat_id}. Должно быть число. Запуск парсера невозможен.")
        start_btn_ref.config(state=tk.NORMAL)
        stop_btn_ref.config(state=tk.DISABLED)
        messagebox.showerror("Ошибка запуска", "Некорректный ID беседы VK. Убедитесь, что это число.")
        return
    tg_token = settings.get("tg_token")
    tg_chat_id = settings.get("tg_chat_id")
    if use_telegram_flag and (not tg_token or tg_chat_id is None):
        add_log("⚠️ Telegram выбран, но токен или ID чата Telegram не настроены. Отправка в Telegram будет пропущена.")
        use_telegram_flag = False
    start_btn_ref.config(state=tk.DISABLED)
    stop_btn_ref.config(state=tk.NORMAL)
    stop_event.clear()
    threading.Thread(
        target=bot_worker,
        args=(params, vk_token, vk_peer_id, vk_chat_id_int, tg_token, tg_chat_id, use_telegram_flag, stop_event, start_btn_ref, stop_btn_ref),
        daemon=True
    ).start()
    add_log("✅ Поток парсера запущен.")

def stop_bot(start_btn_ref, stop_btn_ref):
    global stop_event
    add_log("Команда STOP получена. Остановка парсера...")
    stop_event.set()
    start_btn_ref.config(state=tk.NORMAL)
    stop_btn_ref.config(state=tk.DISABLED)

def main():
    global _global_log_window_instance
    settings = load_settings()
    print("Загруженные настройки:", settings)
    root = tk.Tk()
    root.withdraw()
    _global_log_window_instance = LogWindow(root)
    add_log("Приложение 'Парсер в чаты' запущено.")

    if not check_license(MY_USER_ID):
        _global_log_window_instance.top.destroy()
        root.destroy()
        return

    # --- Первичная настройка (VK или VK+Telegram) ---
    print("До wizard")
    add_log("До wizard")
    settings = load_settings()

    if not settings.get("vk_token") or not settings.get("vk_chat_id"):
        add_log("Начинается первичная настройка приложения.")
        temp_settings_for_wizard = {}
        root.deiconify()
        selected_platform = initial_platform_wizard(root)
        root.withdraw()
        if selected_platform is None:
            add_log("Первичная настройка отменена пользователем.")
            _global_log_window_instance.top.destroy()
            messagebox.showerror("Ошибка", "Первичная настройка не завершена. Приложение будет закрыто.")
            root.destroy()
            return
        ok_configured = False
        if selected_platform == "vk":
            ok_configured = first_run_wizard_vk(root, temp_settings_for_wizard)
        elif selected_platform == "both":
            if telegram_wizard(root, temp_settings_for_wizard):
                ok_configured = first_run_wizard_vk(root, temp_settings_for_wizard)
            else:
                ok_configured = False
        if ok_configured:
            settings.update(temp_settings_for_wizard)
            save_settings(settings)
            add_log("Первичная настройка завершена успешно.")
        else:
            add_log("Первичная настройка не была завершена или прошла с ошибками. Закрытие приложения.")
            _global_log_window_instance.top.destroy()
            messagebox.showerror("Ошибка", "Первичная настройка не завершена. Приложение будет закрыто.")
            root.destroy()
            return

    tg_chat_id_from_settings = settings.get("tg_chat_id")
    if isinstance(tg_chat_id_from_settings, str) and tg_chat_id_from_settings.lstrip('-').isdigit():
        settings["tg_chat_id"] = int(tg_chat_id_from_settings)
        save_settings(settings)
    telegram_is_configured = bool(settings.get("tg_token") and settings.get("tg_chat_id") is not None)
    add_log(f"Telegram настроен: {telegram_is_configured} (токен: {'есть' if settings.get('tg_token') else 'нет'}, chat_id: {settings.get('tg_chat_id')})")

    root.deiconify()
    root.title("ПАРСЕР В ЧАТЫ")
    root.configure(bg=BG_MAIN)
    WIDTH, HEIGHT = 800, 700
    root.geometry(f"{WIDTH}x{HEIGHT}")
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    x = (screen_width / 2) - (WIDTH / 2)
    y = (screen_height / 2) - (HEIGHT / 2)
    root.geometry(f'{WIDTH}x{HEIGHT}+{int(x)}+{int(y)}')
    root.resizable(True, True)

    border_frame = tk.Frame(root, bg=BORDER_COLOR, bd=2, relief="solid")
    border_frame.pack(padx=12, pady=12, fill=tk.BOTH, expand=1)
    canvas = tk.Canvas(border_frame, bg=BG_MAIN, highlightthickness=0)
    canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=1)
    scrollbar = tk.Scrollbar(border_frame, orient="vertical", command=canvas.yview)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
    canvas.configure(yscrollcommand=scrollbar.set)
    main_settings_frame = tk.Frame(canvas, bg=BG_FRAME)
    frm_id = canvas.create_window((0, 0), window=main_settings_frame, anchor="nw")

    def on_canvas_resize(event):
        canvas.itemconfig(frm_id, width=event.width)
    canvas.bind("<Configure>", on_canvas_resize)
    def on_frm_configure(event):
        canvas.configure(scrollregion=canvas.bbox("all"))
    main_settings_frame.bind("<Configure>", on_frm_configure)

    if platform.system() == "Windows":
        root.bind_all("<MouseWheel>", lambda event: canvas.yview_scroll(int(-1 * (event.delta / 120)), "units"))
    elif platform.system() == "Darwin":
        root.bind_all("<MouseWheel>", lambda event: canvas.yview_scroll(int(-1 * event.delta), "units"))
    else:
        root.bind_all("<Button-4>", lambda event: canvas.yview_scroll(-1, "units"))
        root.bind_all("<Button-5>", lambda event: canvas.yview_scroll(1, "units"))

    main_settings_frame.grid_columnconfigure(0, weight=1)
    main_settings_frame.grid_columnconfigure(1, weight=1)
    row_idx = 0

    row_idx += 1
    title_lbl = tk.Label(main_settings_frame, text="ПАРСЕР В ЧАТЫ", font=BIG_BOLD_FONT, bg=BG_FRAME)
    title_lbl.grid(row=row_idx, column=0, columnspan=2, pady=(18, 10), sticky="ew")
    row_idx += 1

    tk.Label(main_settings_frame, text="Ссылки на источники VK (по одной на строку):", font=MED_FONT, bg=BG_FRAME).grid(row=row_idx, column=0, sticky="nw", pady=3, padx=(10,0))
    sources_text_frame = tk.Frame(main_settings_frame, bg=BG_FRAME)
    sources_text_frame.grid(row=row_idx, column=1, pady=3, sticky="nsew", padx=(0,10))
    sources_text = tk.Text(sources_text_frame, width=32, height=6, font=MED_FONT, bg="white", spacing3=4, wrap="word", relief="groove", bd=1)
    sources_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=1)
    sources_scroll = tk.Scrollbar(sources_text_frame, orient="vertical", command=sources_text.yview)
    sources_scroll.pack(side=tk.RIGHT, fill=tk.Y)
    sources_text.configure(yscrollcommand=sources_scroll.set)
    add_super_paste(sources_text)
    sources_text.insert("1.0", "\n".join(settings.get("sources", [])))

    row_idx += 1
    tk.Label(main_settings_frame, text="ID беседы VK:", font=MED_FONT, bg=BG_FRAME).grid(row=row_idx, column=0, sticky="w", pady=3, padx=(10,0))
    vk_chat_id_display = settings.get("vk_chat_id", "Не настроено")
    tk.Label(main_settings_frame, text=str(vk_chat_id_display), font=MED_FONT, bg=BG_FRAME).grid(row=row_idx, column=1, sticky="w", pady=3, padx=(0,10))

    row_idx += 1
    tk.Label(main_settings_frame, text="Время запуска (HH:MM):", font=MED_FONT, bg=BG_FRAME).grid(row=row_idx, column=0, sticky="w", pady=3, padx=(10,0))
    start_time_entry = tk.Entry(main_settings_frame, width=10, font=MED_FONT, bg="white", relief="groove", bd=1)
    start_time_entry.insert(0, settings.get("start_time", "06:00"))
    start_time_entry.grid(row=row_idx, column=1, sticky="w", pady=3, padx=(0,10))
    add_super_paste(start_time_entry)

    row_idx += 1
    tk.Label(main_settings_frame, text="Время завершения (HH:MM):", font=MED_FONT, bg=BG_FRAME).grid(row=row_idx, column=0, sticky="w", pady=3, padx=(10,0))
    end_time_entry = tk.Entry(main_settings_frame, width=10, font=MED_FONT, bg="white", relief="groove", bd=1)
    end_time_entry.insert(0, settings.get("end_time", "22:00"))
    end_time_entry.grid(row=row_idx, column=1, sticky="w", pady=3, padx=(0,10))
    add_super_paste(end_time_entry)

    row_idx += 1
    tk.Label(main_settings_frame, text="Частота парсинга (сек):", font=MED_FONT, bg=BG_FRAME).grid(row=row_idx, column=0, sticky="w", pady=3, padx=(10,0))
    freq_entry = tk.Entry(main_settings_frame, width=10, font=MED_FONT, bg="white", relief="groove", bd=1)
    freq_entry.insert(0, str(settings.get("freq", 60)))
    freq_entry.grid(row=row_idx, column=1, sticky="w", pady=3, padx=(0,10))
    add_super_paste(freq_entry)

    row_idx += 1
    antispam_enabled_var = tk.BooleanVar(value=settings.get("antispam_enabled", True))
    tk.Checkbutton(main_settings_frame, text="Включить антиспам", font=MED_FONT, bg=BG_FRAME, variable=antispam_enabled_var,
                   activebackground=BG_FRAME, activeforeground="black", selectcolor=BG_FRAME, relief="flat").grid(row=row_idx, column=0, sticky="w", columnspan=2, pady=3, padx=(10,0))

    row_idx += 1
    tk.Label(main_settings_frame, text="Окно антиспама (сек):", font=MED_FONT, bg=BG_FRAME).grid(row=row_idx, column=0, sticky="w", pady=3, padx=(10,0))
    antispam_window_entry = tk.Entry(main_settings_frame, width=10, font=MED_FONT, bg="white", relief="groove", bd=1)
    antispam_window_entry.insert(0, str(settings.get("antispam_window_sec", 86400)))
    antispam_window_entry.grid(row=row_idx, column=1, sticky="w", pady=3, padx=(0,10))
    add_super_paste(antispam_window_entry)

    row_idx += 1
    antispam_notify_telegram_var = tk.BooleanVar(value=settings.get("antispam_notify_telegram", True))
    tk.Checkbutton(main_settings_frame, text="Уведомления о спамерах в Telegram", font=MED_FONT, bg=BG_FRAME, variable=antispam_notify_telegram_var,
                   activebackground=BG_FRAME, activeforeground="black", selectcolor=BG_FRAME, relief="flat").grid(row=row_idx, column=0, sticky="w", columnspan=2, pady=3, padx=(10,0))

    row_idx += 1
    tk.Label(main_settings_frame, text="Наценка %:", font=MED_FONT, bg=BG_FRAME).grid(row=row_idx, column=0, sticky="w", pady=3, padx=(10,0))
    price_percent_entry = tk.Entry(main_settings_frame, width=10, font=MED_FONT, bg="white", relief="groove", bd=1)
    price_percent_entry.insert(0, str(settings.get("price_percent", 0.0)))
    price_percent_entry.grid(row=row_idx, column=1, sticky="w", pady=3, padx=(0,10))
    add_super_paste(price_percent_entry)

    row_idx += 1
    tk.Label(main_settings_frame, text="Надбавка (руб):", font=MED_FONT, bg=BG_FRAME).grid(row=row_idx, column=0, sticky="w", pady=3, padx=(10,0))
    price_delta_entry = tk.Entry(main_settings_frame, width=10, font=MED_FONT, bg="white", relief="groove", bd=1)
    price_delta_entry.insert(0, str(settings.get("price_delta", 0)))
    price_delta_entry.grid(row=row_idx, column=1, sticky="w", pady=3, padx=(0,10))
    add_super_paste(price_delta_entry)

    row_idx += 1
    remove_links_var = tk.BooleanVar(value=settings.get("remove_links", False))
    tk.Checkbutton(main_settings_frame, text="Удалять ссылки", font=MED_FONT, bg=BG_FRAME, variable=remove_links_var,
                   activebackground=BG_FRAME, activeforeground="black", selectcolor=BG_FRAME, relief="flat").grid(row=row_idx, column=0, sticky="w", columnspan=2, pady=3, padx=(10,0))

    row_idx += 1
    remove_emoji_var = tk.BooleanVar(value=settings.get("remove_emoji", False))
    tk.Checkbutton(main_settings_frame, text="Удалять эмодзи", font=MED_FONT, bg=BG_FRAME, variable=remove_emoji_var,
                   activebackground=BG_FRAME, activeforeground="black", selectcolor=BG_FRAME, relief="flat").grid(row=row_idx, column=0, sticky="w", columnspan=2, pady=3, padx=(10,0))

    row_idx += 1
    tk.Label(main_settings_frame, text="Пользовательские стоп-слова (через запятую):", font=MED_FONT, bg=BG_FRAME).grid(row=row_idx, column=0, sticky="nw", pady=3, padx=(10,0))
    stopwords_entry = tk.Entry(main_settings_frame, width=32, font=MED_FONT, bg="white", relief="groove", bd=1)
    stopwords_entry.insert(0, settings.get("stopwords", ""))
    stopwords_entry.grid(row=row_idx, column=1, sticky="ew", pady=3, padx=(0,10))
    add_super_paste(stopwords_entry)

    row_idx += 1
    limit_photos_var = tk.BooleanVar(value=settings.get("limit_photos", False))
    tk.Checkbutton(main_settings_frame, text="Ограничить количество фото:", font=MED_FONT, bg=BG_FRAME, variable=limit_photos_var,
                   activebackground=BG_FRAME, activeforeground="black", selectcolor=BG_FRAME, relief="flat").grid(row=row_idx, column=0, sticky="w", pady=3, padx=(10,0))
    limit_photos_count_entry = tk.Entry(main_settings_frame, width=5, font=MED_FONT, bg="white", relief="groove", bd=1)
    limit_photos_count_entry.insert(0, str(settings.get("limit_photos_count", 1)))
    limit_photos_count_entry.grid(row=row_idx, column=1, sticky="w", pady=3, padx=(0,10))
    add_super_paste(limit_photos_count_entry)

    row_idx += 1
    tk.Label(main_settings_frame, text="Режим парсинга:", font=MED_FONT, bg=BG_FRAME).grid(row=row_idx, column=0, sticky="w", pady=3, padx=(10,0))
    mode_var = tk.StringVar(value=settings.get("mode", "count"))
    mode_frame = tk.Frame(main_settings_frame, bg=BG_FRAME)
    mode_frame.grid(row=row_idx, column=1, sticky="w", pady=3, padx=(0,10))
    tk.Radiobutton(mode_frame, text="По количеству", variable=mode_var, value="count", font=MED_FONT, bg=BG_FRAME, activebackground=BG_FRAME, activeforeground="black", selectcolor=BG_FRAME, relief="flat").pack(side=tk.LEFT)
    tk.Radiobutton(mode_frame, text="За часы", variable=mode_var, value="date", font=MED_FONT, bg=BG_FRAME, activebackground=BG_FRAME, activeforeground="black", selectcolor=BG_FRAME, relief="flat").pack(side=tk.LEFT, padx=(10,0))

    row_idx += 1
    tk.Label(main_settings_frame, text="Количество постов / часов:", font=MED_FONT, bg=BG_FRAME).grid(row=row_idx, column=0, sticky="w", pady=3, padx=(10,0))
    count_hours_entry = tk.Entry(main_settings_frame, width=10, font=MED_FONT, bg="white", relief="groove", bd=1)
    if settings.get("mode", "count") == "count":
        count_hours_entry.insert(0, str(settings.get("count", 10)))
    else:
        count_hours_entry.insert(0, str(settings.get("hours", 24)))
    count_hours_entry.grid(row=row_idx, column=1, sticky="w", pady=3, padx=(0,10))
    add_super_paste(count_hours_entry)

    row_idx += 1
    use_telegram_var = tk.BooleanVar(value=telegram_is_configured)
    tg_check = tk.Checkbutton(main_settings_frame, text="Использовать Telegram", font=MED_FONT, bg=BG_FRAME, variable=use_telegram_var,
                              activebackground=BG_FRAME, activeforeground="black", selectcolor=BG_FRAME, relief="flat")
    tg_check.grid(row=row_idx, column=0, sticky="w", columnspan=2, pady=3, padx=(10,0))
    if not telegram_is_configured:
        tg_check.config(state=tk.DISABLED, text="Использовать Telegram (не настроен)")
    else:
        tg_check.config(state=tk.NORMAL)

    row_idx += 1
    tk.Label(main_settings_frame, text="", bg=BG_FRAME).grid(row=row_idx, column=0, columnspan=2, pady=5)
    row_idx += 1

    button_frame = tk.Frame(main_settings_frame, bg=BG_FRAME)
    button_frame.grid(row=row_idx, column=0, columnspan=2, pady=10)
    start_btn = tk.Button(button_frame, text="START", font=BIG_BOLD_FONT,
                          bg=BG_BTN_START, fg=FG_BTN_START, relief="raised", bd=2,
                          width=10, height=1, cursor="hand2")
    start_btn.pack(side=tk.LEFT, padx=10)
    stop_btn = tk.Button(button_frame, text="STOP", font=BIG_BOLD_FONT,
                         bg=BG_BTN_STOP, fg=FG_BTN_STOP, relief="raised", bd=2,
                         width=10, height=1, state=tk.DISABLED, cursor="hand2")
    stop_btn.pack(side=tk.LEFT, padx=10)

    row_idx += 1
    log_button = tk.Button(main_settings_frame, text="Показать лог", font=MED_FONT,
                           command=_global_log_window_instance.top.deiconify,
                           bg="#e0e0e0", relief="raised", bd=1, cursor="hand2")
    log_button.grid(row=row_idx, column=0, columnspan=2, pady=15)

    def on_start_click():
        params = {
            "sources": sources_text.get("1.0", tk.END).strip().splitlines(),
            "start_time": start_time_entry.get().strip(),
            "end_time": end_time_entry.get().strip(),
            "freq": freq_entry.get().strip(),
            "price_percent": price_percent_entry.get().strip(),
            "price_delta": price_delta_entry.get().strip(),
            "remove_links": remove_links_var.get(),
            "remove_emoji": remove_emoji_var.get(),
            "stopwords": stopwords_entry.get().strip(),
            "limit_photos": limit_photos_var.get(),
            "limit_photos_count": limit_photos_count_entry.get().strip(),
            "mode": mode_var.get(),
            "count": count_hours_entry.get().strip() if mode_var.get() == "count" else None,
            "hours": count_hours_entry.get().strip() if mode_var.get() == "date" else None,
            "antispam_enabled": antispam_enabled_var.get(),
            "antispam_window_sec": antispam_window_entry.get().strip(),
            "antispam_notify_telegram": antispam_notify_telegram_var.get(),
        }
        try:
            params["freq"] = int(params["freq"])
            if params["freq"] < 10:
                messagebox.showwarning("Предупреждение", "Частота парсинга должна быть не менее 10 секунд.")
                params["freq"] = 10
            params["antispam_window_sec"] = int(params["antispam_window_sec"])
            if params["antispam_window_sec"] < 30:
                messagebox.showwarning("Предупреждение", "Окно антиспама должно быть не менее 30 секунд.")
                params["antispam_window_sec"] = 30
            params["price_percent"] = float(params["price_percent"])
            params["price_delta"] = int(params["price_delta"])
            if params["limit_photos"]:
                params["limit_photos_count"] = int(params["limit_photos_count"])
                if params["limit_photos_count"] < 0: params["limit_photos_count"] = 0
            if params["mode"] == "count":
                params["count"] = int(params["count"])
                if params["count"] < 1:
                    messagebox.showwarning("Предупреждение", "Количество постов должно быть не менее 1.")
                    params["count"] = 1
            elif params["mode"] == "date":
                params["hours"] = int(params["hours"])
                if params["hours"] < 1:
                    messagebox.showwarning("Предупреждение", "Количество часов должно быть не менее 1.")
                    params["hours"] = 1
        except ValueError as e:
            add_log(f"Ошибка ввода данных: {e}")
            messagebox.showerror("Ошибка ввода", f"Некорректное значение в числовом поле: {e}\nУбедитесь, что введены только числа.")
            return
        settings_to_save = {
            **settings,
            "sources": params["sources"],
            "start_time": params["start_time"],
            "end_time": params["end_time"],
            "freq": params["freq"],
            "price_percent": params["price_percent"],
            "price_delta": params["price_delta"],
            "remove_links": params["remove_links"],
            "remove_emoji": params["remove_emoji"],
            "stopwords": stopwords_entry.get().strip(),
            "limit_photos": params["limit_photos"],
            "limit_photos_count": params["limit_photos_count"],
            "mode": mode_var.get(),
            "count": params["count"],
            "hours": params["hours"],
            "antispam_enabled": params["antispam_enabled"],
            "antispam_window_sec": params["antispam_window_sec"],
            "antispam_notify_telegram": params["antispam_notify_telegram"],
        }
        save_settings(settings_to_save)
        add_log("Настройки сохранены.")
        start_bot(params, start_btn, stop_btn, use_telegram_var.get())

    start_btn.config(command=on_start_click)
    stop_btn.config(command=lambda: stop_bot(start_btn, stop_btn))
    root.mainloop()

if __name__ == "__main__":
    if not HAS_TKINTER:
        from run_server import main as server_main
        server_main()
    else:
        main()
