#!/usr/bin/env python3
"""
Утилита для обновления VK токена в settings.json без GUI.

Использование:
    python3 update_token.py

Инструкция по получению токена:
    1. Откройте в браузере: https://vkhost.github.io/
    2. Нажмите кнопку "VK Admin"
    3. На странице VK нажмите "Разрешить"
    4. Скопируйте адресную строку браузера (URL начнётся с https://oauth.vk.com/...)
    5. Вставьте скопированный URL в это поле
"""
import re
import json
import os
import sys

SETTINGS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.json")


def extract_token(url: str):
    m = re.search(r'access_token=([a-zA-Z0-9\-_\.]+)', url)
    if m:
        return m.group(1)
    return None


def main():
    print("=" * 60)
    print("Обновление VK токена")
    print("=" * 60)
    print()
    print("Инструкция:")
    print("  1. Откройте в браузере: https://vkhost.github.io/")
    print("  2. Нажмите кнопку 'VK Admin'")
    print("  3. На странице VK нажмите 'Разрешить'")
    print("  4. Скопируйте адресную строку браузера")
    print("  5. Вставьте URL ниже")
    print()

    try:
        url = input("Вставьте URL из браузера: ").strip()
    except (KeyboardInterrupt, EOFError):
        print("\nОтменено.")
        sys.exit(0)

    if not url:
        print("Ошибка: URL не введён.")
        sys.exit(1)

    token = extract_token(url)
    if not token:
        print("Ошибка: не удалось найти access_token в URL.")
        print("Убедитесь, что скопировали полный адрес после нажатия 'Разрешить'.")
        sys.exit(1)

    # Загружаем settings.json
    if not os.path.exists(SETTINGS_FILE):
        print(f"Ошибка: файл {SETTINGS_FILE} не найден.")
        sys.exit(1)

    with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
        settings = json.load(f)

    old_token = settings.get("vk_token", "")
    old_preview = old_token[:20] + "..." if len(old_token) > 20 else old_token

    settings["vk_token"] = token

    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(settings, f, ensure_ascii=False, indent=4)

    new_preview = token[:20] + "..." if len(token) > 20 else token
    print()
    print(f"Старый токен: {old_preview}")
    print(f"Новый токен:  {new_preview}")
    print()
    print("✅ Токен успешно обновлён в settings.json")
    print("Перезапустите run_server.py для применения изменений.")


if __name__ == "__main__":
    main()
