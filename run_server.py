#!/usr/bin/env python3
"""
Headless server mode for VK parser bot.
Runs without GUI, auto-starts parser and antispam/order monitoring.
Schedule: uses MSK timezone (UTC+3) for working hours.

Usage:
    python3 run_server.py

Logs are written to stdout and to server.log file.
Press Ctrl+C to stop gracefully.
"""
import signal
import sys
import threading
import datetime
import time
import os
import logging

# Set up file + console logging
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "server.log")
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("vk_parser_server")

# Change to script directory so settings.json and data files are found
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Import from main module
from vk_photo_bot_gui import (
    load_settings, bot_worker, add_log, MSK_TZ
)

# Global stop event for graceful shutdown
server_stop_event = threading.Event()


def signal_handler(signum, frame):
    """Handle SIGINT/SIGTERM for graceful shutdown."""
    sig_name = signal.Signals(signum).name
    logger.info(f"Received {sig_name}, stopping...")
    server_stop_event.set()


def main():
    logger.info("=" * 60)
    logger.info("VK Parser Server Mode (headless)")
    logger.info(f"Working directory: {os.getcwd()}")
    logger.info(f"Log file: {LOG_FILE}")
    logger.info("=" * 60)

    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Load settings
    settings = load_settings()
    if not settings:
        logger.error("Failed to load settings.json")
        sys.exit(1)

    vk_token = settings.get("vk_token")
    vk_chat_id = settings.get("vk_chat_id")

    if not vk_token or not vk_chat_id:
        logger.error("VK token or chat ID not configured in settings.json")
        sys.exit(1)

    try:
        vk_peer_id = 2000000000 + int(vk_chat_id)
        vk_chat_id_int = int(vk_chat_id)
    except ValueError:
        logger.error(f"Invalid VK chat ID format: {vk_chat_id}")
        sys.exit(1)

    tg_token = settings.get("tg_token")
    tg_chat_id = settings.get("tg_chat_id")
    if isinstance(tg_chat_id, str) and tg_chat_id.lstrip('-').isdigit():
        tg_chat_id = int(tg_chat_id)

    use_telegram = bool(tg_token and tg_chat_id is not None)

    # Build params dict (same as GUI does)
    params = {
        "sources": settings.get("sources", []),
        "start_time": settings.get("start_time", "07:00"),
        "end_time": settings.get("end_time", "23:00"),
        "freq": settings.get("freq", 360),
        "price_percent": settings.get("price_percent", 50),
        "price_delta": settings.get("price_delta", 125),
        "remove_links": settings.get("remove_links", True),
        "remove_emoji": settings.get("remove_emoji", True),
        "stopwords": settings.get("stopwords", ""),
        "limit_photos": settings.get("limit_photos", True),
        "limit_photos_count": settings.get("limit_photos_count", 4),
        "mode": settings.get("mode", "date"),
        "count": settings.get("count", None),
        "hours": settings.get("hours", 24),
        "antispam_enabled": settings.get("antispam_enabled", True),
        "antispam_window_sec": settings.get("antispam_window_sec", 300),
        "antispam_notify_telegram": settings.get("antispam_notify_telegram", True),
        "order_notify_enabled": settings.get("order_notify_enabled", False),
        "order_notify_vk_id": settings.get("order_notify_vk_id", ""),
        "order_chat_link": settings.get("order_chat_link", ""),
    }

    now_msk = datetime.datetime.now(MSK_TZ)
    logger.info(f"Current MSK time: {now_msk.strftime('%H:%M:%S')}")
    logger.info(f"Schedule: {params['start_time']} - {params['end_time']} MSK")
    logger.info(f"Sources: {params['sources']}")
    logger.info(f"Frequency: {params['freq']}s")
    logger.info(f"Antispam: {'ON' if params['antispam_enabled'] else 'OFF'}")
    logger.info(f"Order notifications: {'ON -> ' + str(params['order_notify_vk_id']) if params['order_notify_enabled'] else 'OFF'}")
    logger.info(f"Telegram: {'ON' if use_telegram else 'OFF'}")
    logger.info("-" * 60)
    logger.info("Parser starting... Press Ctrl+C to stop.")

    # Start bot_worker in a thread (headless mode: no GUI button refs)
    worker_thread = threading.Thread(
        target=bot_worker,
        args=(params, vk_token, vk_peer_id, vk_chat_id_int, tg_token, tg_chat_id, use_telegram, server_stop_event),
        daemon=True
    )
    worker_thread.start()

    # Wait for stop signal
    try:
        while not server_stop_event.is_set():
            server_stop_event.wait(timeout=1)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, stopping...")
        server_stop_event.set()

    # Wait for worker to finish
    worker_thread.join(timeout=10)
    logger.info("Server stopped.")


if __name__ == "__main__":
    main()
