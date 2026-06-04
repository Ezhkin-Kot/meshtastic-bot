#!/usr/bin/env python3
"""
Meshtastic → Telegram relay bot.
Reads messages from a Meshtastic node via serial and forwards them to a Telegram chat.
"""

import asyncio
import logging
import os
import signal
import sys
from datetime import datetime
from typing import Optional

import meshtastic
import meshtastic.serial_interface
from pubsub import pub
from telegram.error import TelegramError
from telegram.ext import Application
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration (loaded from environment variables)
# ---------------------------------------------------------------------------

load_dotenv()

TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID: int = int(os.environ["TELEGRAM_CHAT_ID"])
SERIAL_PORT: str = os.environ.get("SERIAL_PORT", "/dev/ttyUSB0")
MESHTASTIC_CHANNEL: int = int(os.environ.get("MESHTASTIC_CHANNEL", "0"))
RECONNECT_DELAY: int = int(os.environ.get("RECONNECT_DELAY", "15"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("meshtastic-bot")

# ---------------------------------------------------------------------------
# Globals & Event Loop Bridge
# ---------------------------------------------------------------------------

telegram_app: Optional[Application] = None
mesh_interface: Optional[meshtastic.serial_interface.SerialInterface] = None
loop: Optional[asyncio.AbstractEventLoop] = None
shutdown_event: Optional[asyncio.Event] = None


def escape_markdown(text: str) -> str:
    """Escapes markdown special characters to prevent Telegram parsing errors."""
    for char in ["_", "*", "`", "["]:
        text = text.replace(char, f"\\{char}")
    return text


# ---------------------------------------------------------------------------
# Meshtastic callbacks
# ---------------------------------------------------------------------------


def on_receive(packet: dict, interface) -> None:
    """Called by the meshtastic library for every received packet."""
    try:
        decoded = packet.get("decoded", {})
        portnum = decoded.get("portnum", "")

        if portnum != "TEXT_MESSAGE_APP":
            return

        channel = packet.get("channel", 0)
        if MESHTASTIC_CHANNEL != -1 and channel != MESHTASTIC_CHANNEL:
            log.debug("Ignoring packet from channel %d", channel)
            return

        text: str = decoded.get("text", "").strip()
        if not text:
            return

        from_id: str = packet.get("fromId", "unknown")
        node_info = interface.nodes.get(from_id, {})
        user = node_info.get("user", {})
        long_name: str = user.get("longName") or user.get("shortName") or from_id

        # Signal quality
        rx_snr = packet.get("rxSnr")
        rx_rssi = packet.get("rxRssi")
        signal_str = ""
        if rx_snr is not None or rx_rssi is not None:
            parts = []
            if rx_snr is not None:
                parts.append(f"SNR {rx_snr:.1f} dB")
            if rx_rssi is not None:
                parts.append(f"RSSI {rx_rssi} dBm")
            signal_str = f" _({', '.join(parts)})_"

        timestamp = datetime.now().strftime("%H:%M:%S")

        # Безопасно экранируем имена и текст, сохраняя нашу разметку структуры
        safe_name = escape_markdown(long_name)
        safe_text = escape_markdown(text)

        tg_message = (
            f"📡 *[Meshtastic]* `{timestamp}`\n"
            f"👤 *{safe_name}* (ch {channel}):\n"
            f"{safe_text}{signal_str}"
        )

        log.info("Message from %s (ch %d): %s", long_name, channel, text)

        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(send_to_telegram(tg_message), loop)

    except Exception:
        log.exception("Error processing received packet")


def on_connection(interface, topic=pub.AUTO_TOPIC) -> None:
    log.info("Meshtastic node connection state updated")


# ---------------------------------------------------------------------------
# Telegram helpers
# ---------------------------------------------------------------------------


async def send_to_telegram(text: str) -> None:
    if telegram_app is None:
        log.error("Telegram app not initialised yet")
        return
    try:
        await telegram_app.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=text,
            parse_mode="Markdown",
        )
    except TelegramError as exc:
        log.error("Failed to send Telegram message: %s", exc)


# ---------------------------------------------------------------------------
# Serial connection management
# ---------------------------------------------------------------------------


def connect_mesh() -> Optional[meshtastic.serial_interface.SerialInterface]:
    try:
        iface = meshtastic.serial_interface.SerialInterface(SERIAL_PORT)
        pub.subscribe(on_receive, "meshtastic.receive")
        pub.subscribe(on_connection, "meshtastic.connection.established")
        log.info("Connected to Meshtastic node on %s", SERIAL_PORT)
        return iface
    except Exception as exc:
        log.error("Could not connect to Meshtastic node: %s", exc)
        return None


def disconnect_mesh() -> None:
    global mesh_interface

    # 1. Сначала отписываемся от событий, указывая правильные пары (callback, топик)
    try:
        pub.unsubscribe(on_receive, "meshtastic.receive")
    except Exception:
        pass
    try:
        pub.unsubscribe(on_connection, "meshtastic.connection.established")
    except Exception:
        pass

    # 2. Закрываем интерфейс
    if mesh_interface:
        try:
            mesh_interface.close()
        except Exception:
            pass
        mesh_interface = None
    log.info("Meshtastic interface disconnected and cleaned up")


# ---------------------------------------------------------------------------
# Main Control Loop
# ---------------------------------------------------------------------------


async def main() -> None:
    global telegram_app, mesh_interface, loop, shutdown_event

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    # Нативный для asyncio перехват системных сигналов в Linux
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)

    # Инициализация Telegram
    telegram_app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    await telegram_app.initialize()
    await telegram_app.start()
    log.info("Telegram bot started (chat_id=%d)", TELEGRAM_CHAT_ID)

    await send_to_telegram("🟢 Meshtastic relay bot started.")

    try:
        while not shutdown_event.is_set():
            if mesh_interface is None:
                mesh_interface = connect_mesh()
                if mesh_interface is None:
                    log.info("Retrying connection in %d seconds…", RECONNECT_DELAY)
                    # Корректное ожидание, прерываемое сигналом завершения
                    try:
                        await asyncio.wait_for(
                            shutdown_event.wait(), timeout=RECONNECT_DELAY
                        )
                    except asyncio.TimeoutError:
                        pass
                    continue

            # Проверяем состояние стрима
            if (
                hasattr(mesh_interface, "_startConfig")
                and mesh_interface.stream is None
            ):
                log.warning("Serial stream lost, initiating reconnect…")
                disconnect_mesh()
                # Ждем перед следующим кругом, чтобы не спамить в случае физического отсутствия платы
                try:
                    await asyncio.wait_for(
                        shutdown_event.wait(), timeout=RECONNECT_DELAY
                    )
                except asyncio.TimeoutError:
                    pass
                continue

            # Спим короткими интервалами, проверяя флаг завершения
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=1)
            except asyncio.TimeoutError:
                pass

    finally:
        log.info("Shutting down process initiated…")
        disconnect_mesh()
        try:
            await send_to_telegram("🔴 Meshtastic relay bot stopped.")
        except Exception:
            pass
        await telegram_app.stop()
        await telegram_app.shutdown()
        log.info("Bye.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
