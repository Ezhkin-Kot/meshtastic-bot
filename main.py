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
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID: int = int(os.environ["TELEGRAM_CHAT_ID"])
SERIAL_PORT: str = os.environ.get("SERIAL_PORT", "/dev/ttyUSB0")
RECONNECT_DELAY: int = int(os.environ.get("RECONNECT_DELAY", "15"))

TOPIC_GENERAL: Optional[int] = (
    int(os.environ.get("TOPIC_GENERAL")) if os.environ.get("TOPIC_GENERAL") else None
)
TOPIC_DIRECT: Optional[int] = (
    int(os.environ.get("TOPIC_DIRECT")) if os.environ.get("TOPIC_DIRECT") else None
)
TOPIC_SYSTEM: Optional[int] = (
    int(os.environ.get("TOPIC_SYSTEM")) if os.environ.get("TOPIC_SYSTEM") else None
)

MY_PORTABLE_NODE_ID: str = os.environ.get("MY_PORTABLE_NODE_ID", "").strip()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("meshtastic-bot")

telegram_app: Optional[Application] = None
mesh_interface: Optional[meshtastic.serial_interface.SerialInterface] = None
loop: Optional[asyncio.AbstractEventLoop] = None
shutdown_event: Optional[asyncio.Event] = None


def escape_markdown(text: str) -> str:
    for char in ["_", "*", "`", "["]:
        text = text.replace(char, f"\\{char}")
    return text


# ---------------------------------------------------------------------------
# Meshtastic callbacks
# ---------------------------------------------------------------------------


def on_receive(packet: dict, interface) -> None:
    try:
        decoded = packet.get("decoded", {})
        portnum = decoded.get("portnum", "")

        if portnum != "TEXT_MESSAGE_APP":
            return

        text: str = decoded.get("text", "").strip()
        if not text:
            return

        from_id: str = packet.get("fromId", "unknown")
        to_id: str = packet.get("toId", "unknown")
        node_info = interface.nodes.get(from_id, {})
        user = node_info.get("user", {})
        long_name: str = user.get("longName") or user.get("shortName") or from_id

        # Определение типа сообщения и выбор топика
        # Если toId равен "^all", это гарантированно публичный широковещательный канал
        if to_id == "^all":
            is_dm = False
            # Если поля channel нет в пакете, по умолчанию это 0 (LongFast)
            channel = packet.get("channel", 0)
            target_topic = TOPIC_GENERAL
        else:
            # Если toId не равен "^all", значит сообщение адресовано конкретно вашей ноде (это ЛС)
            is_dm = True
            channel = None

            # Фильтр по вашей портативной ноде
            if (
                MY_PORTABLE_NODE_ID == "ALL"
                or not MY_PORTABLE_NODE_ID
                or from_id == MY_PORTABLE_NODE_ID
            ):
                target_topic = TOPIC_DIRECT
                log.info("Received valid DM from target node %s", from_id)
            else:
                # Если ЛС от кого-то другого, все равно шлем в топик для ЛС
                target_topic = TOPIC_DIRECT
                log.info("Received DM from another node %s", from_id)

        # Формирование строки сигнала
        rx_snr = packet.get("rxSnr")
        rx_rssi = packet.get("rxRssi")
        signal_str = ""
        if rx_snr is not None or rx_rssi is not None:
            parts = []
            if rx_snr is not None:
                parts.append(f"SNR {rx_snr:.1f} dB")
            if rx_rssi is not None:
                parts.append(f"RSSI {rx_rssi} dBm")
            signal_str = f"\n_({', '.join(parts)})_"

        timestamp = datetime.now().strftime("%H:%M:%S")
        safe_name = escape_markdown(long_name)
        safe_text = escape_markdown(text)

        type_label = "👤" if is_dm else f"📡 Channel {channel}:"
        tg_message = (
            f"*{type_label}* `{safe_name}`: "
            f"{safe_text}\n{signal_str}\n`[{timestamp}]`"
        )

        log.info("Message routes to topic %s: %s", target_topic, text)

        if loop and loop.is_running():
            asyncio.run_coroutine_threadsafe(
                send_to_telegram(tg_message, target_topic), loop
            )

    except Exception:
        log.exception("Error processing received packet")


def on_connection(interface, topic=pub.AUTO_TOPIC) -> None:
    log.info("Meshtastic node connection state updated")


# ---------------------------------------------------------------------------
# Telegram helpers
# ---------------------------------------------------------------------------


async def send_to_telegram(text: str, thread_id: Optional[int] = None) -> None:
    """Sends a message, optionally targeting a specific topic (thread)."""
    if telegram_app is None:
        log.error("Telegram app not initialised yet")
        return
    try:
        await telegram_app.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=text,
            parse_mode="Markdown",
            message_thread_id=thread_id,
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
    try:
        pub.unsubscribe(on_receive, "meshtastic.receive")
    except Exception:
        pass
    try:
        pub.unsubscribe(on_connection, "meshtastic.connection.established")
    except Exception:
        pass

    if mesh_interface:
        try:
            mesh_interface.close()
        except Exception:
            pass
        mesh_interface = None


# ---------------------------------------------------------------------------
# Main Control Loop
# ---------------------------------------------------------------------------


async def main() -> None:
    global telegram_app, mesh_interface, loop, shutdown_event

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)

    telegram_app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    await telegram_app.initialize()
    await telegram_app.start()
    log.info("Telegram bot started.")

    await send_to_telegram("🟢 Meshtastic relay bot started.", TOPIC_SYSTEM)

    try:
        while not shutdown_event.is_set():
            if mesh_interface is None:
                mesh_interface = connect_mesh()
                if mesh_interface is None:
                    log.info("Retrying connection in %d seconds…", RECONNECT_DELAY)
                    try:
                        await asyncio.wait_for(
                            shutdown_event.wait(), timeout=RECONNECT_DELAY
                        )
                    except asyncio.TimeoutError:
                        pass
                    continue

            if (
                hasattr(mesh_interface, "_startConfig")
                and mesh_interface.stream is None
            ):
                log.warning("Serial stream lost, initiating reconnect…")
                disconnect_mesh()
                try:
                    await asyncio.wait_for(
                        shutdown_event.wait(), timeout=RECONNECT_DELAY
                    )
                except asyncio.TimeoutError:
                    pass
                continue

            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=1)
            except asyncio.TimeoutError:
                pass

    finally:
        log.info("Shutting down process initiated…")
        disconnect_mesh()
        try:
            await send_to_telegram("🔴 Meshtastic relay bot stopped.", TOPIC_SYSTEM)
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
