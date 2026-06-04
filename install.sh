#!/usr/bin/bash

set -e

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
CURRENT_USER=$(whoami)

if [ ! -f "$SCRIPT_DIR/.env" ]; then
  echo ".env file not found. Run 'cp .env.example .env' and fill it."
  exit 1
fi

SERVICE_FILE="/tmp/meshtastic-tg.service"

cat <<EOF > "$SERVICE_FILE"
[Unit]
Description=Meshtastic to Telegram Relay Bot
After=network.target

[Service]
Type=simple
User=$CURRENT_USER
WorkingDirectory=$SCRIPT_DIR
EnvironmentFile=$SCRIPT_DIR/.env
ExecStart=$SCRIPT_DIR/venv/bin/python $SCRIPT_DIR/bot.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo mv "$SERVICE_FILE" /etc/systemd/system/meshtastic-tg.service

sudo systemctl daemon-reload
sudo systemctl enable --now meshtastic-tg.service

echo "Bot is running. Use 'sudo systemctl status meshtastic-tg.service' to check status."
