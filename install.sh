#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
sudo apt update && sudo apt install -y python3-venv ffmpeg nodejs
python3 -m venv venv
./venv/bin/pip install -U pip -r requirements.txt

read -rp "Gemini API key (aistudio.google.com/apikey): " K
printf 'GEMINI_API_KEY=%s\n' "$K" > gemini.env
chmod 600 gemini.env

sudo tee /etc/systemd/system/yt-ar.service >/dev/null <<EOF
[Unit]
Description=YouTube Arabic subtitle server
After=network-online.target
Wants=network-online.target

[Service]
User=$USER
WorkingDirectory=$PWD
EnvironmentFile=$PWD/gemini.env
ExecStart=$PWD/venv/bin/uvicorn server:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now yt-ar
echo "Open: http://$(hostname -I | awk '{print $1}'):8000"
