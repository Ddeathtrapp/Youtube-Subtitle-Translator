#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
sudo apt update && sudo apt install -y python3-venv ffmpeg nodejs
python3 -m venv venv
./venv/bin/pip install -U pip -r requirements.txt

./venv/bin/python -c "import huggingface_hub as h, transformers as t; h.snapshot_download('JustFrederik/nllb-200-distilled-600M-ct2-int8'); t.AutoTokenizer.from_pretrained('facebook/nllb-200-distilled-600M')"

sudo tee /etc/systemd/system/yt-ar.service >/dev/null <<EOF
[Unit]
Description=YouTube Arabic subtitle server
After=network-online.target
Wants=network-online.target

[Service]
User=$USER
WorkingDirectory=$PWD
ExecStart=$PWD/venv/bin/uvicorn server:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable yt-ar
sudo systemctl restart yt-ar
echo "Open: http://$(hostname -I | awk '{print $1}'):8000"
