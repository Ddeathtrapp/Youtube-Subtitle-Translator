#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
sudo apt update && sudo apt install -y python3-venv ffmpeg nodejs curl iw
python3 -m venv venv
./venv/bin/pip install -U pip -r requirements.txt

./venv/bin/python -c "import huggingface_hub as h, transformers as t; h.snapshot_download('JustFrederik/nllb-200-distilled-600M-ct2-int8'); t.AutoTokenizer.from_pretrained('facebook/nllb-200-distilled-600M')"

sudo tee /etc/systemd/system/yt-ar.service >/dev/null <<UNIT
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
UNIT

sudo systemctl daemon-reload
sudo systemctl enable yt-ar
sudo systemctl restart yt-ar

# --- Tailscale: reach the app from anywhere on your tailnet (Serve only, never Funnel) ---
# One-time, in the Tailscale admin console (DNS page): turn on MagicDNS and HTTPS Certificates.
command -v tailscale >/dev/null || curl -fsSL https://tailscale.com/install.sh | sh
if ! tailscale ip -4 >/dev/null 2>&1; then
  echo "Log in to Tailscale: open the URL it prints (on your phone is fine)"
  sudo tailscale up
fi
# HTTPS on the tailnet name -> local app. --bg keeps it across reboots. LAN access on :8000 keeps working.
sudo tailscale serve --bg --https=443 http://127.0.0.1:8000 || echo "tailscale serve failed: enable HTTPS Certificates in the admin console, then rerun this script"

# Optional firewall (off by default): FIREWALL=1 bash install.sh  -> only tailnet + home LAN (ssh and :8000) can connect
if [ "${FIREWALL:-0}" = 1 ]; then
  LAN=$(ip -4 -o route show scope link | awk '$3!="tailscale0"{print $1; exit}')
  if [ -n "$LAN" ]; then
    sudo apt install -y ufw
    sudo ufw default deny incoming
    sudo ufw default allow outgoing
    sudo ufw allow in on tailscale0
    sudo ufw allow from "$LAN" to any port 22,8000 proto tcp
    sudo ufw --force enable
  else
    echo "No LAN route found - firewall skipped"
  fi
fi

echo "LAN:       http://$(hostname -I | awk '{print $1}'):8000"
TS=$(tailscale status --json 2>/dev/null | ./venv/bin/python -c "import sys,json;print(json.load(sys.stdin)['Self']['DNSName'].rstrip('.'))" 2>/dev/null) && echo "Tailscale: https://$TS"
