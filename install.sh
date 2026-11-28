#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"
sudo apt update && sudo apt install -y python3-venv ffmpeg nodejs curl iw openssl avahi-daemon
python3 -m venv venv
./venv/bin/pip install -U pip -r requirements.txt

./venv/bin/python -c "import huggingface_hub as h, transformers as t; h.snapshot_download('JustFrederik/nllb-200-distilled-600M-ct2-int8'); t.AutoTokenizer.from_pretrained('facebook/nllb-200-distilled-600M')"

# --- local HTTPS: own certificate authority + a server certificate for this laptop (safe to rerun) ---
# Install certs/ca.crt once on the iPhone (open http://<laptop-ip>:8000/ca.crt), then use https://<hostname>.local:8443
mkdir -p certs
[ -f certs/ca.key ] || openssl req -x509 -newkey rsa:2048 -nodes -keyout certs/ca.key -out certs/ca.crt -days 3650 \
  -subj "/CN=yt-ar local CA" -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"
SAN="DNS:$(hostname).local,DNS:$(hostname),DNS:localhost,IP:127.0.0.1$(for i in $(hostname -I); do printf ',IP:%s' "$i"; done)"
if [ ! -f certs/server.crt ] || [ "$(cat certs/san.txt 2>/dev/null)" != "$SAN" ] || ! openssl x509 -checkend 2592000 -noout -in certs/server.crt >/dev/null; then
  openssl req -newkey rsa:2048 -nodes -keyout certs/server.key -out certs/server.csr -subj "/CN=$(hostname)"
  printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth\nkeyUsage=digitalSignature,keyEncipherment\nbasicConstraints=CA:FALSE\n' "$SAN" > certs/ext.cnf
  openssl x509 -req -in certs/server.csr -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -out certs/server.crt -days 800 -extfile certs/ext.cnf
  echo "$SAN" > certs/san.txt
fi
chmod 600 certs/*.key
# --- end local HTTPS ---

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

# --- Optional Tailscale (off by default): TAILSCALE=1 bash install.sh. Serve only, never Funnel. ---
# One-time, in the Tailscale admin console (DNS page): turn on MagicDNS and HTTPS Certificates.
if [ "${TAILSCALE:-0}" = 1 ]; then
  command -v tailscale >/dev/null || curl -fsSL https://tailscale.com/install.sh | sh
  if ! tailscale ip -4 >/dev/null 2>&1; then
    echo "Log in to Tailscale: open the URL it prints (on your phone is fine)"
    sudo tailscale up
  fi
  sudo tailscale serve --bg --https=443 http://127.0.0.1:8000 || echo "tailscale serve failed: enable HTTPS Certificates in the admin console, then rerun"
fi

# Optional firewall (off by default): FIREWALL=1 bash install.sh  -> only the home LAN (ssh, web, mDNS) and the tailnet can connect
if [ "${FIREWALL:-0}" = 1 ]; then
  LAN=$(ip -4 -o route show scope link | awk '$3!="tailscale0"{print $1; exit}')
  if [ -n "$LAN" ]; then
    sudo apt install -y ufw
    sudo ufw default deny incoming
    sudo ufw default allow outgoing
    sudo ufw allow in on tailscale0
    sudo ufw allow from "$LAN" to any port 22,8000,8443 proto tcp
    sudo ufw allow from "$LAN" to any port 5353 proto udp
    sudo ufw --force enable
  else
    echo "No LAN route found - firewall skipped"
  fi
fi

echo "HTTP:  http://$(hostname -I | awk '{print $1}'):8000"
echo "HTTPS: https://$(hostname).local:8443  (first install the certificate on the iPhone: open the HTTP address + /ca.crt)"
if [ "${TAILSCALE:-0}" = 1 ]; then
  TS=$(tailscale status --json 2>/dev/null | ./venv/bin/python -c "import sys,json;print(json.load(sys.stdin)['Self']['DNSName'].rstrip('.'))" 2>/dev/null) && echo "Tailscale: https://$TS"
fi
