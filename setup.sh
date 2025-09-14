#!/usr/bin/env bash
# setup.sh — simplest one-shot (no env files). Run from repo root.

set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

# Resolve absolute repo path from THIS file (not $PWD)
SCRIPT_PATH="$(readlink -f "$0")"
REPO_DIR="$(dirname "$SCRIPT_PATH")"
echo "Repo dir: $REPO_DIR"

# Sanity check
test -f "$REPO_DIR/package.json" || { echo "ERROR: package.json not found in $REPO_DIR"; exit 1; }

echo "==> Installing OS prerequisites…"
apt-get update
apt-get install -y curl ca-certificates build-essential \
  fonts-liberation libasound2 libatk-bridge2.0-0 libatk1.0-0 libc6 \
  libcairo2 libcups2 libdbus-1-3 libdrm2 libexpat1 libfontconfig1 \
  libgbm1 libglib2.0-0 libgtk-3-0 libnspr4 libnss3 libpango-1.0-0 \
  libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxdamage1 libxext6 \
  libxfixes3 libxrandr2 libxrender1 libxss1 libxtst6 wget xdg-utils

if ! command -v node >/dev/null 2>&1; then
  echo "==> Installing Node.js LTS…"
  curl -fsSL https://deb.nodesource.com/setup_lts.x | bash -
  apt-get install -y nodejs
fi

echo "==> Installing npm dependencies in $REPO_DIR …"
if [ -f "$REPO_DIR/package-lock.json" ]; then
  npm --prefix "$REPO_DIR" ci
else
  npm --prefix "$REPO_DIR" install
fi

echo "==> Creating systemd service to run: npm start"
SERVICE_PATH="/etc/systemd/system/whatsapp-server.service"
cat >"$SERVICE_PATH" <<EOF
[Unit]
Description=WhatsApp Server (npm start)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=${REPO_DIR}
ExecStart=/usr/bin/npm run start --silent
Restart=always
RestartSec=3
Environment=PUPPETEER_SKIP_DOWNLOAD=false

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now whatsapp-server

echo "==> Done."
systemctl status whatsapp-server --no-pager || true
echo "Logs: journalctl -u whatsapp-server -f"
