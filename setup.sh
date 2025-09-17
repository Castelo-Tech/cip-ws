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

echo "==> Node: $(node -v 2>/dev/null || echo 'not found'), npm: $(npm -v 2>/dev/null || echo 'not found')"

echo "==> Installing npm dependencies in $REPO_DIR …"
if [ -f "$REPO_DIR/package-lock.json" ]; then
  npm --prefix "$REPO_DIR" ci
else
  npm --prefix "$REPO_DIR" install
fi

# Ensure runtime data dirs exist and are owned by the service user (root here)
mkdir -p "$REPO_DIR/.wwebjs_auth" "$REPO_DIR/.wwebjs_cache"
chown -R root:root "$REPO_DIR/.wwebjs_auth" "$REPO_DIR/.wwebjs_cache"

echo "==> Creating/Updating systemd service to run: npm start"
SERVICE_NAME="whatsapp-server"
SERVICE_PATH="/etc/systemd/system/${SERVICE_NAME}.service"

# Backup existing unit if present
if [ -f "$SERVICE_PATH" ]; then
  cp -f "$SERVICE_PATH" "${SERVICE_PATH}.bak.$(date +%Y%m%d%H%M%S)"
fi

cat >"$SERVICE_PATH" <<EOF
[Unit]
Description=WhatsApp Server (npm start)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=${REPO_DIR}
# Keep your current entrypoint
ExecStart=/usr/bin/npm run start --silent
# Make sure data dirs exist before starting
ExecStartPre=/usr/bin/mkdir -p ${REPO_DIR}/.wwebjs_auth ${REPO_DIR}/.wwebjs_cache
ExecStartPre=/usr/bin/chown -R root:root ${REPO_DIR}/.wwebjs_auth ${REPO_DIR}/.wwebjs_cache
# Graceful shutdown + resiliency
KillSignal=SIGINT
TimeoutStopSec=20
Restart=always
RestartSec=3
# Useful env
Environment=NODE_ENV=production
Environment=PUPPETEER_SKIP_DOWNLOAD=false
# Give Puppeteer/WS more file descriptors
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}"

echo "==> Done."
systemctl status "${SERVICE_NAME}" --no-pager || true
echo "Logs: journalctl -u ${SERVICE_NAME} -f"
