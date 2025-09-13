# ---- Base: small, stable Debian with Node 18 ----
FROM node:18-bullseye-slim

# 1) Prevent Puppeteer from downloading Chromium (we'll install Chrome ourselves)
ENV PUPPETEER_SKIP_DOWNLOAD=true
# 2) So your server's chrome auto-detection finds it
ENV CHROME_PATH=/usr/bin/google-chrome

# 3) OS deps & Google Chrome
RUN apt-get update && apt-get install -y \
    ca-certificates gnupg wget curl unzip \
    # Common libs Puppeteer/Chrome need
    libx11-6 libx11-xcb1 libxcb1 libxcomposite1 libxcursor1 libxdamage1 \
    libxi6 libxtst6 libnss3 libxrandr2 libasound2 libpangocairo-1.0-0 \
    libatk1.0-0 libcups2 libatspi2.0-0 libdrm2 libgbm1 libpango-1.0-0 \
    libgtk-3-0 fonts-liberation \
 && rm -rf /var/lib/apt/lists/*

# Add Google’s repo & install Chrome Stable
RUN set -eux; \
    wget -qO- https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor > /usr/share/keyrings/google-linux.gpg; \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list; \
    apt-get update && apt-get install -y google-chrome-stable && rm -rf /var/lib/apt/lists/*; \
    ln -sf /usr/bin/google-chrome /usr/bin/chromium || true

# 4) App setup
WORKDIR /usr/src/app

# Copy only package files first for layer caching
COPY package*.json ./

# Install deps (respects package-lock.json if present)
RUN npm install --omit=dev

# Copy the rest of your app (index.js, etc.)
COPY . .

# 5) Expose port & run
EXPOSE 3001

# In containers Chrome runs fine as non-root, but we already pass --no-sandbox in your code.
# Use a smaller /dev/shm to avoid Chrome crash in small containers
# (or add '--disable-dev-shm-usage' to your Puppeteer args if you prefer).
CMD ["node", "--trace-warnings", "index.js"]
