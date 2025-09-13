// index.js (ESM) — minimal WA server with CORS * and WS stream
// Start: npm install && npm start

import express from 'express';
import cors from 'cors';
import http from 'http';
import fs from 'fs';
import { WebSocketServer } from 'ws';
import wwebjs from 'whatsapp-web.js';
const { Client, NoAuth } = wwebjs;

const PORT = 3001;
const SESSION_ID = 'session-a';

const app = express();
app.use(express.json());

// CORS: allow all
app.use(cors({ origin: '*', methods: ['GET','POST','OPTIONS'], allowedHeaders: ['Content-Type'] }));
app.options('*', (req, res) => res.sendStatus(204)); // handle preflight quickly

// ---------------- logging helper ----------------
function log(...args) {
  const t = new Date().toISOString().split('T')[1].replace('Z','');
  console.log(`[${t}]`, ...args);
}

// ---------------- WS hub ----------------
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });
const sockets = new Set();

function broadcast(payload) {
  const msg = JSON.stringify(payload);
  for (const ws of sockets) if (ws.readyState === ws.OPEN) ws.send(msg);
}

server.on('upgrade', (req, socket, head) => {
  if (!req.url || !req.url.startsWith('/ws')) return socket.destroy();
  wss.handleUpgrade(req, socket, head, (ws) => {
    sockets.add(ws);
    ws.on('close', () => sockets.delete(ws));
    ws.send(JSON.stringify({ type: 'hello', ts: Date.now(), sessionId: SESSION_ID }));
  });
});

// ---------------- Chrome/Chromium resolution ----------------
function fileExists(p) { try { return p && fs.existsSync(p); } catch { return false; } }
function resolveChromePath() {
  const candidates = [
    process.env.CHROME_PATH,
    '/usr/bin/google-chrome-stable',
    '/usr/bin/google-chrome',
    '/usr/bin/chromium-browser',
    '/usr/bin/chromium',
    '/snap/bin/chromium'
  ].filter(Boolean);
  for (const p of candidates) if (fileExists(p)) return p;
  return null;
}
const executablePath = resolveChromePath();
if (executablePath) {
  log('Chrome executable detected at:', executablePath);
} else {
  log('⚠️  No system Chrome/Chromium detected. Puppeteer will try bundled browser.');
  log('    If it hangs, install Chrome/Chromium and common libs.');
}

// ---------------- WhatsApp client (NoAuth) ----------------
let started = false;
let status = 'idle';  // 'idle'|'scanning'|'ready'|'disconnected'|'auth_failure'|'error'
let lastQr = null;
let lastError = null;

const client = new Client({
  authStrategy: new NoAuth(), // scan QR after /init
  puppeteer: {
    executablePath: executablePath || undefined,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  }
});

// visibility
client.on('loading_screen', (percent, msg) => log('loading_screen:', percent, msg));
client.on('change_state', (state) => log('change_state:', state));

client.on('qr', (qr) => {
  lastQr = qr;
  status = 'scanning';
  log('QR received (emitting to clients)');
  broadcast({ type: 'qr', ts: Date.now(), sessionId: SESSION_ID, qr });
});

client.on('ready', () => {
  status = 'ready';
  lastQr = null;
  log('Client is ready');
  broadcast({ type: 'ready', ts: Date.now(), sessionId: SESSION_ID });
});

client.on('message_create', (m) => {
  // Inbound + outbound (incl. phone-sent on multi-device)
  broadcast({
    type: 'message',
    ts: Date.now(),
    sessionId: SESSION_ID,
    id: m.id?._serialized,
    chatId: m.fromMe ? m.to : m.from,
    fromMe: m.fromMe,
    body: m.body,
    messageType: m.type,
    hasMedia: !!m.hasMedia,
    waTimestamp: m.timestamp
  });
});

client.on('message_ack', (m, ack) => {
  broadcast({
    type: 'message_ack',
    ts: Date.now(),
    sessionId: SESSION_ID,
    id: m.id?._serialized,
    chatId: m.fromMe ? m.to : m.from,
    ack
  });
});

client.on('disconnected', (reason) => {
  status = 'disconnected';
  log('Disconnected:', reason);
  broadcast({ type: 'disconnected', ts: Date.now(), sessionId: SESSION_ID, reason });
});

client.on('auth_failure', (err) => {
  status = 'auth_failure';
  lastError = String(err);
  log('Auth failure:', lastError);
  broadcast({ type: 'auth_failure', ts: Date.now(), sessionId: SESSION_ID, err: lastError });
});

client.on('error', (err) => {
  status = 'error';
  lastError = String(err?.message || err);
  log('Client error:', lastError);
  broadcast({ type: 'error', ts: Date.now(), sessionId: SESSION_ID, err: lastError });
});

// -------------- safety timer: detect "no QR" hang --------------
let initTimer = null;
function armInitWatchdog() {
  clearTimeout(initTimer);
  initTimer = setTimeout(() => {
    if (status === 'scanning' && !lastQr) {
      status = 'error';
      lastError = 'Browser failed to start or QR not generated. Install Chrome/Chromium and required libs.';
      log('⛔ No QR after timeout.');
      broadcast({ type: 'error', ts: Date.now(), sessionId: SESSION_ID, err: lastError });
    }
  }, 15000);
}

// ---------------- HTTP API ----------------
app.post('/init', async (_req, res) => {
  try {
    if (!started) {
      started = true;
      status = 'scanning';
      lastError = null;
      lastQr = null;
      log('Initializing WhatsApp client…');
      client.initialize();
      armInitWatchdog();
    }
    res.json({ ok: true, status });
  } catch (e) {
    status = 'error';
    lastError = e.message;
    log('Init error:', lastError);
    res.status(500).json({ ok: false, error: lastError });
  }
});

app.get('/status', (_req, res) => res.json({ status, error: lastError || null }));
app.get('/qr', (_req, res) => res.json({ qr: lastQr }));

app.post('/send', async (req, res) => {
  try {
    const { chatId, body } = req.body || {};
    const msg = await client.sendMessage(String(chatId), String(body ?? ''));
    broadcast({ type: 'sent', ts: Date.now(), sessionId: SESSION_ID, id: msg.id._serialized, chatId, body });
    res.json({ id: msg.id._serialized });
  } catch (e) {
    log('Send error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get('/health', (_req, res) => res.json({ up: true, status, hasQr: !!lastQr }));

// ---------------- Start ----------------
server.listen(PORT, () => log(`HTTP http://localhost:${PORT} | WS ws://localhost:${PORT}/ws`));
