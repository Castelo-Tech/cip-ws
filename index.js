// Minimal multi-session WA server (ESM) with LocalAuth persistence on VM disk.
// Features:
// - Create/init sessions on demand (QR flow) → /init?session=session-a
// - Query status/QR → /status?session=… , /qr?session=…
// - Send text → POST /send { sessionId, chatId, body }
// - Single WS hub → ws://host:3001/ws[?sessionId=…] (events include sessionId)
//
// Start: npm install && npm start

import express from 'express';
import cors from 'cors';
import http from 'http';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer } from 'ws';
import wwebjs from 'whatsapp-web.js';
const { Client, LocalAuth } = wwebjs;

const PORT = 3001;
const __filename = fileURLToPath(import.meta.url);
const __dirname  = path.dirname(__filename);

// ---- basic app ----
const app = express();
app.use(express.json());
app.use(cors({ origin: '*', methods: ['GET','POST','OPTIONS'], allowedHeaders: ['Content-Type'] }));
app.options('*', (req, res) => res.sendStatus(204));

// ---- data path for LocalAuth (persists on this VM) ----
const DATA_PATH = path.join(__dirname, '.wwebjs_auth');
if (!fs.existsSync(DATA_PATH)) fs.mkdirSync(DATA_PATH, { recursive: true });

// ---- WS hub (subscribe to "*" or a specific sessionId) ----
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });
const subscribers = new Map(); // channel -> Set<ws>

function sub(channel, ws) {
  if (!subscribers.has(channel)) subscribers.set(channel, new Set());
  subscribers.get(channel).add(ws);
  ws.on('close', () => subscribers.get(channel)?.delete(ws));
}
function pub(channel, payload) {
  const msg = JSON.stringify(payload);
  subscribers.get(channel)?.forEach(ws => ws.readyState === ws.OPEN && ws.send(msg));
  subscribers.get('*')?.forEach(ws => ws.readyState === ws.OPEN && ws.send(msg));
}
server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url || '', 'http://x');
  if (url.pathname !== '/ws') return socket.destroy();
  const sessionId = url.searchParams.get('sessionId'); // optional
  wss.handleUpgrade(req, socket, head, (ws) => {
    sub(sessionId || '*', ws);
    ws.send(JSON.stringify({ type: 'hello', ts: Date.now(), sessionId: sessionId || '*' }));
  });
});

// ---- multi-session state ----
const clients = new Map();       // sessionId -> Client
const status  = new Map();       // sessionId -> 'idle'|'starting'|'scanning'|'ready'|'disconnected'|'auth_failure'|'error'
const lastQr  = new Map();       // sessionId -> latest raw QR
const say = (...a) => console.log('[wa]', ...a);

// ---- create or return a session client ----
function getOrCreate(sessionId) {
  if (!sessionId) throw new Error('sessionId required');
  if (clients.has(sessionId)) return clients.get(sessionId);

  const client = new Client({
    authStrategy: new LocalAuth({ clientId: sessionId, dataPath: DATA_PATH }),
    puppeteer: { args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] }
  });

  status.set(sessionId, 'starting');

  client.on('qr', (qr) => {
    status.set(sessionId, 'scanning');
    lastQr.set(sessionId, qr);
    pub(sessionId, { type: 'qr', ts: Date.now(), sessionId, qr });
  });

  client.on('ready', () => {
    status.set(sessionId, 'ready');
    lastQr.delete(sessionId);
    say(`✅ [${sessionId}] ready`);
    pub(sessionId, { type: 'ready', ts: Date.now(), sessionId });
  });

  // inbound + outbound (including messages you send from your phone)
  client.on('message_create', (m) => {
    pub(sessionId, {
      type: 'message',
      ts: Date.now(),
      sessionId,
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
    pub(sessionId, {
      type: 'message_ack',
      ts: Date.now(),
      sessionId,
      id: m.id?._serialized,
      chatId: m.fromMe ? m.to : m.from,
      ack
    });
  });

  client.on('disconnected', (reason) => {
    status.set(sessionId, 'disconnected');
    pub(sessionId, { type: 'disconnected', ts: Date.now(), sessionId, reason });
  });

  client.on('auth_failure', (err) => {
    status.set(sessionId, 'auth_failure');
    pub(sessionId, { type: 'auth_failure', ts: Date.now(), sessionId, err: String(err) });
  });

  client.on('error', (err) => {
    status.set(sessionId, 'error');
    pub(sessionId, { type: 'error', ts: Date.now(), sessionId, err: String(err?.message || err) });
  });

  clients.set(sessionId, client);
  client.initialize();
  return client;
}

// ---- HTTP API ----

// 1) Create/init a session (scan QR if not linked yet)
app.post('/init', (req, res) => {
  try {
    const sid = String(req.query.session || req.body?.sessionId || '').trim();
    if (!sid) return res.status(400).json({ error: 'session required' });
    getOrCreate(sid);
    res.json({ ok: true, sessionId: sid, status: status.get(sid) || 'starting' });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// 2) Show status / qr (per session)
app.get('/status', (req, res) => {
  const sid = String(req.query.session || '').trim();
  if (!sid || !status.has(sid)) return res.status(404).json({ error: 'unknown session' });
  res.json({ sessionId: sid, status: status.get(sid) });
});
app.get('/qr', (req, res) => {
  const sid = String(req.query.session || '').trim();
  if (!sid || !clients.has(sid)) return res.status(404).json({ error: 'unknown session' });
  res.json({ sessionId: sid, qr: lastQr.get(sid) || null });
});

// 3) Send a text message
//    body: { sessionId, chatId, body }
app.post('/send', async (req, res) => {
  try {
    const sid   = String(req.body?.sessionId || '').trim();
    const chat  = String(req.body?.chatId    || '').trim();
    const text  = String(req.body?.body      ?? '');
    if (!sid || !chat) return res.status(400).json({ error: 'sessionId & chatId required' });
    const client = clients.get(sid) || getOrCreate(sid);
    const msg = await client.sendMessage(chat, text);
    pub(sid, { type: 'sent', ts: Date.now(), sessionId: sid, id: msg.id?._serialized, chatId: chat, body: text });
    res.json({ id: msg.id?._serialized });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Health
app.get('/health', (_req, res) => res.json({ up: true, sessions: Array.from(status.entries()).map(([k,v])=>({sessionId:k,status:v})) }));

// ---- start ----
server.listen(PORT, () => console.log(`HTTP http://localhost:${PORT} | WS ws://localhost:${PORT}/ws`));
