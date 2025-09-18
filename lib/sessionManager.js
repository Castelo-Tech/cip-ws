// lib/sessionManager.js
// Multi-account, multi-label WA session manager using LocalAuth.
// Emits events: { type, ts, accountId, label (sessionId), waId?, ... }
// Includes sendText/sendMedia, media download, and contacts/chats helpers.

import { EventEmitter } from 'events';
import fs from 'fs';
import path from 'path';
import wwebjs from 'whatsapp-web.js';

import { sleep, rand, normalizeChatId } from './session/utils.js';
import { makeContacts } from './session/contacts.js';
import { makeChats } from './session/chats.js';

const { Client, LocalAuth, MessageMedia } = wwebjs;

export function createSessionManager({ dataPath = './.wwebjs_auth', registry }) {
  const DATA_PATH = path.resolve(dataPath);
  if (!fs.existsSync(DATA_PATH)) fs.mkdirSync(DATA_PATH, { recursive: true });

  const ev = new EventEmitter();                 // emits 'evt'
  const clients = new Map();                     // key -> Client
  const states  = new Map();                     // key -> status
  const qrs     = new Map();                     // key -> qr
  const selfIds = new Map();                     // key -> waId

  // Lightweight message cache for media retrieval (hasMedia only)
  // key: `${accountId}::${label}::${messageId}` -> { msgRef, expiresAt }
  const mediaMsgCache = new Map();
  const MEDIA_CACHE_TTL_MS = 15 * 60 * 1000; // 15 minutes

  // Helpers for keys
  const keyOf = ({ accountId, label }) => `${accountId}::${label}`;
  const msgKeyOf = ({ accountId, label, messageId }) => `${accountId}::${label}::${messageId}`;

  function parseClientIdFromDir(dirName) {
    if (!dirName?.startsWith('session-')) return null;
    const id = dirName.slice('session-'.length);
    const idx = id.indexOf('__');
    if (idx <= 0) return null;
    return { accountId: id.slice(0, idx), label: id.slice(idx + 2) };
  }

  function emit(meta, type, extra = {}) {
    const evt = {
      type,
      ts: Date.now(),
      accountId: meta.accountId,
      sessionId: meta.label,
      waId: selfIds.get(keyOf(meta)) || null,
      ...extra,
    };
    ev.emit('evt', evt);
  }

  function rememberIfMedia(meta, msg) {
    try {
      if (!msg?.hasMedia) return;
      const id = msg?.id?._serialized;
      if (!id) return;
      const k = msgKeyOf({ accountId: meta.accountId, label: meta.label, messageId: id });
      mediaMsgCache.set(k, { msgRef: msg, expiresAt: Date.now() + MEDIA_CACHE_TTL_MS });
    } catch {}
  }

  // periodic cleanup
  setInterval(() => {
    const now = Date.now();
    for (const [k, v] of mediaMsgCache.entries()) {
      if (!v || v.expiresAt <= now) mediaMsgCache.delete(k);
    }
  }, 60 * 1000);

  function bind(meta, client) {
    const key = keyOf(meta);
    states.set(key, 'starting');
    registry.setStatus(meta.accountId, meta.label, 'starting');

    client.on('qr', (qr) => {
      states.set(key, 'scanning');
      qrs.set(key, qr);
      emit(meta, 'qr', { qr });
    });

    client.on('ready', async () => {
      const id =
        client?.info?.wid?._serialized ||
        client?.info?.me?._serialized ||
        null;
      if (id) {
        selfIds.set(key, id);
        await registry.setReady(meta.accountId, meta.label, id);
      }
      states.set(key, 'ready');
      qrs.delete(key);
      emit(meta, 'ready', id ? { self: { waId: id, label: meta.label } } : {});
    });

    client.on('disconnected', (reason) => {
      states.set(key, 'disconnected');
      registry.setStatus(meta.accountId, meta.label, 'disconnected');
      emit(meta, 'disconnected', { reason });
    });

    client.on('auth_failure', (err) => {
      states.set(key, 'auth_failure');
      registry.setStatus(meta.accountId, meta.label, 'auth_failure');
      emit(meta, 'auth_failure', { err: String(err) });
    });

    client.on('error', (err) => {
      states.set(key, 'error');
      registry.setStatus(meta.accountId, meta.label, 'error');
      emit(meta, 'error', { err: String(err?.message || err) });
    });

    // Passive observation only (no send)
    client.on('message_create', (m) => {
      rememberIfMedia(meta, m);

      const id = m?.id?._serialized;
      const base = {
        id,
        chatId: m.fromMe ? m.to : m.from,
        fromMe: m.fromMe,
        body: m.body,
        messageType: m.type,
        hasMedia: !!m.hasMedia,
        waTimestamp: m.timestamp,
      };

      const mediaHint = m.hasMedia && id
        ? { mediaUrlPath: `/media/${encodeURIComponent(id)}?accountId=${encodeURIComponent(meta.accountId)}&label=${encodeURIComponent(meta.label)}` }
        : {};

      emit(meta, 'message', { ...base, ...mediaHint });
    });
  }

  function init(meta) {
    const key = keyOf(meta);
    if (clients.has(key)) return clients.get(key);

    const pArgs = ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'];
    const executablePath = process.env.CHROME_PATH || undefined; // if unset, Puppeteer's Chromium is used
    const client = new Client({
      authStrategy: new LocalAuth({
        clientId: `${meta.accountId}__${meta.label}`,
        dataPath: DATA_PATH,
      }),
      puppeteer: {
        args: pArgs,
        executablePath,
      },
    });
    bind(meta, client);
    clients.set(key, client);
    client.initialize();
    return client;
  }

  async function stop(meta) {
    const key = keyOf(meta);
    const c = clients.get(key);
    if (!c) return;
    try {
      await c.destroy();
    } finally {
      clients.delete(key);
    }
    states.set(key, 'stopped');
    registry.setStatus(meta.accountId, meta.label, 'stopped');
    emit(meta, 'stopped');
  }

  async function destroy(meta) {
    const key = keyOf(meta);
    const c = clients.get(key);
    if (c) {
      try { await c.logout().catch(() => {}); } catch {}
      try { await c.destroy(); } catch {}
      clients.delete(key);
    }
    try {
      const dir = path.join(DATA_PATH, `session-${meta.accountId}__${meta.label}`);
      fs.rmSync(dir, { recursive: true, force: true });
    } catch {}
    states.delete(key);
    qrs.delete(key);
    selfIds.delete(key);
    await registry.remove(meta.accountId, meta.label);
    emit(meta, 'destroyed');
  }

  function status(meta) { return states.get(keyOf(meta)) || null; }
  function qr(meta)     { return qrs.get(keyOf(meta)) || null; }

  function listRunning(accountId) {
    const out = [];
    for (const [key] of clients.entries()) {
      const [aid, label] = key.split('::');
      if (accountId && aid !== accountId) continue;
      out.push({
        accountId: aid,
        label,
        status: states.get(key) || null,
        waId: selfIds.get(key) || null,
        hasQr: qrs.has(key),
      });
    }
    out.sort((a, b) => a.label.localeCompare(b.label));
    return out;
  }

  async function restoreAllFromFs() {
    try {
      const items = fs.readdirSync(DATA_PATH, { withFileTypes: true });
      let count = 0;
      for (const d of items) {
        if (!d.isDirectory()) continue;
        const meta = parseClientIdFromDir(d.name);
        if (!meta) continue;
        if (!clients.has(keyOf(meta))) {
          init(meta);
          count++;
        }
      }
      return count;
    } catch (e) {
      console.error('restoreAllFromFs failed', e);
      return 0;
    }
  }

  // ---------- Sending ----------
  async function getReadyClient(meta) {
    const k = keyOf(meta);
    const c = clients.get(k);
    if (!c) throw new Error('client not initialized');
    const st = states.get(k);
    if (st !== 'ready') throw new Error(`client not ready (status=${st})`);
    return c;
  }

  async function sendText({ accountId, label, to, text, options = {} }) {
    const client = await getReadyClient({ accountId, label });
    const chatId = normalizeChatId(to);
    if (!chatId) throw new Error('invalid "to"');

    const msg = await client.sendMessage(chatId, String(text), options);
    emit({ accountId, label }, 'sent', {
      id: msg?.id?._serialized,
      chatId,
      body: String(text),
      messageType: 'chat',
      fromMe: true,
      waTimestamp: msg?.timestamp || Date.now(),
    });
    return msg;
  }

  async function sendMedia({ accountId, label, to, media, options = {} }) {
    const client = await getReadyClient({ accountId, label });
    const chatId = normalizeChatId(to);
    if (!chatId) throw new Error('invalid "to"');

    let mm = null;
    if (media?.data && media?.mimetype) {
      mm = new MessageMedia(String(media.mimetype), String(media.data), media?.filename || null, media?.filesize || null);
    } else if (media?.url) {
      mm = await MessageMedia.fromUrl(String(media.url));
    } else if (media?.localPath) {
      mm = MessageMedia.fromFilePath(String(media.localPath));
    } else {
      throw new Error('invalid media payload');
    }

    const msg = await client.sendMessage(chatId, mm, options);
    emit({ accountId, label }, 'sent', {
      id: msg?.id?._serialized,
      chatId,
      body: options?.caption || '',
      messageType: 'media',
      fromMe: true,
      waTimestamp: msg?.timestamp || Date.now(),
    });
    return msg;
  }

  async function downloadMessageMedia({ accountId, label, messageId }) {
    const k = msgKeyOf({ accountId, label, messageId });
    const entry = mediaMsgCache.get(k);
    if (!entry?.msgRef) return null;

    const m = await entry.msgRef.downloadMedia();
    if (!m?.data) return null;
    return { mimetype: m.mimetype || 'application/octet-stream', filename: m.filename || null, dataB64: m.data };
  }

  // ---------- Feature facades (contacts/chats) ----------
  const contacts = makeContacts({ getReadyClient });
  const chats    = makeChats({ getReadyClient });

  // Public API (unchanged)
  return {
    init, stop, destroy, status, qr,
    listRunning, restoreAllFromFs,
    on: ev.on.bind(ev),
    off: ev.off?.bind(ev) || ((...args) => ev.removeListener(...args)),

    sendText,
    sendMedia,
    downloadMessageMedia,

    // Contacts
    getContacts: contacts.getContacts,
    lookupContactsByNumbers: contacts.lookupContactsByNumbers,
    checkContactByNumber: contacts.checkContactByNumber,

    // Chats
    getChats: chats.getChats,
    getChatByNumber: chats.getChatByNumber,
    getChatsByNumbers: chats.getChatsByNumbers,
  };
}
