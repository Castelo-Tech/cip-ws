// server.js
// Main wiring: REST + WS, Firebase Admin auth, Firestore ACL/RBAC.
// Sessions are per-account and per-label. Each WS connection binds to one accountId and is ACL-filtered.

// ---------- core ----------
import express from 'express';
import cors from 'cors';
import http from 'http';

// ---------- firebase admin ----------
import { initializeApp, applicationDefault } from 'firebase-admin/app';
import { getAuth } from 'firebase-admin/auth';
import { getFirestore } from 'firebase-admin/firestore';

// ---------- local modules ----------
import { createMetadata } from './lib/metadata.js';
import { createRbac } from './lib/rbac.js';
import { createSessionManager } from './lib/sessionManager.js';
import { createWsHub } from './lib/wsHub.js';
import { createSessionRegistry } from './lib/sessionRegistry.js';

const PORT = 3001;

// ---------- Firebase Admin init (uses GCE ADC) ----------
initializeApp({ credential: applicationDefault() });
const db = getFirestore();
const authAdmin = getAuth();

// ---------- Express ----------
const app = express();
app.use(express.json({ limit: '25mb' })); // allow base64 media payloads
app.use(
  cors({
    origin: '*',
    methods: ['GET', 'POST', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
  })
);
app.options('*', (_req, res) => res.sendStatus(204));

// ---------- Utilities ----------
const meta = createMetadata();
const rbac = createRbac({ db });
const registry = createSessionRegistry({ db });
const sessions = createSessionManager({ dataPath: './.wwebjs_auth', registry }); // WA LocalAuth + Firestore registry

// 🔸 Boot-time restore (idempotent).
(async () => {
  try {
    const restored = (await sessions.restoreAllFromFs?.()) ?? null;
    if (restored !== null) {
      console.log(`[boot] restored ${restored} WA session(s) from disk`);
    } else {
      console.log('[boot] restoreAllFromFs() not available; skip disk restore');
    }
  } catch (e) {
    console.error('[boot] restoreAllFromFs failed', e);
  }
})();

// Helper: bearer token → Firebase user (uid)
async function requireUser(req, res, next) {
  try {
    const hdr = String(req.headers.authorization || '');
    const token = hdr.startsWith('Bearer ') ? hdr.slice(7) : '';
    if (!token) return res.status(401).json({ error: 'missing Authorization Bearer token' });
    const decoded = await authAdmin.verifyIdToken(token);
    req.user = { uid: decoded.uid, email: decoded.email || null };
    next();
  } catch (e) {
    return res.status(401).json({ error: 'invalid token' });
  }
}

// Small helper: is user allowed to act on this session?
async function ensureAllowed(req, res, accountId, label, { requireAdmin = false } = {}) {
  const allowed = await rbac.allowedSessions(accountId, req.user.uid);
  if (!allowed.role) {
    res.status(403).json({ error: 'not a member' });
    return null;
  }
  if (requireAdmin && allowed.role !== 'Administrator') {
    res.status(403).json({ error: 'not an Administrator' });
    return null;
  }
  if (allowed.role === 'Administrator') return allowed;
  if (!allowed.sessions.includes(label)) {
    res.status(403).json({ error: 'session not allowed by ACL' });
    return null;
  }
  return allowed;
}

// ---------- REST: Health ----------
app.get('/healthz', (_req, res) => {
  res.json({ ok: true, ts: Date.now() });
});

// ---------- REST: Server self-assign to account.wsServer ----------
app.post('/admin/assignServer', requireUser, async (req, res) => {
  const { accountId, labels = {} } = req.body || {};
  if (!accountId) return res.status(400).json({ error: 'accountId required' });

  // admin check
  const role = await rbac.getRole(accountId, req.user.uid);
  if (role !== 'Administrator') return res.status(403).json({ error: 'not an Administrator' });

  // gather VM meta
  const [ip, name, zone, project] = await Promise.all([
    meta.externalIp(),
    meta.instanceName(),
    meta.zone(),
    meta.projectId()
  ]);

  await db.collection('accounts').doc(accountId).set(
    {
      wsServer: {
        assignedAt: new Date(),
        ip,
        instance: name,
        zone,
        project,
        labels: Object(labels),
      },
    },
    { merge: true }
  );

  res.json({ ok: true, accountId, wsServer: { ip, instance: name, zone, project, labels } });
});

// ---------- REST: Sessions (Admin) ----------
app.get('/sessions', requireUser, async (req, res) => {
  const accountId = String(req.query.accountId || '');
  if (!accountId) return res.status(400).json({ error: 'accountId required' });

  // member check (any role can list)
  const role = await rbac.getRole(accountId, req.user.uid);
  if (!role) return res.status(403).json({ error: 'not a member' });

  res.json(await registry.list(accountId));
});

app.post('/sessions/init', requireUser, async (req, res) => {
  const { accountId, label } = req.body || {};
  if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

  const allowed = await ensureAllowed(req, res, accountId, label, { requireAdmin: true });
  if (!allowed) return;

  sessions.init({ accountId, label });
  res.json({ ok: true, accountId, label, status: sessions.status({ accountId, label }) || 'starting' });
});

app.post('/sessions/stop', requireUser, async (req, res) => {
  const { accountId, label } = req.body || {};
  if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

  const allowed = await ensureAllowed(req, res, accountId, label, { requireAdmin: true });
  if (!allowed) return;

  await sessions.stop({ accountId, label });
  res.json({ ok: true, accountId, label, status: sessions.status({ accountId, label }) || 'stopped' });
});

app.post('/sessions/destroy', requireUser, async (req, res) => {
  const { accountId, label } = req.body || {};
  if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

  const allowed = await ensureAllowed(req, res, accountId, label, { requireAdmin: true });
  if (!allowed) return;

  await sessions.destroy({ accountId, label });
  res.json({ ok: true, accountId, label });
});

app.get('/status', requireUser, async (req, res) => {
  const accountId = String(req.query.accountId || '');
  const label = String(req.query.label || req.query.session || '');
  if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

  const role = await rbac.getRole(accountId, req.user.uid);
  if (!role) return res.status(403).json({ error: 'not a member' });

  res.json({
    accountId,
    label,
    status: sessions.status({ accountId, label }),
    waId: await registry.getWaId(accountId, label),
  });
});

app.get('/qr', requireUser, async (req, res) => {
  const accountId = String(req.query.accountId || '');
  const label = String(req.query.label || req.query.session || '');
  if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

  const role = await rbac.getRole(accountId, req.user.uid);
  if (!role) return res.status(403).json({ error: 'not a member' });

  res.json({ accountId, label, qr: sessions.qr({ accountId, label }) || null });
});

// ---------- REST: ACL (Admin manages per-account ACL docs) ----------
app.get('/acl/users', requireUser, async (req, res) => {
  const accountId = String(req.query.accountId || '');
  if (!accountId) return res.status(400).json({ error: 'accountId required' });

  const role = await rbac.getRole(accountId, req.user.uid);
  if (role !== 'Administrator') return res.status(403).json({ error: 'not an Administrator' });

  res.json(await rbac.listAcl(accountId));
});

app.post('/acl/grant', requireUser, async (req, res) => {
  const { accountId, uid, sessions: allowed } = req.body || {};
  if (!accountId || !uid || !Array.isArray(allowed) || !allowed.length)
    return res.status(400).json({ error: 'accountId, uid, sessions[] required' });

  const role = await rbac.getRole(accountId, req.user.uid);
  if (role !== 'Administrator') return res.status(403).json({ error: 'not an Administrator' });

  await rbac.setAcl(accountId, uid, allowed);
  res.json({ ok: true });
});

app.post('/acl/revoke', requireUser, async (req, res) => {
  const { accountId, uid } = req.body || {};
  if (!accountId || !uid) return res.status(400).json({ error: 'accountId, uid required' });

  const role = await rbac.getRole(accountId, req.user.uid);
  if (role !== 'Administrator') return res.status(403).json({ error: 'not an Administrator' });

  await rbac.setAcl(accountId, uid, []);
  res.json({ ok: true });
});

// ---------- NEW: Sessions live-truth & manual restore ----------
app.get('/sessions/running', requireUser, async (req, res) => {
  const accountId = String(req.query.accountId || '');
  if (!accountId) return res.status(400).json({ error: 'accountId required' });

  const role = await rbac.getRole(accountId, req.user.uid);
  if (!role) return res.status(403).json({ error: 'not a member' });

  if (typeof sessions.listRunning === 'function') {
    return res.json(sessions.listRunning(accountId));
  }

  const list = await registry.list(accountId);
  const augmented = await Promise.all(
    list.map(async (s) => {
      const status = sessions.status ? sessions.status({ accountId: s.accountId, label: s.label }) : s.status;
      return {
        accountId: s.accountId,
        label: s.label,
        status: status || s.status || null,
        waId: s.waId || null,
        hasQr: false,
      };
    })
  );
  res.json(augmented);
});

app.post('/sessions/restore', requireUser, async (req, res) => {
  const { accountId } = req.body || {};
  if (!accountId) return res.status(400).json({ error: 'accountId required' });

  const allowed = await ensureAllowed(req, res, accountId, 'any', { requireAdmin: true });
  if (!allowed) return;

  if (typeof sessions.restoreAllFromFs !== 'function') {
    return res.json({ ok: true, restored: null, note: 'restoreAllFromFs() not available in session manager' });
  }
  const n = await sessions.restoreAllFromFs();
  res.json({ ok: true, restored: n });
});

// ---------- Messaging ----------
app.post('/messages/send', requireUser, async (req, res) => {
  const { accountId, label, to, text, options = {} } = req.body || {};
  if (!accountId || !label || !to || !text) {
    return res.status(400).json({ error: 'accountId, label, to, text required' });
  }

  const allowed = await ensureAllowed(req, res, accountId, label);
  if (!allowed) return;

  const st = sessions.status({ accountId, label });
  if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

  try {
    const msg = await sessions.sendText({ accountId, label, to, text, options });
    res.json({ ok: true, id: msg?.id?._serialized || null, timestamp: msg?.timestamp || Date.now() });
  } catch (e) {
    res.status(500).json({ error: 'send failed', detail: String(e?.message || e) });
  }
});

app.post('/messages/sendMedia', requireUser, async (req, res) => {
  const { accountId, label, to, media, options = {} } = req.body || {};
  if (!accountId || !label || !to || !media) {
    return res.status(400).json({ error: 'accountId, label, to, media required' });
  }

  const allowed = await ensureAllowed(req, res, accountId, label);
  if (!allowed) return;

  const st = sessions.status({ accountId, label });
  if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

  try {
    const msg = await sessions.sendMedia({ accountId, label, to, media, options });
    res.json({ ok: true, id: msg?.id?._serialized || null, timestamp: msg?.timestamp || Date.now() });
  } catch (e) {
    res.status(500).json({ error: 'send failed', detail: String(e?.message || e) });
  }
});

// ---------- Media download ----------
app.get('/media/:messageId', requireUser, async (req, res) => {
  const accountId = String(req.query.accountId || '');
  const label = String(req.query.label || '');
  const messageId = String(req.params.messageId || '');
  const disposition = String(req.query.disposition || 'inline');

  if (!accountId || !label || !messageId) {
    return res.status(400).json({ error: 'accountId, label, messageId required' });
  }
  const allowed = await ensureAllowed(req, res, accountId, label);
  if (!allowed) return;

  try {
    const media = await sessions.downloadMessageMedia({ accountId, label, messageId });
    if (!media) return res.status(404).json({ error: 'media not available' });

    const filename = media.filename || `wa-${messageId}`;
    res.setHeader('Content-Type', media.mimetype || 'application/octet-stream');
    res.setHeader('Content-Length', Buffer.byteLength(media.dataB64 || '', 'base64'));
    res.setHeader('Content-Disposition', `${disposition}; filename="${filename.replace(/"/g, '')}"`);
    res.end(Buffer.from(media.dataB64, 'base64'));
  } catch (e) {
    res.status(500).json({ error: 'download failed', detail: String(e?.message || e) });
  }
});

// ---------- Contacts ----------
app.get('/contacts', requireUser, async (req, res) => {
  const accountId = String(req.query.accountId || '');
  const label = String(req.query.label || '');
  const withDetails = String(req.query.details || 'false') === 'true';

  if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

  const allowed = await ensureAllowed(req, res, accountId, label);
  if (!allowed) return;

  const st = sessions.status({ accountId, label });
  if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

  try {
    const list = await sessions.getContacts({ accountId, label, withDetails });
    res.json({ ok: true, count: list.length, contacts: list });
  } catch (e) {
    res.status(500).json({ error: 'contacts_failed', detail: String(e?.message || e) });
  }
});

app.post('/contacts/lookup', requireUser, async (req, res) => {
  const { accountId, label, numbers, countryCode, withDetails = true } = req.body || {};
  if (!accountId || !label || !Array.isArray(numbers) || numbers.length === 0) {
    return res.status(400).json({ error: 'accountId, label, numbers[] required' });
  }

  const allowed = await ensureAllowed(req, res, accountId, label);
  if (!allowed) return;

  const st = sessions.status({ accountId, label });
  if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

  try {
    const out = await sessions.lookupContactsByNumbers({
      accountId, label, numbers, countryCode: countryCode || null, withDetails: !!withDetails
    });
    res.json({ ok: true, results: out });
  } catch (e) {
    res.status(500).json({ error: 'lookup_failed', detail: String(e?.message || e) });
  }
});

// NEW: single-number check (on-demand details only when asked)
// GET /contacts/check?accountId=...&label=...&number=...&countryCode=...&details=true|false
app.get('/contacts/check', requireUser, async (req, res) => {
  const accountId = String(req.query.accountId || '');
  const label = String(req.query.label || '');
  const number = String(req.query.number || '');
  const countryCode = req.query.countryCode ? String(req.query.countryCode) : null;
  const withDetails = String(req.query.details || 'false') === 'true';

  if (!accountId || !label || !number) {
    return res.status(400).json({ error: 'accountId, label, number required' });
  }
  const allowed = await ensureAllowed(req, res, accountId, label);
  if (!allowed) return;

  const st = sessions.status({ accountId, label });
  if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

  try {
    const out = await sessions.checkContactByNumber({ accountId, label, number, countryCode, withDetails });
    res.json({ ok: true, ...out });
  } catch (e) {
    res.status(500).json({ error: 'check_failed', detail: String(e?.message || e) });
  }
});

// ---------- NEW: Chats ----------
/**
 * GET /chats?accountId=...&label=...
 * Returns a lightweight list of chats (no messages).
 */
app.get('/chats', requireUser, async (req, res) => {
  const accountId = String(req.query.accountId || '');
  const label = String(req.query.label || '');
  if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

  const allowed = await ensureAllowed(req, res, accountId, label);
  if (!allowed) return;

  const st = sessions.status({ accountId, label });
  if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

  try {
    const chats = await sessions.getChats({ accountId, label });
    res.json({ ok: true, count: chats.length, chats });
  } catch (e) {
    res.status(500).json({ error: 'chats_failed', detail: String(e?.message || e) });
  }
});

/**
 * GET /chats/byNumber?accountId=...&label=...&number=...&countryCode=...
 * Looks up WA ID for number; returns chat metadata if it exists.
 */
app.get('/chats/byNumber', requireUser, async (req, res) => {
  const accountId = String(req.query.accountId || '');
  const label = String(req.query.label || '');
  const number = String(req.query.number || '');
  const countryCode = req.query.countryCode ? String(req.query.countryCode) : null;
  if (!accountId || !label || !number) return res.status(400).json({ error: 'accountId, label, number required' });

  const allowed = await ensureAllowed(req, res, accountId, label);
  if (!allowed) return;

  const st = sessions.status({ accountId, label });
  if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

  try {
    const result = await sessions.getChatByNumber({ accountId, label, number, countryCode });
    res.json({ ok: true, ...result });
  } catch (e) {
    res.status(500).json({ error: 'chat_lookup_failed', detail: String(e?.message || e) });
  }
});

app.post('/chats/byNumbers', requireUser, async (req, res) => {
  const { accountId, label, numbers, countryCode, withMessages = false, messagesLimit = 20 } = req.body || {};
  if (!accountId || !label || !Array.isArray(numbers) || numbers.length === 0) {
    return res.status(400).json({ error: 'accountId, label, numbers[] required' });
  }

  const allowed = await ensureAllowed(req, res, accountId, label);
  if (!allowed) return;

  const st = sessions.status({ accountId, label });
  if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

  try {
    const out = await sessions.getChatsByNumbers({
      accountId,
      label,
      numbers,
      countryCode: countryCode || null,
      withMessages: !!withMessages,
      messagesLimit: Math.max(1, Math.min(100, Number(messagesLimit) || 20)),
    });
    res.json({ ok: true, results: out });
  } catch (e) {
    res.status(500).json({ error: 'chat_bulk_lookup_failed', detail: String(e?.message || e) });
  }
});

// ---------- WS hub ----------
const server = http.createServer(app);
createWsHub({
  server,
  authAdmin,
  rbac,
  sessions,
  maxConnections: 2000,
});

// ---------- Start ----------
server.listen(PORT, () =>
  console.log(
    `HTTP http://0.0.0.0:${PORT} | WS ws://<host>:${PORT}/ws?accountId=<aid>&token=<FIREBASE_ID_TOKEN>`
  )
);
