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

// ---------- local modules (untouched) ----------
import { createMetadata } from './lib/metadata.js';
import { createRbac } from './lib/rbac.js';
import { createSessionManager } from './lib/sessionManager.js';
import { createWsHub } from './lib/wsHub.js';
import { createSessionRegistry } from './lib/sessionRegistry.js';

// ---------- route modules (new) ----------
import { buildHealthRouter } from './routes/health.js';
import { buildAdminRouter } from './routes/admin.js';
import { buildSessionsRouter } from './routes/sessions.js';
import { buildAclRouter } from './routes/acl.js';
import { buildMessagesRouter } from './routes/messages.js';
import { buildMediaRouter } from './routes/media.js';
import { buildContactsRouter } from './routes/contacts.js';
import { buildChatsRouter } from './routes/chats.js';

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

// ---------- Mount routers (all original endpoints preserved) ----------
app.use(buildHealthRouter());
app.use(buildAdminRouter({ db, meta, rbac, requireUser }));
app.use(buildSessionsRouter({ rbac, registry, sessions, requireUser, ensureAllowed }));
app.use(buildAclRouter({ rbac, requireUser }));
app.use(buildMessagesRouter({ sessions, requireUser, ensureAllowed }));
app.use(buildMediaRouter({ sessions, requireUser, ensureAllowed }));
app.use(buildContactsRouter({ sessions, requireUser, ensureAllowed }));
app.use(buildChatsRouter({ sessions, requireUser, ensureAllowed }));

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
