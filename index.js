// Main wiring: REST + WS, Firebase Admin auth, Firestore ACL/RBAC.
// No messaging endpoints. Sessions are per-account and per-label.
// Each WS connection binds to *one* accountId and is ACL-filtered.

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
app.use(express.json());
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

// 🔸 Boot-time restore (idempotent). Uses optional chaining so it won't break if the helper isn't present yet.
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

  const role = await rbac.getRole(accountId, req.user.uid);
  if (role !== 'Administrator') return res.status(403).json({ error: 'not an Administrator' });

  sessions.init({ accountId, label });
  res.json({ ok: true, accountId, label, status: sessions.status({ accountId, label }) || 'starting' });
});

app.post('/sessions/stop', requireUser, async (req, res) => {
  const { accountId, label } = req.body || {};
  if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

  const role = await rbac.getRole(accountId, req.user.uid);
  if (role !== 'Administrator') return res.status(403).json({ error: 'not an Administrator' });

  await sessions.stop({ accountId, label });
  res.json({ ok: true, accountId, label, status: sessions.status({ accountId, label }) || 'stopped' });
});

app.post('/sessions/destroy', requireUser, async (req, res) => {
  const { accountId, label } = req.body || {};
  if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

  const role = await rbac.getRole(accountId, req.user.uid);
  if (role !== 'Administrator') return res.status(403).json({ error: 'not an Administrator' });

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

// Returns what is actually running in memory. If the session manager
// doesn’t expose listRunning(), we fall back to registry + per-row status().
app.get('/sessions/running', requireUser, async (req, res) => {
  const accountId = String(req.query.accountId || '');
  if (!accountId) return res.status(400).json({ error: 'accountId required' });

  const role = await rbac.getRole(accountId, req.user.uid);
  if (!role) return res.status(403).json({ error: 'not a member' });

  // Preferred path (live truth from session manager)
  if (typeof sessions.listRunning === 'function') {
    return res.json(sessions.listRunning(accountId));
  }

  // Fallback: registry + augment with live statuses if available
  const list = await registry.list(accountId);
  const augmented = await Promise.all(
    list.map(async (s) => {
      const status = sessions.status ? sessions.status({ accountId: s.accountId, label: s.label }) : s.status;
      return {
        accountId: s.accountId,
        label: s.label,
        status: status || s.status || null,
        waId: s.waId || null,
        hasQr: false, // unknown without manager map; left as false in fallback
      };
    })
  );
  res.json(augmented);
});

// Manually scan .wwebjs_auth and bring sessions back.
// If restoreAllFromFs() isn’t present yet, we simply tell the client that
// restore is not available (no breakage).
app.post('/sessions/restore', requireUser, async (req, res) => {
  const { accountId } = req.body || {};
  if (!accountId) return res.status(400).json({ error: 'accountId required' });

  const role = await rbac.getRole(accountId, req.user.uid);
  if (role !== 'Administrator') return res.status(403).json({ error: 'not an Administrator' });

  if (typeof sessions.restoreAllFromFs !== 'function') {
    return res.json({ ok: true, restored: null, note: 'restoreAllFromFs() not available in session manager' });
  }
  const n = await sessions.restoreAllFromFs();
  res.json({ ok: true, restored: n });
});

// ---------- WS hub (auth via token + Firestore ACL; live ACL updates) ----------
const server = http.createServer(app);
createWsHub({
  server,
  authAdmin,
  rbac,
  sessions, // for event stream
  maxConnections: 2000,
});

// ---------- Start ----------
server.listen(PORT, () =>
  console.log(
    `HTTP http://0.0.0.0:${PORT} | WS ws://<host>:${PORT}/ws?accountId=<aid>&token=<FIREBASE_ID_TOKEN>`
  )
);
