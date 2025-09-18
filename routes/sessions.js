import { Router } from 'express';

export function buildSessionsRouter({ rbac, registry, sessions, requireUser, ensureAllowed }) {
  const r = Router();

  // List sessions
  r.get('/sessions', requireUser, async (req, res) => {
    const accountId = String(req.query.accountId || '');
    if (!accountId) return res.status(400).json({ error: 'accountId required' });

    const role = await rbac.getRole(accountId, req.user.uid);
    if (!role) return res.status(403).json({ error: 'not a member' });

    res.json(await registry.list(accountId));
  });

  // Init
  r.post('/sessions/init', requireUser, async (req, res) => {
    const { accountId, label } = req.body || {};
    if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

    const allowed = await ensureAllowed(req, res, accountId, label, { requireAdmin: true });
    if (!allowed) return;

    sessions.init({ accountId, label });
    res.json({ ok: true, accountId, label, status: sessions.status({ accountId, label }) || 'starting' });
  });

  // Stop
  r.post('/sessions/stop', requireUser, async (req, res) => {
    const { accountId, label } = req.body || {};
    if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

    const allowed = await ensureAllowed(req, res, accountId, label, { requireAdmin: true });
    if (!allowed) return;

    await sessions.stop({ accountId, label });
    res.json({ ok: true, accountId, label, status: sessions.status({ accountId, label }) || 'stopped' });
  });

  // Destroy
  r.post('/sessions/destroy', requireUser, async (req, res) => {
    const { accountId, label } = req.body || {};
    if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

    const allowed = await ensureAllowed(req, res, accountId, label, { requireAdmin: true });
    if (!allowed) return;

    await sessions.destroy({ accountId, label });
    res.json({ ok: true, accountId, label });
  });

  // Status
  r.get('/status', requireUser, async (req, res) => {
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

  // QR
  r.get('/qr', requireUser, async (req, res) => {
    const accountId = String(req.query.accountId || '');
    const label = String(req.query.label || req.query.session || '');
    if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

    const role = await rbac.getRole(accountId, req.user.uid);
    if (!role) return res.status(403).json({ error: 'not a member' });

    res.json({ accountId, label, qr: sessions.qr({ accountId, label }) || null });
  });

  // Sessions live-truth
  r.get('/sessions/running', requireUser, async (req, res) => {
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

  // Manual restore
  r.post('/sessions/restore', requireUser, async (req, res) => {
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

  return r;
}
