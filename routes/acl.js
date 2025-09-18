import { Router } from 'express';

export function buildAclRouter({ rbac, requireUser }) {
  const r = Router();

  r.get('/acl/users', requireUser, async (req, res) => {
    const accountId = String(req.query.accountId || '');
    if (!accountId) return res.status(400).json({ error: 'accountId required' });

    const role = await rbac.getRole(accountId, req.user.uid);
    if (role !== 'Administrator') return res.status(403).json({ error: 'not an Administrator' });

    res.json(await rbac.listAcl(accountId));
  });

  r.post('/acl/grant', requireUser, async (req, res) => {
    const { accountId, uid, sessions: allowed } = req.body || {};
    if (!accountId || !uid || !Array.isArray(allowed) || !allowed.length)
      return res.status(400).json({ error: 'accountId, uid, sessions[] required' });

    const role = await rbac.getRole(accountId, req.user.uid);
    if (role !== 'Administrator') return res.status(403).json({ error: 'not an Administrator' });

    await rbac.setAcl(accountId, uid, allowed);
    res.json({ ok: true });
  });

  r.post('/acl/revoke', requireUser, async (req, res) => {
    const { accountId, uid } = req.body || {};
    if (!accountId || !uid) return res.status(400).json({ error: 'accountId, uid required' });

    const role = await rbac.getRole(accountId, req.user.uid);
    if (role !== 'Administrator') return res.status(403).json({ error: 'not an Administrator' });

    await rbac.setAcl(accountId, uid, []);
    res.json({ ok: true });
  });

  return r;
}
