import { Router } from 'express';

export function buildAdminRouter({ db, meta, rbac, requireUser }) {
  const r = Router();

  // Server self-assign to account.wsServer
  r.post('/admin/assignServer', requireUser, async (req, res) => {
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
      meta.projectId(),
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

  return r;
}
