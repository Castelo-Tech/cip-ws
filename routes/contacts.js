import { Router } from 'express';

export function buildContactsRouter({ sessions, requireUser, ensureAllowed }) {
  const r = Router();

  // All contacts
  r.get('/contacts', requireUser, async (req, res) => {
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

  // Bulk lookup
  r.post('/contacts/lookup', requireUser, async (req, res) => {
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
        accountId,
        label,
        numbers,
        countryCode: countryCode || null,
        withDetails: !!withDetails,
      });
      res.json({ ok: true, results: out });
    } catch (e) {
      res.status(500).json({ error: 'lookup_failed', detail: String(e?.message || e) });
    }
  });

  // Single-number check
  r.get('/contacts/check', requireUser, async (req, res) => {
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

  return r;
}
