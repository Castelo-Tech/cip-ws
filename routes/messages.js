import { Router } from 'express';

export function buildMessagesRouter({ sessions, requireUser, ensureAllowed }) {
  const r = Router();

  // Send text
  r.post('/messages/send', requireUser, async (req, res) => {
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

  // Send media
  r.post('/messages/sendMedia', requireUser, async (req, res) => {
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

  return r;
}
