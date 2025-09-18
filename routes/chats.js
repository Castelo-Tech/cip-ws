import { Router } from 'express';

export function buildChatsRouter({ sessions, requireUser, ensureAllowed }) {
  const r = Router();

  // List chats (no messages)
  r.get('/chats', requireUser, async (req, res) => {
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

  // Chat by a single number
  r.get('/chats/byNumber', requireUser, async (req, res) => {
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

  // Chats by numbers (optionally with messages)
  r.post('/chats/byNumbers', requireUser, async (req, res) => {
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

  return r;
}
