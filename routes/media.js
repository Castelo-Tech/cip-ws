import { Router } from 'express';
import { Buffer } from 'node:buffer';

export function buildMediaRouter({ sessions, requireUser, ensureAllowed }) {
  const r = Router();

  r.get('/media/:messageId', requireUser, async (req, res) => {
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

  return r;
}
