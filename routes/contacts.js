import { Router } from 'express';

/**
 * Ensure a "directory" prefix exists in GCS by creating a zero-byte marker object.
 * GCS is flat; folders are just prefixes. This is purely for convenience.
 */
async function ensurePrefix(bucket, prefix) {
  // Normalize to end with a single trailing slash
  const clean = String(prefix).replace(/\/+$/, '') + '/';

  // If something already exists under the prefix, we're good.
  const [files] = await bucket.getFiles({ prefix: clean, maxResults: 1 });
  if (files && files.length > 0) return clean;

  // Create a directory marker object (optional, but matches your requirement)
  try {
    await bucket.file(clean).save('', {
      resumable: false,
      metadata: { contentType: 'application/x-directory' },
    });
  } catch {
    // no-op; even if marker creation fails, later file writes will still create the prefix
  }
  return clean;
}

export function buildContactsRouter({ sessions, requireUser, ensureAllowed, bucket }) {
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

      // --- Write contacts.json to GCS bucket ---
      let storageInfo = { ok: false, bucket: bucket?.name || null, object: null, error: null };
      if (bucket) {
        try {
          const basePrefix = await ensurePrefix(bucket, `${accountId}`);
          const subPrefix  = await ensurePrefix(bucket, `${accountId}/whatsapp`);
          const objectName = `${subPrefix}contacts.json`;

          const payload = {
            accountId,
            label,
            generatedAt: new Date().toISOString(),
            count: list.length,
            contacts: list,
          };

          await bucket.file(objectName).save(JSON.stringify(payload, null, 2), {
            resumable: false,
            contentType: 'application/json',
            metadata: {
              cacheControl: 'no-cache',
              metadata: { accountId, label, source: 'contacts_endpoint' },
            },
          });

          storageInfo = { ok: true, bucket: bucket.name, object: objectName, error: null };
        } catch (e) {
          storageInfo = { ok: false, bucket: bucket.name, object: null, error: String(e?.message || e) };
          // We don't fail the endpoint if storage write fails — we still return the contacts.
          console.error('[contacts] storage write failed:', e);
        }
      }

      // Keep the response shape compatible; add storage info as extra (non-breaking).
      res.json({ ok: true, count: list.length, contacts: list, storage: storageInfo });
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
