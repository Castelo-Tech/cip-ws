import { Router } from 'express';

/**
 * Ensure a "directory" prefix exists in GCS by creating a zero-byte marker object.
 * GCS is flat; folders are just prefixes. This is purely for convenience.
 */
async function ensurePrefix(bucket, prefix) {
  const clean = String(prefix).replace(/\/+$/, '') + '/';
  const [files] = await bucket.getFiles({ prefix: clean, maxResults: 1 });
  if (files && files.length > 0) return clean;

  try {
    await bucket.file(clean).save('', {
      resumable: false,
      metadata: { contentType: 'application/x-directory' },
    });
  } catch {
    // non-fatal; subsequent writes still create the prefix implicitly
  }
  return clean;
}

async function fileExists(file) {
  try {
    const [exists] = await file.exists();
    return !!exists;
  } catch {
    return false;
  }
}

async function readJsonIfExists(file) {
  try {
    const [buf] = await file.download();
    const text = buf?.toString('utf8') || '';
    if (!text.trim()) return null;
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function dedupeMerge(existingArr = [], incomingArr = []) {
  // key by id (preferred) else number
  const keyOf = (c) => (c?.id && String(c.id)) || (c?.number && String(c.number)) || null;

  const map = new Map();
  for (const c of existingArr) {
    const k = keyOf(c);
    if (k) map.set(k, c);
  }
  let appended = 0;
  for (const c of incomingArr) {
    const k = keyOf(c);
    if (!k) continue;
    if (!map.has(k)) {
      map.set(k, c);
      appended++;
    }
    // NOTE: per your request we do NOT "enhance" existing objects,
    // i.e., we don't mutate/patch fields if already present.
  }
  const merged = Array.from(map.values());
  // stable-ish order: by number/id
  merged.sort((a, b) => {
    const ka = String(a.number || a.id || '');
    const kb = String(b.number || b.id || '');
    return ka.localeCompare(kb);
  });
  return { merged, appended };
}

function buildStats(list = []) {
  const total = list.length;
  const countIf = (fn) => list.reduce((n, x) => (fn(x) ? n + 1 : n), 0);
  const byType = {
    private: countIf((x) => x?.type === 'private'),
    group: countIf((x) => x?.type === 'group'),
    other: countIf((x) => x?.type && !['private', 'group'].includes(x.type)),
  };
  const flags = {
    isWAContact: countIf((x) => !!x?.isWAContact),
    isMyContact: countIf((x) => !!x?.isMyContact),
    isBusiness: countIf((x) => !!x?.isBusiness),
    isEnterprise: countIf((x) => !!x?.isEnterprise),
    hasChat: countIf((x) => !!x?.hasChat),
  };
  const details = {
    withProfilePicUrl: countIf((x) => typeof x?.profilePicUrl === 'string' && x.profilePicUrl.length > 0),
    withAbout: countIf((x) => typeof x?.about === 'string' && x.about.length > 0),
  };
  // union of keys seen (so you can see "all fields we gathered")
  const fieldSet = new Set();
  for (const c of list) Object.keys(c || {}).forEach((k) => fieldSet.add(k));
  const fields = Array.from(fieldSet).sort();

  return { total, byType, flags, details, fields };
}

export function buildContactsRouter({ sessions, requireUser, ensureAllowed, bucket }) {
  const r = Router();

  // All contacts (and persist per-session private contacts to storage)
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
      const allContacts = await sessions.getContacts({ accountId, label, withDetails });

      // Build stats for UI
      const statsAll = buildStats(allContacts);
      const privOnly = allContacts.filter((c) => c?.type === 'private');
      const statsPrivate = buildStats(privOnly);

      // --- Write contacts.json (private only) to GCS bucket under per-session path ---
      let storageInfo = {
        ok: false,
        bucket: bucket?.name || null,
        object: null,
        mode: null,         // "create" | "merge"
        appended: 0,        // how many new contacts appended (merge)
        totalStored: null,  // final contacts count in stored file
        error: null
      };

      if (bucket) {
        try {
          const basePrefix = await ensurePrefix(bucket, `${accountId}`);
          const whatsPrefix = await ensurePrefix(bucket, `${accountId}/whatsapp`);
          const sessPrefix = await ensurePrefix(bucket, `${accountId}/whatsapp/${label}`);
          const objectName = `${sessPrefix}contacts.json`;
          const file = bucket.file(objectName);

          const existing = (await fileExists(file)) ? await readJsonIfExists(file) : null;
          const existingContacts = Array.isArray(existing?.contacts) ? existing.contacts : [];

          // Merge by id/number; do NOT mutate existing entries
          const { merged, appended } = dedupeMerge(existingContacts, privOnly);

          const payload = {
            accountId,
            label,
            generatedAt: new Date().toISOString(),
            count: merged.length,
            contacts: merged,       // ONLY type: "private"
            // optional provenance fields to help with debugging
            _meta: {
              wroteFromDetailsMode: !!withDetails,
              previousCount: existingContacts.length || 0,
            },
          };

          await file.save(JSON.stringify(payload, null, 2), {
            resumable: false,
            contentType: 'application/json',
            metadata: {
              cacheControl: 'no-cache',
              metadata: { accountId, label, source: 'contacts_endpoint' },
            },
          });

          storageInfo = {
            ok: true,
            bucket: bucket.name,
            object: objectName,
            mode: existing ? 'merge' : 'create',
            appended,
            totalStored: merged.length,
            error: null,
          };
        } catch (e) {
          storageInfo = {
            ok: false,
            bucket: bucket?.name || null,
            object: null,
            mode: null,
            appended: 0,
            totalStored: null,
            error: String(e?.message || e),
          };
          // We don't fail the endpoint if storage write fails — we still return contacts & stats.
          console.error('[contacts] storage write failed:', e);
        }
      }

      // Response keeps original contacts list (all types), plus stats & storage info
      res.json({
        ok: true,
        count: allContacts.length,
        contacts: allContacts,
        stats: {
          all: statsAll,
          privateOnly: statsPrivate,
        },
        storage: storageInfo,
      });
    } catch (e) {
      res.status(500).json({ error: 'contacts_failed', detail: String(e?.message || e) });
    }
  });

  // Bulk lookup (unchanged)
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

  // Single-number check (unchanged)
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
