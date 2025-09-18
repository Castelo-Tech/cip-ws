// routes/contacts.js
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

// Upsert merge: add new contacts; update missing fields on existing ones.
// Returns { merged, appended, updated }.
function upsertMerge(existingArr = [], incomingArr = []) {
  const keyOf = (c) => (c?.id && String(c.id)) || (c?.number && String(c.number)) || null;

  const map = new Map();
  for (const c of existingArr) {
    const k = keyOf(c);
    if (k) map.set(k, { ...c });
  }

  let appended = 0;
  let updated = 0;

  for (const inc of incomingArr) {
    const k = keyOf(inc);
    if (!k) continue;

    if (!map.has(k)) {
      map.set(k, { ...inc });
      appended++;
      continue;
    }

    // Shallow "fill in" merge: only set fields that are currently null/undefined/empty.
    // Prefer incoming values for profilePicUrl/about if existing is falsy.
    const cur = map.get(k);
    let changed = false;

    for (const [field, value] of Object.entries(inc)) {
      const curVal = cur[field];
      const wantOverwrite =
        (curVal === null || curVal === undefined || curVal === '') ||
        ((field === 'profilePicUrl' || field === 'about') && !curVal && !!value);

      if (wantOverwrite && value !== undefined) {
        cur[field] = value;
        changed = true;
      }
    }

    if (changed) {
      map.set(k, cur);
      updated++;
    }
  }

  const merged = Array.from(map.values());
  merged.sort((a, b) => {
    const ka = String(a.number || a.id || '');
    const kb = String(b.number || b.id || '');
    return ka.localeCompare(kb);
  });

  return { merged, appended, updated };
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
  const fieldSet = new Set();
  for (const c of list) Object.keys(c || {}).forEach((k) => fieldSet.add(k));
  const fields = Array.from(fieldSet).sort();

  return { total, byType, flags, details, fields };
}

export function buildContactsRouter({ sessions, requireUser, ensureAllowed, bucket }) {
  const r = Router();

  // Narrowed contacts + per-session persistence + sequential enrichment
  r.get('/contacts', requireUser, async (req, res) => {
    const accountId = String(req.query.accountId || '');
    const label = String(req.query.label || '');
    // We ignore ?details for the base fetch: we will enrich the filtered subset regardless.

    if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

    const allowed = await ensureAllowed(req, res, accountId, label);
    if (!allowed) return;

    const st = sessions.status({ accountId, label });
    if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

    try {
      // 1) Fetch baseline (no details here; we will enrich subset sequentially)
      const all = await sessions.getContacts({ accountId, label, withDetails: false });

      // 2) Filter down to the required subset
      const subset = all.filter(
        (c) => c?.type === 'private' && !!c?.isMyContact && !!c?.isWAContact
      );

      // --- Prepare storage path ---
      let storageInfo = {
        ok: false,
        bucket: bucket?.name || null,
        object: null,
        mode: null,
        error: null,
        phase1: { appended: 0, updated: 0, totalStored: null },
        phase2: { appended: 0, updated: 0, totalStored: null },
      };

      let objectName = null;
      let file = null;
      let existingContacts = [];
      let mergedAfterPhase1 = [];

      if (bucket) {
        try {
          await ensurePrefix(bucket, `${accountId}`);
          await ensurePrefix(bucket, `${accountId}/whatsapp`);
          const sessPrefix = await ensurePrefix(bucket, `${accountId}/whatsapp/${label}`);
          objectName = `${sessPrefix}contacts.json`;
          file = bucket.file(objectName);

          const existing = (await fileExists(file)) ? await readJsonIfExists(file) : null;
          existingContacts = Array.isArray(existing?.contacts) ? existing.contacts : [];

          storageInfo.mode = existing ? 'merge' : 'create';

          // 3) PHASE 1 — quick write of the filtered subset (upsert)
          {
            const { merged, appended, updated } = upsertMerge(existingContacts, subset);
            mergedAfterPhase1 = merged;

            const payload1 = {
              accountId,
              label,
              generatedAt: new Date().toISOString(),
              count: merged.length,
              contacts: merged,
              _meta: {
                phase: 1,
                previousCount: existingContacts.length || 0,
                filter: { type: 'private', isMyContact: true, isWAContact: true },
              },
            };

            await file.save(JSON.stringify(payload1, null, 2), {
              resumable: false,
              contentType: 'application/json',
              metadata: {
                cacheControl: 'no-cache',
                metadata: { accountId, label, source: 'contacts_endpoint' },
              },
            });

            storageInfo.phase1 = { appended, updated, totalStored: merged.length };
            storageInfo.ok = true;
            storageInfo.bucket = bucket.name;
            storageInfo.object = objectName;
          }

          // 4) PHASE 2 — sequential enrichment (avatar + bio) over the subset, then upsert again
          const enriched = await sessions.enrichContactsSequential({
            accountId, label, contacts: subset
          });

          const { merged, appended, updated } = upsertMerge(mergedAfterPhase1, enriched);

          const payload2 = {
            accountId,
            label,
            generatedAt: new Date().toISOString(),
            count: merged.length,
            contacts: merged,
            _meta: {
              phase: 2,
              previousCount: mergedAfterPhase1.length,
              filter: { type: 'private', isMyContact: true, isWAContact: true },
              enrichment: { mode: 'sequential', jitter: { perItemMs: [45,110], everyN: 25, pauseMs: [200,450] } },
            },
          };

          await file.save(JSON.stringify(payload2, null, 2), {
            resumable: false,
            contentType: 'application/json',
            metadata: {
              cacheControl: 'no-cache',
              metadata: { accountId, label, source: 'contacts_endpoint' },
            },
          });

          storageInfo.phase2 = { appended, updated, totalStored: merged.length };
        } catch (e) {
          storageInfo.ok = false;
          storageInfo.error = String(e?.message || e);
          // We still return the enriched subset below even if storage fails.
          console.error('[contacts] storage write failed:', e);
        }
      }

      // 5) Always return ONLY the filtered subset; return the enriched form
      let subsetForResponse = [];
      try {
        subsetForResponse = await sessions.enrichContactsSequential({
          accountId, label, contacts: subset
        });
      } catch {
        // Best effort; fall back to base subset if enrich failed
        subsetForResponse = subset;
      }

      const stats = buildStats(subsetForResponse);

      res.json({
        ok: true,
        count: subsetForResponse.length,
        contacts: subsetForResponse,   // ONLY the filtered (and enriched) subset
        stats,                         // stats for the returned subset
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
