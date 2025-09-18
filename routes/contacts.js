// routes/contacts.js
import { Router } from 'express';
import { sleep, rand, normalizeChatId } from '../lib/session/utils.js';

// ===== GCS helpers =====
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

// ===== Merge helpers =====
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

    // Fill-only update (don’t clobber non-empty existing fields).
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

// ===== Number normalization helpers =====
function normalizeDigits(s) {
  return String(s || '').replace(/[^\d]/g, '');
}
function numberFromContact(c) {
  if (c?.number) return normalizeDigits(c.number);
  const id = String(c?.id || '');
  if (id.endsWith('@c.us')) return normalizeDigits(id.split('@')[0]);
  return '';
}

// ===== In-memory job management =====
const JOBS = new Map(); // jobId -> job object
const QUEUES = new Map(); // key accountId::label -> [jobId]
const RUNNING = new Set(); // keys currently running

function queueKey({ accountId, label }) {
  return `${accountId}::${label}`;
}
function makeJobId() {
  return `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

async function saveJson(file, obj) {
  await file.save(JSON.stringify(obj, null, 2), {
    resumable: false,
    contentType: 'application/json',
    metadata: { cacheControl: 'no-cache' },
  });
}

export function buildContactsRouter({ sessions, requireUser, ensureAllowed, bucket }) {
  const r = Router();

  // ---------- /contacts (simplified) ----------
  // - Fetch contacts from the session (no enrichment here)
  // - Filter to: type=private && isMyContact && isWAContact
  // - Upsert-merge into contacts.json (per-session path)
  // - Return only the filtered subset + stats
  r.get('/contacts', requireUser, async (req, res) => {
    const accountId = String(req.query.accountId || '');
    const label = String(req.query.label || '');

    if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

    const allowed = await ensureAllowed(req, res, accountId, label);
    if (!allowed) return;

    const st = sessions.status({ accountId, label });
    if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

    try {
      const all = await sessions.getContacts({ accountId, label, withDetails: false });

      const subset = all.filter(
        (c) =>
          c?.type === 'private' &&
          !!c?.isMyContact &&
          !!c?.isWAContact &&
          typeof c?.id === 'string' &&
          c.id.endsWith('@c.us')
      );

      let storageInfo = {
        ok: false,
        bucket: bucket?.name || null,
        object: null,
        mode: null,
        error: null,
        totalStored: null,
      };

      if (bucket) {
        try {
          await ensurePrefix(bucket, `${accountId}`);
          await ensurePrefix(bucket, `${accountId}/whatsapp`);
          const sessPrefix = await ensurePrefix(bucket, `${accountId}/whatsapp/${label}`);
          const objectName = `${sessPrefix}contacts.json`;
          const file = bucket.file(objectName);

          const existing = (await fileExists(file)) ? await readJsonIfExists(file) : null;
          const existingContacts = Array.isArray(existing?.contacts) ? existing.contacts : [];
          const { merged } = upsertMerge(existingContacts, subset);

          const payload = {
            accountId,
            label,
            generatedAt: new Date().toISOString(),
            count: merged.length,
            contacts: merged,
            _meta: {
              filter: { type: 'private', isMyContact: true, isWAContact: true },
            },
          };

          await saveJson(file, payload);

          storageInfo = {
            ok: true,
            bucket: bucket.name,
            object: objectName,
            mode: existing ? 'merge' : 'create',
            error: null,
            totalStored: merged.length,
          };
        } catch (e) {
          storageInfo.error = String(e?.message || e);
          console.error('[contacts] write failed:', e);
        }
      }

      res.json({
        ok: true,
        count: subset.length,
        contacts: subset,
        stats: buildStats(subset),
        storage: storageInfo,
      });
    } catch (e) {
      res.status(500).json({ error: 'contacts_failed', detail: String(e?.message || e) });
    }
  });

  // ---------- NEW: async enrichment job ----------
  // POST /contacts/enrich/start
  // body: { accountId, label, numbers: string[] }
  //
  // Steps:
  // 1) Read per-session contacts.json as base.
  // 2) Split input numbers into "present in contacts.json" vs "absent".
  // 3) Enqueue a job:
  //    - present: enrich avatar/bio (sequential, moderate jitter)
  //    - absent: check registration (sequential), include registered with details
  // 4) Write/merge into enriched_contacts.json (copy/superset of contacts.json)
  // 5) Return { jobId } immediately; poll status with GET /contacts/enrich/status
  r.post('/contacts/enrich/start', requireUser, async (req, res) => {
    const { accountId, label, numbers } = req.body || {};
    if (!accountId || !label || !Array.isArray(numbers) || numbers.length === 0) {
      return res.status(400).json({ error: 'accountId, label, numbers[] required' });
    }

    const allowed = await ensureAllowed(req, res, accountId, label);
    if (!allowed) return;

    const st = sessions.status({ accountId, label });
    if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

    try {
      // Prepare storage paths
      await ensurePrefix(bucket, `${accountId}`);
      await ensurePrefix(bucket, `${accountId}/whatsapp`);
      const sessPrefix = await ensurePrefix(bucket, `${accountId}/whatsapp/${label}`);

      const contactsFile = bucket.file(`${sessPrefix}contacts.json`);
      const enrichedFile = bucket.file(`${sessPrefix}enriched_contacts.json`);

      const base = (await fileExists(contactsFile)) ? await readJsonIfExists(contactsFile) : { contacts: [] };
      const baseContacts = Array.isArray(base?.contacts) ? base.contacts : [];

      // Ensure enriched file exists as a copy (if missing)
      if (!(await fileExists(enrichedFile))) {
        const payload = {
          accountId,
          label,
          generatedAt: new Date().toISOString(),
          count: baseContacts.length,
          contacts: baseContacts,
          _meta: { source: 'bootstrap_from_contacts_json' },
        };
        await saveJson(enrichedFile, payload);
      }

      // Build “present” vs “absent” sets by normalized number
      const presentNumbersSet = new Set(baseContacts.map((c) => numberFromContact(c)).filter(Boolean));
      const inNumbers = numbers.map((x) => normalizeDigits(x)).filter(Boolean);

      const present = [];
      const absent = [];
      for (const n of inNumbers) {
        if (presentNumbersSet.has(n)) present.push(n);
        else absent.push(n);
      }

      // Create the job
      const jobId = makeJobId();
      const key = queueKey({ accountId, label });
      const job = {
        id: jobId,
        accountId,
        label,
        status: 'queued', // 'queued' | 'running' | 'done' | 'error'
        createdAt: Date.now(),
        startedAt: null,
        finishedAt: null,
        input: { total: inNumbers.length, numbers: inNumbers },
        sets: { present, absent },
        progress: {
          presentTotal: present.length, presentDone: 0,
          absentTotal: absent.length, absentDone: 0,
        },
        storage: {
          bucket: bucket.name,
          enrichedObject: `${sessPrefix}enriched_contacts.json`,
        },
        error: null,
      };

      JOBS.set(jobId, job);
      if (!QUEUES.has(key)) QUEUES.set(key, []);
      QUEUES.get(key).push(jobId);

      // Kick the runner
      runQueueForKey({ key, sessions, bucket }).catch((e) => {
        console.error('[enrich runner] unexpected error:', e);
      });

      res.json({
        ok: true,
        jobId,
        presentCount: present.length,
        absentCount: absent.length,
      });
    } catch (e) {
      res.status(500).json({ error: 'enrich_start_failed', detail: String(e?.message || e) });
    }
  });

  // GET /contacts/enrich/status?accountId=...&label=...&jobId=...
  r.get('/contacts/enrich/status', requireUser, async (req, res) => {
    const accountId = String(req.query.accountId || '');
    const label = String(req.query.label || '');
    const jobId = String(req.query.jobId || '');

    if (!accountId || !label || !jobId) {
      return res.status(400).json({ error: 'accountId, label, jobId required' });
    }
    const allowed = await ensureAllowed(req, res, accountId, label);
    if (!allowed) return;

    const job = JOBS.get(jobId);
    if (!job || job.accountId !== accountId || job.label !== label) {
      return res.status(404).json({ error: 'job_not_found' });
    }

    res.json({ ok: true, job });
  });

  return r;
}

// ===== Runner implementation (sequential processing with light jitter) =====
async function runQueueForKey({ key, sessions, bucket }) {
  if (RUNNING.has(key)) return; // already running
  RUNNING.add(key);

  try {
    const q = QUEUES.get(key) || [];
    while (q.length) {
      const jobId = q.shift();
      const job = JOBS.get(jobId);
      if (!job) continue;

      job.status = 'running';
      job.startedAt = Date.now();

      try {
        await processJob({ job, sessions, bucket });
        job.status = 'done';
        job.finishedAt = Date.now();
      } catch (e) {
        job.status = 'error';
        job.error = String(e?.message || e);
        job.finishedAt = Date.now();
      }
    }
  } finally {
    RUNNING.delete(key);
  }
}

async function processJob({ job, sessions, bucket }) {
  const { accountId, label } = job;

  // Prepare storage paths
  const sessPrefix = `${accountId}/whatsapp/${label}/`;
  const enrichedFile = bucket.file(`${sessPrefix}enriched_contacts.json`);
  const contactsFile = bucket.file(`${sessPrefix}contacts.json`);

  // Load base enriched (or bootstrap from contacts)
  let enriched = (await fileExists(enrichedFile)) ? await readJsonIfExists(enrichedFile) : null;
  if (!enriched) {
    const base = (await fileExists(contactsFile)) ? await readJsonIfExists(contactsFile) : { contacts: [] };
    enriched = {
      accountId,
      label,
      generatedAt: new Date().toISOString(),
      count: Array.isArray(base.contacts) ? base.contacts.length : 0,
      contacts: Array.isArray(base.contacts) ? base.contacts : [],
      _meta: { source: 'bootstrap_at_job_runtime' },
    };
    await saveJson(enrichedFile, enriched);
  }

  let baseContacts = Array.isArray(enriched.contacts) ? enriched.contacts : [];
  baseContacts = baseContacts.filter((c) => !c?.id || String(c.id).endsWith('@c.us'));

  // Build lookup maps for present contacts
  const byNumber = new Map(baseContacts.map((c) => [numberFromContact(c), c]));

  // ---- Phase A: enrich present contacts (avatar/bio) ----
  if (job.sets.present.length) {
    // Gather contacts for present numbers
    const presentContacts = job.sets.present
      .map((n) => byNumber.get(n))
      .filter(Boolean);

    // Use the session’s sequential enrichment helper
    const enrichedList = await sessions.enrichContactsSequential({
      accountId,
      label,
      contacts: presentContacts,
    });

    // Merge and update progress (as one step)
    const { merged } = upsertMerge(baseContacts, enrichedList);
    enriched.contacts = merged;
    enriched.count = merged.length;
    enriched.generatedAt = new Date().toISOString();
    await saveJson(enrichedFile, enriched);

    job.progress.presentDone = job.progress.presentTotal; // batch done
  }

  // ---- Phase B: check absent numbers (registration + details) ----
  for (const n of job.sets.absent) {
    // Moderate jitter between checks to be nice
    await sleep(rand(60, 140));

    // We use single-number check to update progress per item
    const result = await sessions.checkContactByNumber({
      accountId,
      label,
      number: n,
      countryCode: null,
      withDetails: true,
    });

    if (result?.registered && result?.waId) {
      const waId = String(result.waId || '');
      if (!waId.endsWith('@c.us')) {
        job.progress.absentDone += 1;
        continue; // skip @lid and anything else
      }
      const contact = {
        id: waId,
        number: result.normalized || n,
        name: result.contact?.name || null,
        pushname: result.contact?.pushname || null,
        shortName: result.contact?.shortName || null,
        isWAContact: true,
        isMyContact: !!result.contact?.isMyContact, // likely false for non-contacts
        isBusiness: !!result.contact?.isBusiness,
        isEnterprise: !!result.contact?.isEnterprise,
        hasChat: !!result.hasChat,
        type: 'private',
        profilePicUrl: result.contact?.profilePicUrl || null,
        about: result.contact?.about || null,
      };

      const { merged } = upsertMerge(enriched.contacts, [contact]);
      enriched.contacts = merged;
      enriched.count = merged.length;
      enriched.generatedAt = new Date().toISOString();
      await saveJson(enrichedFile, enriched);
    }

    job.progress.absentDone += 1;
  }

  // Final save (idempotent)
  enriched.generatedAt = new Date().toISOString();
  await saveJson(enrichedFile, enriched);
}
