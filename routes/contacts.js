// routes/contacts.js
import { Router } from 'express';
import { FieldValue } from 'firebase-admin/firestore';

/* ===============================
 * Session-scoped Firestore paths
 * =============================== */
function sessRefs(db, accountId, label) {
  const base = db.collection('accounts').doc(accountId).collection('sessions').doc(label);
  return {
    base,
    contacts: base.collection('contacts'),
    // chats kept for forward-compat (unused here)
    chats: base.collection('chats'),
  };
}

/* =====================
 * Normalization helpers
 * ===================== */
function normalizeDigits(s) {
  return String(s || '').replace(/[^\d]/g, '');
}
function numberFromContact(c) {
  if (c?.number) return normalizeDigits(c.number);
  const id = String(c?.id || '');
  if (id.endsWith('@c.us')) return normalizeDigits(id.split('@')[0]);
  return '';
}
function docIdForDigits(digits) { return normalizeDigits(digits); }
function waIdIsCUs(id) { return typeof id === 'string' && id.endsWith('@c.us'); }

/* =========================
 * Stats (for GET /contacts)
 * ========================= */
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

/* ========================
 * Firestore upsert helpers
 * ======================== */
function diffFillOnly(existing, incoming) {
  // Only include fields where existing is empty/undefined/null.
  // Always allow fill-only for profilePicUrl/about.
  const out = {};
  for (const [k, v] of Object.entries(incoming)) {
    const cur = existing?.[k];
    const wantOverwrite =
      (cur === null || cur === undefined || cur === '') ||
      ((k === 'profilePicUrl' || k === 'about') && !cur && !!v);
    if (wantOverwrite && v !== undefined) out[k] = v;
  }
  return out;
}
function changedFields(existing, updates) {
  const changed = [];
  for (const [k, v] of Object.entries(updates)) {
    const cur = existing?.[k];
    if (v !== undefined && v !== cur) changed.push(k);
  }
  return changed;
}
async function batchedSetOrUpdate(db, ops) {
  // ops: array of {ref, set?:object, update?:object}
  // Firestore batch limit = 500 writes; chunk to ~450
  const CHUNK = 450;
  for (let i = 0; i < ops.length; i += CHUNK) {
    const batch = db.batch();
    const slice = ops.slice(i, i + CHUNK);
    for (const op of slice) {
      if (op.set) {
        batch.set(op.ref, op.set, { merge: true });
      } else if (op.update && Object.keys(op.update).length) {
        batch.update(op.ref, op.update);
      }
    }
    await batch.commit();
  }
}
async function batchedGetAll(db, refs, chunk = 300) {
  const snaps = [];
  for (let i = 0; i < refs.length; i += chunk) {
    const slice = refs.slice(i, i + chunk);
    const part = slice.length ? await db.getAll(...slice) : [];
    snaps.push(...part);
  }
  return snaps;
}

/* =============================
 * Always-refreshed boolean flags
 * ============================= */
const ALWAYS_UPDATE_FLAGS = new Set(['registered', 'isWAContact', 'isMyContact', 'hasChat']);

/* ========================
 * In-memory job management
 * ======================== */
const JOBS = new Map();       // jobId -> job object
const QUEUES = new Map();     // key accountId::label -> [jobId]
const RUNNING = new Set();    // keys currently running
function queueKey({ accountId, label }) { return `${accountId}::${label}`; }
function makeJobId() { return `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`; }

function makeStage(key, label) {
  return { key, label, status: 'pending', total: 0, done: 0, error: null, startedAt: null, finishedAt: null };
}
function stageSet(job, key, patch) {
  const st = job.stages.find((s) => s.key === key);
  if (!st) return; // stage might be omitted (e.g., enrich off)
  Object.assign(st, patch);
  job.lastUpdatedAt = Date.now();
}

/* ==========================
 * Shared contact list helpers
 * ========================== */
function filterStrictCUs(contacts) {
  return contacts.filter(
    (c) =>
      c?.type === 'private' &&
      !!c?.isMyContact &&
      !!c?.isWAContact &&
      typeof c?.id === 'string' &&
      waIdIsCUs(c.id)
  );
}
function digitsForList(list) {
  return list.map((c) => numberFromContact(c)).filter(Boolean);
}
async function snapshotExistingByDigits(db, contactsCol, digitsList) {
  const docRefs = digitsList.map((d) => contactsCol.doc(docIdForDigits(d)));
  const existingSnaps = docRefs.length ? await batchedGetAll(db, docRefs, 300) : [];
  const existingByDigits = new Map();
  for (let i = 0; i < existingSnaps.length; i++) {
    const d = digitsList[i];
    const snap = existingSnaps[i];
    if (d && snap?.exists) existingByDigits.set(d, snap.data());
  }
  return existingByDigits;
}
function upsertContactsOps(subset, digitsList, existingByDigits, contactsCol) {
  const ops = [];
  let appended = 0, updated = 0;
  const appendedDigits = new Set();

  for (let i = 0; i < subset.length; i++) {
    const c = subset[i];
    const digits = digitsList[i];
    if (!digits) continue;

    const ref = contactsCol.doc(docIdForDigits(digits));
    const incoming = {
      id: c.id,
      number: digits,
      name: c?.name || null,
      pushname: c?.pushname || null,
      shortName: c?.shortName || null,
      isWAContact: !!c?.isWAContact,
      isMyContact: !!c?.isMyContact,
      isBusiness: !!c?.isBusiness,
      isEnterprise: !!c?.isEnterprise,
      hasChat: !!c?.hasChat,
      registered: true, // for real WA contacts from device
      type: 'private',
      updatedAt: FieldValue.serverTimestamp(),
    };

    const existing = existingByDigits.get(digits);
    if (!existing) {
      ops.push({ ref, set: { ...incoming, createdAt: FieldValue.serverTimestamp() } });
      appended++;
      appendedDigits.add(digits);
    } else {
      const baseDiff = diffFillOnly(existing, incoming);
      for (const f of ALWAYS_UPDATE_FLAGS) {
        if (incoming[f] !== undefined && incoming[f] !== existing[f]) baseDiff[f] = incoming[f];
      }
      if (Object.keys(baseDiff).length) {
        ops.push({ ref, update: { ...baseDiff, updatedAt: FieldValue.serverTimestamp() } });
        updated++;
      }
    }
  }

  return { ops, appended, updated, appendedDigits };
}

/* ============
 * The Router
 * ============ */
export function buildContactsRouter({ db, sessions, requireUser, ensureAllowed }) {
  const r = Router();

  /* -----------------------------------------
   * GET /contacts  (ad-hoc fetch + diff writes)
   * ?details=true → fill-only enrichment (pics/about)
   * ----------------------------------------- */
  r.get('/contacts', requireUser, async (req, res) => {
    const accountId = String(req.query.accountId || '');
    const label = String(req.query.label || '');
    const wantDetailsAtEnd = ['1', 'true', 'yes'].includes(String(req.query.details || '').toLowerCase());

    if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });
    const allowed = await ensureAllowed(req, res, accountId, label);
    if (!allowed) return;

    const st = sessions.status({ accountId, label });
    if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

    const phases = {
      contacts: { ok: false, appended: 0, updated: 0 },
      enrich: { ok: !wantDetailsAtEnd, updated: 0, error: null },
    };

    try {
      // Phase 1: baseline contacts (no details)
      const all = await sessions.getContacts({ accountId, label, withDetails: false });
      const subset = filterStrictCUs(all);
      const digitsList = digitsForList(subset);

      const { contacts: contactsCol } = sessRefs(db, accountId, label);
      const existingByDigits = await snapshotExistingByDigits(db, contactsCol, digitsList);

      const { ops, appended, updated, appendedDigits } =
        upsertContactsOps(subset, digitsList, existingByDigits, contactsCol);

      if (ops.length) await batchedSetOrUpdate(db, ops);
      phases.contacts = { ok: true, appended, updated };

      // Phase 2: targeted enrich (fill-only), optional via ?details=true
      let enrichUpdated = 0;
      if (wantDetailsAtEnd && subset.length) {
        const needEnrich = subset.filter((c, i) => {
          const d = digitsList[i];
          const ex = existingByDigits.get(d);
          return appendedDigits.has(d) || (ex && !ex.profilePicUrl && !ex.about);
        });

        if (needEnrich.length) {
          const enrichedList = await sessions.enrichContactsSequential({ accountId, label, contacts: needEnrich });
          const eops = [];
          for (const c of enrichedList) {
            const digits = numberFromContact(c);
            if (!digits || !waIdIsCUs(c?.id)) continue;

            const incoming = {
              profilePicUrl: c?.profilePicUrl ?? null,
              about: typeof c?.about === 'string' && c.about.length ? c.about : null,
            };
            const ex = existingByDigits.get(digits) || {};
            const diff = diffFillOnly(ex, incoming);
            if (Object.keys(diff).length) {
              const changed = changedFields(ex, diff);
              eops.push({
                ref: contactsCol.doc(docIdForDigits(digits)),
                update: {
                  ...diff,
                  enrichedAt: FieldValue.serverTimestamp(),
                  ...(changed.length ? { enrichedFields: FieldValue.arrayUnion(...changed) } : {}),
                  updatedAt: FieldValue.serverTimestamp(),
                },
              });
              enrichUpdated++;
            }
          }
          if (eops.length) await batchedSetOrUpdate(db, eops);
        }
        phases.enrich = { ok: true, updated: enrichUpdated, error: null };
      }

      res.json({
        ok: true,
        count: subset.length,
        contacts: subset,
        stats: buildStats(subset),
        persistence: {
          store: 'firestore',
          path: `/accounts/${accountId}/sessions/${label}/contacts/*`,
          appended: phases.contacts.appended,
          updated: phases.contacts.updated,
          enrichUpdated: phases.enrich.updated,
        },
        phases,
      });
    } catch (e) {
      res.status(500).json({ error: 'contacts_failed', detail: String(e?.message || e) });
    }
  });

  /* ---------------------------------------------------------
   * JOB: FULL SYNC (contacts only; NO ENRICH BY DEFAULT)
   * POST /contacts/sync/start   { accountId, label, enrich?: boolean }
   * GET  /contacts/sync/status  { accountId, label, jobId }
   * --------------------------------------------------------- */
  r.post('/contacts/sync/start', requireUser, async (req, res) => {
    const { accountId, label, enrich = false } = req.body || {};
    if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

    const allowed = await ensureAllowed(req, res, accountId, label);
    if (!allowed) return;

    const st = sessions.status({ accountId, label });
    if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

    const jobId = makeJobId();
    const key = queueKey({ accountId, label });
    const job = {
      id: jobId,
      kind: 'fullsync',
      accountId, label,
      status: 'queued',
      createdAt: Date.now(),
      startedAt: null,
      finishedAt: null,
      lastUpdatedAt: null,
      params: { enrich: !!enrich },
      storage: { store: 'firestore', path: `/accounts/${accountId}/sessions/${label}/*` },
      stages: [
        makeStage('read_contacts',   'Read contacts (no details)'),
        makeStage('write_contacts',  'Write/merge contacts'),
        ...(enrich ? [makeStage('enrich_contacts', 'Enrich pics/about (fill-only)')] : []),
      ],
      summary: { contactsAppended: 0, contactsUpdated: 0, enrichUpdated: 0 },
      error: null,
    };

    JOBS.set(jobId, job);
    if (!QUEUES.has(key)) QUEUES.set(key, []);
    QUEUES.get(key).push(jobId);
    runQueueForKey({ key, db, sessions }).catch((e) => console.error('[sync runner] unexpected error:', e));

    res.json({ ok: true, jobId });
  });

  r.get('/contacts/sync/status', requireUser, async (req, res) => {
    const accountId = String(req.query.accountId || '');
    const label = String(req.query.label || '');
    const jobId = String(req.query.jobId || '');
    if (!accountId || !label || !jobId) return res.status(400).json({ error: 'accountId, label, jobId required' });

    const allowed = await ensureAllowed(req, res, accountId, label);
    if (!allowed) return;

    const job = JOBS.get(jobId);
    if (!job || job.accountId !== accountId || job.label !== label) return res.status(404).json({ error: 'job_not_found' });
    res.json({ ok: true, job });
  });

  /* ----------------------------------------------------------------------
   * JOB: LOOKUP (bulk numbers → upsert → targeted enrich if still missing)
   * POST /contacts/lookup/start  { accountId, label, numbers[], countryCode? }
   * GET  /contacts/lookup/status { accountId, label, jobId }
   * (Aliases kept for back-compat: /contacts/enrich/start|status)
   * ---------------------------------------------------------------------- */
  async function startLookupJob(req, res) {
    const { accountId, label, numbers, countryCode = null } = req.body || {};
    if (!accountId || !label || !Array.isArray(numbers) || numbers.length === 0) {
      return res.status(400).json({ error: 'accountId, label, numbers[] required' });
    }
    if (numbers.length > 200) return res.status(413).json({ error: 'too_many_numbers', limit: 200 });

    const allowed = await ensureAllowed(req, res, accountId, label);
    if (!allowed) return;

    const st = sessions.status({ accountId, label });
    if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

    const jobId = makeJobId();
    const key = queueKey({ accountId, label });
    const job = {
      id: jobId,
      kind: 'lookup',
      accountId, label,
      status: 'queued',
      createdAt: Date.now(),
      startedAt: null,
      finishedAt: null,
      lastUpdatedAt: null,
      params: { numbers, countryCode },
      storage: { store: 'firestore', path: `/accounts/${accountId}/sessions/${label}/contacts/*` },
      stages: [
        makeStage('normalize_numbers', 'Normalize input numbers'),
        makeStage('lookup_numbers',    'Lookup & fetch details'),
        makeStage('write_numbers',     'Upsert into contacts collection'),
        makeStage('enrich_missing',    'Enrich missing pics/about (registered only)'),
      ],
      summary: { inputs: numbers.length, normalized: 0, registered: 0, unregistered: 0, written: 0, enrichUpdated: 0 },
      error: null,
    };

    JOBS.set(jobId, job);
    if (!QUEUES.has(key)) QUEUES.set(key, []);
    QUEUES.get(key).push(jobId);
    runQueueForKey({ key, db, sessions }).catch((e) => console.error('[lookup runner] unexpected error:', e));

    res.json({ ok: true, jobId, presentCount: null, absentCount: null });
  }
  r.post('/contacts/lookup/start', requireUser, startLookupJob);
  r.post('/contacts/enrich/start', requireUser, startLookupJob); // alias/back-compat

  function getLookupStatus(req, res) {
    const accountId = String(req.query.accountId || '');
    const label = String(req.query.label || '');
    const jobId = String(req.query.jobId || '');
    if (!accountId || !label || !jobId) return res.status(400).json({ error: 'accountId, label, jobId required' });

    return (async () => {
      const allowed = await ensureAllowed(req, res, accountId, label);
      if (!allowed) return;

      const job = JOBS.get(jobId);
      if (!job || job.accountId !== accountId || job.label !== label) return res.status(404).json({ error: 'job_not_found' });

      res.json({ ok: true, job });
    })().catch((e) => res.status(500).json({ error: 'status_failed', detail: String(e?.message || e) }));
  }
  r.get('/contacts/lookup/status', requireUser, getLookupStatus);
  r.get('/contacts/enrich/status', requireUser, getLookupStatus); // alias/back-compat

  return r;
}

/* =================
 * Queue coordinator
 * ================= */
async function runQueueForKey({ key, db, sessions }) {
  if (RUNNING.has(key)) return;
  RUNNING.add(key);
  try {
    const q = QUEUES.get(key) || [];
    while (q.length) {
      const jobId = q.shift();
      const job = JOBS.get(jobId);
      if (!job) continue;

      job.status = 'running';
      job.startedAt = Date.now();
      job.lastUpdatedAt = Date.now();

      try {
        if (job.kind === 'fullsync') {
          await processFullSyncJob({ job, db, sessions });
        } else if (job.kind === 'lookup' || job.kind === 'numbers') {
          await processLookupJob({ job, db, sessions }); // unified processor
        } else {
          throw new Error(`unknown job kind: ${job.kind}`);
        }
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

/* ==================
 * Job: FULL SYNC
 * ================== */
async function processFullSyncJob({ job, db, sessions }) {
  const { accountId, label } = job;
  const { contacts: contactsCol } = sessRefs(db, accountId, label);

  // Stage 1: read_contacts
  stageSet(job, 'read_contacts', { status: 'running', startedAt: Date.now() });
  const all = await sessions.getContacts({ accountId, label, withDetails: false });
  const subset = filterStrictCUs(all);
  const digitsList = digitsForList(subset);
  stageSet(job, 'read_contacts', { status: 'done', finishedAt: Date.now(), total: subset.length, done: subset.length });

  // Stage 2: write_contacts
  stageSet(job, 'write_contacts', { status: 'running', startedAt: Date.now(), total: subset.length, done: 0 });
  const existingByDigits = await snapshotExistingByDigits(db, contactsCol, digitsList);

  const { ops, appended, updated, appendedDigits } =
    upsertContactsOps(subset, digitsList, existingByDigits, contactsCol);

  if (ops.length) await batchedSetOrUpdate(db, ops);
  stageSet(job, 'write_contacts', { status: 'done', finishedAt: Date.now(), done: subset.length });
  job.summary.contactsAppended = appended;
  job.summary.contactsUpdated = updated;

  // Stage 3 (optional): enrich_contacts
  if (!job.params?.enrich) return;

  stageSet(job, 'enrich_contacts', { status: 'running', startedAt: Date.now() });
  let enrichUpdated = 0;

  if (subset.length) {
    const needEnrich = subset.filter((c, i) => {
      const d = digitsList[i];
      const ex = existingByDigits.get(d);
      // only appended or still missing both details
      return appendedDigits.has(d) || (ex && !ex.profilePicUrl && !ex.about);
    });

    if (needEnrich.length) {
      const enrichedList = await sessions.enrichContactsSequential({ accountId, label, contacts: needEnrich });
      const eops = [];
      for (const c of enrichedList) {
        const digits = numberFromContact(c);
        if (!digits || !waIdIsCUs(c?.id)) continue;

        const ex = existingByDigits.get(digits) || {};
        const incoming = {
          profilePicUrl: c?.profilePicUrl ?? null,
          about: typeof c?.about === 'string' && c.about.length ? c.about : null,
        };
        const diff = diffFillOnly(ex, incoming);
        if (Object.keys(diff).length) {
          const changed = changedFields(ex, diff);
          eops.push({
            ref: contactsCol.doc(docIdForDigits(digits)),
            update: {
              ...diff,
              enrichedAt: FieldValue.serverTimestamp(),
              ...(changed.length ? { enrichedFields: FieldValue.arrayUnion(...changed) } : {}),
              updatedAt: FieldValue.serverTimestamp(),
            },
          });
          enrichUpdated++;
        }
      }
      if (eops.length) await batchedSetOrUpdate(db, eops);
    }
  }

  job.summary.enrichUpdated = enrichUpdated;
  stageSet(job, 'enrich_contacts', { status: 'done', finishedAt: Date.now(), done: enrichUpdated });
}

/* ==================
 * Job: LOOKUP (numbers)
 * ================== */
async function processLookupJob({ job, db, sessions }) {
  const { accountId, label } = job;
  const { contacts: contactsCol } = sessRefs(db, accountId, label);
  const countryCode = job?.params?.countryCode ?? null;

  // Stage 1: normalize_numbers
  stageSet(job, 'normalize_numbers', { status: 'running', startedAt: Date.now() });
  const input = Array.isArray(job.params?.numbers) ? job.params.numbers : [];
  const normalized = input.map((x) => normalizeDigits(x)).filter(Boolean);
  stageSet(job, 'normalize_numbers', { status: 'done', finishedAt: Date.now(), total: input.length, done: normalized.length });
  job.summary.normalized = normalized.length;

  // Stage 2: lookup_numbers
  stageSet(job, 'lookup_numbers', { status: 'running', startedAt: Date.now(), total: normalized.length, done: 0 });
  const results = await sessions.lookupContactsByNumbers({
    accountId,
    label,
    numbers: normalized,
    countryCode,       // forwarded if provided
    withDetails: true, // keep details here
  });
  const registeredCount = results.reduce((n, r) => (r?.registered ? n + 1 : n), 0);
  stageSet(job, 'lookup_numbers', { status: 'done', finishedAt: Date.now(), done: results.length });
  job.summary.registered = registeredCount;
  job.summary.unregistered = results.length - registeredCount;

  // Stage 3: write_numbers (upsert; include unregistered w/ registered=false)
  stageSet(job, 'write_numbers', { status: 'running', startedAt: Date.now(), total: results.length, done: 0 });

  const docRefs = normalized.map((d) => contactsCol.doc(docIdForDigits(d)));
  const existingSnaps = docRefs.length ? await batchedGetAll(db, docRefs, 300) : [];
  const existingByDigits = new Map();
  for (let i = 0; i < existingSnaps.length; i++) {
    const d = normalized[i];
    const snap = existingSnaps[i];
    if (d && snap?.exists) existingByDigits.set(d, snap.data());
  }

  const ops = [];
  let written = 0;
  const appendedDigits = new Set();
  const detailsPresent = new Map(); // track if details already stored via payload

  for (const r of results) {
    const digits = normalizeDigits(r.normalized || r.input);
    if (!digits) continue;

    const existing = existingByDigits.get(digits);
    const ref = contactsCol.doc(docIdForDigits(digits));

    const payload = {
      id: (r.waId && waIdIsCUs(r.waId)) ? r.waId : (existing?.id || null),
      number: digits,
      name: r.contact?.name || existing?.name || null,
      pushname: r.contact?.pushname || existing?.pushname || null,
      shortName: r.contact?.shortName || existing?.shortName || null,
      isWAContact: !!r.registered,
      isMyContact: !!r.contact?.isMyContact,
      isBusiness: !!r.contact?.isBusiness,
      isEnterprise: !!r.contact?.isEnterprise,
      hasChat: !!r.hasChat,
      registered: !!r.registered,
      type: 'private',
      profilePicUrl: r.contact?.profilePicUrl || null,
      about: r.contact?.about || null,
      updatedAt: FieldValue.serverTimestamp(),
    };

    detailsPresent.set(digits, !!(payload.profilePicUrl || payload.about));

    if (!existing) {
      ops.push({ ref, set: { ...payload, createdAt: FieldValue.serverTimestamp() } });
      written++;
      appendedDigits.add(digits);
    } else {
      const baseDiff = diffFillOnly(existing, payload);
      for (const f of ALWAYS_UPDATE_FLAGS) {
        if (payload[f] !== undefined && payload[f] !== existing[f]) baseDiff[f] = payload[f];
      }
      if (payload.id && payload.id !== existing.id) baseDiff.id = payload.id;

      if (Object.keys(baseDiff).length) {
        ops.push({ ref, update: { ...baseDiff, updatedAt: FieldValue.serverTimestamp() } });
        written++;
      }
    }
  }

  if (ops.length) await batchedSetOrUpdate(db, ops);
  stageSet(job, 'write_numbers', { status: 'done', finishedAt: Date.now(), done: results.length });
  job.summary.written = written;

  // Stage 4: enrich_missing — ONLY if still missing after Stage 3
  stageSet(job, 'enrich_missing', { status: 'running', startedAt: Date.now() });
  let enrichUpdated = 0;

  const need = [];
  for (const r of results) {
    const digits = normalizeDigits(r.normalized || r.input);
    if (!digits) continue;

    // if the write step already stored details, skip enrichment
    if (detailsPresent.get(digits)) continue;

    const ex = existingByDigits.get(digits);
    const isRegistered = !!r.registered || !!ex?.registered;
    if (!isRegistered) continue;

    const waId = (r.waId && waIdIsCUs(r.waId)) ? r.waId : (ex?.id && waIdIsCUs(ex.id) ? ex.id : null);
    if (!waId) continue;

    const wasMissingBefore = !ex?.profilePicUrl && !ex?.about;
    const shouldEnrich = appendedDigits.has(digits) || wasMissingBefore;

    if (shouldEnrich) need.push({ id: waId, number: digits, type: 'private' });
  }

  if (need.length) {
    const enriched = await sessions.enrichContactsSequential({ accountId, label, contacts: need });
    const eops = [];
    for (const c of enriched) {
      const digits = numberFromContact(c);
      if (!digits || !waIdIsCUs(c?.id)) continue;

      const ex = existingByDigits.get(digits) || {};
      const incoming = {
        profilePicUrl: c?.profilePicUrl ?? null,
        about: typeof c?.about === 'string' && c.about.length ? c.about : null,
      };
      const diff = diffFillOnly(ex, incoming);
      if (Object.keys(diff).length) {
        const changed = changedFields(ex, diff);
        eops.push({
          ref: contactsCol.doc(docIdForDigits(digits)),
          update: {
            ...diff,
            enrichedAt: FieldValue.serverTimestamp(),
            ...(changed.length ? { enrichedFields: FieldValue.arrayUnion(...changed) } : {}),
            updatedAt: FieldValue.serverTimestamp(),
          },
        });
        enrichUpdated++;
      }
    }
    if (eops.length) await batchedSetOrUpdate(db, eops);
  }

  job.summary.enrichUpdated = enrichUpdated;
  stageSet(job, 'enrich_missing', { status: 'done', finishedAt: Date.now(), done: enrichUpdated });
}
