// routes/contacts.js
import { Router } from 'express';
import { FieldValue } from 'firebase-admin/firestore';
import { sleep, rand } from '../lib/session/utils.js';

// ===== Path + id helpers (session-scoped Firestore structure) =====
function sessRefs(db, accountId, label) {
  const base = db.collection('accounts').doc(accountId).collection('sessions').doc(label);
  return {
    base,
    contacts: base.collection('contacts'),
    chats: base.collection('chats'),
  };
}

// For message writes under a chat doc (unused now; kept for compatibility)
function messagesCol(db, accountId, label, chatDocId) {
  const ses = db.collection('accounts').doc(accountId).collection('sessions').doc(label);
  return ses.collection('chats').doc(chatDocId).collection('messages');
}

// ===== Normalization helpers =====
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

// ===== Stats (for response) =====
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

// ===== Firestore upsert utils =====
function diffFillOnly(existing, incoming) {
  // Only include fields where existing is empty/undefined/null OR "profilePicUrl/about" when currently empty.
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
      if (op.set) batch.set(op.ref, op.set, { merge: true });
      if (op.update && Object.keys(op.update).length) batch.set(op.ref, op.update, { merge: true });
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

// ===== Fields we always refresh (not fill-only) =====
const ALWAYS_UPDATE_FLAGS = new Set(['registered', 'isWAContact', 'isMyContact', 'hasChat']);

// ===== Snippet helper for last message =====
function snippetOf(body = '', max = 160) {
  const s = String(body || '');
  if (s.length <= max) return s;
  return s.slice(0, max - 1) + '…';
}

// ===== In-memory job management =====
const JOBS = new Map(); // jobId -> job object
const QUEUES = new Map(); // key accountId::label -> [jobId]
const RUNNING = new Set(); // keys currently running

function queueKey({ accountId, label }) { return `${accountId}::${label}`; }
function makeJobId() { return `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`; }

function makeStage(key, label) {
  return { key, label, status: 'pending', total: 0, done: 0, error: null, startedAt: null, finishedAt: null };
}
function stageSet(job, key, patch) {
  const st = job.stages.find((s) => s.key === key);
  if (!st) return;
  Object.assign(st, patch);
  job.lastUpdatedAt = Date.now();
}

// ===== Router =====
export function buildContactsRouter({ db, sessions, requireUser, ensureAllowed }) {
  const r = Router();

  // ---------- GET /contacts (kept for ad-hoc/manual runs) ----------
  // Sequential (non-job) run:
  //   1) contacts (no details)
  //   2) chats (NO message history) – only lastMessage snippet
  //   3) enrichment (pics/about) fill-only
  r.get('/contacts', requireUser, async (req, res) => {
    const accountId = String(req.query.accountId || '');
    const label = String(req.query.label || '');
    const includeChats = String(req.query.includeChats || '') === '1';
    const wantDetailsAtEnd = ['1', 'true', 'yes'].includes(String(req.query.details || '').toLowerCase());

    if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });
    const allowed = await ensureAllowed(req, res, accountId, label);
    if (!allowed) return;

    const st = sessions.status({ accountId, label });
    if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

    const phases = {
      contacts: { ok: false, appended: 0, updated: 0 },
      chats: { ok: !includeChats, chatsPersisted: 0, error: null },
      enrich: { ok: !wantDetailsAtEnd, updated: 0, error: null },
    };

    try {
      // ---- Phase 1: CONTACTS (no details) ----
      const all = await sessions.getContacts({ accountId, label, withDetails: false });

      // STRICT filter: private + isMyContact + isWAContact + @c.us
      const subset = all.filter(
        (c) =>
          c?.type === 'private' &&
          !!c?.isMyContact &&
          !!c?.isWAContact &&
          typeof c?.id === 'string' &&
          waIdIsCUs(c.id)
      );

      const { contacts: contactsCol, chats: chatsCol } = sessRefs(db, accountId, label);
      const digitsList = subset.map((c) => numberFromContact(c)).filter(Boolean);

      const docRefs = digitsList.map((d) => contactsCol.doc(docIdForDigits(d)));
      const existingSnaps = docRefs.length ? await batchedGetAll(db, docRefs, 300) : [];
      const existingByDigits = new Map();
      for (let i = 0; i < existingSnaps.length; i++) {
        const d = digitsList[i];
        const snap = existingSnaps[i];
        if (d && snap?.exists) existingByDigits.set(d, snap.data());
      }

      const ops = [];
      let appended = 0, updated = 0;
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
        } else {
          // fill-only for most fields, but ALWAYS update core flags if changed
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
      if (ops.length) await batchedSetOrUpdate(db, ops);
      phases.contacts = { ok: true, appended, updated };

      // ---- Phase 2: CHATS (only lastMessage snippet; NO messages persisted) ----
      let chatsPersisted = 0;
      if (includeChats) {
        try {
          const numbersWithChat = subset
            .filter((c, idx) => c?.hasChat && !!digitsList[idx])
            .map((_, idx) => digitsList[idx]);

          if (numbersWithChat.length) {
            const results = await sessions.getChatsByNumbers({
              accountId,
              label,
              numbers: numbersWithChat,
              countryCode: null,
              withMessages: false, // IMPORTANT: don't fetch messages
            });

            const chatOps = [];
            for (const r of results) {
              if (!r?.exists || !r?.waId || !waIdIsCUs(r.waId)) continue;
              const digits = normalizeDigits(r.normalized || r.input);
              if (!digits) continue;

              const chatDocId = docIdForDigits(digits);
              const cref = chatsCol.doc(chatDocId);

              const lm = r.chat?.lastMessage || null;
              const payload = {
                id: r.chat?.id || r.waId,
                number: digits,
                name: r.chat?.name || null,
                isGroup: !!r.chat?.isGroup, // should be false for private
                unreadCount: r.chat?.unreadCount ?? null,
                archived: !!r.chat?.archived,
                pinned: !!r.chat?.pinned,
                isReadOnly: !!r.chat?.isReadOnly,
                timestamp: r.chat?.timestamp ?? null, // last activity unix
                // last message snippet (lightweight)
                lastMessageId: lm?.id || null,
                lastMessageBody: lm ? snippetOf(lm.body || '') : null,
                lastMessageType: lm?.type || null,
                lastMessageFromMe: lm?.fromMe ?? null,
                lastMessageTimestamp: lm?.timestamp ?? null,
                lastMessageHasMedia: lm?.hasMedia ?? null,
                updatedAt: FieldValue.serverTimestamp(),
              };
              chatOps.push({ ref: cref, set: { ...payload, createdAt: FieldValue.serverTimestamp() } });
            }

            if (chatOps.length) {
              await batchedSetOrUpdate(db, chatOps);
              chatsPersisted = chatOps.length;
            }
          }
          phases.chats = { ok: true, chatsPersisted, error: null };
        } catch (e) {
          phases.chats = { ok: false, chatsPersisted: 0, error: String(e?.message || e) };
        }
      }

      // ---- Phase 3: ENRICH (pics/about) ----
      let enrichUpdated = 0;
      if (wantDetailsAtEnd) {
        try {
          const refSnaps = docRefs.length ? await batchedGetAll(db, docRefs, 300) : [];
          const existingMap = new Map();
          for (let i = 0; i < refSnaps.length; i++) {
            const d = digitsList[i];
            const snap = refSnaps[i];
            if (d && snap?.exists) existingMap.set(d, snap.data());
          }

          const needEnrich = subset.filter((c, i) => {
            const d = digitsList[i];
            const ex = existingMap.get(d);
            return ex && (!ex.profilePicUrl && !ex.about);
          });

          if (needEnrich.length) {
            const enrichedList = await sessions.enrichContactsSequential({ accountId, label, contacts: needEnrich });
            const eops = [];
            for (const c of enrichedList) {
              const digits = numberFromContact(c);
              if (!digits || !waIdIsCUs(c?.id)) continue;

              const existing = existingMap.get(digits) || {};
              const incoming = {
                profilePicUrl: c?.profilePicUrl ?? null,
                about: typeof c?.about === 'string' && c.about.length ? c.about : null,
              };
              const diff = diffFillOnly(existing, incoming);
              if (Object.keys(diff).length) {
                const changed = changedFields(existing, diff);
                eops.push({
                  ref: sessRefs(db, accountId, label).contacts.doc(docIdForDigits(digits)),
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
        } catch (e) {
          phases.enrich = { ok: false, updated: 0, error: String(e?.message || e) };
        }
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
          chatsPersisted: phases.chats.chatsPersisted,
          messagesPersisted: 0, // always 0 now
          enrichUpdated: phases.enrich.updated,
        },
        phases,
      });
    } catch (e) {
      res.status(500).json({ error: 'contacts_failed', detail: String(e?.message || e) });
    }
  });

  // ---------- JOB: FULL SYNC ----------
  // POST /contacts/sync/start
  // body: { accountId, label, messagesLimit?: number (ignored now; kept for compatibility) }
  r.post('/contacts/sync/start', requireUser, async (req, res) => {
    const { accountId, label, messagesLimit } = req.body || {};
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
      params: { messagesLimit: Math.max(1, Math.min(100, Number(messagesLimit) || 10)) }, // not used anymore
      storage: { store: 'firestore', path: `/accounts/${accountId}/sessions/${label}/*` },
      // Stages
      stages: [
        makeStage('read_contacts',   'Read contacts (no details)'),
        makeStage('write_contacts',  'Write/merge contacts'),
        makeStage('persist_chats',   'Write chats (lastMessage snippet only)'),
        makeStage('enrich_contacts', 'Enrich pics/about (fill-only)'),
      ],
      // Summaries
      summary: { contactsAppended: 0, contactsUpdated: 0, chatsPersisted: 0, messagesPersisted: 0, enrichUpdated: 0 },
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

  // ---------- JOB: NUMBERS WORKFLOW (stage-based) ----------
  // POST /contacts/enrich/start    (backwards-compatible route name)
  // body: { accountId, label, numbers: string[] }
  // Stages:
  //  - normalize_numbers
  //  - lookup_numbers (registered? hasChat? details)
  //  - write_numbers (upsert; add unregistered too with registered=false)
  //  - enrich_registered_missing (fill-only pictures/about for those needing it)
  r.post('/contacts/enrich/start', requireUser, async (req, res) => {
    const { accountId, label, numbers } = req.body || {};
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
      kind: 'numbers',
      accountId, label,
      status: 'queued',
      createdAt: Date.now(),
      startedAt: null,
      finishedAt: null,
      lastUpdatedAt: null,
      params: { numbers },
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
    runQueueForKey({ key, db, sessions }).catch((e) => console.error('[enrich runner] unexpected error:', e));

    res.json({ ok: true, jobId, presentCount: null, absentCount: null });
  });

  r.get('/contacts/enrich/status', requireUser, async (req, res) => {
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

  return r;
}

// ===== Queue runner =====
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
        } else if (job.kind === 'numbers') {
          await processNumbersJob({ job, db, sessions });
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

// ===== Job processors =====
async function processFullSyncJob({ job, db, sessions }) {
  const { accountId, label } = job;
  const { contacts: contactsCol, chats: chatsCol } = sessRefs(db, accountId, label);

  // Stage 1: read_contacts
  stageSet(job, 'read_contacts', { status: 'running', startedAt: Date.now() });
  const all = await sessions.getContacts({ accountId, label, withDetails: false });

  // STRICT filter: private + isMyContact + isWAContact + @c.us
  const subset = all.filter(
    (c) =>
      c?.type === 'private' &&
      !!c?.isMyContact &&
      !!c?.isWAContact &&
      typeof c?.id === 'string' &&
      waIdIsCUs(c.id)
  );

  const digitsList = subset.map((c) => numberFromContact(c)).filter(Boolean);
  stageSet(job, 'read_contacts', { status: 'done', finishedAt: Date.now(), total: subset.length, done: subset.length });

  // Stage 2: write_contacts
  stageSet(job, 'write_contacts', { status: 'running', startedAt: Date.now(), total: subset.length, done: 0 });
  const docRefs = digitsList.map((d) => contactsCol.doc(docIdForDigits(d)));
  const existingSnaps = docRefs.length ? await batchedGetAll(db, docRefs, 300) : [];
  const existingByDigits = new Map();
  for (let i = 0; i < existingSnaps.length; i++) {
    const d = digitsList[i];
    const snap = existingSnaps[i];
    if (d && snap?.exists) existingByDigits.set(d, snap.data());
  }

  const ops = [];
  let appended = 0, updated = 0;
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
      registered: true,
      type: 'private',
      updatedAt: FieldValue.serverTimestamp(),
    };
    const existing = existingByDigits.get(digits);
    if (!existing) {
      ops.push({ ref, set: { ...incoming, createdAt: FieldValue.serverTimestamp() } });
      appended++;
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
  if (ops.length) await batchedSetOrUpdate(db, ops);
  stageSet(job, 'write_contacts', { status: 'done', finishedAt: Date.now(), done: subset.length });
  job.summary.contactsAppended = appended;
  job.summary.contactsUpdated = updated;

  // Stage 3: persist_chats (only lastMessage snippet)
  stageSet(job, 'persist_chats', { status: 'running', startedAt: Date.now() });
  let chatsPersisted = 0;
  const numbersWithChat = subset
    .filter((c, idx) => c?.hasChat && !!digitsList[idx])
    .map((_, idx) => digitsList[idx]);

  if (numbersWithChat.length) {
    const results = await sessions.getChatsByNumbers({
      accountId,
      label,
      numbers: numbersWithChat,
      countryCode: null,
      withMessages: false, // no history
    });

    const chatOps = [];
    for (const r of results) {
      if (!r?.exists || !r?.waId || !waIdIsCUs(r.waId)) continue;
      const digits = normalizeDigits(r.normalized || r.input);
      if (!digits) continue;

      const chatDocId = docIdForDigits(digits);
      const cref = chatsCol.doc(chatDocId);

      const lm = r.chat?.lastMessage || null;
      const payload = {
        id: r.chat?.id || r.waId,
        number: digits,
        name: r.chat?.name || null,
        isGroup: !!r.chat?.isGroup,
        unreadCount: r.chat?.unreadCount ?? null,
        archived: !!r.chat?.archived,
        pinned: !!r.chat?.pinned,
        isReadOnly: !!r.chat?.isReadOnly,
        timestamp: r.chat?.timestamp ?? null,
        lastMessageId: lm?.id || null,
        lastMessageBody: lm ? snippetOf(lm.body || '') : null,
        lastMessageType: lm?.type || null,
        lastMessageFromMe: lm?.fromMe ?? null,
        lastMessageTimestamp: lm?.timestamp ?? null,
        lastMessageHasMedia: lm?.hasMedia ?? null,
        updatedAt: FieldValue.serverTimestamp(),
      };
      chatOps.push({ ref: cref, set: { ...payload, createdAt: FieldValue.serverTimestamp() } });
    }
    if (chatOps.length) { await batchedSetOrUpdate(db, chatOps); chatsPersisted = chatOps.length; }
  }

  job.summary.chatsPersisted = chatsPersisted;
  job.summary.messagesPersisted = 0; // no message history saved
  stageSet(job, 'persist_chats', { status: 'done', finishedAt: Date.now(), total: numbersWithChat.length, done: numbersWithChat.length });

  // Stage 4: enrich_contacts
  stageSet(job, 'enrich_contacts', { status: 'running', startedAt: Date.now() });
  let enrichUpdated = 0;
  if (subset.length) {
    const refSnaps = docRefs.length ? await batchedGetAll(db, docRefs, 300) : [];
    const existingMap = new Map();
    for (let i = 0; i < refSnaps.length; i++) {
      const d = digitsList[i];
      const snap = refSnaps[i];
      if (d && snap?.exists) existingMap.set(d, snap.data());
    }
    const needEnrich = subset.filter((c, i) => {
      const d = digitsList[i];
      const ex = existingMap.get(d);
      return ex && (!ex.profilePicUrl && !ex.about);
    });

    if (needEnrich.length) {
      const enrichedList = await sessions.enrichContactsSequential({ accountId, label, contacts: needEnrich });
      const eops = [];
      for (const c of enrichedList) {
        const digits = numberFromContact(c);
        if (!digits || !waIdIsCUs(c?.id)) continue;

        const existing = existingMap.get(digits) || {};
        const incoming = {
          profilePicUrl: c?.profilePicUrl ?? null,
          about: typeof c?.about === 'string' && c.about.length ? c.about : null,
        };
        const diff = diffFillOnly(existing, incoming);
        if (Object.keys(diff).length) {
          const changed = changedFields(existing, diff);
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

async function processNumbersJob({ job, db, sessions }) {
  const { accountId, label } = job;
  const { contacts: contactsCol } = sessRefs(db, accountId, label);

  // Stage 1: normalize_numbers
  stageSet(job, 'normalize_numbers', { status: 'running', startedAt: Date.now() });
  const input = Array.isArray(job.params.numbers) ? job.params.numbers : [];
  const normalized = input.map((x) => normalizeDigits(x)).filter(Boolean);
  stageSet(job, 'normalize_numbers', { status: 'done', finishedAt: Date.now(), total: input.length, done: normalized.length });
  job.summary.normalized = normalized.length;

  // Stage 2: lookup_numbers
  stageSet(job, 'lookup_numbers', { status: 'running', startedAt: Date.now(), total: normalized.length, done: 0 });
  const results = await sessions.lookupContactsByNumbers({
    accountId,
    label,
    numbers: normalized,
    countryCode: null,
    withDetails: true,
  });
  const registeredCount = results.reduce((n, r) => (r?.registered ? n + 1 : n), 0);
  stageSet(job, 'lookup_numbers', { status: 'done', finishedAt: Date.now(), done: results.length });
  job.summary.registered = registeredCount;
  job.summary.unregistered = results.length - registeredCount;

  // Stage 3: write_numbers (upsert; include unregistered with registered=false)
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
  for (const r of results) {
    const digits = normalizeDigits(r.normalized || r.input);
    if (!digits) continue;

    const existing = existingByDigits.get(digits);
    const ref = contactsCol.doc(docIdForDigits(digits));

    const payload = {
      id: (r.waId && waIdIsCUs(r.waId)) ? r.waId : null,
      number: digits,
      name: r.contact?.name || null,
      pushname: r.contact?.pushname || null,
      shortName: r.contact?.shortName || null,
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

    if (!existing) {
      ops.push({ ref, set: { ...payload, createdAt: FieldValue.serverTimestamp() } });
      written++;
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

  // Stage 4: enrich_missing (registered only, and only if both fields missing)
  stageSet(job, 'enrich_missing', { status: 'running', startedAt: Date.now() });
  const refSnaps = docRefs.length ? await batchedGetAll(db, docRefs, 300) : [];
  const need = [];
  const mapByDigits = new Map();
  for (let i = 0; i < refSnaps.length; i++) {
    const d = normalized[i];
    const snap = refSnaps[i];
    const ex = snap?.exists ? snap.data() : null;
    if (!ex) continue;
    mapByDigits.set(d, ex);
    if (ex.registered && !ex.profilePicUrl && !ex.about && ex.id && waIdIsCUs(ex.id)) {
      need.push({ id: ex.id, number: ex.number, type: 'private' });
    }
  }

  let enrichUpdated = 0;
  if (need.length) {
    const enriched = await sessions.enrichContactsSequential({ accountId, label, contacts: need });
    const eops = [];
    for (const c of enriched) {
      const digits = numberFromContact(c);
      if (!digits || !waIdIsCUs(c?.id)) continue;

      const existing = mapByDigits.get(digits) || {};
      const incoming = {
        profilePicUrl: c?.profilePicUrl ?? null,
        about: typeof c?.about === 'string' && c.about.length ? c.about : null,
      };
      const diff = diffFillOnly(existing, incoming);
      if (Object.keys(diff).length) {
        const changed = changedFields(existing, diff);
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
