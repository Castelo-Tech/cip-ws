// routes/contacts.js
import { Router } from 'express';
import {
  FieldValue,
} from 'firebase-admin/firestore';
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
function docIdForDigits(digits) {
  return normalizeDigits(digits); // contact docId = numeric phone
}
function waIdIsCUs(id) {
  return typeof id === 'string' && id.endsWith('@c.us');
}

// ===== Merge helpers (fill-only upsert; returns {merged, appended, updated}) =====
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

  const merged = Array.from(map.values()).sort((a, b) => {
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

// ===== In-memory job management (unchanged API) =====
const JOBS = new Map(); // jobId -> job object
const QUEUES = new Map(); // key accountId::label -> [jobId]
const RUNNING = new Set(); // keys currently running

function queueKey({ accountId, label }) {
  return `${accountId}::${label}`;
}
function makeJobId() {
  return `job_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

// ===== Router =====
export function buildContactsRouter({ db, sessions, requireUser, ensureAllowed }) {
  const r = Router();

  // ---------- GET /contacts ----------
  // - Pull live contacts from WA (no enrichment here).
  // - Filter to private + isMyContact + isWAContact + @c.us
  // - Upsert into Firestore under /accounts/{aid}/sessions/{label}/contacts/{digits}
  // - Optionally also persist chats for those contacts with hasChat (?includeChats=1)
  r.get('/contacts', requireUser, async (req, res) => {
    const accountId = String(req.query.accountId || '');
    const label = String(req.query.label || '');
    const includeChats = String(req.query.includeChats || '') === '1';

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
          waIdIsCUs(c.id)
      );

      // Prepare existing docs map (by digits)
      const { contacts: contactsCol, chats: chatsCol } = sessRefs(db, accountId, label);

      const digitsList = subset.map((c) => numberFromContact(c)).filter(Boolean);
      const docRefs = digitsList.map((d) => contactsCol.doc(docIdForDigits(d)));
      const existingSnaps = docRefs.length ? await db.getAll(...docRefs) : [];
      const existingByDigits = new Map();
      for (let i = 0; i < existingSnaps.length; i++) {
        const d = digitsList[i];
        const snap = existingSnaps[i];
        if (d && snap?.exists) existingByDigits.set(d, snap.data());
      }

      // Build upserts
      const ops = [];
      let appended = 0;
      let updated = 0;

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
          type: 'private',
          updatedAt: FieldValue.serverTimestamp(),
        };

        const existing = existingByDigits.get(digits);
        if (!existing) {
          ops.push({
            ref,
            set: {
              ...incoming,
              createdAt: FieldValue.serverTimestamp(),
            },
          });
          appended++;
        } else {
          const diff = diffFillOnly(existing, incoming);
          if (Object.keys(diff).length) {
            ops.push({ ref, update: { ...diff, updatedAt: FieldValue.serverTimestamp() } });
            updated++;
          } else if (incoming.hasChat !== existing.hasChat) {
            // Keep hasChat up to date even if fill-only had no other fields
            ops.push({
              ref,
              update: { hasChat: incoming.hasChat, updatedAt: FieldValue.serverTimestamp() },
            });
            updated++;
          }
        }
      }

      if (ops.length) await batchedSetOrUpdate(db, ops);

      // Optional: persist chats for contacts that have a chat
      let chatsPersisted = 0;
      if (includeChats) {
        const numbersWithChat = subset
          .filter((c, idx) => c?.hasChat && !!digitsList[idx])
          .map((_, idx) => digitsList[idx]);

        if (numbersWithChat.length) {
          const results = await sessions.getChatsByNumbers({
            accountId,
            label,
            numbers: numbersWithChat,
            countryCode: null,
            withMessages: false,
            messagesLimit: 1,
          });

          const chatOps = [];
          for (const r of results) {
            if (!r?.exists || !r?.waId || !waIdIsCUs(r.waId)) continue;
            const digits = normalizeDigits(r.normalized || r.input);
            if (!digits) continue;
            const ref = chatsCol.doc(docIdForDigits(digits));
            const payload = {
              id: r.chat?.id || r.waId,
              number: digits,
              name: r.chat?.name || null,
              isGroup: !!r.chat?.isGroup, // should be false for private
              unreadCount: r.chat?.unreadCount ?? null,
              archived: !!r.chat?.archived,
              pinned: !!r.chat?.pinned,
              isReadOnly: !!r.chat?.isReadOnly,
              updatedAt: FieldValue.serverTimestamp(),
            };
            chatOps.push({ ref, set: { ...payload, createdAt: FieldValue.serverTimestamp() } });
          }
          if (chatOps.length) {
            await batchedSetOrUpdate(db, chatOps);
            chatsPersisted = chatOps.length;
          }
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
          appended,
          updated,
          chatsPersisted: includeChats ? chatsPersisted : 0,
        },
      });
    } catch (e) {
      res.status(500).json({ error: 'contacts_failed', detail: String(e?.message || e) });
    }
  });

  // ---------- POST /contacts/enrich/start ----------
  // body: { accountId, label, numbers: string[] }
  // - Splits numbers into present (in contacts collection) vs absent.
  // - Enrich present (avatar/bio) with small jitter; add absent if registered.
  // - Writes back to Firestore; marks enrichedAt + enrichedFields.
  // NOTE: hard cap of 200 numbers per job (reject if above).
  r.post('/contacts/enrich/start', requireUser, async (req, res) => {
    const { accountId, label, numbers } = req.body || {};
    if (!accountId || !label || !Array.isArray(numbers) || numbers.length === 0) {
      return res.status(400).json({ error: 'accountId, label, numbers[] required' });
    }

    if (numbers.length > 200) {
      return res.status(413).json({ error: 'too_many_numbers', limit: 200 });
    }

    const allowed = await ensureAllowed(req, res, accountId, label);
    if (!allowed) return;

    const st = sessions.status({ accountId, label });
    if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

    try {
      const { contacts: contactsCol } = sessRefs(db, accountId, label);
      // Load base contacts present in Firestore
      const snap = await contactsCol.get();
      const baseContacts = snap.docs
        .map((d) => d.data())
        .filter((c) => !c?.id || waIdIsCUs(c.id));

      const presentNumbersSet = new Set(baseContacts.map((c) => numberFromContact(c)).filter(Boolean));
      const inNumbers = numbers.map((x) => normalizeDigits(x)).filter(Boolean);

      const present = [];
      const absent = [];
      for (const n of inNumbers) {
        if (presentNumbersSet.has(n)) present.push(n);
        else absent.push(n);
      }

      const jobId = makeJobId();
      const key = queueKey({ accountId, label });
      const job = {
        id: jobId,
        accountId,
        label,
        status: 'queued',
        createdAt: Date.now(),
        startedAt: null,
        finishedAt: null,
        input: { total: inNumbers.length, numbers: inNumbers },
        sets: { present, absent },
        progress: {
          presentTotal: present.length,
          presentDone: 0,
          absentTotal: absent.length,
          absentDone: 0,
        },
        storage: {
          store: 'firestore',
          path: `/accounts/${accountId}/sessions/${label}/contacts/*`,
        },
        error: null,
      };

      JOBS.set(jobId, job);
      if (!QUEUES.has(key)) QUEUES.set(key, []);
      QUEUES.get(key).push(jobId);

      runQueueForKey({ key, db, sessions }).catch((e) => {
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

  // ---------- GET /contacts/enrich/status ----------
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

// ===== Queue runner (uses Firestore instead of GCS) =====
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

      try {
        await processJob({ job, db, sessions });
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

async function processJob({ job, db, sessions }) {
  const { accountId, label } = job;
  const { contacts: contactsCol } = sessRefs(db, accountId, label);

  // Load base list
  let baseDocs = await contactsCol.get();
  let baseContacts = baseDocs.docs.map((d) => d.data()).filter((c) => !c?.id || waIdIsCUs(c.id));

  // Map by digits
  const byDigits = new Map(baseContacts.map((c) => [numberFromContact(c), c]));

  // ---- Phase A: enrich present contacts (avatar/bio) ----
  if (job.sets.present.length) {
    // Build contact stubs with WA id for the enrich helper
    const presentContacts = job.sets.present
      .map((n) => byDigits.get(n))
      .filter(Boolean);

    // Use existing sequential helper (keeps structure); internal jitter is modest.
    const enrichedList = await sessions.enrichContactsSequential({
      accountId,
      label,
      contacts: presentContacts,
    });

    // Write selective updates to Firestore
    const ops = [];
    for (const c of enrichedList) {
      const digits = numberFromContact(c);
      if (!digits || !waIdIsCUs(c?.id)) continue;

      const ref = contactsCol.doc(docIdForDigits(digits));
      const existing = byDigits.get(digits) || {};

      const incoming = {
        profilePicUrl: c?.profilePicUrl ?? null,
        about: typeof c?.about === 'string' && c.about.length ? c.about : null,
      };

      const diff = diffFillOnly(existing, incoming);
      const changed = changedFields(existing, diff);
      if (changed.length) {
        ops.push({
          ref,
          update: {
            ...diff,
            enrichedAt: FieldValue.serverTimestamp(),
            enrichedFields: FieldValue.arrayUnion(...changed),
            updatedAt: FieldValue.serverTimestamp(),
          },
        });
        // update local mirror for subsequent diffs
        byDigits.set(digits, { ...existing, ...diff });
      }
    }
    if (ops.length) await batchedSetOrUpdate(db, ops);

    job.progress.presentDone = job.progress.presentTotal;
  }

  // ---- Phase B: check absent numbers (registration + details) ----
  const opsAbsent = [];
  for (const n of job.sets.absent) {
    // Very small jitter (fast mode)
    await sleep(rand(8, 25));

    const result = await sessions.checkContactByNumber({
      accountId,
      label,
      number: n,
      countryCode: null,
      withDetails: true,
    });

    if (result?.registered && result?.waId && waIdIsCUs(result.waId)) {
      const digits = normalizeDigits(result.normalized || n);
      if (!digits) {
        job.progress.absentDone += 1;
        continue;
      }

      const ref = contactsCol.doc(docIdForDigits(digits));
      const payload = {
        id: result.waId,
        number: digits,
        name: result.contact?.name || null,
        pushname: result.contact?.pushname || null,
        shortName: result.contact?.shortName || null,
        isWAContact: true,
        isMyContact: !!result.contact?.isMyContact,
        isBusiness: !!result.contact?.isBusiness,
        isEnterprise: !!result.contact?.isEnterprise,
        hasChat: !!result.hasChat,
        type: 'private',
        profilePicUrl: result.contact?.profilePicUrl || null,
        about: result.contact?.about || null,
        createdAt: FieldValue.serverTimestamp(),
        updatedAt: FieldValue.serverTimestamp(),
        enrichedAt: FieldValue.serverTimestamp(),
        enrichedFields: ['profilePicUrl', 'about'],
      };
      opsAbsent.push({ ref, set: payload });
      byDigits.set(digits, payload); // keep mirror coherent
    }

    job.progress.absentDone += 1;
  }
  if (opsAbsent.length) await batchedSetOrUpdate(db, opsAbsent);
}
