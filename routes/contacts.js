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
    chats: base.collection('chats'), // forward-compat, unused here
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
 * Stats (for responses)
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
  // Fill-only for general fields (don’t clobber existing values).
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
      if (op.set) batch.set(op.ref, op.set, { merge: true });
      else if (op.update && Object.keys(op.update).length) batch.update(op.ref, op.update);
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
      registered: true, // device contacts are real WA contacts
      type: 'private',
      updatedAt: FieldValue.serverTimestamp(),
    };

    const existing = existingByDigits.get(digits);
    if (!existing) {
      ops.push({ ref, set: { ...incoming, createdAt: FieldValue.serverTimestamp() } });
      appended++;
    } else {
      // Don’t touch enriched fields; only fill what’s empty and refresh flags.
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
  return { ops, appended, updated };
}

/* ============
 * The Router
 * ============ */
export function buildContactsRouter({ db, sessions, requireUser, ensureAllowed }) {
  const r = Router();

  /* ---------------------------------------------------
   * GET /contacts
   * Synchronous, idempotent:
   *  - Reads WA contacts (no details)
   *  - Writes only diffs to Firestore (fill-only + flags)
   *  - NO enrichment
   * --------------------------------------------------- */
  r.get('/contacts', requireUser, async (req, res) => {
    const accountId = String(req.query.accountId || '');
    const label = String(req.query.label || '');
    if (!accountId || !label) return res.status(400).json({ error: 'accountId, label required' });

    const allowed = await ensureAllowed(req, res, accountId, label);
    if (!allowed) return;

    const st = sessions.status({ accountId, label });
    if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

    try {
      // 1) WA contacts (no details)
      const all = await sessions.getContacts({ accountId, label, withDetails: false });
      const subset = filterStrictCUs(all);
      const digitsList = digitsForList(subset);

      // 2) Firestore snapshot for those digits
      const { contacts: contactsCol } = sessRefs(db, accountId, label);
      const existingByDigits = await snapshotExistingByDigits(db, contactsCol, digitsList);

      // 3) Upsert diffs (fill-only + refreshed flags)
      const { ops, appended, updated } =
        upsertContactsOps(subset, digitsList, existingByDigits, contactsCol);
      if (ops.length) await batchedSetOrUpdate(db, ops);

      // 4) Respond (no enrichment done)
      res.json({
        ok: true,
        count: subset.length,
        stats: buildStats(subset),
        persistence: {
          store: 'firestore',
          path: `/accounts/${accountId}/sessions/${label}/contacts/*`,
          appended, updated,
        },
      });
    } catch (e) {
      res.status(500).json({ error: 'contacts_failed', detail: String(e?.message || e) });
    }
  });

  /* ----------------------------------------------------------------
   * POST /contacts/lookup
   * Body: { accountId, label, numbers: string[], countryCode?: string }
   *
   * For each input number:
   *  - If it exists in Firestore and has a valid waId: ENRICH (fill-only).
   *  - Else, lookup via WA; if registered/found, UPSERT + ENRICH (fill-only).
   *    (Unregistered numbers are ignored by default.)
   * ---------------------------------------------------------------- */
  r.post('/contacts/lookup', requireUser, async (req, res) => {
    const { accountId, label, numbers, countryCode = null } = req.body || {};
    if (!accountId || !label || !Array.isArray(numbers) || !numbers.length) {
      return res.status(400).json({ error: 'accountId, label, numbers[] required' });
    }

    const allowed = await ensureAllowed(req, res, accountId, label);
    if (!allowed) return;

    const st = sessions.status({ accountId, label });
    if (st !== 'ready') return res.status(409).json({ error: 'session not ready', status: st || null });

    try {
      const normalized = numbers.map((x) => normalizeDigits(x)).filter(Boolean);
      const unique = Array.from(new Set(normalized));
      const { contacts: contactsCol } = sessRefs(db, accountId, label);

      // Load existing docs
      const docRefs = unique.map((d) => contactsCol.doc(docIdForDigits(d)));
      const snaps = docRefs.length ? await batchedGetAll(db, docRefs, 300) : [];
      const existingByDigits = new Map();
      for (let i = 0; i < snaps.length; i++) {
        const d = unique[i];
        const snap = snaps[i];
        if (d && snap?.exists) existingByDigits.set(d, snap.data());
      }

      // Partition numbers
      const toEnrich = []; // docs in DB with valid waId (registered-ish)
      const toLookup = []; // unknown or lacking waId
      for (const d of unique) {
        const ex = existingByDigits.get(d);
        if (ex?.id && waIdIsCUs(ex.id)) toEnrich.push({ id: ex.id, number: d, type: 'private' });
        else toLookup.push(d);
      }

      // Step A: Lookup unknowns in WA (with details)
      let lookupResults = [];
      if (toLookup.length) {
        lookupResults = await sessions.lookupContactsByNumbers({
          accountId, label, numbers: toLookup, countryCode, withDetails: true,
        });
      }

      // Prepare writes for lookup results (registered only)
      const ops = [];
      let appended = 0, updated = 0, registeredFound = 0;

      for (const r of lookupResults) {
        const digits = normalizeDigits(r.normalized || r.input);
        if (!digits) continue;

        if (r?.registered && r?.waId && waIdIsCUs(r.waId)) {
          registeredFound++;
          const ref = contactsCol.doc(docIdForDigits(digits));
          const existing = existingByDigits.get(digits);

          const payload = {
            id: r.waId,
            number: digits,
            name: r.contact?.name || existing?.name || null,
            pushname: r.contact?.pushname || existing?.pushname || null,
            shortName: r.contact?.shortName || existing?.shortName || null,
            isWAContact: true,
            isMyContact: !!r.contact?.isMyContact,
            isBusiness: !!r.contact?.isBusiness,
            isEnterprise: !!r.contact?.isEnterprise,
            hasChat: !!r.hasChat,
            registered: true,
            type: 'private',
            // details (may already include pic/about from lookup)
            profilePicUrl: r.contact?.profilePicUrl || null,
            about: r.contact?.about || null,
            updatedAt: FieldValue.serverTimestamp(),
          };

          if (!existing) {
            ops.push({ ref, set: { ...payload, createdAt: FieldValue.serverTimestamp() } });
            appended++;
            existingByDigits.set(digits, { ...payload }); // so enrich step can see it
            // Also add to toEnrich list in case details still missing
            toEnrich.push({ id: r.waId, number: digits, type: 'private' });
          } else {
            const baseDiff = diffFillOnly(existing, payload);
            for (const f of ALWAYS_UPDATE_FLAGS) {
              if (payload[f] !== undefined && payload[f] !== existing[f]) baseDiff[f] = payload[f];
            }
            if (payload.id && payload.id !== existing.id) baseDiff.id = payload.id;

            if (Object.keys(baseDiff).length) {
              ops.push({ ref, update: { ...baseDiff, updatedAt: FieldValue.serverTimestamp() } });
              updated++;
              existingByDigits.set(digits, { ...existing, ...baseDiff }); // refresh local view
            }
            // Ensure it’s considered for enrich if still missing details
            toEnrich.push({ id: payload.id, number: digits, type: 'private' });
          }
        }
        // if not registered, ignore (keeps collection clean)
      }

      if (ops.length) await batchedSetOrUpdate(db, ops);

      // Step B: Enrich the set we can (only if missing details)
      const needForEnrich = [];
      for (const c of toEnrich) {
        const ex = existingByDigits.get(c.number) || {};
        if (!ex?.id || !waIdIsCUs(ex.id)) continue; // safety
        const missingBoth = !ex.profilePicUrl && !ex.about;
        const missingOne = (!ex.profilePicUrl && c?.profilePicUrl !== null) || (!ex.about && c?.about !== null);
        if (missingBoth || missingOne) needForEnrich.push({ id: ex.id, number: c.number, type: 'private' });
      }

      let enrichUpdated = 0;
      if (needForEnrich.length) {
        const enriched = await sessions.enrichContactsSequential({ accountId, label, contacts: needForEnrich });
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

      res.json({
        ok: true,
        inputs: unique.length,
        foundInDb: unique.length - toLookup.length,
        lookedUp: toLookup.length,
        registeredFound,
        appended,
        updated,
        enrichUpdated,
      });
    } catch (e) {
      res.status(500).json({ error: 'lookup_failed', detail: String(e?.message || e) });
    }
  });

  return r;
}
