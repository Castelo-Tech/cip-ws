// Contacts & lookups feature (pure logic). Injects a ready-client getter.
import { sleep, rand, buildRawNumber } from './utils.js';

export function makeContacts({ getReadyClient }) {
  // Get all contacts (optionally enrich with profilePicUrl + about + hasChat)
  async function getContacts({ accountId, label, withDetails = false }) {
    const client = await getReadyClient({ accountId, label });

    let chatIdSet = new Set();
    try {
      const chats = await client.getChats();
      chatIdSet = new Set(chats.map((c) => c?.id?._serialized).filter(Boolean));
    } catch {}

    const contacts = await client.getContacts();
    const out = [];

    let i = 0;
    for (const c of contacts) {
      const id = c?.id?._serialized || null;
      const base = {
        id,
        number: c?.number || null,
        name: c?.name || null,
        pushname: c?.pushname || null,
        shortName: c?.shortName || null,
        isWAContact: !!c?.isWAContact,
        isMyContact: !!c?.isMyContact,
        isBusiness: !!c?.isBusiness,
        isEnterprise: !!c?.isEnterprise,
        hasChat: id ? chatIdSet.has(id) : false,
        type: c?.isGroup ? 'group' : 'private',
      };

      if (withDetails && id && !c?.isGroup) {
        // light, staggered enrichment
        await sleep(rand(80, 220));
        if ((i++ % 7) === 0) await sleep(rand(600, 1500));

        let profilePicUrl = null;
        let about = null;
        try { profilePicUrl = await c.getProfilePicUrl(); } catch {}
        try { about = await c.getAbout(); } catch {}

        out.push({ ...base, profilePicUrl: profilePicUrl || null, about });
      } else {
        out.push(base);
      }
    }

    return out;
  }

  // Lookup list of numbers: on-WhatsApp? then optional enrichment; also marks hasChat
  async function lookupContactsByNumbers({ accountId, label, numbers = [], countryCode = null, withDetails = true }) {
    const client = await getReadyClient({ accountId, label });

    let chatIdSet = new Set();
    try {
      const chats = await client.getChats();
      chatIdSet = new Set(chats.map((c) => c?.id?._serialized).filter(Boolean));
    } catch {}

    const results = [];
    let i = 0;

    for (const raw of numbers) {
      // mild staggering for batches
      await sleep(rand(120, 420));
      if ((i++ % 9) === 0) await sleep(rand(800, 1800));

      const normalized = buildRawNumber(raw, countryCode);
      if (!normalized) {
        results.push({ input: String(raw), normalized: null, registered: false, waId: null, hasChat: false, contact: null });
        continue;
      }

      let numId = null;
      try {
        numId = await client.getNumberId(normalized); // null if not registered
      } catch {
        numId = null;
      }

      if (!numId || (!numId._serialized && !numId.serialized)) {
        results.push({ input: String(raw), normalized, registered: false, waId: null, hasChat: false, contact: null });
        continue;
      }

      const waId = numId._serialized || numId.serialized; // e.g. "521...@c.us"
      const hasChat = chatIdSet.has(waId);

      let contactPayload = null;
      if (withDetails) {
        try {
          const contact = await client.getContactById(waId);
          let profilePicUrl = null;
          let about = null;
          try { profilePicUrl = await contact.getProfilePicUrl(); } catch {}
          try { about = await contact.getAbout(); } catch {}

          contactPayload = {
            id: contact?.id?._serialized || waId,
            number: contact?.number || normalized,
            name: contact?.name || null,
            pushname: contact?.pushname || null,
            shortName: contact?.shortName || null,
            isWAContact: !!contact?.isWAContact,
            isMyContact: !!contact?.isMyContact,
            isBusiness: !!contact?.isBusiness,
            isEnterprise: !!contact?.isEnterprise,
            profilePicUrl: profilePicUrl || null,
            about,
          };
        } catch {
          contactPayload = { id: waId };
        }
      }

      results.push({
        input: String(raw),
        normalized,
        registered: true,
        waId,
        hasChat,
        contact: contactPayload,
      });
    }

    return results;
  }

  // Single-number check (fast path, minimal delay) — ideal for on-demand UI clicks
  async function checkContactByNumber({ accountId, label, number, countryCode = null, withDetails = false }) {
    const client = await getReadyClient({ accountId, label });

    const normalized = buildRawNumber(number, countryCode);
    if (!normalized) {
      return { input: String(number), normalized: null, registered: false, waId: null, hasChat: false, contact: null };
    }

    let numId = null;
    try { numId = await client.getNumberId(normalized); } catch { numId = null; }

    if (!numId || (!numId._serialized && !numId.serialized)) {
      return { input: String(number), normalized, registered: false, waId: null, hasChat: false, contact: null };
    }

    const waId = numId._serialized || numId.serialized;

    let hasChat = false;
    try {
      const chats = await client.getChats();
      const set = new Set(chats.map((c) => c?.id?._serialized).filter(Boolean));
      hasChat = set.has(waId);
    } catch {}

    let contactPayload = null;
    if (withDetails) {
      try {
        const contact = await client.getContactById(waId);
        let profilePicUrl = null;
        let about = null;
        try { profilePicUrl = await contact.getProfilePicUrl(); } catch {}
        try { about = await contact.getAbout(); } catch {}
        contactPayload = {
          id: contact?.id?._serialized || waId,
          number: contact?.number || normalized,
          name: contact?.name || null,
          pushname: contact?.pushname || null,
          shortName: contact?.shortName || null,
          isWAContact: !!contact?.isWAContact,
          isMyContact: !!contact?.isMyContact,
          isBusiness: !!contact?.isBusiness,
          isEnterprise: !!contact?.isEnterprise,
          profilePicUrl: profilePicUrl || null,
          about,
        };
      } catch {
        contactPayload = { id: waId };
      }
    }

    return {
      input: String(number),
      normalized,
      registered: true,
      waId,
      hasChat,
      contact: contactPayload,
    };
  }

  return { getContacts, lookupContactsByNumbers, checkContactByNumber };
}
