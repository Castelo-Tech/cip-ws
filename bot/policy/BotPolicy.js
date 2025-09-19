// bot/policy/BotPolicy.js
// Lightweight policy reader with a 60s TTL cache.
// - Session-level toggle & lists: /accounts/{aid}/sessions/{label} { bot: {...} }
// - Per-chat override (optional):
//     EITHER  /accounts/{aid}/sessions/{label}/threads/{chatId}              (fields on thread doc)
//     OR      /accounts/{aid}/sessions/{label}/threads/{chatId}/settings/__root__ (doc in subcollection)

export class BotPolicy {
  constructor({ db, ttlMs = 60_000 }) {
    this.db = db;
    this.ttlMs = ttlMs;
    this._sessionCache = new Map(); // `${aid}::${label}` -> { at, data }
    this._chatCache = new Map();    // `${aid}::${label}::${chatId}` -> { at, data }
    this._selfIds = new Map();      // aid -> { at, waIds:Set<string> }
  }

  _now() { return Date.now(); }
  _fresh(entry) { return !!entry && (this._now() - entry.at) < this.ttlMs; }
  _sessKey(aid, label) { return `${aid}::${label}`; }
  _chatKey(aid, label, chatId) { return `${aid}::${label}::${chatId}`; }

  async _getSession(aid, label) {
    const k = this._sessKey(aid, label);
    const cached = this._sessionCache.get(k);
    if (this._fresh(cached)) return cached.data;

    const snap = await this.db.collection('accounts').doc(aid)
      .collection('sessions').doc(label).get();

    const raw = snap.exists ? (snap.data() || {}) : {};
    const bot = raw.bot || {};
    const data = {
      enabled: bot.enabled !== false,                 // default ON
      receiveFromBots: bot.receiveFromBots === true,  // default OFF
      mode: bot.mode || 'all',                        // "all" | "allowlist" | "blocklist"
      allowlist: Array.isArray(bot.allowlist) ? bot.allowlist.map(String) : [],
      blocklist: Array.isArray(bot.blocklist) ? bot.blocklist.map(String) : [],
    };

    this._sessionCache.set(k, { at: this._now(), data });
    return data;
  }

  async _getChat(aid, label, chatId) {
    const k = this._chatKey(aid, label, chatId);
    const cached = this._chatCache.get(k);
    if (this._fresh(cached)) return cached.data;

    const threads = this.db.collection('accounts').doc(aid)
      .collection('sessions').doc(label).collection('threads').doc(chatId);

    // Preferred: subcollection doc /threads/{chatId}/settings/__root__
    let data = {};
    try {
      const rootDoc = await threads.collection('settings').doc('__root__').get();
      if (rootDoc.exists) {
        const raw = rootDoc.data() || {};
        data = {
          botEnabled: typeof raw.botEnabled === 'boolean' ? raw.botEnabled : null,
          preferredModality: raw.preferredModality || null,
        };
      } else {
        // Fallback: fields on the thread doc itself
        const threadDoc = await threads.get();
        if (threadDoc.exists) {
          const raw = threadDoc.data() || {};
          data = {
            botEnabled: typeof raw.botEnabled === 'boolean' ? raw.botEnabled : null,
            preferredModality: raw.preferredModality || null,
          };
        }
      }
    } catch {
      data = {};
    }

    this._chatCache.set(k, { at: this._now(), data });
    return data;
  }

  async _getSelfWaIds(aid) {
    const cached = this._selfIds.get(aid);
    if (this._fresh(cached)) return cached.waIds;

    const col = await this.db.collection('accounts').doc(aid).collection('sessions').get();
    const waIds = new Set(col.docs.map(d => String(d.get('waId') || '')).filter(Boolean));
    this._selfIds.set(aid, { at: this._now(), waIds });
    return waIds;
  }

  // Should we process this inbound message into the buffer?
  async allowProcess({ aid, label, chatId, senderWaId }) {
    const sess = await this._getSession(aid, label);
    if (!sess.enabled) return false;

    // Loop guard: if sender is one of *our* WA IDs and receiveFromBots=false → skip
    if (!sess.receiveFromBots && senderWaId) {
      const selfIds = await this._getSelfWaIds(aid);
      if (selfIds.has(senderWaId)) return false;
    }

    // Session allow/block modes
    if (sess.mode === 'allowlist' && sess.allowlist.length && !sess.allowlist.includes(chatId)) {
      return false;
    }
    if (sess.mode === 'blocklist' && sess.blocklist.length && sess.blocklist.includes(chatId)) {
      return false;
    }

    // Per-chat override
    const chat = await this._getChat(aid, label, chatId);
    if (chat.botEnabled === false) return false;

    return true;
  }

  // Should we send a ready response now?
  async allowSend({ aid, label, chatId }) {
    const sess = await this._getSession(aid, label);
    if (!sess.enabled) return false;

    if (sess.mode === 'allowlist' && sess.allowlist.length && !sess.allowlist.includes(chatId)) {
      return false;
    }
    if (sess.mode === 'blocklist' && sess.blocklist.length && sess.blocklist.includes(chatId)) {
      return false;
    }

    const chat = await this._getChat(aid, label, chatId);
    if (chat.botEnabled === false) return false;

    return true;
  }
}
