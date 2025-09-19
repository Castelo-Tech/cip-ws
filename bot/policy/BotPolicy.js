// bot/policy/BotPolicy.js
// Simple policy with 60s cache:
// - Session-level toggle: /accounts/{aid}/sessions/{label}.bot.enabled (default true)
// - Per-chat opt-out (optional): either thread doc field or settings/__root__ doc
// - Skip if inbound sender equals this session's own waId

export class BotPolicy {
  constructor({ db, ttlMs = 60_000 }) {
    this.db = db;
    this.ttlMs = ttlMs;
    this._sessionCache = new Map(); // `${aid}::${label}` -> { at, data }
    this._chatCache = new Map();    // `${aid}::${label}::${chatId}` -> { at, data }
  }

  _now() { return Date.now(); }
  _fresh(entry) { return !!entry && (this._now() - entry.at) < this.ttlMs; }
  _sessKey(aid, label) { return `${aid}::${label}`; }
  _chatKey(aid, label, chatId) { return `${aid}::${label}::${chatId}`; }

  async _getSession(aid, label) {
    const k = this._sessKey(aid, label);
    const cached = this._sessionCache.get(k);
    if (this._fresh(cached)) return cached.data;

    const snap = await this.db
      .collection('accounts').doc(aid)
      .collection('sessions').doc(label)
      .get();

    const raw = snap.exists ? (snap.data() || {}) : {};
    const bot = raw.bot || {};
    const data = {
      enabled: bot.enabled !== false,           // default ON
      selfWaId: raw.waId || null                // session's own number (e.g. "52...@c.us")
    };

    this._sessionCache.set(k, { at: this._now(), data });
    return data;
  }

  async _getChat(aid, label, chatId) {
    const k = this._chatKey(aid, label, chatId);
    const cached = this._chatCache.get(k);
    if (this._fresh(cached)) return cached.data;

    const threads = this.db
      .collection('accounts').doc(aid)
      .collection('sessions').doc(label)
      .collection('threads').doc(chatId);

    let data = {};
    try {
      // Preferred: /threads/{chatId}/settings/__root__
      const rootDoc = await threads.collection('settings').doc('__root__').get();
      if (rootDoc.exists) {
        const raw = rootDoc.data() || {};
        data = {
          botEnabled: typeof raw.botEnabled === 'boolean' ? raw.botEnabled : null
        };
      } else {
        // Fallback: fields on thread doc
        const threadDoc = await threads.get();
        if (threadDoc.exists) {
          const raw = threadDoc.data() || {};
          data = {
            botEnabled: typeof raw.botEnabled === 'boolean' ? raw.botEnabled : null
          };
        }
      }
    } catch {
      data = {};
    }

    this._chatCache.set(k, { at: this._now(), data });
    return data;
  }

  // Should we process this inbound message into the buffer?
  async allowProcess({ aid, label, chatId, senderWaId }) {
    const sess = await this._getSession(aid, label);
    if (!sess.enabled) return false;

    // If sender is the *same number* as this session, never trigger.
    if (sess.selfWaId && senderWaId && sess.selfWaId === senderWaId) return false;

    const chat = await this._getChat(aid, label, chatId);
    if (chat.botEnabled === false) return false;

    return true;
  }

  // Should we send a ready response now? (in case toggled off mid-flight)
  async allowSend({ aid, label, chatId }) {
    const sess = await this._getSession(aid, label);
    if (!sess.enabled) return false;

    const chat = await this._getChat(aid, label, chatId);
    if (chat.botEnabled === false) return false;

    return true;
  }
}
