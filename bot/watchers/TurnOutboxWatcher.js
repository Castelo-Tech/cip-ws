// bot/watchers/TurnOutboxWatcher.js
// Watches Turn docs (status:"ready") per active session and sends replies via sessions.*
// Uses a collectionGroup('turns') query filtered by accountId, label, status.
// The first time, Firestore may ask you to create the composite index (follow the console link).

export class TurnOutboxWatcherHub {
  constructor({ db, sessions }) {
    this.db = db;
    this.sessions = sessions;
    this.watchers = new Map(); // key = aid::label -> { unsub }
  }

  async start() {
    // Start watchers for sessions that are already running
    const current = (typeof this.sessions.listRunning === 'function')
      ? this.sessions.listRunning('') // empty => all accounts
      : [];

    for (const s of current) {
      if (s.status === 'ready') {
        this._ensureWatcher(s.accountId, s.label);
      }
    }

    // Dynamically add/remove watchers as sessions change
    this.sessions.on('evt', (evt) => {
      if (!evt) return;
      const { accountId, sessionId: label } = evt;
      if (!accountId || !label) return;

      if (evt.type === 'ready') {
        this._ensureWatcher(accountId, label);
      }
      if (evt.type === 'stopped' || evt.type === 'disconnected' || evt.type === 'destroyed') {
        this._dropWatcher(accountId, label);
      }
    });

    console.log('[TurnOutboxWatcherHub] started');
  }

  _key(aid, label) { return `${aid}::${label}`; }

  _dropWatcher(aid, label) {
    const k = this._key(aid, label);
    const w = this.watchers.get(k);
    if (w?.unsub) { try { w.unsub(); } catch {} }
    this.watchers.delete(k);
    console.log('[TurnOutboxWatcherHub] watcher dropped', k);
  }

  _ensureWatcher(aid, label) {
    const k = this._key(aid, label);
    if (this.watchers.has(k)) return;

    // Use a collection group query; equality filters across (meta.accountId, meta.label, status)
    const q = this.db.collectionGroup('turns')
      .where('meta.accountId', '==', aid)
      .where('meta.label', '==', label)
      .where('status', '==', 'ready');

    const unsub = q.onSnapshot({
      includeMetadataChanges: false
    }, async (snap) => {
      for (const doc of snap.docChanges()) {
        if (doc.type === 'added' || doc.type === 'modified') {
          const d = doc.doc.data() || {};
          if (d.status !== 'ready') continue;
          try {
            await this._processTurnDoc(doc.doc.ref, d);
          } catch (e) {
            console.error('[TurnOutboxWatcherHub] processTurnDoc error', e);
          }
        }
      }
    }, (err) => {
      console.error('[TurnOutboxWatcherHub] snapshot error', err);
    });

    this.watchers.set(k, { unsub });
    console.log('[TurnOutboxWatcherHub] watcher started', k);
  }

  async _processTurnDoc(ref, data) {
    // Claim the doc atomically: status ready -> sending
    const claimed = await this.db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      if (!snap.exists) return null;
      const cur = snap.data() || {};
      if (cur.status !== 'ready' || cur.waMessageId) return null;
      tx.update(ref, { status: 'sending', claimedAt: new Date() });
      return cur;
    });

    if (!claimed) return; // already handled or not ready

    const meta = claimed.meta || {};
    const response = claimed.response || {};
    const accountId = String(meta.accountId || '');
    const label = String(meta.label || '');
    const chatId = String(meta.chatId || '');
    if (!accountId || !label || !chatId) {
      await ref.update({ status: 'error', error: { stage: 'validate', detail: 'missing meta' } });
      return;
    }

    try {
      let waMessageId = null;

      if ((response.modality || 'text') === 'text') {
        const text = String(response.text || '').trim();
        if (!text) throw new Error('empty text response');

        const msg = await this.sessions.sendText({
          accountId, label, to: chatId, text
        });

        waMessageId = msg?.id?._serialized || null;
      } else {
        // Phase 1: no voice/media delivery. If a "voice" ready sneaks in, fall back to text.
        const fallback = String(response.text || '').trim() || 'Mensaje listo (voz no habilitada aún).';
        const msg = await this.sessions.sendText({
          accountId, label, to: chatId, text: fallback
        });
        waMessageId = msg?.id?._serialized || null;
      }

      await ref.update({
        status: 'delivered',
        deliveredAt: new Date(),
        waMessageId
      });
    } catch (e) {
      console.error('[TurnOutboxWatcherHub] send failed', e);
      await ref.update({
        status: 'error',
        error: { stage: 'send', detail: String(e?.message || e) }
      });
    }
  }
}
