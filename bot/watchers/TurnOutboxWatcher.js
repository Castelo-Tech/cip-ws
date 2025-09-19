// bot/watchers/TurnOutboxWatcher.js
// Watches Firestore "turns" (status:"ready") per active session and sends replies via sessions.*
// Uses Node Admin Firestore SDK signature for onSnapshot.

export class TurnOutboxWatcherHub {
  constructor({ db, sessions, policy }) {
    this.db = db;
    this.sessions = sessions;
    this.policy = policy;
    this.watchers = new Map(); // key = `${aid}::${label}` -> { unsub }
  }

  async start() {
    // Kick off watchers for sessions already running & ready (best effort)
    try {
      const current =
        typeof this.sessions.listRunning === 'function'
          ? this.sessions.listRunning('') // '' => all accounts
          : [];
      for (const s of current) {
        if (s?.status === 'ready') {
          this._ensureWatcher(s.accountId, s.label);
        }
      }
    } catch (e) {
      console.error('[TurnOutboxWatcherHub.start] listRunning failed', e);
    }

    // Dynamically add/remove watchers as sessions change
    this.sessions.on('evt', (evt) => {
      if (!evt) return;
      const aid = String(evt.accountId || '');
      const label = String(evt.sessionId || evt.label || '');
      if (!aid || !label) return;

      if (evt.type === 'ready') {
        this._ensureWatcher(aid, label);
      } else if (
        evt.type === 'stopped' ||
        evt.type === 'disconnected' ||
        evt.type === 'destroyed'
      ) {
        this._dropWatcher(aid, label);
      }
    });

    console.log('[TurnOutboxWatcherHub] started');
  }

  _key(aid, label) {
    return `${aid}::${label}`;
  }

  _dropWatcher(aid, label) {
    const k = this._key(aid, label);
    const w = this.watchers.get(k);
    if (w?.unsub) {
      try { w.unsub(); } catch {}
    }
    this.watchers.delete(k);
    console.log('[TurnOutboxWatcherHub] watcher dropped', k);
  }

  _ensureWatcher(aid, label) {
    const k = this._key(aid, label);
    if (this.watchers.has(k)) return;

    // Collection group query across turns (filtered per session)
    const q = this.db
      .collectionGroup('turns')
      .where('meta.accountId', '==', aid)
      .where('meta.label', '==', label)
      .where('status', '==', 'ready');

    // Admin SDK signature: onSnapshot(onNext, onError?)
    const unsub = q.onSnapshot(
      async (snap) => {
        for (const change of snap.docChanges()) {
          if (change.type !== 'added' && change.type !== 'modified') continue;

          const data = change.doc.data() || {};
          if (data.status !== 'ready') continue;

          try {
            await this._processTurnDoc(change.doc.ref, data);
          } catch (e) {
            console.error('[TurnOutboxWatcherHub] processTurnDoc error', e);
          }
        }
      },
      (err) => {
        console.error('[TurnOutboxWatcherHub] snapshot error', err);
      }
    );

    this.watchers.set(k, { unsub });
    console.log('[TurnOutboxWatcherHub] watcher started', k);
  }

  async _processTurnDoc(ref, data) {
    // Claim atomically to avoid double-send
    const claimed = await this.db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      if (!snap.exists) return null;
      const cur = snap.data() || {};
      if (cur.status !== 'ready' || cur.waMessageId) return null;
      tx.update(ref, { status: 'sending', claimedAt: new Date() });
      return cur;
    });

    if (!claimed) return; // already handled or not in the right state

    const meta = claimed.meta || {};
    const response = claimed.response || {};
    const accountId = String(meta.accountId || '');
    const label = String(meta.label || '');
    const chatId = String(meta.chatId || '');
    if (!accountId || !label || !chatId) {
      await ref.update({
        status: 'error',
        error: { stage: 'validate', detail: 'missing meta.accountId/label/chatId' },
      });
      return;
    }

    // Double-check policy before sending (session/chat might have been toggled off)
    const allow = await this.policy.allowSend({ aid: accountId, label, chatId });
    if (!allow) {
      await ref.update({ status: 'skipped', skippedAt: new Date(), error: null });
      return;
    }

    try {
      let waMessageId = null;

      // Phase 1 supports text; if "voice" sneaks in, fall back to text content.
      const modality = String(response.modality || 'text');
      if (modality === 'text') {
        const text = String(response.text || '').trim();
        if (!text) throw new Error('empty text response');

        const msg = await this.sessions.sendText({
          accountId,
          label,
          to: chatId,
          text,
        });
        waMessageId = msg?.id?._serialized || null;
      } else {
        const fallback = String(response.text || '').trim() || 'Mensaje listo (voz no habilitada aún).';
        const msg = await this.sessions.sendText({
          accountId,
          label,
          to: chatId,
          text: fallback,
        });
        waMessageId = msg?.id?._serialized || null;
      }

      await ref.update({
        status: 'delivered',
        deliveredAt: new Date(),
        waMessageId,
        error: null,
      });
    } catch (e) {
      console.error('[TurnOutboxWatcherHub] send failed', e);
      await ref.update({
        status: 'error',
        error: { stage: 'send', detail: String(e?.message || e) },
      });
    }
  }
}
