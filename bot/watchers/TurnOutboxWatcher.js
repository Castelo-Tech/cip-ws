// bot/watchers/TurnOutboxWatcher.js
export class TurnOutboxWatcherHub {
  constructor({ db, sessions, policy }) {
    this.db = db;
    this.sessions = sessions;
    this.policy = policy;
    this.watchers = new Map();
  }

  async start() {
    try {
      const current = typeof this.sessions.listRunning === 'function'
        ? this.sessions.listRunning('')
        : [];
      for (const s of current) {
        if (s?.status === 'ready') this._ensureWatcher(s.accountId, s.label);
      }
    } catch (e) { console.error('[TurnOutboxWatcherHub.start] listRunning failed', e); }

    this.sessions.on('evt', (evt) => {
      if (!evt) return;
      const aid = String(evt.accountId || '');
      const label = String(evt.sessionId || evt.label || '');
      if (!aid || !label) return;
      if (evt.type === 'ready') this._ensureWatcher(aid, label);
      else if (['stopped','disconnected','destroyed'].includes(evt.type)) this._dropWatcher(aid, label);
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

    const q = this.db.collectionGroup('turns')
      .where('meta.accountId', '==', aid)
      .where('meta.label', '==', label)
      .where('status', '==', 'ready');

    const unsub = q.onSnapshot(
      async (snap) => {
        for (const change of snap.docChanges()) {
          if (!['added','modified'].includes(change.type)) continue;
          const data = change.doc.data() || {};
          if (data.status !== 'ready') continue;
          try { await this._processTurnDoc(change.doc.ref, data); }
          catch (e) { console.error('[TurnOutboxWatcherHub] processTurnDoc error', e); }
        }
      },
      (err) => { console.error('[TurnOutboxWatcherHub] snapshot error', err); }
    );

    this.watchers.set(k, { unsub });
    console.log('[TurnOutboxWatcherHub] watcher started', k);
  }

  async _processTurnDoc(ref, data) {
    const claimed = await this.db.runTransaction(async (tx) => {
      const snap = await tx.get(ref);
      if (!snap.exists) return null;
      const cur = snap.data() || {};
      if (cur.status !== 'ready' || cur.waMessageId) return null;
      tx.update(ref, { status: 'sending', claimedAt: new Date() });
      return cur;
    });
    if (!claimed) return;

    const meta = claimed.meta || {};
    const response = claimed.response || {};
    const accountId = String(meta.accountId || '');
    const label = String(meta.label || '');
    const chatId = String(meta.chatId || '');
    if (!accountId || !label || !chatId) {
      await ref.update({ status: 'error', error: { stage: 'validate', detail: 'missing meta.accountId/label/chatId' } });
      return;
    }

    const allow = await this.policy.allowSend({ aid: accountId, label, chatId });
    if (!allow) { await ref.update({ status: 'skipped', skippedAt: new Date(), error: null }); return; }

    try {
      let waMessageId = null;
      const modality = String(response.modality || 'text');

      if (modality === 'voice' && response.audio?.url) {
        // send audio, caption = text (if any)
        const media = { url: response.audio.url };
        const options = { caption: (response.text || '').trim(), sendAudioAsVoice: true };
        const msg = await this.sessions.sendMedia({ accountId, label, to: chatId, media, options });
        waMessageId = msg?.id?._serialized || null;
      } else {
        const text = String(response.text || '').trim() || 'Mensaje listo.';
        const msg = await this.sessions.sendText({ accountId, label, to: chatId, text });
        waMessageId = msg?.id?._serialized || null;
      }

      await ref.update({ status: 'delivered', deliveredAt: new Date(), waMessageId, error: null });
    } catch (e) {
      console.error('[TurnOutboxWatcherHub] send failed', e);
      await ref.update({ status: 'error', error: { stage: 'send', detail: String(e?.message || e) } });
    }
  }
}
