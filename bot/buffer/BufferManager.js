// bot/buffer/BufferManager.js
import { assembleTurn } from './TurnAssembler.js';
import { isFinalizer } from './MessageHints.js';

const VOICE_TYPES = new Set(['ptt', 'audio', 'voice']); // whatsapp-web.js uses 'ptt' for voice notes

export class BufferManager {
  constructor({ db, config, policy, mediaStore }) {
    this.db = db;
    this.cfg = config;
    this.policy = policy;
    this.mediaStore = mediaStore;
    this.map = new Map();
    this.gcTimer = null;
  }

  keyOf(meta) { return `${meta.accountId}::${meta.label}::${meta.chatId}`; }

  startGC() { if (!this.gcTimer) this.gcTimer = setInterval(() => this.gcSweep(), 60_000); }
  stopGC() { if (this.gcTimer) clearInterval(this.gcTimer); this.gcTimer = null; }

  gcSweep() {
    const now = Date.now();
    const idle = this.cfg.gcIdleMs || (30 * 60_000);
    for (const [k, st] of this.map.entries()) {
      if (!st || !st.lastAt) continue;
      if (now - st.lastAt > idle) {
        try { if (st.timer) clearTimeout(st.timer); } catch {}
        this.map.delete(k);
      }
    }
  }

  // evt from sessionManager: { id, accountId, sessionId(label), chatId, fromMe, body, messageType, hasMedia, waTimestamp, mediaUrlPath? }
  push(evt) {
    if (!evt || evt.fromMe) return; // never react to our own outbound

    const accountId = String(evt.accountId || '');
    const label = String(evt.sessionId || evt.label || '');
    const chatId = String(evt.chatId || '');
    if (!accountId || !label || !chatId) return;

    const senderWaId = chatId; // 1:1
    this.policy.allowProcess({ aid: accountId, label, chatId, senderWaId })
      .then((allow) => {
        if (!allow) return;
        return this._pushAllowed(evt, { accountId, label, chatId });
      })
      .catch((e) => console.error('[BufferManager] policy.allowProcess error', e));
  }

  async _pushAllowed(evt, ids) {
    const { accountId, label, chatId } = ids;

    const tsRaw = Number(evt.waTimestamp || Date.now());
    const ts = tsRaw < 10_000_000_000 ? tsRaw * 1000 : tsRaw;

    const k = this.keyOf({ accountId, label, chatId });
    const st = this.map.get(k) || { items: [], timer: null, openedAt: ts, lastAt: ts };

    // text
    const text = String(evt.body || '').trim();
    if (text) st.items.push({ ts, type: 'text', text });

    // voice note?
    if (evt.hasMedia && VOICE_TYPES.has(String(evt.messageType || '').toLowerCase())) {
      try {
        const saved = await this.mediaStore.saveInboundVoice({
          accountId, label, chatId,
          messageId: String(evt.id || ''),
          waTimestamp: evt.waTimestamp
        });
        st.items.push({
          ts,
          type: 'voice',
          gcsUri: saved.gcsUri,
          contentType: saved.contentType,
          filename: saved.filename
        });
      } catch (e) {
        console.error('[BufferManager] saveInboundVoice failed', e);
      }
    }

    st.lastAt = ts;
    if (!st.openedAt) st.openedAt = ts;

    if (st.timer) clearTimeout(st.timer);
    const wantsImmediate = text && isFinalizer(text, this.cfg.finalizerWords);
    const delay = wantsImmediate ? 0 : (this.cfg.debounceMs || 30_000);
    st.timer = setTimeout(() => this.flushKey(k), delay);

    this.map.set(k, st);
  }

  async flushKey(k) {
    const st = this.map.get(k);
    if (!st || !st.items.length) return;
    this.map.delete(k);

    try {
      const [accountId, label, chatId] = k.split('::');

      const turn = assembleTurn({
        items: st.items,
        meta: { accountId, label, chatId },
        finalizerWords: this.cfg.finalizerWords,
        explicitCfg: {
          voicePhrases: this.cfg.explicitVoicePhrases,
          textPhrases: this.cfg.explicitTextPhrases,
        },
      });

      const ref = this.cfg.paths.threadTurnDoc(this.db, {
        accountId, label, chatId, windowId: turn.windowId
      });

      await ref.set({
        status: 'pending',
        openedAt: turn.openedAt,
        closedAt: turn.closedAt,
        meta: turn.meta,
        hints: turn.hints,
        items: turn.items,
        response: null,
        processedAt: null,
        deliveredAt: null,
        waMessageId: null,
        error: null
      }, { merge: true });
      // AI worker flips pending → ready
    } catch (e) {
      console.error('[BufferManager.flushKey] write failed', k, e);
    }
  }
}
