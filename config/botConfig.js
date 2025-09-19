// config/botConfig.js
// Central knobs for the bot pipeline (Phase 1).

export default {
  // How long to wait after the *last* inbound message before flushing a turn.
  debounceMs: 30_000, // 30s

  // Garbage-collect per-chat buffers after this much inactivity.
  gcIdleMs: 30 * 60_000, // 30 minutes

  // Optional: cap a single window if user keeps typing forever (0 = disabled).
  hardCapMs: 0,

  // Heuristics for early flush (finalizer words trigger immediate flush).
  finalizerWords: [
    'gracias', 'saludos', 'listo', 'ya envié', 'ya envie', 'ok', 'va', 'perfecto', 'quedo atento'
  ],

  // Heuristics for explicit modality asks (Phase 1 only sets hints; Phase 3 will act).
  explicitVoicePhrases: [
    'en audio', 'nota de voz', 'mándame audio', 'mandame audio', 'por audio', 'en voz'
  ],
  explicitTextPhrases: [
    'en texto', 'por texto', 'escríbeme', 'escribeme', 'escrito'
  ],

  // Firestore path builder (do not change the structure; tools rely on it).
  paths: {
    threadTurnDoc(db, { accountId, label, chatId, windowId }) {
      return db
        .collection('accounts').doc(accountId)
        .collection('sessions').doc(label)
        .collection('threads').doc(chatId)
        .collection('turns').doc(windowId);
    }
  }
};
