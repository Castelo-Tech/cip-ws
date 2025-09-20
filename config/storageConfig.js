// config/storageConfig.js
// Hardcoded bucket + path helpers for media persisted by the VM.

const BUCKET_NAME = 'castelo-insure-platform.firebasestorage.app';

export default {
  // We’ll always use this bucket; no env fallback.
  bucket: BUCKET_NAME,

  paths: {
    inboundVoice({ accountId, label, chatId, ts, messageId, ext = 'ogg' }) {
      // Where inbound WhatsApp voice notes are stored
      return `wa/${accountId}/${label}/inbound/${chatId}/${ts}/${messageId}.${ext}`;
    },
  },
};
