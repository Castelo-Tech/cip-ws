// media/MediaStore.js
// Downloads WhatsApp media via sessionManager and uploads to Cloud Storage.
// Returns a GCS URI (plus a few details) you can store on the Turn item.

import admin from 'firebase-admin';
import storageCfg from '../config/storageConfig.js';

export class MediaStore {
  constructor({ sessions }) {
    this.sessions = sessions;
    // Use the hardcoded bucket from storageConfig.js
    this.bucket = storageCfg.bucket
      ? admin.storage().bucket(storageCfg.bucket)
      : admin.storage().bucket(); // fallback to default if ever needed
  }

  _extFromMime(m) {
    if (!m) return 'bin';
    if (m.includes('ogg')) return 'ogg';
    if (m.includes('mpeg')) return 'mp3';
    if (m.includes('wav')) return 'wav';
    if (m.includes('mp4')) return 'mp4';
    return 'bin';
  }

  /**
   * Save an inbound voice message to Cloud Storage.
   * Requires the message to still be in the sessionManager's in-memory media cache.
   */
  async saveInboundVoice({ accountId, label, chatId, messageId, waTimestamp }) {
    const media = await this.sessions.downloadMessageMedia({ accountId, label, messageId });
    if (!media || !media.dataB64) throw new Error('no media available (cache expired?)');

    const buffer = Buffer.from(media.dataB64, 'base64');
    const mimetype = media.mimetype || 'application/octet-stream';
    const ext = this._extFromMime(mimetype);

    // Convert WA timestamp (seconds) to ms if needed
    const ts = Number(waTimestamp) || Date.now();
    const tsMs = ts < 10_000_000_000 ? ts * 1000 : ts;

    const objectPath = storageCfg.paths.inboundVoice({
      accountId, label, chatId, ts: tsMs, messageId, ext,
    });

    const file = this.bucket.file(objectPath);
    await file.save(buffer, {
      contentType: mimetype,
      resumable: false,
      metadata: { cacheControl: 'public, max-age=31536000' },
    });

    return {
      gcsUri: `gs://${this.bucket.name}/${objectPath}`,
      contentType: mimetype,
      filename: media.filename || `voice.${ext}`,
      bytes: buffer.length,
    };
  }
}
