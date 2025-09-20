// media/MediaStore.js
// Downloads WA media via sessionManager and uploads to Cloud Storage.
// Returns a GCS URI (+ metadata) you can put into the Turn items.

import admin from 'firebase-admin';
import storageCfg from '../config/storageConfig.js';

export class MediaStore {
  constructor({ sessions }) {
    this.sessions = sessions;
    this.bucket = storageCfg.bucket
      ? admin.storage().bucket(storageCfg.bucket)
      : admin.storage().bucket(); // default app bucket
  }

  _extFromMime(m) {
    if (!m) return 'bin';
    if (m.includes('ogg')) return 'ogg';
    if (m.includes('mpeg')) return 'mp3';
    if (m.includes('wav')) return 'wav';
    if (m.includes('mp4')) return 'mp4';
    return 'bin';
  }

  async saveInboundVoice({ accountId, label, chatId, messageId, waTimestamp }) {
    // 1) pull from WhatsApp (needs the live message cache)
    const media = await this.sessions.downloadMessageMedia({ accountId, label, messageId });
    if (!media || !media.dataB64) throw new Error('no media available (cache expired?)');

    const buffer = Buffer.from(media.dataB64, 'base64');
    const mimetype = media.mimetype || 'application/octet-stream';
    const ext = this._extFromMime(mimetype);

    const tsMs = (Number(waTimestamp) || Date.now()) * (waTimestamp < 10_000_000_000 ? 1000 : 1);
    const objectPath = storageCfg.paths.inboundVoice({
      accountId, label, chatId, ts: tsMs, messageId, ext
    });

    // 2) upload to GCS
    const file = this.bucket.file(objectPath);
    await file.save(buffer, {
      contentType: mimetype,
      resumable: false,
      metadata: { cacheControl: 'public, max-age=31536000' }
    });

    return {
      gcsUri: `gs://${this.bucket.name}/${objectPath}`,
      contentType: mimetype,
      filename: media.filename || `voice.${ext}`,
      bytes: buffer.length
    };
  }
}
