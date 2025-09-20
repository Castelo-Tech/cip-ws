// config/storageConfig.js
// Resolves the bucket to use and path helpers.

const PROJECT_ID =
  process.env.GOOGLE_CLOUD_PROJECT ||
  process.env.GCLOUD_PROJECT ||
  process.env.GCP_PROJECT ||
  process.env.PROJECT_ID ||
  '';

const DEFAULT_BUCKET = process.env.FIREBASE_STORAGE_BUCKET ||
  (PROJECT_ID ? `${PROJECT_ID}.appspot.com` : '');

export default {
  bucket: DEFAULT_BUCKET, // if empty, admin.storage().bucket() fallback will be used
  paths: {
    inboundVoice({ accountId, label, chatId, ts, messageId, ext = 'ogg' }) {
      // path where we store inbound voice notes
      return `wa/${accountId}/${label}/inbound/${chatId}/${ts}/${messageId}.${ext}`;
    }
  }
};
