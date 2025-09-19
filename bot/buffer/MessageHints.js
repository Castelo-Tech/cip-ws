// bot/buffer/MessageHints.js
// Small helpers to detect finalizers and explicit modality hints.

export function isFinalizer(text, finalizerWords = []) {
  const s = String(text || '').toLowerCase();
  if (!s) return false;
  return finalizerWords.some(w => s.includes(w));
}

export function explicitModality(text, { voicePhrases = [], textPhrases = [] } = {}) {
  const s = String(text || '').toLowerCase();
  if (!s) return null;
  if (voicePhrases.some(p => s.includes(p))) return 'voice';
  if (textPhrases.some(p => s.includes(p))) return 'text';
  return null;
}

// Very lightweight language hint (Phase 1 only; you can replace later).
export function guessLang(text) {
  const s = String(text || '');
  if (!s) return null;
  // naive hint: presence of accented vowels suggests Spanish
  return /[áéíóúñ¿¡]/i.test(s) ? 'es-MX' : null;
}
