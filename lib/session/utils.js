// Shared helpers used across session features.

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export const rand = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;

/**
 * Normalize a recipient into a WhatsApp chat id.
 * - If it already contains "@", return as-is (supports c.us and g.us).
 * - Else, strip non-digits and append "@c.us".
 */
export function normalizeChatId(to) {
  const s = String(to || '').trim();
  if (!s) return null;
  if (s.includes('@')) return s;
  const digits = s.replace(/[^\d]/g, '');
  return digits ? `${digits}@c.us` : null;
}

/**
 * Build a best-effort numeric string from input and optional explicit country code.
 * - If countryCode is provided and not present in the digits, prefix it.
 * - Returns null if no digits at all.
 */
export function buildRawNumber(input, countryCode) {
  const s = String(input || '').trim();
  const digits = s.replace(/[^\d]/g, '');
  if (!digits) return null;

  if (countryCode) {
    const cc = String(countryCode).replace(/[^\d]/g, '');
    if (cc && !digits.startsWith(cc)) {
      return `${cc}${digits}`;
    }
  }
  return digits;
}
