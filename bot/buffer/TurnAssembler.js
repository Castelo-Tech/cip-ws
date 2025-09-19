// bot/buffer/TurnAssembler.js
// Assembles buffered items into a Turn doc payload (Phase 1: text-only).

import { explicitModality, guessLang } from './MessageHints.js';

export function assembleTurn({ items = [], meta = {}, finalizerWords = [], explicitCfg = {} }) {
  if (!Array.isArray(items) || items.length === 0) {
    throw new Error('assembleTurn: empty items');
  }

  // Ensure time ordering
  const ordered = [...items].sort((a, b) => (a.ts || 0) - (b.ts || 0));
  const openedAt = ordered[0].ts || Date.now();
  const closedAt = ordered[ordered.length - 1].ts || Date.now();

  // Merge tiny bursts into single lines, keep larger lines separate
  const SMALL = 14; // chars
  const merged = [];
  let buf = '';

  for (const it of ordered) {
    if (it.type !== 'text') continue; // Phase 1: text only
    const t = (it.text || '').trim();
    if (!t) continue;

    if (t.length <= SMALL) {
      buf = buf ? `${buf} ${t}` : t;
    } else {
      if (buf) { merged.push({ ts: it.ts, type: 'text', text: buf }); buf = ''; }
      merged.push({ ts: it.ts, type: 'text', text: t });
    }
  }
  if (buf) merged.push({ ts: closedAt, type: 'text', text: buf });

  // Build hints
  const lastText = merged.length ? merged[merged.length - 1].text : '';
  let explicit = null;
  for (const m of merged) {
    explicit = explicit || explicitModality(m.text, explicitCfg);
  }
  const lang = guessLang(merged.map(m => m.text).join(' ')) || 'es-MX';

  const hints = {
    lastInbound: 'text',
    explicit,   // "voice" | "text" | null
    lang
  };

  const windowId = `${meta.accountId}.${meta.label}.${meta.chatId}.${openedAt}`;

  return {
    windowId,
    openedAt,
    closedAt,
    meta: {
      accountId: meta.accountId,
      label: meta.label,
      chatId: meta.chatId,
      windowId
    },
    hints,
    items: merged // [{ ts, type:'text', text }]
  };
}
