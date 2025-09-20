// bot/buffer/TurnAssembler.js
// Assemble buffered items (text + voice) into a Turn.

import { explicitModality, guessLang } from './MessageHints.js';

export function assembleTurn({ items = [], meta = {}, finalizerWords = [], explicitCfg = {} }) {
  if (!Array.isArray(items) || items.length === 0) throw new Error('assembleTurn: empty items');

  const ordered = [...items].sort((a, b) => (a.ts || 0) - (b.ts || 0));
  const openedAt = ordered[0].ts || Date.now();
  const closedAt = ordered[ordered.length - 1].ts || Date.now();

  // merge short text bursts; keep non-text (voice) items in order
  const SMALL = 14;
  const out = [];
  let buf = '';

  for (const it of ordered) {
    if (it.type === 'text') {
      const t = (it.text || '').trim();
      if (!t) continue;
      if (t.length <= SMALL) {
        buf = buf ? `${buf} ${t}` : t;
      } else {
        if (buf) { out.push({ ts: it.ts, type: 'text', text: buf }); buf = ''; }
        out.push({ ts: it.ts, type: 'text', text: t });
      }
    } else {
      // flush pending text before pushing non-text
      if (buf) { out.push({ ts: it.ts, type: 'text', text: buf }); buf = ''; }
      // pass-through voice/audio items with their metadata
      out.push(it);
    }
  }
  if (buf) out.push({ ts: closedAt, type: 'text', text: buf });

  // hints
  const textsOnly = out.filter(i => i.type === 'text').map(i => i.text).join(' ');
  let explicit = null;
  for (const m of out) {
    if (m.type === 'text') explicit = explicit || explicitModality(m.text, explicitCfg);
  }
  const lastInbound = out.length ? out[out.length - 1].type : 'text';
  const lang = guessLang(textsOnly) || 'es-MX';

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
    hints: { lastInbound, explicit, lang },
    items: out // now includes text + voice entries
  };
}
