// Chats feature (pure logic). Injects a ready-client getter.
import { sleep, rand, buildRawNumber } from './utils.js';

export function makeChats({ getReadyClient }) {
  // Lightweight list of chats (no messages)
  async function getChats({ accountId, label }) {
    const client = await getReadyClient({ accountId, label });
    const chats = await client.getChats();
    return chats
      .map((c) => ({
        id: c?.id?._serialized || null,
        name: c?.name || null,
        isGroup: !!c?.isGroup,
        unreadCount: typeof c?.unreadCount === 'number' ? c.unreadCount : null,
        archived: !!c?.archived,
        pinned: !!c?.pinned,
        isReadOnly: !!c?.isReadOnly,
      }))
      .filter((x) => x.id);
  }

  // Find chat by phone number; returns registration + whether a chat exists + basic chat meta if found
  async function getChatByNumber({ accountId, label, number, countryCode = null }) {
    const client = await getReadyClient({ accountId, label });

    const normalized = buildRawNumber(number, countryCode);
    if (!normalized) {
      return { input: String(number), normalized: null, registered: false, waId: null, exists: false, chat: null };
    }

    let numId = null;
    try { numId = await client.getNumberId(normalized); } catch { numId = null; }

    if (!numId || (!numId._serialized && !numId.serialized)) {
      return { input: String(number), normalized, registered: false, waId: null, exists: false, chat: null };
    }

    const waId = numId._serialized || numId.serialized;

    // Try quickly via getChatById, fall back to scanning list
    let chat = null;
    try { chat = await client.getChatById(waId); } catch {}
    if (!chat) {
      try {
        const list = await client.getChats();
        chat = list.find((c) => c?.id?._serialized === waId) || null;
      } catch {}
    }

    if (!chat) {
      return { input: String(number), normalized, registered: true, waId, exists: false, chat: null };
    }

    const payload = {
      id: chat?.id?._serialized || waId,
      name: chat?.name || null,
      isGroup: !!chat?.isGroup,
      unreadCount: typeof chat?.unreadCount === 'number' ? chat.unreadCount : null,
      archived: !!chat?.archived,
      pinned: !!chat?.pinned,
      isReadOnly: !!chat?.isReadOnly,
    };

    return { input: String(number), normalized, registered: true, waId, exists: true, chat: payload };
  }

  // Bulk chats by numbers, optional last N messages
  async function getChatsByNumbers({ accountId, label, numbers = [], countryCode = null, withMessages = false, messagesLimit = 20 }) {
    const client = await getReadyClient({ accountId, label });

    // Preload chats to avoid repeated RPCs
    let chatsList = [];
    try { chatsList = await client.getChats(); } catch {}
    const chatMap = new Map(chatsList.map((c) => [c?.id?._serialized, c]).filter(([k]) => !!k));

    const out = [];
    let i = 0;

    for (const raw of numbers) {
      // light stagger to avoid bursty patterns if callers paste long lists
      if (numbers.length > 3) {
        await sleep(rand(60, 160));
        if ((i++ % 12) === 0) await sleep(rand(400, 900));
      }

      const normalized = buildRawNumber(raw, countryCode);
      if (!normalized) {
        out.push({ input: String(raw), normalized: null, registered: false, waId: null, exists: false, chat: null, messages: [] });
        continue;
      }

      let numId = null;
      try { numId = await client.getNumberId(normalized); } catch { numId = null; }

      if (!numId || (!numId._serialized && !numId.serialized)) {
        out.push({ input: String(raw), normalized, registered: false, waId: null, exists: false, chat: null, messages: [] });
        continue;
      }

      const waId = numId._serialized || numId.serialized;

      // Prefer direct fetch; otherwise look up in cached list
      let chat = null;
      try { chat = await client.getChatById(waId); } catch {}
      if (!chat) chat = chatMap.get(waId) || null;

      if (!chat) {
        out.push({ input: String(raw), normalized, registered: true, waId, exists: false, chat: null, messages: [] });
        continue;
      }

      const payload = {
        id: chat?.id?._serialized || waId,
        name: chat?.name || null,
        isGroup: !!chat?.isGroup,
        unreadCount: typeof chat?.unreadCount === 'number' ? chat.unreadCount : null,
        archived: !!chat?.archived,
        pinned: !!chat?.pinned,
        isReadOnly: !!chat?.isReadOnly,
      };

      let messages = [];
      if (withMessages) {
        try {
          const list = await chat.fetchMessages({ limit: Math.max(1, Math.min(100, Number(messagesLimit) || 20)) });
          messages = (list || [])
            .map((m) => ({
              id: m?.id?._serialized || null,
              chatId: m?.fromMe ? m?.to : m?.from,
              fromMe: !!m?.fromMe,
              body: m?.body ?? '',
              type: m?.type || null,
              timestamp: m?.timestamp || null,
              hasMedia: !!m?.hasMedia,
              // Note: media downloads via /media/:messageId depend on live cache.
            }))
            .filter((x) => x.id);
        } catch {
          messages = [];
        }
      }

      out.push({
        input: String(raw),
        normalized,
        registered: true,
        waId,
        exists: true,
        chat: payload,
        messages,
      });
    }

    return out;
  }

  return { getChats, getChatByNumber, getChatsByNumbers };
}
