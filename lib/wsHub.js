// Single WS endpoint /ws?accountId=...&token=FIREBASE_ID_TOKEN
// - Verifies Firebase ID token
// - Checks membership & role
// - Builds allowed sessions[] (Admin → all, else ACL doc)
// - Subscribes to Firestore for live updates of ACL/sessions
// - Streams only events for that accountId + allowed sessions

import { WebSocketServer } from 'ws';

export function createWsHub({ server, authAdmin, rbac, sessions, maxConnections = 2000 }) {
  const wss = new WebSocketServer({ noServer: true });
  const conns = new Set(); // {ws, uid, accountId, allowed:Set, extra?, alive, unsub?:fn}

  function canSee(c, evt) {
    if (evt.accountId !== c.accountId) return false;
    if (!c.allowed.has(evt.sessionId)) return false;
    const f = c.extra;
    if (!f) return true;
    if (f.types?.length && !f.types.includes(evt.type)) return false;
    if (typeof f.fromMe === 'boolean' && evt.fromMe !== f.fromMe) return false;
    if (f.chats?.length) {
      const cid = evt.chatId || '';
      if (!f.chats.includes(cid)) return false;
    }
    return true;
  }

  function push(evt) {
    const msg = JSON.stringify(evt);
    for (const c of conns) {
      if (c.ws.readyState !== c.ws.OPEN) continue;
      if (canSee(c, evt)) { try { c.ws.send(msg); } catch {} }
    }
  }

  // attach to WA event bus
  sessions.on('evt', push);

  server.on('upgrade', async (req, socket, head) => {
    const url = new URL(req.url || '', 'http://x');
    if (url.pathname !== '/ws') return socket.destroy();
    if (conns.size >= maxConnections) return socket.destroy();

    const accountId = url.searchParams.get('accountId') || '';
    const token = url.searchParams.get('token') || '';
    if (!accountId || !token) return socket.destroy();

    // verify token
    let uid = null;
    try {
      const decoded = await authAdmin.verifyIdToken(token);
      uid = decoded.uid;
    } catch {
      return socket.destroy();
    }

    // check membership + initial allowed
    const initial = await rbac.allowedSessions(accountId, uid);
    if (!initial.role) return socket.destroy();

    const allowedSet = new Set(initial.sessions.map(String));
    wss.handleUpgrade(req, socket, head, (ws) => {
      const conn = { ws, uid, accountId, allowed: allowedSet, extra: null, alive: true, unsub: null };

      // live ACL updates
      conn.unsub = rbac.subscribeAllowed({ accountId, uid }, ({ sessions }) => {
        conn.allowed = new Set((sessions || []).map(String));
        try { conn.ws.send(JSON.stringify({ type: 'acl_update', ts: Date.now(), sessions: Array.from(conn.allowed) })); } catch {}
        // if now empty, close
        if (conn.allowed.size === 0) {
          try { conn.ws.close(4403, 'ACL empty'); } catch {}
        }
      });

      conns.add(conn);

      ws.on('pong', () => (conn.alive = true));
      ws.on('close', () => { conns.delete(conn); try { conn.unsub?.(); } catch {} });

      ws.on('message', (buf) => {
        // Optional narrowing
        try {
          const m = JSON.parse(String(buf));
          if (m?.type === 'subscribe') {
            const f = m.filters || {};
            if (Array.isArray(f.sessions) && f.sessions.length) {
              conn.allowed = new Set(f.sessions.map(String).filter(s => conn.allowed.has(s)));
            }
            conn.extra = {
              types: Array.isArray(f.types) ? f.types : undefined,
              chats: Array.isArray(f.chats) ? f.chats : undefined,
              fromMe: typeof f.fromMe === 'boolean' ? f.fromMe : undefined
            };
            ws.send(JSON.stringify({ type:'subscribed', ts:Date.now(), sessions:Array.from(conn.allowed), filters:conn.extra }));
          }
        } catch {}
      });

      ws.send(JSON.stringify({ type:'hello', ts:Date.now(), accountId, sessions:Array.from(conn.allowed) }));
    });
  });

  // heartbeat cleanup
  setInterval(() => {
    for (const c of conns) {
      if (!c.alive) { try { c.ws.terminate(); } catch {} conns.delete(c); try { c.unsub?.(); } catch {} continue; }
      c.alive = false;
      try { c.ws.ping(); } catch {}
    }
  }, 30000);
}
