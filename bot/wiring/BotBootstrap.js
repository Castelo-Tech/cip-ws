// bot/wiring/BotBootstrap.js
// Wires your session event bus into the BufferManager, and starts the outbox watcher.

import { BufferManager } from '../buffer/BufferManager.js';
import botConfigDefaults from '../../config/botConfig.js';
import { TurnOutboxWatcherHub } from '../watchers/TurnOutboxWatcher.js';

export function initBot({ db, sessions, config = {} }) {
  const cfg = { ...botConfigDefaults, ...config };
  const buffers = new BufferManager({ db, config: cfg });
  buffers.startGC();

  // 1) Wire inbound messages into buffers
  sessions.on('evt', (evt) => {
    try {
      if (!evt || evt.type !== 'message') return;
      buffers.push(evt);
    } catch (e) {
      console.error('[BotBootstrap] push failed', e);
    }
  });

  // 2) Start the outbox watcher hub (ready → send → delivered)
  const hub = new TurnOutboxWatcherHub({ db, sessions });
  hub.start().catch((e) => console.error('[OutboxWatcherHub.start] error', e));

  console.log('[BotBootstrap] Bot initialized: debounce=%dms, gcIdle=%dms',
    cfg.debounceMs, cfg.gcIdleMs);
}
