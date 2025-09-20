// bot/wiring/BotBootstrap.js
import { BufferManager } from '../buffer/BufferManager.js';
import botConfigDefaults from '../../config/botConfig.js';
import { TurnOutboxWatcherHub } from '../watchers/TurnOutboxWatcher.js';
import { BotPolicy } from '../policy/BotPolicy.js';
import { MediaStore } from '../../media/MediaStore.js';

export function initBot({ db, sessions, config = {} }) {
  const cfg = { ...botConfigDefaults, ...config };
  const policy = new BotPolicy({ db });
  const mediaStore = new MediaStore({ sessions });

  const buffers = new BufferManager({ db, config: cfg, policy, mediaStore });
  buffers.startGC();

  sessions.on('evt', (evt) => {
    try {
      if (!evt || evt.type !== 'message') return;
      buffers.push(evt);
    } catch (e) {
      console.error('[BotBootstrap] push failed', e);
    }
  });

  const hub = new TurnOutboxWatcherHub({ db, sessions, policy });
  hub.start().catch((e) => console.error('[OutboxWatcherHub.start] error', e));

  console.log('[BotBootstrap] Bot initialized: debounce=%dms, gcIdle=%dms', cfg.debounceMs, cfg.gcIdleMs);
}
