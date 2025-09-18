import { Router } from 'express';

export function buildHealthRouter() {
  const r = Router();
  r.get('/healthz', (_req, res) => {
    res.json({ ok: true, ts: Date.now() });
  });
  return r;
}
