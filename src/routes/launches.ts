import { Router, type Request, type Response } from 'express';
import { asyncHandler } from '../middleware/errorHandler.js';
import type { ServiceGetters } from './types.js';

export function createLaunchesRouter(services: ServiceGetters): Router {
  const router = Router();
  const { getLaunchpadService } = services;

  // Launches currently accepting commitments, read on-chain and cached for
  // CACHE_LIVE_LAUNCHES_TTL (default 5 min). RPC failures surface as 5xx.
  router.get('/api/launches/live', asyncHandler(async (_req: Request, res: Response) => {
    const snapshot = await getLaunchpadService().getLiveLaunches();
    res.json({ count: snapshot.launches.length, ...snapshot });
  }));

  return router;
}
