import { Router, type Request, type Response } from 'express';
import type { ServiceGetters } from './types.js';

export function createAdminRouter(services: ServiceGetters): Router {
  const router = Router();
  const { getDuneCacheService, getTenMinuteVolumeFetcherService } = services;

  // Cache status
  router.get('/api/cache/status', (req: Request, res: Response) => {
    const duneCacheService = getDuneCacheService();
    
    if (!duneCacheService) {
      return res.status(400).json({
        error: 'Dune cache service not configured',
        message: 'DUNE_API_KEY environment variable is required',
      });
    }

    const status = duneCacheService.getCacheStatus();
    res.json({
      lastUpdated: status.lastUpdated.toISOString(),
      isRefreshing: status.isRefreshing,
      poolMetricsCount: status.poolMetricsCount,
      cacheAgeSeconds: Math.round(status.cacheAgeMs / 1000),
      isInitialized: status.isInitialized,
      refreshIntervalSeconds: parseInt(process.env.DUNE_CACHE_REFRESH_INTERVAL || '3600'),
    });
  });

  // Force cache refresh
  router.post('/api/cache/refresh', async (req: Request, res: Response) => {
    const duneCacheService = getDuneCacheService();
    
    if (!duneCacheService) {
      return res.status(400).json({
        error: 'Dune cache service not configured',
        message: 'DUNE_API_KEY environment variable is required',
      });
    }

    const statusBefore = duneCacheService.getCacheStatus();
    if (statusBefore.isRefreshing) {
      return res.status(409).json({
        error: 'Refresh already in progress',
        message: 'A cache refresh is already running. Please wait for it to complete.',
      });
    }

    duneCacheService.forceRefresh().catch(err => {
      console.error('[Cache] Force refresh failed:', err);
    });

    res.json({
      message: 'Cache refresh started',
      previousLastUpdated: statusBefore.lastUpdated.toISOString(),
    });
  });

  // 10-Minute volume status
  router.get('/api/ten-minute-volume/status', async (req: Request, res: Response) => {
    const tenMinuteVolumeFetcherService = getTenMinuteVolumeFetcherService();
    
    if (!tenMinuteVolumeFetcherService) {
      return res.status(400).json({
        error: '10-Minute Volume Fetcher service not configured',
        message: 'DUNE_API_KEY and DUNE_TEN_MINUTE_VOLUME_QUERY_ID are required',
      });
    }

    const status = tenMinuteVolumeFetcherService.getStatus();
    res.json({
      isInitialized: status.initialized,
      isRunning: status.isRunning,
      databaseConnected: status.databaseConnected,
      lastRefreshTime: status.lastRefreshTime,
      queryId: status.queryId,
      refreshInProgress: status.refreshInProgress,
      description: 'PRIMARY service for /api/tickers 24h volume (10-min granularity)',
    });
  });

  return router;
}
