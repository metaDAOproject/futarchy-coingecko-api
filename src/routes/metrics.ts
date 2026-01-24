import { Router, type Request, type Response } from 'express';
import { metricsService } from '../services/metricsService.js';
import { parseJsonParam, parseIntParam, parseRequiredString } from '../utils/validation.js';
import type { ServiceGetters } from './types.js';

export function createMetricsRouter(services: ServiceGetters): Router {
  const router = Router();
  const { getDatabaseService, getDuneCacheService, getHourlyAggregationService, getTenMinuteVolumeFetcherService, getFutarchyService } = services;

  // Prometheus metrics endpoint
  router.get('/metrics', async (req: Request, res: Response) => {
    try {
      await updateMetricsSnapshot(services);
      
      res.set('Content-Type', metricsService.getContentType());
      res.end(await metricsService.getMetrics());
    } catch (error: any) {
      console.error('[Metrics] Error generating metrics:', error);
      res.status(500).end('Error generating metrics');
    }
  });

  // Metrics history from database
  router.get('/api/metrics/history/:metricName', async (req: Request, res: Response) => {
    const databaseService = getDatabaseService();
    
    if (!databaseService.isAvailable()) {
      return res.status(503).json({
        error: 'Database not connected',
        message: 'Metrics history requires database connection',
      });
    }

    // Validate metric name
    const metricNameResult = parseRequiredString(req.params.metricName, 'metricName');
    if (!metricNameResult.success) {
      return res.status(400).json(metricNameResult.error);
    }
    
    // Validate hours parameter
    const hoursResult = parseIntParam(req.query.hours as string, 'hours', {
      defaultValue: 24,
      min: 1,
      max: 168, // 1 week max
    });
    if (!hoursResult.success) {
      return res.status(400).json(hoursResult.error);
    }
    
    // Validate labels JSON
    const labelsResult = parseJsonParam<Record<string, string>>(
      req.query.labels as string,
      'labels'
    );
    if (!labelsResult.success) {
      return res.status(400).json(labelsResult.error);
    }

    try {
      const data = await databaseService.getRecentMetrics(
        metricNameResult.value,
        hoursResult.value,
        labelsResult.value
      );
      
      res.json({
        metric: metricNameResult.value,
        hours: hoursResult.value,
        count: data.length,
        data,
      });
    } catch (error: any) {
      res.status(500).json({
        error: 'Failed to get metrics history',
        message: error.message,
      });
    }
  });

  return router;
}

// Helper function to update all metrics
async function updateMetricsSnapshot(services: ServiceGetters): Promise<void> {
  const databaseService = services.getDatabaseService();
  const duneCacheService = services.getDuneCacheService();
  const hourlyAggregationService = services.getHourlyAggregationService();
  const tenMinuteVolumeFetcherService = services.getTenMinuteVolumeFetcherService();
  const futarchyService = services.getFutarchyService();

  metricsService.setDatabaseConnected(databaseService.isAvailable());
  
  if (databaseService.isAvailable()) {
    try {
      const dailyCount = await databaseService.getDailyRecordCount();
      const hourlyCount = await databaseService.getHourlyRecordCount();
      const tenMinCount = await databaseService.getTenMinuteRecordCount();
      const buySellCount = await databaseService.getBuySellRecordCount();
      
      metricsService.setDatabaseRecordCount('daily_volumes', dailyCount);
      metricsService.setDatabaseRecordCount('hourly_volumes', hourlyCount);
      metricsService.setDatabaseRecordCount('ten_minute_volumes', tenMinCount);
      metricsService.setDatabaseRecordCount('daily_buy_sell_volumes', buySellCount);

      const dailyTokens = await databaseService.getTokenCount();
      const hourlyTokens = await databaseService.getHourlyTokenCount();
      metricsService.setDatabaseTokenCount('daily_volumes', dailyTokens);
      metricsService.setDatabaseTokenCount('hourly_volumes', hourlyTokens);

      const latestDaily = await databaseService.getLatestDate();
      const latestHourly = await databaseService.getLatestHour();
      const latestTenMin = await databaseService.getLatestTenMinuteBucket();
      const latestBuySell = await databaseService.getLatestBuySellDate();
      
      metricsService.setDatabaseLatestDate('daily_volumes', latestDaily);
      metricsService.setDatabaseLatestDate('hourly_volumes', latestHourly);
      metricsService.setDatabaseLatestDate('ten_minute_volumes', latestTenMin);
      metricsService.setDatabaseLatestDate('daily_buy_sell_volumes', latestBuySell);
    } catch (error) {
      console.error('[Metrics] Error fetching database metrics:', error);
    }
  }

  if (duneCacheService) {
    const status = duneCacheService.getCacheStatus();
    metricsService.setServiceStatus('dune_cache', status.isInitialized);
    metricsService.setRefreshInProgress('dune_cache', status.isRefreshing);
    if (status.lastUpdated) {
      metricsService.setLastRefreshTime('dune_cache', status.lastUpdated);
      metricsService.updateTimeSinceLastRefresh('dune_cache', status.lastUpdated.getTime());
    }
  }

  if (hourlyAggregationService) {
    metricsService.setServiceStatus('hourly_volume', hourlyAggregationService.isInitialized);
    metricsService.setRefreshInProgress('hourly_volume', false);
  }

  if (tenMinuteVolumeFetcherService) {
    const status = tenMinuteVolumeFetcherService.getStatus();
    metricsService.setServiceStatus('ten_minute_volume', status.initialized);
    metricsService.setRefreshInProgress('ten_minute_volume', status.refreshInProgress);
    if (status.lastRefreshTime) {
      metricsService.setLastRefreshTime('ten_minute_volume', new Date(status.lastRefreshTime));
      metricsService.updateTimeSinceLastRefresh('ten_minute_volume', new Date(status.lastRefreshTime).getTime());
    }
  }

  try {
    const daos = await futarchyService.getAllDaos();
    metricsService.setActiveDaosCount(daos.length);
  } catch (error) {
    // Ignore errors during metrics collection
  }
}
