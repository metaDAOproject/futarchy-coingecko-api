import { createApp, type Services } from './app.js';
import { FutarchyService } from './services/futarchyService.js';
import { PriceService } from './services/priceService.js';
import { DuneService } from './services/duneService.js';
import { DuneCacheService } from './services/duneCacheService.js';
import { SolanaService } from './services/solanaService.js';
import { LaunchpadService } from './services/launchpadService.js';
import { DatabaseService } from './services/databaseService.js';
import { HourlyAggregationService } from './services/hourlyAggregationService.js';
import { TenMinuteVolumeFetcherService } from './services/tenMinuteVolumeFetcherService.js';
import { DailyAggregationService } from './services/dailyAggregationService.js';
import { MeteoraVolumeFetcherService } from './services/meteoraVolumeFetcherService.js';
import { config } from './config.js';
import { logger } from './utils/logger.js';
import { scheduleWithoutPileup, scheduleDailyAtUTC, type ScheduledTask } from './utils/scheduling.js';
import { saveHealthSnapshots } from './routes/health.js';
import type { ServiceGetters } from './routes/types.js';
import type { Server } from 'http';

function initializeServices(): Services {
  const futarchyService = new FutarchyService();
  const priceService = new PriceService();
  const databaseService = new DatabaseService();
  const solanaService = new SolanaService();
  const launchpadService = new LaunchpadService();

  let duneService: DuneService | null = null;
  if (config.dune.apiKey) {
    duneService = new DuneService();
  }

  let duneCacheService: DuneCacheService | null = null;
  let hourlyAggregationService: HourlyAggregationService | null = null;
  let tenMinuteVolumeFetcherService: TenMinuteVolumeFetcherService | null = null;
  let dailyAggregationService: DailyAggregationService | null = null;
  let meteoraVolumeFetcherService: MeteoraVolumeFetcherService | null = null;

  if (duneService) {
    duneCacheService = new DuneCacheService(duneService, databaseService, futarchyService);
    hourlyAggregationService = new HourlyAggregationService(databaseService, duneService, futarchyService);
    tenMinuteVolumeFetcherService = new TenMinuteVolumeFetcherService(duneService, databaseService, futarchyService);
    dailyAggregationService = new DailyAggregationService(duneService, databaseService, futarchyService);
    meteoraVolumeFetcherService = new MeteoraVolumeFetcherService(duneService, databaseService);
  }

  return {
    futarchyService,
    priceService,
    databaseService,
    duneService,
    duneCacheService,
    solanaService,
    launchpadService,
    hourlyAggregationService,
    tenMinuteVolumeFetcherService,
    dailyAggregationService,
    meteoraVolumeFetcherService,
  };
}

async function startServices(services: Services): Promise<void> {
  if (services.hourlyAggregationService) {
    logger.info('Starting Hourly Aggregation service');
    try {
      await services.hourlyAggregationService.start();
      logger.info('Hourly Aggregation service started');
    } catch (error) {
      logger.error('Failed to start Hourly Aggregation service', error);
    }
  }

  if (services.tenMinuteVolumeFetcherService) {
    logger.info('Starting 10-Minute Volume Fetcher service');
    try {
      await services.tenMinuteVolumeFetcherService.start();
      logger.info('10-Minute Volume Fetcher service started');
    } catch (error) {
      logger.error('Failed to start 10-Minute Volume Fetcher service', error);
    }
  }

  if (services.dailyAggregationService) {
    logger.info('Starting Daily Aggregation service');
    try {
      await services.dailyAggregationService.initialize();
      services.dailyAggregationService.start();
      logger.info('Daily Aggregation service started');
    } catch (error) {
      logger.error('Failed to start Daily Aggregation service', error);
    }
  }

  if (services.duneCacheService) {
    logger.info('Starting Dune cache service');
    try {
      await services.duneCacheService.start();
      logger.info('Dune cache service started');
    } catch (error) {
      logger.error('Failed to start Dune cache service', error);
    }
  }
}

async function stopServices(services: Services, scheduledTasks: ScheduledTask[]): Promise<void> {
  scheduledTasks.forEach(task => task.stop());
  services.duneCacheService?.stop();
  services.hourlyAggregationService?.stop();
  services.tenMinuteVolumeFetcherService?.stop();
  services.dailyAggregationService?.stop();
  services.meteoraVolumeFetcherService?.stop();
  await services.databaseService.close();
}

function createServiceGetters(services: Services): ServiceGetters {
  return {
    getFutarchyService: () => services.futarchyService,
    getPriceService: () => services.priceService,
    getDuneService: () => services.duneService ?? null,
    getDuneCacheService: () => services.duneCacheService ?? null,
    getSolanaService: () => {
      if (!services.solanaService) throw new Error('Solana service not available');
      return services.solanaService;
    },
    getLaunchpadService: () => {
      if (!services.launchpadService) throw new Error('Launchpad service not available');
      return services.launchpadService;
    },
    getDatabaseService: () => services.databaseService,
    getHourlyAggregationService: () => services.hourlyAggregationService ?? null,
    getTenMinuteVolumeFetcherService: () => services.tenMinuteVolumeFetcherService ?? null,
    getDailyAggregationService: () => services.dailyAggregationService ?? null,
    getMeteoraVolumeFetcherService: () => services.meteoraVolumeFetcherService ?? null,
  };
}

function startScheduledTasks(services: Services): ScheduledTask[] {
  const tasks: ScheduledTask[] = [];
  const serviceGetters = createServiceGetters(services);

  // Health snapshots every 5 minutes
  const healthSnapshotTask = scheduleWithoutPileup(
    async () => {
      await saveHealthSnapshots(serviceGetters);
    },
    {
      name: 'HealthSnapshot',
      intervalMs: 5 * 60 * 1000,
      onError: (error) => logger.error('Error saving health snapshot', error),
    }
  );
  tasks.push(healthSnapshotTask);
  logger.info('Health snapshots scheduled every 5 minutes');

  // Prune old metrics daily at 03:00 UTC
  const metricsPruneTask = scheduleDailyAtUTC(
    async () => {
      if (services.databaseService.isAvailable()) {
        await services.databaseService.pruneOldMetrics(30);
        logger.info('Old metrics pruned (keeping last 30 days)');
      }
    },
    {
      name: 'MetricsPrune',
      hourUTC: 3,
      onError: (error) => logger.error('Error pruning metrics', error),
    }
  );
  tasks.push(metricsPruneTask);
  logger.info('Metrics pruning scheduled daily at 03:00 UTC');

  return tasks;
}

async function main(): Promise<void> {
  const services = initializeServices();
  const app = createApp({ services });
  let scheduledTasks: ScheduledTask[] = [];

  const server: Server = app.listen(config.server.port, async () => {
    logger.info('Server started', {
      port: config.server.port,
      tickersUrl: `http://localhost:${config.server.port}/api/tickers`,
      healthUrl: `http://localhost:${config.server.port}/health`,
    });

    await startServices(services);
    scheduledTasks = startScheduledTasks(services);

    // Save initial health snapshot after startup
    const serviceGetters = createServiceGetters(services);
    setTimeout(async () => {
      try {
        await saveHealthSnapshots(serviceGetters);
        logger.info('Initial health snapshot saved');
      } catch (error) {
        logger.error('Error saving initial health snapshot', error);
      }
    }, 10000);
  });

  server.timeout = config.server.requestTimeout;
  server.keepAliveTimeout = config.server.keepAliveTimeout;
  server.headersTimeout = config.server.keepAliveTimeout + 1000;

  const shutdown = async (signal: string): Promise<void> => {
    logger.info(`${signal} received, shutting down gracefully`);
    await stopServices(services, scheduledTasks);
    server.close(() => {
      logger.info('Server closed');
      process.exit(0);
    });
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main().catch((error) => {
  logger.error('Failed to start server', error);
  process.exit(1);
});
