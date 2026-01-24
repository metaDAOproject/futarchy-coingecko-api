import type { FutarchyService } from '../services/futarchyService.js';
import type { PriceService } from '../services/priceService.js';
import type { DuneService } from '../services/duneService.js';
import type { DuneCacheService } from '../services/duneCacheService.js';
import type { SolanaService } from '../services/solanaService.js';
import type { LaunchpadService } from '../services/launchpadService.js';
import type { DatabaseService } from '../services/databaseService.js';
import type { HourlyAggregationService } from '../services/hourlyAggregationService.js';
import type { TenMinuteVolumeFetcherService } from '../services/tenMinuteVolumeFetcherService.js';
import type { DailyAggregationService } from '../services/dailyAggregationService.js';
import type { MeteoraVolumeFetcherService } from '../services/meteoraVolumeFetcherService.js';

/**
 * Service getters passed to route handlers.
 * This allows routes to access services without direct imports,
 * enabling lazy initialization and easier testing.
 */
export interface ServiceGetters {
  getFutarchyService: () => FutarchyService;
  getPriceService: () => PriceService;
  getDuneService: () => DuneService | null;
  getDuneCacheService: () => DuneCacheService | null;
  getSolanaService: () => SolanaService;
  getLaunchpadService: () => LaunchpadService;
  getDatabaseService: () => DatabaseService;
  getHourlyAggregationService: () => HourlyAggregationService | null;
  getTenMinuteVolumeFetcherService: () => TenMinuteVolumeFetcherService | null;
  getDailyAggregationService: () => DailyAggregationService | null;
  getMeteoraVolumeFetcherService: () => MeteoraVolumeFetcherService | null;
}
