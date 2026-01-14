import express, { type Request, type Response, type NextFunction } from 'express';
import { FutarchyService } from './services/futarchyService.js';
import { PriceService } from './services/priceService.js';
import { DuneService } from './services/duneService.js';
import { DuneCacheService } from './services/duneCacheService.js';
import { SolanaService } from './services/solanaService.js';
import { LaunchpadService } from './services/launchpadService.js';
import { DatabaseService } from './services/databaseService.js';
import { VolumeHistoryService } from './services/volumeHistoryService.js';
import { HourlyVolumeService } from './services/hourlyVolumeService.js';
import { TenMinuteVolumeService } from './services/tenMinuteVolumeService.js';
import { DailyBuySellVolumeService } from './services/dailyBuySellVolumeService.js';
import { metricsService } from './services/metricsService.js';
import { config } from './config.js';
import type { CoinGeckoTicker } from './types/coingecko.js';
import { PublicKey } from '@solana/web3.js';
import BN from 'bn.js';

const app = express();

// Lazy initialization of services to avoid loading at module import time
// This allows tests to mock services before they're accessed
let futarchyServiceInstance: FutarchyService | null = null;
let priceServiceInstance: PriceService | null = null;
let duneServiceInstance: DuneService | null = null;
let duneCacheServiceInstance: DuneCacheService | null = null;
let solanaServiceInstance: SolanaService | null = null;
let launchpadServiceInstance: LaunchpadService | null = null;
let databaseServiceInstance: DatabaseService | null = null;
let volumeHistoryServiceInstance: VolumeHistoryService | null = null;
let hourlyVolumeServiceInstance: HourlyVolumeService | null = null;
let tenMinuteVolumeServiceInstance: TenMinuteVolumeService | null = null;
let dailyBuySellVolumeServiceInstance: DailyBuySellVolumeService | null = null;

function getFutarchyService(): FutarchyService {
  if (!futarchyServiceInstance) {
    futarchyServiceInstance = new FutarchyService();
  }
  return futarchyServiceInstance;
}

function getPriceService(): PriceService {
  if (!priceServiceInstance) {
    priceServiceInstance = new PriceService();
  }
  return priceServiceInstance;
}

function getDuneService(): DuneService | null {
  // Only require API key - queryId is optional since we can create temporary queries
  if (!duneServiceInstance && config.dune.apiKey) {
    duneServiceInstance = new DuneService();
  }
  return duneServiceInstance;
}

function getDatabaseService(): DatabaseService {
  if (!databaseServiceInstance) {
    databaseServiceInstance = new DatabaseService();
  }
  return databaseServiceInstance;
}

function getVolumeHistoryService(): VolumeHistoryService | null {
  if (!volumeHistoryServiceInstance) {
    const duneService = getDuneService();
    const databaseService = getDatabaseService();
    const futarchyService = getFutarchyService();
    if (duneService) {
      volumeHistoryServiceInstance = new VolumeHistoryService(databaseService, duneService, futarchyService);
    }
  }
  return volumeHistoryServiceInstance;
}

function getHourlyVolumeService(): HourlyVolumeService | null {
  if (!hourlyVolumeServiceInstance) {
    const duneService = getDuneService();
    const databaseService = getDatabaseService();
    const futarchyService = getFutarchyService();
    if (duneService) {
      hourlyVolumeServiceInstance = new HourlyVolumeService(databaseService, duneService, futarchyService);
    }
  }
  return hourlyVolumeServiceInstance;
}

function getTenMinuteVolumeService(): TenMinuteVolumeService | null {
  if (!tenMinuteVolumeServiceInstance) {
    const duneService = getDuneService();
    const databaseService = getDatabaseService();
    const futarchyService = getFutarchyService();
    if (duneService) {
      tenMinuteVolumeServiceInstance = new TenMinuteVolumeService(duneService, databaseService, futarchyService);
    }
  }
  return tenMinuteVolumeServiceInstance;
}

function getDailyBuySellVolumeService(): DailyBuySellVolumeService | null {
  if (!dailyBuySellVolumeServiceInstance) {
    const duneService = getDuneService();
    const databaseService = getDatabaseService();
    const futarchyService = getFutarchyService();
    if (duneService) {
      dailyBuySellVolumeServiceInstance = new DailyBuySellVolumeService(duneService, databaseService, futarchyService);
    }
  }
  return dailyBuySellVolumeServiceInstance;
}

function getDuneCacheService(): DuneCacheService | null {
  if (!duneCacheServiceInstance) {
    const duneService = getDuneService();
    const futarchyService = getFutarchyService();
    const volumeHistoryService = getVolumeHistoryService();
    if (duneService) {
      duneCacheServiceInstance = new DuneCacheService(duneService, futarchyService, volumeHistoryService || undefined);
    }
  }
  return duneCacheServiceInstance;
}

function getSolanaService(): SolanaService {
  if (!solanaServiceInstance) {
    solanaServiceInstance = new SolanaService();
  }
  return solanaServiceInstance;
}

function getLaunchpadService(): LaunchpadService {
  if (!launchpadServiceInstance) {
    launchpadServiceInstance = new LaunchpadService();
  }
  return launchpadServiceInstance;
}

// Export for testing purposes
export function setFutarchyService(service: FutarchyService | null): void {
  futarchyServiceInstance = service;
}

export function setPriceService(service: PriceService | null): void {
  priceServiceInstance = service;
}

export function setDuneService(service: DuneService | null): void {
  duneServiceInstance = service;
}

export function setDuneCacheService(service: DuneCacheService | null): void {
  duneCacheServiceInstance = service;
}

export function setSolanaService(service: SolanaService | null): void {
  solanaServiceInstance = service;
}

export function setLaunchpadService(service: LaunchpadService | null): void {
  launchpadServiceInstance = service;
}

// Middleware
app.use(express.json());
app.use((req: Request, res: Response, next: NextFunction) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  next();
});

// Rate limiting
const rateLimitMap = new Map<string, { count: number; resetTime: number }>();

function rateLimit(req: Request, res: Response, next: NextFunction) {
  const ip = req.ip || 'unknown';
  const now = Date.now();
  const limit = rateLimitMap.get(ip);

  if (!limit || now > limit.resetTime) {
    rateLimitMap.set(ip, {
      count: 1,
      resetTime: now + config.server.rateLimit.windowMs,
    });
    return next();
  }

  if (limit.count >= config.server.rateLimit.maxRequests) {
    return res.status(429).json({ error: 'Too many requests' });
  }

  limit.count++;
  next();
}

app.use(rateLimit);

// Metrics middleware - track HTTP request metrics
app.use((req: Request, res: Response, next: NextFunction) => {
  // Skip metrics endpoint to avoid recursion
  if (req.path === '/metrics') {
    return next();
  }

  const startTime = Date.now();
  metricsService.incrementHttpRequestsInFlight();

  // Track response completion
  res.on('finish', () => {
    metricsService.decrementHttpRequestsInFlight();
    const durationSeconds = (Date.now() - startTime) / 1000;
    metricsService.recordHttpRequest(req.method, req.path, res.statusCode, durationSeconds);
  });

  next();
});

// Prometheus metrics endpoint
app.get('/metrics', async (req: Request, res: Response) => {
  try {
    // Update service metrics before returning
    await updateMetricsSnapshot();
    
    res.set('Content-Type', metricsService.getContentType());
    res.end(await metricsService.getMetrics());
  } catch (error: any) {
    console.error('[Metrics] Error generating metrics:', error);
    res.status(500).end('Error generating metrics');
  }
});

// Helper function to update all metrics
async function updateMetricsSnapshot(): Promise<void> {
  const databaseService = getDatabaseService();
  const duneCacheService = getDuneCacheService();
  const hourlyVolumeService = getHourlyVolumeService();
  const tenMinuteVolumeService = getTenMinuteVolumeService();
  const dailyBuySellVolumeService = getDailyBuySellVolumeService();
  const volumeHistoryService = getVolumeHistoryService();

  // Database metrics
  metricsService.setDatabaseConnected(databaseService.isAvailable());
  
  if (databaseService.isAvailable()) {
    try {
      // Record counts
      const dailyCount = await databaseService.getDailyRecordCount();
      const hourlyCount = await databaseService.getHourlyRecordCount();
      const tenMinCount = await databaseService.getTenMinuteRecordCount();
      const buySellCount = await databaseService.getBuySellRecordCount();
      
      metricsService.setDatabaseRecordCount('daily_volumes', dailyCount);
      metricsService.setDatabaseRecordCount('hourly_volumes', hourlyCount);
      metricsService.setDatabaseRecordCount('ten_minute_volumes', tenMinCount);
      metricsService.setDatabaseRecordCount('daily_buy_sell_volumes', buySellCount);

      // Token counts
      const dailyTokens = await databaseService.getTokenCount();
      const hourlyTokens = await databaseService.getHourlyTokenCount();
      metricsService.setDatabaseTokenCount('daily_volumes', dailyTokens);
      metricsService.setDatabaseTokenCount('hourly_volumes', hourlyTokens);

      // Latest dates
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

  // Service status metrics
  if (duneCacheService) {
    const status = duneCacheService.getCacheStatus();
    metricsService.setServiceStatus('dune_cache', status.isInitialized);
    metricsService.setRefreshInProgress('dune_cache', status.isRefreshing);
    if (status.lastUpdated) {
      metricsService.setLastRefreshTime('dune_cache', status.lastUpdated);
      metricsService.updateTimeSinceLastRefresh('dune_cache', status.lastUpdated.getTime());
    }
  }

  if (hourlyVolumeService) {
    metricsService.setServiceStatus('hourly_volume', hourlyVolumeService.isInitialized);
    metricsService.setRefreshInProgress('hourly_volume', false); // Would need to expose this
  }

  if (tenMinuteVolumeService) {
    const status = tenMinuteVolumeService.getStatus();
    metricsService.setServiceStatus('ten_minute_volume', status.initialized);
    metricsService.setRefreshInProgress('ten_minute_volume', status.refreshInProgress);
    if (status.lastRefreshTime) {
      metricsService.setLastRefreshTime('ten_minute_volume', new Date(status.lastRefreshTime));
      metricsService.updateTimeSinceLastRefresh('ten_minute_volume', new Date(status.lastRefreshTime).getTime());
    }
  }

  if (dailyBuySellVolumeService) {
    const status = dailyBuySellVolumeService.getStatus();
    metricsService.setServiceStatus('daily_buy_sell_volume', status.initialized);
    metricsService.setRefreshInProgress('daily_buy_sell_volume', status.isRefreshing);
  }

  if (volumeHistoryService) {
    metricsService.setServiceStatus('volume_history', true); // Would need to expose this
  }

  // Try to get active DAO count
  try {
    const futarchyService = getFutarchyService();
    const daos = await futarchyService.getAllDaos();
    metricsService.setActiveDaosCount(daos.length);
  } catch (error) {
    // Ignore errors during metrics collection
  }
}

// CoinGecko Endpoint 1: /tickers
app.get('/api/tickers', async (req: Request, res: Response) => {
  try {
    const futarchyService = getFutarchyService();
    const priceService = getPriceService();
    const duneCacheService = getDuneCacheService();
    const tenMinuteVolumeService = getTenMinuteVolumeService();
    const hourlyVolumeService = getHourlyVolumeService();
    
    // Fetch all DAOs with their pool data
    const allDaos = await futarchyService.getAllDaos();
    
    // Create mapping from baseMint (token) to DAO address for later lookup
    const tokenToDaoMap = new Map<string, string>();
    for (const dao of allDaos) {
      tokenToDaoMap.set(dao.baseMint.toString().toLowerCase(), dao.daoAddress.toString().toLowerCase());
    }
    
    // Get 24h metrics - priority: 10-min (most accurate) > hourly > DuneCacheService
    let duneMetricsMap = new Map<string, { base_volume_24h: string; target_volume_24h: string; high_24h: string; low_24h: string }>();
    let volumeSource = 'none';
    
    // Try TenMinuteVolumeService first (most accurate rolling 24h, refreshed every 10 min)
    if (tenMinuteVolumeService?.isInitialized && tenMinuteVolumeService.isDatabaseConnected()) {
      const baseMints = allDaos.map(dao => dao.baseMint.toString());
      const tenMinMetrics = await tenMinuteVolumeService.getRolling24hMetrics(baseMints);
      
      if (tenMinMetrics.size > 0) {
        // Remap from token address to DAO address
        for (const [tokenAddress, metrics] of tenMinMetrics.entries()) {
          const daoAddress = tokenToDaoMap.get(tokenAddress.toLowerCase());
          if (daoAddress) {
            duneMetricsMap.set(daoAddress, {
              base_volume_24h: String(metrics.base_volume_24h),
              target_volume_24h: String(metrics.target_volume_24h),
              high_24h: String(metrics.high_24h),
              low_24h: String(metrics.low_24h),
            });
          }
        }
        volumeSource = '10-minute';
        console.log(`[TenMinVolume] Using rolling 24h metrics for ${duneMetricsMap.size} DAOs`);
      }
    }
    
    // Fall back to HourlyVolumeService if 10-minute didn't provide data
    if (duneMetricsMap.size === 0 && hourlyVolumeService?.isInitialized && hourlyVolumeService.isDatabaseConnected()) {
      const baseMints = allDaos.map(dao => dao.baseMint.toString());
      const hourlyMetrics = await hourlyVolumeService.getRolling24hMetrics(baseMints);
      
      if (hourlyMetrics.size > 0) {
        for (const [tokenAddress, metrics] of hourlyMetrics.entries()) {
          const daoAddress = tokenToDaoMap.get(tokenAddress.toLowerCase());
          if (daoAddress) {
            duneMetricsMap.set(daoAddress, {
              base_volume_24h: metrics.base_volume_24h,
              target_volume_24h: metrics.target_volume_24h,
              high_24h: metrics.high_24h,
              low_24h: metrics.low_24h,
            });
          }
        }
        volumeSource = 'hourly';
        console.log(`[HourlyVolume] Using rolling 24h metrics for ${duneMetricsMap.size} DAOs`);
      }
    }
    
    // Fall back to DuneCacheService as last resort
    if (duneMetricsMap.size === 0 && duneCacheService) {
      const cachedMetrics = duneCacheService.getPoolMetrics();
      if (cachedMetrics && cachedMetrics.size > 0) {
        const cacheStatus = duneCacheService.getCacheStatus();
        console.log(`[DuneCache] Using cached metrics (age: ${Math.round(cacheStatus.cacheAgeMs / 1000)}s, ${cachedMetrics.size} entries)`);
        duneMetricsMap = cachedMetrics;
        volumeSource = 'dune-cache';
      } else {
        console.warn('[DuneCache] No cached metrics available yet');
      }
    }
    
    if (duneMetricsMap.size === 0) {
      console.warn('[Tickers] No volume metrics available - set DUNE_API_KEY and configure queries');
    } else {
      console.log(`[Tickers] Using volume source: ${volumeSource}`);
    }
    
    // Generate tickers for all DAOs
    const tickers: CoinGeckoTicker[] = [];
    
    for (const daoData of allDaos) {
      try {
        const { 
          daoAddress, 
          baseMint, 
          quoteMint, 
          baseDecimals, 
          quoteDecimals, 
          baseSymbol,
          baseName,
          quoteSymbol,
          quoteName,
          poolData 
        } = daoData;
        const tickerId = `${baseMint.toString()}_${quoteMint.toString()}`;
        const poolId = daoAddress.toString();
        
        // Calculate price and metrics with validation
        const lastPrice = priceService.calculatePrice(
          poolData.baseReserves,
          poolData.quoteReserves,
          baseDecimals,
          quoteDecimals
        );
        
        // Skip if price calculation failed
        if (!lastPrice) {
          continue;
        }
        
        const priceNum = parseFloat(lastPrice);
        const spread = priceService.calculateSpread(priceNum);
        
        // Skip if spread calculation failed
        if (!spread) {
          continue;
        }
        
        const liquidityUsd = priceService.calculateLiquidityUSD(
          poolData.quoteReserves,
          quoteDecimals
        );
        
        // Skip if liquidity calculation failed
        if (!liquidityUsd) {
          continue;
        }

        // Get 24h metrics from Dune if available, otherwise fallback to fee-based calculation
        const duneMetrics = duneMetricsMap.get(poolId.toLowerCase());
        let baseVolume: string;
        let targetVolume: string;
        let high24h: string | undefined;
        let low24h: string | undefined;

        if (duneMetrics) {
          // Use volume data from database (10-min, hourly, or cache)
          baseVolume = duneMetrics.base_volume_24h;
          targetVolume = duneMetrics.target_volume_24h;
          
          high24h = duneMetrics.high_24h !== '0' ? duneMetrics.high_24h : undefined;
          low24h = duneMetrics.low_24h !== '0' ? duneMetrics.low_24h : undefined;
        } else {
          // No volume data found - show 0 (means no trading volume in last 24h)
          baseVolume = '0';
          targetVolume = '0';
        }

        // Final validation - ensure no NaN values
        if (isNaN(parseFloat(baseVolume)) || isNaN(parseFloat(targetVolume))) {
          continue;
        }

        const ticker: CoinGeckoTicker = {
          ticker_id: tickerId,
          base_currency: baseMint.toString(),
          target_currency: quoteMint.toString(),
          base_symbol: baseSymbol,
          base_name: baseName,
          target_symbol: quoteSymbol,
          target_name: quoteName,
          pool_id: poolId,
          last_price: lastPrice,
          base_volume: baseVolume,
          target_volume: targetVolume,
          liquidity_in_usd: liquidityUsd,
          bid: spread.bid,
          ask: spread.ask,
        };

        // Add high and low if available from Dune
        if (high24h) {
          ticker.high_24h = high24h;
        }
        if (low24h) {
          ticker.low_24h = low24h;
        }

        // Add treasury USDC AUM and vault address if available
        if (daoData.treasuryUsdcAum) {
          ticker.treasury_usdc_aum = daoData.treasuryUsdcAum;
        }
        if (daoData.treasuryVaultAddress) {
          ticker.treasury_vault_address = daoData.treasuryVaultAddress;
        }

        tickers.push(ticker);
      } catch (error) {
        console.error(`Error generating ticker for DAO ${daoData.daoAddress.toString()}:`, error);
        // Continue processing other DAOs
      }
    }

    res.json(tickers);
  } catch (error) {
    console.error('Error in /api/tickers:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Health check
app.get('/health', (req: Request, res: Response) => {
  const duneCacheService = getDuneCacheService();
  const cacheStatus = duneCacheService?.getCacheStatus();
  
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    duneCache: cacheStatus ? {
      lastUpdated: cacheStatus.lastUpdated.toISOString(),
      isRefreshing: cacheStatus.isRefreshing,
      poolMetricsCount: cacheStatus.poolMetricsCount,
      aggregateVolumeTokenCount: cacheStatus.aggregateVolumeTokenCount,
      cacheAgeSeconds: Math.round(cacheStatus.cacheAgeMs / 1000),
      isInitialized: cacheStatus.isInitialized,
    } : null,
  });
});

// Cache status and management endpoint
app.get('/api/cache/status', (req: Request, res: Response) => {
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
    aggregateVolumeTokenCount: status.aggregateVolumeTokenCount,
    cacheAgeSeconds: Math.round(status.cacheAgeMs / 1000),
    isInitialized: status.isInitialized,
    usingVolumeHistoryService: status.usingVolumeHistoryService,
    refreshIntervalSeconds: parseInt(process.env.DUNE_CACHE_REFRESH_INTERVAL || '3600'),
  });
});

// Volume history status endpoint
app.get('/api/volume-history/status', async (req: Request, res: Response) => {
  const volumeHistoryService = getVolumeHistoryService();
  
  if (!volumeHistoryService) {
    return res.status(400).json({
      error: 'Volume History service not configured',
      message: 'DUNE_API_KEY environment variable is required',
    });
  }

  const status = await volumeHistoryService.getStatus();
  res.json({
    isInitialized: status.isInitialized,
    databaseConnected: status.databaseConnected,
    latestDate: status.latestDate,
    tokenCount: status.tokenCount,
    recordCount: status.recordCount,
    lastSyncTime: status.lastSyncTime?.toISOString() || null,
    isRefreshing: status.isRefreshing,
    incrementalQueryId: status.incrementalQueryId,
    schedule: status.schedule,
  });
});

// Force volume history refresh endpoint
app.post('/api/volume-history/refresh', async (req: Request, res: Response) => {
  const volumeHistoryService = getVolumeHistoryService();
  
  if (!volumeHistoryService) {
    return res.status(400).json({
      error: 'Volume History service not configured',
      message: 'DUNE_API_KEY environment variable is required',
    });
  }

  try {
    const statusBefore = await volumeHistoryService.getStatus();
    
    if (statusBefore.isRefreshing) {
      return res.json({
        message: 'Refresh already in progress',
        status: statusBefore,
      });
    }

    // Start refresh in background
    volumeHistoryService.forceRefresh();
    
    res.json({
      message: 'Volume history refresh started',
      statusBefore,
    });
  } catch (error: any) {
    res.status(500).json({
      error: 'Failed to trigger refresh',
      message: error.message,
    });
  }
});

// Hourly volume status endpoint (for rolling 24h metrics)
app.get('/api/hourly-volume/status', async (req: Request, res: Response) => {
  const hourlyVolumeService = getHourlyVolumeService();
  
  if (!hourlyVolumeService) {
    return res.status(400).json({
      error: 'Hourly Volume service not configured',
      message: 'DUNE_API_KEY and DUNE_HOURLY_VOLUME_QUERY_ID are required',
    });
  }

  const status = await hourlyVolumeService.getStatus();
  res.json({
    isInitialized: status.isInitialized,
    databaseConnected: status.databaseConnected,
    latestHour: status.latestHour,
    latestCompleteHour: status.latestCompleteHour,
    tokenCount: status.tokenCount,
    recordCount: status.recordCount,
    lastRefreshTime: status.lastRefreshTime?.toISOString() || null,
    isRefreshing: status.isRefreshing,
    hourlyQueryId: status.hourlyQueryId,
    schedule: status.schedule,
  });
});

// Force hourly volume refresh endpoint
app.post('/api/hourly-volume/refresh', async (req: Request, res: Response) => {
  const hourlyVolumeService = getHourlyVolumeService();
  
  if (!hourlyVolumeService) {
    return res.status(400).json({
      error: 'Hourly Volume service not configured',
      message: 'DUNE_API_KEY and DUNE_HOURLY_VOLUME_QUERY_ID are required',
    });
  }

  try {
    const statusBefore = await hourlyVolumeService.getStatus();
    
    if (statusBefore.isRefreshing) {
      return res.json({
        message: 'Refresh already in progress',
        status: statusBefore,
      });
    }

    // Start refresh in background
    hourlyVolumeService.forceRefresh();
    
    res.json({
      message: 'Hourly volume refresh started',
      statusBefore,
    });
  } catch (error: any) {
    res.status(500).json({
      error: 'Failed to trigger refresh',
      message: error.message,
    });
  }
});

// 10-Minute volume status endpoint (PRIMARY for rolling 24h metrics)
app.get('/api/ten-minute-volume/status', async (req: Request, res: Response) => {
  const tenMinuteVolumeService = getTenMinuteVolumeService();
  
  if (!tenMinuteVolumeService) {
    return res.status(400).json({
      error: '10-Minute Volume service not configured',
      message: 'DUNE_API_KEY and DUNE_TEN_MINUTE_VOLUME_QUERY_ID are required',
    });
  }

  const status = tenMinuteVolumeService.getStatus();
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

// Force 10-minute volume refresh endpoint
app.post('/api/ten-minute-volume/refresh', async (req: Request, res: Response) => {
  const tenMinuteVolumeService = getTenMinuteVolumeService();
  
  if (!tenMinuteVolumeService) {
    return res.status(400).json({
      error: '10-Minute Volume service not configured',
      message: 'DUNE_API_KEY and DUNE_TEN_MINUTE_VOLUME_QUERY_ID are required',
    });
  }

  try {
    const statusBefore = tenMinuteVolumeService.getStatus();
    
    if (statusBefore.refreshInProgress) {
      return res.json({
        message: 'Refresh already in progress',
        status: statusBefore,
      });
    }

    // Start refresh in background (full 24h backfill)
    tenMinuteVolumeService.forceRefresh();
    
    res.json({
      message: '10-minute volume refresh started (full 24h backfill)',
      statusBefore,
    });
  } catch (error: any) {
    res.status(500).json({
      error: 'Failed to trigger refresh',
      message: error.message,
    });
  }
});

// ============================================
// DAILY BUY/SELL VOLUME ENDPOINTS
// ============================================

// Buy/sell volume status endpoint
app.get('/api/buy-sell-volume/status', async (req: Request, res: Response) => {
  const dailyBuySellVolumeService = getDailyBuySellVolumeService();
  
  if (!dailyBuySellVolumeService) {
    return res.status(400).json({
      error: 'Buy/Sell Volume service not configured',
      message: 'DUNE_API_KEY is required',
    });
  }

  const status = dailyBuySellVolumeService.getStatus();
  res.json({
    ...status,
    description: 'Tracks daily buy vs sell volume per token with cumulative totals',
    schedule: 'Daily at 00:05 UTC',
  });
});

// Force buy/sell volume refresh endpoint
app.post('/api/buy-sell-volume/refresh', async (req: Request, res: Response) => {
  const dailyBuySellVolumeService = getDailyBuySellVolumeService();
  
  if (!dailyBuySellVolumeService) {
    return res.status(400).json({
      error: 'Buy/Sell Volume service not configured',
      message: 'DUNE_API_KEY is required',
    });
  }

  try {
    const result = await dailyBuySellVolumeService.forceRefresh();
    res.json(result);
  } catch (error: any) {
    res.status(500).json({
      error: 'Failed to trigger refresh',
      message: error.message,
    });
  }
});

// Get cumulative volume data for all tokens or a specific token
app.get('/api/buy-sell-volume/cumulative', async (req: Request, res: Response) => {
  const dailyBuySellVolumeService = getDailyBuySellVolumeService();
  
  if (!dailyBuySellVolumeService || !dailyBuySellVolumeService.isReady()) {
    return res.status(503).json({
      error: 'Buy/Sell Volume service not ready',
      message: 'Service is initializing or database is not connected',
    });
  }

  try {
    const token = req.query.token as string | undefined;
    const data = await dailyBuySellVolumeService.getCumulativeVolumes(token);
    
    res.json({
      token: token || 'all',
      count: data.length,
      data,
    });
  } catch (error: any) {
    res.status(500).json({
      error: 'Failed to get cumulative volumes',
      message: error.message,
    });
  }
});

// Get aggregated buy/sell stats
app.get('/api/buy-sell-volume/aggregates', async (req: Request, res: Response) => {
  const dailyBuySellVolumeService = getDailyBuySellVolumeService();
  
  if (!dailyBuySellVolumeService || !dailyBuySellVolumeService.isReady()) {
    return res.status(503).json({
      error: 'Buy/Sell Volume service not ready',
      message: 'Service is initializing or database is not connected',
    });
  }

  try {
    const tokensParam = req.query.tokens as string | undefined;
    const tokens = tokensParam ? tokensParam.split(',').map(t => t.trim()) : undefined;
    
    const aggregates = await dailyBuySellVolumeService.getAggregates(tokens);
    
    // Convert Map to object for JSON response
    const result: Record<string, any> = {};
    for (const [token, stats] of aggregates) {
      result[token] = stats;
    }
    
    res.json({
      count: aggregates.size,
      aggregates: result,
    });
  } catch (error: any) {
    res.status(500).json({
      error: 'Failed to get aggregates',
      message: error.message,
    });
  }
});

// ============================================
// HEALTH & MONITORING ENDPOINTS
// ============================================

// Comprehensive health check endpoint
app.get('/api/health', async (req: Request, res: Response) => {
  const databaseService = getDatabaseService();
  const duneCacheService = getDuneCacheService();
  const hourlyVolumeService = getHourlyVolumeService();
  const tenMinuteVolumeService = getTenMinuteVolumeService();
  const dailyBuySellVolumeService = getDailyBuySellVolumeService();

  const health: Record<string, any> = {
    status: 'healthy',
    timestamp: new Date().toISOString(),
    services: {},
    database: {
      connected: databaseService.isAvailable(),
    },
  };

  // Check each service
  if (duneCacheService) {
    const status = duneCacheService.getCacheStatus();
    health.services.dune_cache = {
      initialized: status.isInitialized,
      refreshing: status.isRefreshing,
      lastRefreshTime: status.lastUpdated ? status.lastUpdated.toISOString() : null,
    };
  }

  if (hourlyVolumeService) {
    health.services.hourly_volume = {
      initialized: hourlyVolumeService.isInitialized,
      databaseConnected: hourlyVolumeService.isDatabaseConnected(),
    };
  }

  if (tenMinuteVolumeService) {
    const status = tenMinuteVolumeService.getStatus();
    health.services.ten_minute_volume = {
      initialized: status.initialized,
      running: status.isRunning,
      refreshing: status.refreshInProgress,
      lastRefreshTime: status.lastRefreshTime,
    };
  }

  if (dailyBuySellVolumeService) {
    const status = dailyBuySellVolumeService.getStatus();
    health.services.daily_buy_sell_volume = {
      initialized: status.initialized,
      refreshing: status.isRefreshing,
      queryConfigured: status.queryIdConfigured,
    };
  }

  // Determine overall health status
  const hasUnhealthyService = Object.values(health.services).some(
    (s: any) => s.initialized === false
  );
  
  if (!databaseService.isAvailable()) {
    health.status = 'degraded';
    health.message = 'Database not connected';
  } else if (hasUnhealthyService) {
    health.status = 'degraded';
    health.message = 'One or more services not initialized';
  }

  res.json(health);
});

// Get metrics history from database
app.get('/api/metrics/history/:metricName', async (req: Request, res: Response) => {
  const databaseService = getDatabaseService();
  
  if (!databaseService.isAvailable()) {
    return res.status(503).json({
      error: 'Database not connected',
      message: 'Metrics history requires database connection',
    });
  }

  try {
    const metricName = req.params.metricName as string;
    if (!metricName) {
      return res.status(400).json({ error: 'Metric name is required' });
    }
    
    const hours = parseInt(req.query.hours as string) || 24;
    const labelsParam = req.query.labels as string | undefined;
    const labels = labelsParam ? JSON.parse(labelsParam) : undefined;

    const data = await databaseService.getRecentMetrics(metricName, hours, labels);
    
    res.json({
      metric: metricName,
      hours,
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

// Get service health history from database
app.get('/api/health/history', async (req: Request, res: Response) => {
  const databaseService = getDatabaseService();
  
  if (!databaseService.isAvailable()) {
    return res.status(503).json({
      error: 'Database not connected',
      message: 'Health history requires database connection',
    });
  }

  try {
    const serviceName = req.query.service as string | undefined;
    const hours = parseInt(req.query.hours as string) || 24;

    const data = await databaseService.getServiceHealthHistory(serviceName, hours);
    
    res.json({
      service: serviceName || 'all',
      hours,
      count: data.length,
      data,
    });
  } catch (error: any) {
    res.status(500).json({
      error: 'Failed to get health history',
      message: error.message,
    });
  }
});

// Manually trigger a health snapshot (useful for debugging)
app.post('/api/health/snapshot', async (req: Request, res: Response) => {
  const databaseService = getDatabaseService();
  
  if (!databaseService.isAvailable()) {
    return res.status(503).json({
      error: 'Database not connected',
    });
  }

  try {
    await saveHealthSnapshots();
    res.json({ message: 'Health snapshot saved successfully' });
  } catch (error: any) {
    res.status(500).json({
      error: 'Failed to save health snapshot',
      message: error.message,
    });
  }
});

// Helper function to save health snapshots to database
async function saveHealthSnapshots(): Promise<void> {
  const databaseService = getDatabaseService();
  const duneCacheService = getDuneCacheService();
  const hourlyVolumeService = getHourlyVolumeService();
  const tenMinuteVolumeService = getTenMinuteVolumeService();
  const dailyBuySellVolumeService = getDailyBuySellVolumeService();

  if (!databaseService.isAvailable()) return;

  // Save Dune cache status
  if (duneCacheService) {
    const status = duneCacheService.getCacheStatus();
    await databaseService.insertServiceHealthSnapshot(
      'dune_cache',
      status.isInitialized,
      status.lastUpdated,
      undefined,
      undefined,
      { isRefreshing: status.isRefreshing }
    );
  }

  // Save hourly volume status
  if (hourlyVolumeService) {
    const recordCount = await databaseService.getHourlyRecordCount();
    await databaseService.insertServiceHealthSnapshot(
      'hourly_volume',
      hourlyVolumeService.isInitialized,
      undefined,
      recordCount,
      undefined,
      { databaseConnected: hourlyVolumeService.isDatabaseConnected() }
    );
  }

  // Save 10-minute volume status
  if (tenMinuteVolumeService) {
    const status = tenMinuteVolumeService.getStatus();
    const recordCount = await databaseService.getTenMinuteRecordCount();
    await databaseService.insertServiceHealthSnapshot(
      'ten_minute_volume',
      status.initialized,
      status.lastRefreshTime ? new Date(status.lastRefreshTime) : undefined,
      recordCount,
      undefined,
      { isRunning: status.isRunning, refreshInProgress: status.refreshInProgress }
    );
  }

  // Save daily buy/sell volume status
  if (dailyBuySellVolumeService) {
    const status = dailyBuySellVolumeService.getStatus();
    const recordCount = await databaseService.getBuySellRecordCount();
    await databaseService.insertServiceHealthSnapshot(
      'daily_buy_sell_volume',
      status.initialized,
      undefined,
      recordCount,
      undefined,
      { isRefreshing: status.isRefreshing, queryConfigured: status.queryIdConfigured }
    );
  }

  // Save database metrics
  const dailyCount = await databaseService.getDailyRecordCount();
  const hourlyCount = await databaseService.getHourlyRecordCount();
  const tenMinCount = await databaseService.getTenMinuteRecordCount();
  const buySellCount = await databaseService.getBuySellRecordCount();

  await databaseService.insertMetricsBatch([
    { name: 'database_record_count', value: dailyCount, labels: { table: 'daily_volumes' } },
    { name: 'database_record_count', value: hourlyCount, labels: { table: 'hourly_volumes' } },
    { name: 'database_record_count', value: tenMinCount, labels: { table: 'ten_minute_volumes' } },
    { name: 'database_record_count', value: buySellCount, labels: { table: 'daily_buy_sell_volumes' } },
  ]);
}

// Force cache refresh endpoint (admin/debug use)
app.post('/api/cache/refresh', async (req: Request, res: Response) => {
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

  // Start refresh in background and return immediately
  duneCacheService.forceRefresh().catch(err => {
    console.error('[Cache] Force refresh failed:', err);
  });

  res.json({
    message: 'Cache refresh started',
    previousLastUpdated: statusBefore.lastUpdated.toISOString(),
  });
});

// Token Supply Endpoints

// Get complete supply info for a token
app.get('/api/supply/:mintAddress', async (req: Request, res: Response) => {
  try {
    const mintAddress = req.params.mintAddress;
    const solanaService = getSolanaService();
    const launchpadService = getLaunchpadService();

    if (!mintAddress || !solanaService.isValidPublicKey(mintAddress)) {
      return res.status(400).json({
        error: 'Invalid mint address',
        message: 'The provided address is not a valid Solana public key',
      });
    }

    // Get complete token allocation breakdown (team, futarchyAMM, meteora, additionalTokens)
    const allocation = await launchpadService.getTokenAllocationBreakdown(
      new PublicKey(mintAddress)
    );

    const supplyInfo = await solanaService.getSupplyInfo(mintAddress, {
      teamPerformancePackage: {
        amount: allocation.teamPerformancePackage.amount,
        address: allocation.teamPerformancePackage.address?.toString(),
      },
      futarchyAmmLiquidity: {
        amount: allocation.futarchyAmmLiquidity.amount,
        vaultAddress: allocation.futarchyAmmLiquidity.vaultAddress?.toString(),
      },
      meteoraLpLiquidity: {
        amount: allocation.meteoraLpLiquidity.amount,
        poolAddress: allocation.meteoraLpLiquidity.poolAddress?.toString(),
        vaultAddress: allocation.meteoraLpLiquidity.vaultAddress?.toString(),
      },
      // Include additional token allocation for v0.7 launches
      additionalTokenAllocation: allocation.additionalTokenAllocation ? {
        amount: allocation.additionalTokenAllocation.amount,
        recipient: allocation.additionalTokenAllocation.recipient.toString(),
        claimed: allocation.additionalTokenAllocation.claimed,
        tokenAccountAddress: allocation.additionalTokenAllocation.tokenAccountAddress?.toString(),
      } : undefined,
      daoAddress: allocation.daoAddress?.toString(),
      launchAddress: allocation.launchAddress?.toString(),
      version: allocation.version,
    });

    res.json({
      result: supplyInfo.totalSupply,
      data: supplyInfo,
    });
  } catch (error: any) {
    console.error('Error in /api/supply/:mintAddress:', error);
    res.status(500).json({
      error: 'Failed to fetch supply info',
      message: error.message || 'Internal server error',
    });
  }
});

// Get total supply for a token
app.get('/api/supply/:mintAddress/total', async (req: Request, res: Response) => {
  try {
    const mintAddress = req.params.mintAddress;
    const solanaService = getSolanaService();

    if (!mintAddress || !solanaService.isValidPublicKey(mintAddress)) {
      return res.status(400).json({
        error: 'Invalid mint address',
        message: 'The provided address is not a valid Solana public key',
      });
    }

    const totalSupply = await solanaService.getTotalSupply(mintAddress);

    res.json({
      result: totalSupply,
    });
  } catch (error: any) {
    console.error('Error in /api/supply/:mintAddress/total:', error);
    res.status(500).json({
      error: 'Failed to fetch total supply',
      message: error.message || 'Internal server error',
    });
  }
});

// Get circulating supply for a token
app.get('/api/supply/:mintAddress/circulating', async (req: Request, res: Response) => {
  try {
    const mintAddress = req.params.mintAddress;
    const solanaService = getSolanaService();
    const launchpadService = getLaunchpadService();

    if (!mintAddress || !solanaService.isValidPublicKey(mintAddress)) {
      return res.status(400).json({
        error: 'Invalid mint address',
        message: 'The provided address is not a valid Solana public key',
      });
    }

    // Get complete token allocation breakdown
    const allocation = await launchpadService.getTokenAllocationBreakdown(
      new PublicKey(mintAddress)
    );

    // Use the full getSupplyInfo method which handles all allocations correctly
    // including team performance package, additional tokens (v0.7), and special cases
    const supplyInfo = await solanaService.getSupplyInfo(mintAddress, {
      teamPerformancePackage: {
        amount: allocation.teamPerformancePackage.amount,
        address: allocation.teamPerformancePackage.address?.toString(),
      },
      futarchyAmmLiquidity: {
        amount: allocation.futarchyAmmLiquidity.amount,
        vaultAddress: allocation.futarchyAmmLiquidity.vaultAddress?.toString(),
      },
      meteoraLpLiquidity: {
        amount: allocation.meteoraLpLiquidity.amount,
        poolAddress: allocation.meteoraLpLiquidity.poolAddress?.toString(),
        vaultAddress: allocation.meteoraLpLiquidity.vaultAddress?.toString(),
      },
      // Include additional token allocation for v0.7 launches
      additionalTokenAllocation: allocation.additionalTokenAllocation ? {
        amount: allocation.additionalTokenAllocation.amount,
        recipient: allocation.additionalTokenAllocation.recipient.toString(),
        claimed: allocation.additionalTokenAllocation.claimed,
        tokenAccountAddress: allocation.additionalTokenAllocation.tokenAccountAddress?.toString(),
      } : undefined,
      daoAddress: allocation.daoAddress?.toString(),
      launchAddress: allocation.launchAddress?.toString(),
      version: allocation.version,
    });

    // Include allocation addresses in response
    const response: { 
      result: string; 
      allocation?: {
        teamPerformancePackageAddress?: string;
        futarchyAmmVaultAddress?: string;
        meteoraPoolAddress?: string;
        meteoraVaultAddress?: string;
        additionalTokenAllocation?: {
          amount: string;
          recipient: string;
          claimed: boolean;
        };
        initialTokenAllocation?: {
          amount: string;
          claimed: boolean;
        };
        daoAddress?: string;
        launchAddress?: string;
        version?: string;
      };
    } = {
      result: supplyInfo.circulatingSupply,
    };
    
    // Add allocation details if any are present
    if (allocation.teamPerformancePackage.address || 
        allocation.futarchyAmmLiquidity.vaultAddress || 
        allocation.meteoraLpLiquidity.poolAddress ||
        allocation.additionalTokenAllocation) {
      response.allocation = {
        teamPerformancePackageAddress: allocation.teamPerformancePackage.address?.toString(),
        futarchyAmmVaultAddress: allocation.futarchyAmmLiquidity.vaultAddress?.toString(),
        meteoraPoolAddress: allocation.meteoraLpLiquidity.poolAddress?.toString(),
        meteoraVaultAddress: allocation.meteoraLpLiquidity.vaultAddress?.toString(),
        additionalTokenAllocation: supplyInfo.allocation?.additionalTokenAllocation,
        initialTokenAllocation: supplyInfo.allocation?.initialTokenAllocation,
        daoAddress: allocation.daoAddress?.toString(),
        launchAddress: allocation.launchAddress?.toString(),
        version: allocation.version,
      };
    }

    res.json(response);
  } catch (error: any) {
    console.error('Error in /api/supply/:mintAddress/circulating:', error);
    res.status(500).json({
      error: 'Failed to fetch circulating supply',
      message: error.message || 'Internal server error',
    });
  }
});

// Jupiter-compatible circulating supply endpoint
app.get('/api/supply/:mintAddress/jupiter/circulating', async (req: Request, res: Response) => {
  try {
    const mintAddress = req.params.mintAddress;
    const solanaService = getSolanaService();
    const launchpadService = getLaunchpadService();

    if (!mintAddress || !solanaService.isValidPublicKey(mintAddress)) {
      return res.status(400).json({ error: 'Invalid mint address' });
    }

    const allocation = await launchpadService.getTokenAllocationBreakdown(
      new PublicKey(mintAddress)
    );

    const supplyInfo = await solanaService.getSupplyInfo(mintAddress, {
      teamPerformancePackage: {
        amount: allocation.teamPerformancePackage.amount,
        address: allocation.teamPerformancePackage.address?.toString(),
      },
      futarchyAmmLiquidity: {
        amount: allocation.futarchyAmmLiquidity.amount,
        vaultAddress: allocation.futarchyAmmLiquidity.vaultAddress?.toString(),
      },
      meteoraLpLiquidity: {
        amount: allocation.meteoraLpLiquidity.amount,
        poolAddress: allocation.meteoraLpLiquidity.poolAddress?.toString(),
        vaultAddress: allocation.meteoraLpLiquidity.vaultAddress?.toString(),
      },
      additionalTokenAllocation: allocation.additionalTokenAllocation ? {
        amount: allocation.additionalTokenAllocation.amount,
        recipient: allocation.additionalTokenAllocation.recipient.toString(),
        claimed: allocation.additionalTokenAllocation.claimed,
        tokenAccountAddress: allocation.additionalTokenAllocation.tokenAccountAddress?.toString(),
      } : undefined,
      daoAddress: allocation.daoAddress?.toString(),
      launchAddress: allocation.launchAddress?.toString(),
      version: allocation.version,
    });

    res.json({ circulatingSupply: parseFloat(supplyInfo.circulatingSupply) });
  } catch (error: any) {
    console.error('Error in /api/supply/:mintAddress/jupiter/circulating:', error);
    res.status(500).json({ error: 'Failed to fetch circulating supply' });
  }
});

// Jupiter-compatible total supply endpoint
app.get('/api/supply/:mintAddress/jupiter/total', async (req: Request, res: Response) => {
  try {
    const mintAddress = req.params.mintAddress;
    const solanaService = getSolanaService();

    if (!mintAddress || !solanaService.isValidPublicKey(mintAddress)) {
      return res.status(400).json({ error: 'Invalid mint address' });
    }

    const supplyInfo = await solanaService.getSupplyInfo(mintAddress);

    res.json({ totalSupply: parseFloat(supplyInfo.totalSupply) });
  } catch (error: any) {
    console.error('Error in /api/supply/:mintAddress/jupiter/total:', error);
    res.status(500).json({ error: 'Failed to fetch total supply' });
  }
});

// Manual Dune query execution endpoint
app.post('/api/dune/execute', async (req: Request, res: Response) => {
  try {
    const duneService = getDuneService();
    if (!duneService) {
      return res.status(400).json({ error: 'Dune service not configured' });
    }

    const { queryId, parameters, tokenAddresses, sqlQuery } = req.body;
    
    let result: any;
    
    if (sqlQuery) {
      // Execute raw SQL query
      result = await duneService.executeRawQuery(sqlQuery, 'Manual Query');
    } else if (tokenAddresses) {
      // Execute generated query with token filter
      result = await duneService.executeGeneratedQuery(tokenAddresses);
    } else if (queryId) {
      // Execute existing query by ID
      result = await duneService.executeQueryManually(queryId, parameters);
    } else {
      return res.status(400).json({ 
        error: 'Either queryId, tokenAddresses, or sqlQuery must be provided' 
      });
    }

    res.json(result);
  } catch (error: any) {
    console.error('Error executing Dune query:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// Aggregate Volume Endpoint - Daily volume data with totals for all DAO tokens
app.get('/api/volume/aggregate', async (req: Request, res: Response) => {
  try {
    const duneCacheService = getDuneCacheService();
    
    if (!duneCacheService) {
      return res.status(400).json({ 
        error: 'Dune cache service not configured',
        message: 'DUNE_API_KEY environment variable is required'
      });
    }

    // Get cached aggregate volume data
    const cachedAggregateVolume = duneCacheService.getAggregateVolume();
    
    if (!cachedAggregateVolume) {
      const cacheStatus = duneCacheService.getCacheStatus();
      if (cacheStatus.isRefreshing) {
        return res.status(503).json({
          error: 'Cache is being populated',
          message: 'The server is still loading data. Please try again in a few minutes.',
          cacheStatus: {
            isRefreshing: true,
            isInitialized: cacheStatus.isInitialized,
          }
        });
      }
      return res.status(404).json({
        error: 'No aggregate volume data available',
        message: 'Cache has not been populated yet. Please wait for the next refresh cycle.'
      });
    }

    const cacheStatus = duneCacheService.getCacheStatus();
    console.log(`[Volume] Returning cached aggregate volume (age: ${Math.round(cacheStatus.cacheAgeMs / 1000)}s, ${cachedAggregateVolume.tokens.length} tokens)`);

    res.json(cachedAggregateVolume);
  } catch (error: any) {
    console.error('Error in /api/volume/aggregate:', error);
    res.status(500).json({ 
      error: 'Failed to fetch aggregate volume',
      message: error.message || 'Internal server error'
    });
  }
});

// Get generated SQL query endpoint
app.get('/api/dune/query', (req: Request, res: Response) => {
  try {
    const duneService = getDuneService();
    if (!duneService) {
      return res.status(400).json({ error: 'Dune service not configured' });
    }

    const tokenAddresses = req.query.tokens 
      ? (req.query.tokens as string).split(',').map(t => t.trim())
      : undefined;

    const sqlQuery = duneService.generate24hMetricsQuery(tokenAddresses);
    
    res.json({
      sql: sqlQuery,
      tokenAddresses: tokenAddresses || 'all',
    });
  } catch (error: any) {
    console.error('Error generating query:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// Debug endpoint to test Dune query with current DAOs
app.get('/api/dune/debug', async (req: Request, res: Response) => {
  try {
    const futarchyService = getFutarchyService();
    const duneService = getDuneService();
    
    if (!duneService) {
      return res.status(400).json({ error: 'Dune service not configured' });
    }

    // Fetch all DAOs
    const allDaos = await futarchyService.getAllDaos();
    const baseMintAddresses = allDaos.map(dao => dao.baseMint.toString());
    const daoAddresses = allDaos.map(dao => dao.daoAddress.toString());

    // Build parameters as the service does
    const parameters: Record<string, any> = {};
    if (baseMintAddresses && baseMintAddresses.length > 0) {
      parameters.token_list = baseMintAddresses.join(',');
    } else {
      parameters.token_list = '';
    }

    // Try to execute with parameterized query
    let executionResult: any = null;
    let error: any = null;
    let queryResults: any = null;
    try {
      if (config.dune.queryId) {
        // Use parameterized query
        const executeResult = await (duneService as any).executeQuery(config.dune.queryId, parameters);
        console.log('[Debug] Execution ID:', executeResult.execution_id);
        queryResults = await (duneService as any).getQueryResults(executeResult.execution_id);
        executionResult = { 
          execution_id: executeResult.execution_id, 
          success: true,
          rows: queryResults.rows,
          metadata: queryResults.metadata,
        };
      }
    } catch (e: any) {
      error = {
        message: e.message,
        stack: e.stack,
      };
    }

    res.json({
      config: {
        hasApiKey: !!config.dune.apiKey,
        hasQueryId: !!config.dune.queryId,
        queryId: config.dune.queryId,
      },
      daos: {
        count: allDaos.length,
        baseMintAddresses: baseMintAddresses.slice(0, 10),
        daoAddresses: daoAddresses.slice(0, 10),
      },
      parameters: {
        token_list: parameters.token_list,
        token_list_length: parameters.token_list.length,
        token_count: baseMintAddresses.length,
        token_list_preview: parameters.token_list.substring(0, 200),
      },
      execution: error ? {
        error,
      } : {
        success: true,
        execution_id: executionResult?.execution_id,
        rowsReturned: queryResults?.rows?.length || 0,
        metadata: queryResults?.metadata || {},
        sampleRows: queryResults?.rows?.slice(0, 3) || [],
        allTokens: queryResults?.rows?.map((r: any) => r.token || r.pool_id) || [],
      },
    });
  } catch (error: any) {
    console.error('Error in debug endpoint:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// Root endpoint
app.get('/', (req: Request, res: Response) => {
  const duneCacheService = getDuneCacheService();
  const cacheStatus = duneCacheService?.getCacheStatus();
  
  res.json({
    name: 'Futarchy AMM - CoinGecko API',
    version: '1.0.0',
    documentation: 'https://docs.coingecko.com/reference/exchanges-list',
    endpoints: {
      tickers: '/api/tickers - Returns all DAO tickers with pricing and volume',
      supply: '/api/supply/:mintAddress - Returns complete supply breakdown with allocation details',
      supply_total: '/api/supply/:mintAddress/total - Returns total supply only',
      supply_circulating: '/api/supply/:mintAddress/circulating - Returns circulating supply (excludes team performance package)',
      volume_aggregate: '/api/volume/aggregate - Returns aggregate volume with daily breakdown for all DAO tokens since launch',
      cache_status: '/api/cache/status - Returns Dune cache status information',
      cache_refresh: 'POST /api/cache/refresh - Force a cache refresh',
      health: '/health',
    },
    dex: {
      fork_type: config.dex.forkType,
      factory_address: config.dex.factoryAddress,
      router_address: config.dex.routerAddress,
    },
    supplyBreakdown: {
      description: 'For launchpad tokens, supply is broken down into:',
      circulatingSupply: 'Total supply minus team performance package (liquidity IS circulating)',
      teamPerformancePackage: 'Locked tokens allocated to the team (price-based unlock) - NOT circulating',
      futarchyAmmLiquidity: 'Tokens in the internal FutarchyAMM for spot trading - IS circulating',
      meteoraLpLiquidity: 'Tokens in the external Meteora DAMM pool (POL) - IS circulating',
    },
    caching: {
      description: 'Dune data is cached and refreshed hourly to improve response times',
      refreshInterval: `${parseInt(process.env.DUNE_CACHE_REFRESH_INTERVAL || '3600')} seconds`,
      fetchTimeout: `${parseInt(process.env.DUNE_FETCH_TIMEOUT || '240')} seconds`,
      status: cacheStatus ? {
        isInitialized: cacheStatus.isInitialized,
        poolMetricsCount: cacheStatus.poolMetricsCount,
        lastUpdated: cacheStatus.lastUpdated.toISOString(),
      } : 'Not available (DUNE_API_KEY not set)',
    },
    note: 'This API automatically discovers and aggregates all DAOs from the Futarchy protocol.',
  });
});

// Error handling
app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

// Start server and initialize cache
const server = app.listen(config.server.port, async () => {
  console.log(`✅ CoinGecko API running on port ${config.server.port}`);
  console.log(`📊 Tickers: http://localhost:${config.server.port}/api/tickers`);
  console.log(`📈 Health: http://localhost:${config.server.port}/health`);
  
  // Start the Volume History service (handles DB storage and incremental Dune fetching for historical data)
  const volumeHistoryService = getVolumeHistoryService();
  if (volumeHistoryService) {
    console.log('🗄️ Starting Volume History service...');
    try {
      await volumeHistoryService.start();
      console.log('✅ Volume History service started successfully');
    } catch (error) {
      console.error('❌ Failed to start Volume History service:', error);
    }
  } else {
    console.log('ℹ️ Volume History service not available - requires DUNE_API_KEY');
  }
  
  // Start the Hourly Volume service (handles hourly aggregates, fallback for rolling 24h)
  const hourlyVolumeService = getHourlyVolumeService();
  if (hourlyVolumeService) {
    console.log('⏰ Starting Hourly Volume service...');
    try {
      await hourlyVolumeService.start();
      console.log('✅ Hourly Volume service started successfully');
    } catch (error) {
      console.error('❌ Failed to start Hourly Volume service:', error);
    }
  } else {
    console.log('ℹ️ Hourly Volume service not available - requires DUNE_API_KEY and DUNE_HOURLY_VOLUME_QUERY_ID');
  }
  
  // Start the 10-Minute Volume service (PRIMARY for /api/tickers rolling 24h metrics)
  const tenMinuteVolumeService = getTenMinuteVolumeService();
  if (tenMinuteVolumeService) {
    console.log('📊 Starting 10-Minute Volume service (PRIMARY for /api/tickers)...');
    try {
      await tenMinuteVolumeService.start();
      if (tenMinuteVolumeService.isInitialized) {
        console.log('✅ 10-Minute Volume service started successfully');
      } else {
        console.log('⚠️ 10-Minute Volume service could not initialize (no data in DB and no query ID)');
      }
    } catch (error) {
      console.error('❌ Failed to start 10-Minute Volume service:', error);
    }
  } else {
    console.log('ℹ️ 10-Minute Volume service not available - requires DUNE_API_KEY');
  }
  
  // Start the Daily Buy/Sell Volume service (tracks directional volume)
  const dailyBuySellVolumeService = getDailyBuySellVolumeService();
  if (dailyBuySellVolumeService) {
    console.log('📈 Starting Daily Buy/Sell Volume service...');
    try {
      await dailyBuySellVolumeService.initialize();
      dailyBuySellVolumeService.start();
      if (dailyBuySellVolumeService.isReady()) {
        console.log('✅ Daily Buy/Sell Volume service started successfully');
      } else {
        console.log('⚠️ Daily Buy/Sell Volume service waiting for data or configuration');
      }
    } catch (error) {
      console.error('❌ Failed to start Daily Buy/Sell Volume service:', error);
    }
  } else {
    console.log('ℹ️ Daily Buy/Sell Volume service not available - requires DUNE_API_KEY');
  }
  
  // Start the Dune cache service with hourly refresh
  const duneCacheService = getDuneCacheService();
  if (duneCacheService) {
    console.log('🔄 Starting Dune cache service...');
    try {
      await duneCacheService.start();
      console.log('✅ Dune cache service started successfully');
    } catch (error) {
      console.error('❌ Failed to start Dune cache service:', error);
    }
  } else {
    console.warn('⚠️ Dune cache service not available - set DUNE_API_KEY to enable caching');
  }

  // Start periodic health snapshots (every 5 minutes)
  const HEALTH_SNAPSHOT_INTERVAL = 5 * 60 * 1000; // 5 minutes
  setInterval(async () => {
    try {
      await saveHealthSnapshots();
    } catch (error) {
      console.error('[Health] Error saving periodic health snapshot:', error);
    }
  }, HEALTH_SNAPSHOT_INTERVAL);
  console.log(`📊 Health snapshots will be saved every ${HEALTH_SNAPSHOT_INTERVAL / 60000} minutes`);

  // Prune old metrics data daily (keep 30 days)
  const METRICS_PRUNE_INTERVAL = 24 * 60 * 60 * 1000; // 24 hours
  setInterval(async () => {
    const databaseService = getDatabaseService();
    if (databaseService.isAvailable()) {
      await databaseService.pruneOldMetrics(30);
    }
  }, METRICS_PRUNE_INTERVAL);
  console.log('🧹 Metrics pruning scheduled (keeping last 30 days)');

  // Save initial health snapshot
  setTimeout(async () => {
    try {
      await saveHealthSnapshots();
      console.log('📊 Initial health snapshot saved');
    } catch (error) {
      console.error('[Health] Error saving initial health snapshot:', error);
    }
  }, 10000); // Wait 10 seconds for services to stabilize
});

// Set server timeouts to handle long-running requests (default: 5 minutes)
server.timeout = config.server.requestTimeout;
server.keepAliveTimeout = config.server.keepAliveTimeout;
// Headers timeout should be slightly longer than keepAliveTimeout
server.headersTimeout = config.server.keepAliveTimeout + 1000;
console.log(`⏱️ Server timeouts configured: request=${config.server.requestTimeout}ms, keepAlive=${config.server.keepAliveTimeout}ms`);

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down gracefully...');
  const duneCacheService = getDuneCacheService();
  if (duneCacheService) {
    duneCacheService.stop();
  }
  const volumeHistoryService = getVolumeHistoryService();
  if (volumeHistoryService) {
    volumeHistoryService.stop();
  }
  const hourlyVolumeService = getHourlyVolumeService();
  if (hourlyVolumeService) {
    hourlyVolumeService.stop();
  }
  const tenMinuteVolumeService = getTenMinuteVolumeService();
  if (tenMinuteVolumeService) {
    tenMinuteVolumeService.stop();
  }
  const dailyBuySellVolumeService = getDailyBuySellVolumeService();
  if (dailyBuySellVolumeService) {
    dailyBuySellVolumeService.stop();
  }
  const databaseService = getDatabaseService();
  if (databaseService) {
    await databaseService.close();
  }
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});

process.on('SIGINT', async () => {
  console.log('SIGINT received, shutting down gracefully...');
  const duneCacheService = getDuneCacheService();
  if (duneCacheService) {
    duneCacheService.stop();
  }
  const volumeHistoryService = getVolumeHistoryService();
  if (volumeHistoryService) {
    volumeHistoryService.stop();
  }
  const hourlyVolumeService = getHourlyVolumeService();
  if (hourlyVolumeService) {
    hourlyVolumeService.stop();
  }
  const tenMinuteVolumeService = getTenMinuteVolumeService();
  if (tenMinuteVolumeService) {
    tenMinuteVolumeService.stop();
  }
  const dailyBuySellVolumeService = getDailyBuySellVolumeService();
  if (dailyBuySellVolumeService) {
    dailyBuySellVolumeService.stop();
  }
  const databaseService = getDatabaseService();
  if (databaseService) {
    await databaseService.close();
  }
  server.close(() => {
    console.log('Server closed');
    process.exit(0);
  });
});

export default app;