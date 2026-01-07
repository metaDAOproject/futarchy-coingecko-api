import { DuneService, DunePoolMetrics, DuneAggregateVolumeResponse } from './duneService.js';
import { FutarchyService } from './futarchyService.js';
import { VolumeHistoryService } from './volumeHistoryService.js';

export interface CachedDuneData {
  poolMetrics: Map<string, DunePoolMetrics>;
  aggregateVolume: DuneAggregateVolumeResponse | null;
  lastUpdated: Date;
  isRefreshing: boolean;
}

export class DuneCacheService {
  private duneService: DuneService;
  private futarchyService: FutarchyService;
  private volumeHistoryService: VolumeHistoryService | null = null;
  private cache: CachedDuneData;
  private refreshInterval: ReturnType<typeof setInterval> | null = null;
  private refreshIntervalMs: number;
  private isInitialized: boolean = false;

  constructor(duneService: DuneService, futarchyService: FutarchyService, volumeHistoryService?: VolumeHistoryService) {
    this.duneService = duneService;
    this.futarchyService = futarchyService;
    this.volumeHistoryService = volumeHistoryService || null;
    
    // Default to 1 hour refresh interval, configurable via environment
    this.refreshIntervalMs = parseInt(process.env.DUNE_CACHE_REFRESH_INTERVAL || '3600') * 1000;
    
    // Initialize empty cache
    this.cache = {
      poolMetrics: new Map(),
      aggregateVolume: null,
      lastUpdated: new Date(0), // Epoch - indicates never updated
      isRefreshing: false,
    };
  }

  /**
   * Start the cache refresh cron job
   * Should be called when the server starts
   */
  async start(): Promise<void> {
    console.log(`[DuneCache] Starting cache service with ${this.refreshIntervalMs / 1000}s refresh interval`);
    
    // Mark as initialized immediately - other services (10-min, hourly) handle real-time data
    // DuneCacheService is now a fallback/supplementary cache
    this.isInitialized = true;
    
    // Set up the cron job to refresh every hour (or configured interval)
    this.refreshInterval = setInterval(async () => {
      await this.refreshCache();
    }, this.refreshIntervalMs);
    
    console.log(`[DuneCache] Cache service started, next refresh in ${this.refreshIntervalMs / 1000}s`);
    
    // Do initial refresh in background (non-blocking)
    // Primary volume data comes from 10-min/hourly services with DB backing
    console.log('[DuneCache] Starting background cache refresh...');
    this.refreshCache().catch(err => {
      console.error('[DuneCache] Background refresh failed:', err.message);
    });
  }

  /**
   * Stop the cache refresh cron job
   */
  stop(): void {
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
      this.refreshInterval = null;
      console.log('[DuneCache] Cache service stopped');
    }
  }

  /**
   * Refresh the cache by fetching fresh data from Dune
   */
  async refreshCache(): Promise<void> {
    if (this.cache.isRefreshing) {
      console.log('[DuneCache] Refresh already in progress, skipping');
      return;
    }

    this.cache.isRefreshing = true;
    console.log('[DuneCache] Starting cache refresh...');
    const startTime = Date.now();

    try {
      // Fetch all DAOs to get token addresses
      const allDaos = await this.futarchyService.getAllDaos();
      const baseMintAddresses = allDaos.map(dao => dao.baseMint.toString());
      
      console.log(`[DuneCache] Refreshing data for ${baseMintAddresses.length} tokens`);

      // Refresh pool metrics (24h data)
      try {
        const poolMetrics = await this.duneService.getAllPoolsMetrics24h(baseMintAddresses);
        
        // Create a map from token (baseMint) to DAO address
        const tokenToDaoMap = new Map<string, string>();
        for (const dao of allDaos) {
          tokenToDaoMap.set(dao.baseMint.toString().toLowerCase(), dao.daoAddress.toString().toLowerCase());
        }

        // Remap from token address to DAO address
        const daoMetricsMap = new Map<string, DunePoolMetrics>();
        for (const [tokenAddress, metrics] of poolMetrics.entries()) {
          const daoAddress = tokenToDaoMap.get(tokenAddress.toLowerCase());
          if (daoAddress) {
            daoMetricsMap.set(daoAddress, metrics);
          }
        }

        this.cache.poolMetrics = daoMetricsMap;
        console.log(`[DuneCache] Refreshed pool metrics for ${daoMetricsMap.size} DAOs`);
      } catch (error: any) {
        console.error('[DuneCache] Error refreshing pool metrics:', error.message);
        // Keep existing cache on error
      }

      // Refresh aggregate volume data
      // Use VolumeHistoryService if available (it handles DB storage and incremental fetching)
      try {
        let aggregateVolume: DuneAggregateVolumeResponse;
        
        if (this.volumeHistoryService) {
          // VolumeHistoryService handles DB storage, incremental fetching, and caching
          aggregateVolume = await this.volumeHistoryService.getAggregateVolume(baseMintAddresses);
          console.log(`[DuneCache] Got aggregate volume from VolumeHistoryService for ${aggregateVolume.tokens.length} tokens`);
        } else {
          // Fall back to direct Dune query (no DB storage)
          aggregateVolume = await this.duneService.getAggregateVolume(baseMintAddresses, true);
          console.log(`[DuneCache] Got aggregate volume from Dune for ${aggregateVolume.tokens.length} tokens`);
        }
        
        this.cache.aggregateVolume = aggregateVolume;
      } catch (error: any) {
        console.error('[DuneCache] Error refreshing aggregate volume:', error.message);
        // Keep existing cache on error
      }

      this.cache.lastUpdated = new Date();
      const duration = Date.now() - startTime;
      console.log(`[DuneCache] Cache refresh completed in ${duration}ms`);
    } catch (error: any) {
      console.error('[DuneCache] Error during cache refresh:', error.message);
    } finally {
      this.cache.isRefreshing = false;
    }
  }

  /**
   * Force an immediate cache refresh
   */
  async forceRefresh(): Promise<void> {
    console.log('[DuneCache] Force refresh requested');
    await this.refreshCache();
  }

  /**
   * Get cached pool metrics
   * Returns null if cache is empty and not yet initialized
   */
  getPoolMetrics(): Map<string, DunePoolMetrics> | null {
    if (!this.isInitialized && this.cache.poolMetrics.size === 0) {
      return null;
    }
    return this.cache.poolMetrics;
  }

  /**
   * Get cached aggregate volume data
   * Returns null if cache is empty and not yet initialized
   */
  getAggregateVolume(): DuneAggregateVolumeResponse | null {
    return this.cache.aggregateVolume;
  }

  /**
   * Get cache status information
   */
  getCacheStatus(): {
    lastUpdated: Date;
    isRefreshing: boolean;
    poolMetricsCount: number;
    aggregateVolumeTokenCount: number;
    cacheAgeMs: number;
    isInitialized: boolean;
    usingVolumeHistoryService: boolean;
  } {
    return {
      lastUpdated: this.cache.lastUpdated,
      isRefreshing: this.cache.isRefreshing,
      poolMetricsCount: this.cache.poolMetrics.size,
      aggregateVolumeTokenCount: this.cache.aggregateVolume?.tokens.length || 0,
      cacheAgeMs: Date.now() - this.cache.lastUpdated.getTime(),
      isInitialized: this.isInitialized,
      usingVolumeHistoryService: this.volumeHistoryService !== null,
    };
  }

  /**
   * Get VolumeHistoryService for direct access (e.g., for status endpoint)
   */
  getVolumeHistoryService(): VolumeHistoryService | null {
    return this.volumeHistoryService;
  }

  /**
   * Check if cache has data
   */
  hasData(): boolean {
    return this.cache.poolMetrics.size > 0 || this.cache.aggregateVolume !== null;
  }
}

