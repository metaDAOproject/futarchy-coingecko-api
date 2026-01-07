/**
 * TenMinuteVolumeService
 * 
 * Manages 10-minute granularity volume data for accurate rolling 24h calculations.
 * This is the PRIMARY service for /api/tickers 24h volume data.
 * 
 * Schedule:
 * - Every 10 minutes: Fetch current incomplete bucket + update complete buckets
 * - Stores last 25 hours of data (150 buckets per token)
 * - Prunes data older than 25 hours
 * 
 * 10-minute buckets: :00, :10, :20, :30, :40, :50
 */

import { config } from '../config';
import { DuneService } from './duneService';
import { DatabaseService, TenMinuteVolumeRecord, Rolling24hMetrics } from './databaseService';
import { FutarchyService } from './futarchyService';

export class TenMinuteVolumeService {
  private duneService: DuneService;
  private databaseService: DatabaseService;
  private futarchyService: FutarchyService;
  private refreshIntervalId: NodeJS.Timeout | null = null;
  private isRunning: boolean = false;
  private initialized: boolean = false;
  private lastRefreshTime: Date | null = null;
  private refreshInProgress: boolean = false;

  // 10-minute refresh interval (default)
  private readonly REFRESH_INTERVAL_MS = 10 * 60 * 1000; // 10 minutes

  constructor(
    duneService: DuneService,
    databaseService: DatabaseService,
    futarchyService: FutarchyService
  ) {
    this.duneService = duneService;
    this.databaseService = databaseService;
    this.futarchyService = futarchyService;
  }

  get isInitialized(): boolean {
    return this.initialized;
  }

  isDatabaseConnected(): boolean {
    return this.databaseService.isAvailable();
  }

  /**
   * Initialize the service - check database and optionally backfill
   */
  async initialize(): Promise<void> {
    if (!this.databaseService.isAvailable()) {
      console.log('[TenMinVolume] Database not connected - service disabled');
      return;
    }

    console.log('[TenMinVolume] Initializing 10-minute volume service...');

    try {
      // Check if we have data in the database
      const recordCount = await this.databaseService.getTenMinuteRecordCount();
      console.log(`[TenMinVolume] Current record count in database: ${recordCount}`);

      // Get the latest bucket
      const latestBucket = await this.databaseService.getLatestTenMinuteBucket();
      if (latestBucket) {
        console.log(`[TenMinVolume] Latest bucket in DB: ${latestBucket}`);
      }

      // If we have data, we can serve it even without Dune query ID
      if (recordCount > 0) {
        this.initialized = true;
        console.log(`[TenMinVolume] Service initialized with ${recordCount} existing records`);
      }

      // Only attempt backfill if query ID is configured
      if (config.dune.tenMinuteVolumeQueryId) {
        // Calculate 24 hours ago
        const now = new Date();
        const twentyFourHoursAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);

        // Determine start time for backfill
        let startTime: Date;
        if (latestBucket) {
          const latestDate = new Date(latestBucket);
          // Start from the bucket after the latest one, or 24h ago, whichever is earlier
          startTime = new Date(Math.min(latestDate.getTime(), twentyFourHoursAgo.getTime()));
        } else {
          // No data - backfill last 24 hours
          startTime = twentyFourHoursAgo;
          console.log('[TenMinVolume] No existing data - will backfill last 24 hours');
        }

        // Backfill from start time to now
        console.log(`[TenMinVolume] Backfilling from ${startTime.toISOString()}`);
        await this.fetchAndStore(startTime, true);

        // Prune old data
        await this.databaseService.pruneOldTenMinuteData(25);

        this.initialized = true;
        console.log('[TenMinVolume] Initialization complete');
      } else if (!this.initialized) {
        console.log('[TenMinVolume] No DUNE_TEN_MINUTE_VOLUME_QUERY_ID configured - cannot fetch new data');
        console.log('[TenMinVolume] Service will not be available until query ID is set or data exists in DB');
      }
    } catch (error: any) {
      console.error('[TenMinVolume] Error during initialization:', error.message);
      // Still mark as initialized if we have data so we can serve it
      const recordCount = await this.databaseService.getTenMinuteRecordCount();
      if (recordCount > 0) {
        this.initialized = true;
        console.log('[TenMinVolume] Serving existing data despite initialization error');
      }
    }
  }

  /**
   * Start the background refresh loop
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      console.log('[TenMinVolume] Service already running');
      return;
    }

    // Initialize first (will check DB and potentially backfill)
    await this.initialize();

    // If no query ID, we can still serve existing data but won't refresh
    if (!config.dune.tenMinuteVolumeQueryId) {
      if (this.initialized) {
        console.log('[TenMinVolume] Serving existing DB data (no refresh loop - set DUNE_TEN_MINUTE_VOLUME_QUERY_ID to enable)');
      }
      return;
    }

    this.isRunning = true;

    // Calculate time to next 10-minute boundary
    const now = new Date();
    const currentMinute = now.getMinutes();
    const nextBoundary = Math.ceil((currentMinute + 1) / 10) * 10;
    const minutesToNext = nextBoundary - currentMinute;
    const msToNext = (minutesToNext * 60 - now.getSeconds()) * 1000 - now.getMilliseconds();

    console.log(`[TenMinVolume] Starting refresh loop. Next refresh in ${Math.round(msToNext / 1000)}s (at :${String(nextBoundary % 60).padStart(2, '0')})`);

    // Schedule first refresh at next 10-minute boundary
    setTimeout(async () => {
      await this.refresh();
      
      // Then start regular interval
      this.refreshIntervalId = setInterval(async () => {
        await this.refresh();
      }, this.REFRESH_INTERVAL_MS);
    }, msToNext);
  }

  /**
   * Stop the background refresh loop
   */
  stop(): void {
    if (this.refreshIntervalId) {
      clearInterval(this.refreshIntervalId);
      this.refreshIntervalId = null;
    }
    this.isRunning = false;
    console.log('[TenMinVolume] Service stopped');
  }

  /**
   * Perform a refresh - fetch recent 10-minute data
   */
  async refresh(): Promise<void> {
    if (this.refreshInProgress) {
      console.log('[TenMinVolume] Refresh already in progress, skipping');
      return;
    }

    this.refreshInProgress = true;
    console.log('[TenMinVolume] Starting refresh...');

    try {
      // Fetch data from the last 20 minutes to catch any delayed data
      const startTime = new Date(Date.now() - 20 * 60 * 1000);
      await this.fetchAndStore(startTime, false);

      // Mark previous buckets as complete
      const currentBucket = this.getCurrentBucketStart();
      await this.databaseService.markTenMinuteBucketsComplete(currentBucket.toISOString());

      // Prune old data periodically
      await this.databaseService.pruneOldTenMinuteData(25);

      this.lastRefreshTime = new Date();
      console.log('[TenMinVolume] Refresh complete');
    } catch (error: any) {
      console.error('[TenMinVolume] Error during refresh:', error.message);
    } finally {
      this.refreshInProgress = false;
    }
  }

  /**
   * Force a full refresh (backfill last 24h)
   */
  async forceRefresh(): Promise<void> {
    console.log('[TenMinVolume] Force refresh requested - backfilling last 24h');
    const startTime = new Date(Date.now() - 24 * 60 * 60 * 1000);
    await this.fetchAndStore(startTime, true);
    await this.databaseService.pruneOldTenMinuteData(25);
    this.lastRefreshTime = new Date();
  }

  /**
   * Get the start of the current 10-minute bucket
   */
  private getCurrentBucketStart(): Date {
    const now = new Date();
    const bucketMinute = Math.floor(now.getMinutes() / 10) * 10;
    return new Date(
      now.getFullYear(),
      now.getMonth(),
      now.getDate(),
      now.getHours(),
      bucketMinute,
      0,
      0
    );
  }

  /**
   * Fetch 10-minute data from Dune and store in database
   */
  private async fetchAndStore(startTime: Date, markHistoricalComplete: boolean): Promise<void> {
    const queryId = config.dune.tenMinuteVolumeQueryId;
    if (!queryId) return;

    try {
      // Get the filtered list of DAOs from FutarchyService
      const allDaos = await this.futarchyService.getAllDaos();
      const tokenAddresses = allDaos.map(dao => dao.baseMint.toString());

      console.log(`[TenMinVolume] Fetching from Dune query ${queryId} with start_time: ${startTime.toISOString()} for ${tokenAddresses.length} tokens`);

      const parameters: Record<string, any> = {
        start_time: startTime.toISOString().replace('T', ' ').replace('Z', ''),
      };

      // Format token list
      if (tokenAddresses.length > 0) {
        parameters.token_list = tokenAddresses.map(token => `'${token}'`).join(', ');
      } else {
        parameters.token_list = "'__ALL__'";
      }

      // Execute the query
      const result = await this.duneService.executeQueryManually(queryId, parameters);

      if (!result || !result.rows) {
        console.log('[TenMinVolume] No results from Dune');
        return;
      }

      const rows = result.rows;
      console.log(`[TenMinVolume] Received ${rows.length} rows from Dune`);

      if (rows.length === 0) return;

      // Transform to records
      const records: TenMinuteVolumeRecord[] = rows.map((row: any) => ({
        token: row.token,
        bucket: this.parseDuneBucket(row.bucket),
        base_volume: row.base_volume || '0',
        target_volume: row.target_volume || '0',
        high: row.high || '0',
        low: row.low || '0',
        trade_count: parseInt(row.trade_count || '0'),
      }));

      // For historical backfill, mark all as complete except current bucket
      if (markHistoricalComplete) {
        const currentBucket = this.getCurrentBucketStart();
        const completeRecords = records.filter(r => new Date(r.bucket) < currentBucket);
        const incompleteRecords = records.filter(r => new Date(r.bucket) >= currentBucket);

        if (completeRecords.length > 0) {
          await this.databaseService.upsertTenMinuteVolumes(completeRecords, true);
        }
        if (incompleteRecords.length > 0) {
          await this.databaseService.upsertTenMinuteVolumes(incompleteRecords, false);
        }
      } else {
        // Regular refresh - mark past buckets complete
        const currentBucket = this.getCurrentBucketStart();
        for (const record of records) {
          const isComplete = new Date(record.bucket) < currentBucket;
          await this.databaseService.upsertTenMinuteVolumes([record], isComplete);
        }
      }

    } catch (error: any) {
      console.error('[TenMinVolume] Error fetching from Dune:', error.message);
      throw error;
    }
  }

  /**
   * Parse Dune's bucket timestamp format
   */
  private parseDuneBucket(bucket: string): string {
    // Dune returns bucket as "2026-01-07 12:30:00" or similar
    if (bucket.includes('T')) {
      return bucket; // Already ISO format
    }
    // Convert to ISO format
    return bucket.replace(' ', 'T') + 'Z';
  }

  /**
   * Get rolling 24h metrics from the 10-minute data
   * This is the PRIMARY method for /api/tickers 24h volume
   */
  async getRolling24hMetrics(baseMintAddresses?: string[]): Promise<Map<string, { base_volume_24h: number; target_volume_24h: number; high_24h: number; low_24h: number }>> {
    if (!this.initialized || !this.databaseService.isAvailable()) {
      console.log('[TenMinVolume] Service not ready, returning empty metrics');
      return new Map();
    }

    try {
      // Get rolling 24h data from database
      const dbMetrics = await this.databaseService.getRolling24hFromTenMinute(baseMintAddresses);

      // Convert to metrics format with numbers
      const result = new Map<string, { base_volume_24h: number; target_volume_24h: number; high_24h: number; low_24h: number }>();
      for (const [token, metrics] of dbMetrics) {
        result.set(token, {
          base_volume_24h: parseFloat(metrics.base_volume_24h) || 0,
          target_volume_24h: parseFloat(metrics.target_volume_24h) || 0,
          high_24h: parseFloat(metrics.high_24h) || 0,
          low_24h: parseFloat(metrics.low_24h) || 0,
        });
      }

      console.log(`[TenMinVolume] Returning rolling 24h metrics for ${result.size} tokens`);
      return result;
    } catch (error: any) {
      console.error('[TenMinVolume] Error getting rolling 24h metrics:', error.message);
      return new Map();
    }
  }

  /**
   * Get service status for monitoring
   */
  getStatus(): {
    initialized: boolean;
    isRunning: boolean;
    databaseConnected: boolean;
    lastRefreshTime: string | null;
    queryId: number | undefined;
    refreshInProgress: boolean;
  } {
    return {
      initialized: this.initialized,
      isRunning: this.isRunning,
      databaseConnected: this.databaseService.isAvailable(),
      lastRefreshTime: this.lastRefreshTime?.toISOString() || null,
      queryId: config.dune.tenMinuteVolumeQueryId,
      refreshInProgress: this.refreshInProgress,
    };
  }
}

