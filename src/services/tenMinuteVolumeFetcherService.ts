/**
 * TenMinuteVolumeFetcherService
 * 
 * Fetches 10-minute granularity volume data from Dune API for accurate rolling 24h calculations.
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
import { scheduleAtBoundary, type ScheduledTask } from '../utils/scheduling';
import { logger } from '../utils/logger.js';

export class TenMinuteVolumeFetcherService {
  private duneService: DuneService;
  private databaseService: DatabaseService;
  private futarchyService: FutarchyService;
  private refreshTask: ScheduledTask | null = null;
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
      logger.info('[TenMinVolume] Database not connected - service disabled');
      return;
    }

    logger.info('[TenMinVolume] Initializing 10-minute volume service...');

    try {
      const recordCount = await this.databaseService.getTenMinuteRecordCount();
      logger.info(`[TenMinVolume] Current record count in database: ${recordCount}`);

      const latestBucket = await this.databaseService.getLatestTenMinuteBucket();
      if (latestBucket) {
        logger.info(`[TenMinVolume] Latest bucket in DB: ${latestBucket}`);
      }

      if (recordCount > 0) {
        this.initialized = true;
        logger.info(`[TenMinVolume] Service initialized with ${recordCount} existing records`);
      }

      if (config.dune.tenMinuteVolumeQueryId) {
        const now = new Date();
        const twentyFourHoursAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);

        let startTime: Date;
        if (latestBucket) {
          const latestDate = new Date(latestBucket);
          startTime = new Date(Math.min(latestDate.getTime(), twentyFourHoursAgo.getTime()));
        } else {
          startTime = twentyFourHoursAgo;
          logger.info('[TenMinVolume] No existing data - will backfill last 24 hours');
        }

        logger.info(`[TenMinVolume] Backfilling from ${startTime.toISOString()}`);
        await this.fetchAndStore(startTime, true);

        this.initialized = true;
        logger.info('[TenMinVolume] Initialization complete');
      } else if (!this.initialized) {
        logger.info('[TenMinVolume] No DUNE_TEN_MINUTE_VOLUME_QUERY_ID configured - cannot fetch new data');
        logger.info('[TenMinVolume] Service will not be available until query ID is set or data exists in DB');
      }
    } catch (error: any) {
      logger.error('[TenMinVolume] Error during initialization', error);
      const recordCount = await this.databaseService.getTenMinuteRecordCount();
      if (recordCount > 0) {
        this.initialized = true;
        logger.info('[TenMinVolume] Serving existing data despite initialization error');
      }
    }
  }

  /**
   * Start the background refresh loop
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      logger.info('[TenMinVolume] Service already running');
      return;
    }

    await this.initialize();

    if (!config.dune.tenMinuteVolumeQueryId) {
      if (this.initialized) {
        logger.info('[TenMinVolume] Serving existing DB data (no refresh loop - set DUNE_TEN_MINUTE_VOLUME_QUERY_ID to enable)');
      }
      return;
    }

    this.isRunning = true;

    this.refreshTask = scheduleAtBoundary(
      () => this.refresh(),
      {
        name: 'TenMinVolume',
        boundaryMinutes: 10,
        bufferSeconds: 5,
        onError: (error) => logger.error('[TenMinVolume] Refresh error', error),
      }
    );
    
    logger.info('[TenMinVolume] Started with non-pileup scheduling at 10-minute boundaries');
  }

  /**
   * Stop the background refresh loop
   */
  stop(): void {
    if (this.refreshTask) {
      this.refreshTask.stop();
      this.refreshTask = null;
    }
    this.isRunning = false;
  }

  /**
   * Perform a refresh - fetch recent 10-minute data
   */
  async refresh(): Promise<void> {
    if (this.refreshInProgress) {
      logger.info('[TenMinVolume] Refresh already in progress, skipping');
      return;
    }

    this.refreshInProgress = true;
    logger.info('[TenMinVolume] Starting refresh...');

    try {
      const startTime = new Date(Date.now() - 20 * 60 * 1000);
      await this.fetchAndStore(startTime, false);

      const currentBucket = this.getCurrentBucketStart();
      await this.databaseService.markTenMinuteBucketsComplete(currentBucket.toISOString());

      this.lastRefreshTime = new Date();
      logger.info('[TenMinVolume] Refresh complete');
    } catch (error: any) {
      logger.error('[TenMinVolume] Error during refresh', error);
    } finally {
      this.refreshInProgress = false;
    }
  }

  /**
   * Force a full refresh (backfill last 24h)
   */
  async forceRefresh(): Promise<void> {
    logger.info('[TenMinVolume] Force refresh requested - backfilling last 24h');
    const startTime = new Date(Date.now() - 24 * 60 * 60 * 1000);
    await this.fetchAndStore(startTime, true);
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
   * Public method for backfilling missing extended fields
   * Fetches data from Dune for a specific time range and updates only missing fields
   * Only fetches buckets that actually need updating (checks database first)
   * @param startTime Start time for the query (format: YYYY-MM-DD HH:MM:SS)
   * @param endTime Optional end time for the query (format: YYYY-MM-DD HH:MM:SS). If not provided, queries until now.
   */
  async backfillExtendedFields(startTime: string, endTime?: string): Promise<number> {
    const queryId = config.dune.tenMinuteVolumeQueryId;
    if (!queryId) {
      logger.info('[TenMinVolume] No query ID configured for backfill');
      return 0;
    }

    try {
      if (!this.databaseService.pool) {
        logger.info('[TenMinVolume] Database not available for backfill check');
        return 0;
      }

      const startDate = new Date(startTime);
      // Use provided endTime or calculate end of day
      const endDate = endTime ? new Date(endTime) : new Date(startDate);
      if (!endTime) {
        endDate.setDate(endDate.getDate() + 1); // Full day window (24 hours) if no endTime provided
      }

      const missingCheck = await this.databaseService.pool.query(`
        SELECT COUNT(*) as missing_count
        FROM ten_minute_volumes
        WHERE bucket >= $1 AND bucket < $2
          AND (buy_volume IS NULL OR buy_volume = 0 OR
               sell_volume IS NULL OR sell_volume = 0 OR
               average_price IS NULL OR average_price = 0)
      `, [startDate.toISOString(), endDate.toISOString()]);

      const missingCount = parseInt(missingCheck.rows[0]?.missing_count || '0');
      if (missingCount === 0) {
        // No missing fields in this range, skip
        return 0;
      }

      // Get the filtered list of DAOs from FutarchyService
      const allDaos = await this.futarchyService.getAllDaos();
      const tokenAddresses = allDaos.map(dao => dao.baseMint.toString());

      logger.info(`[TenMinVolume] Backfilling ${missingCount} records from ${startTime} for ${tokenAddresses.length} tokens`);

      const parameters: Record<string, any> = {
        start_time: startTime,
      };

      // Always provide end_time - use far future date if not provided (effectively no limit)
      if (endTime) {
        parameters.end_time = endTime;
      } else {
        // Use far future date to effectively have no limit (avoids SQL parsing issues)
        const farFuture = new Date('2099-12-31 23:59:59');
        parameters.end_time = farFuture.toISOString().replace('T', ' ').replace('Z', '').slice(0, 19);
      }

      // Format token list
      if (tokenAddresses.length > 0) {
        parameters.token_list = tokenAddresses.map(token => `'${token}'`).join(', ');
      } else {
        parameters.token_list = "'__ALL__'";
      }

      const result = await (this.duneService as any).executeQueryManually(queryId, parameters);

      if (!result || !result.rows || result.rows.length === 0) {
        logger.info('[TenMinVolume] No results from Dune for backfill');
        return 0;
      }

      const rows = result.rows;
      logger.info(`[TenMinVolume] Received ${rows.length} rows from Dune for backfill`);

      // Transform to records with extended fields
      const records: TenMinuteVolumeRecord[] = rows.map((row: any) => ({
        token: row.token,
        bucket: this.parseDuneBucket(row.bucket),
        base_volume: row.base_volume || '0',
        target_volume: row.target_volume || '0',
        buy_volume: row.buy_volume || '0',
        sell_volume: row.sell_volume || '0',
        high: row.high || '0',
        low: row.low || '0',
        average_price: row.average_price || '0',
        trade_count: parseInt(row.trade_count || '0'),
        usdc_fees: row.usdc_fees || '0',
        token_fees: row.token_fees || '0',
        token_fees_usdc: row.token_fees_usdc || '0',
        sell_volume_usdc: row.sell_volume_usdc || '0',
      }));

      // Filter to only records that need updating (have missing fields)
      const recordsToUpdate = records.filter(record => {
        // Check if this specific bucket needs updating
        // We'll let the upsert logic handle this, but we can optimize by checking first
        return true; // Let upsert handle the filtering
      });

      // Batch upsert records (will only update missing fields due to our safe upsert logic)
      const currentBucket = this.getCurrentBucketStart();
      
      // Separate complete and incomplete records for batch processing
      const completeRecords = recordsToUpdate.filter(r => new Date(r.bucket) < currentBucket);
      const incompleteRecords = recordsToUpdate.filter(r => new Date(r.bucket) >= currentBucket);
      
      let totalUpserted = 0;
      
      // Batch upsert complete records
      if (completeRecords.length > 0) {
        const count = await this.databaseService.upsertTenMinuteVolumes(completeRecords, true);
        totalUpserted += count;
      }
      
      // Batch upsert incomplete records
      if (incompleteRecords.length > 0) {
        const count = await this.databaseService.upsertTenMinuteVolumes(incompleteRecords, false);
        totalUpserted += count;
      }

      logger.info(`[TenMinVolume] Backfilled ${totalUpserted} records (batched)`);
      return totalUpserted;
    } catch (error: any) {
      if (error.message.includes('402') || error.message.includes('Payment Required')) {
        throw error;
      }
      logger.error('[TenMinVolume] Error during backfill', error);
      return 0;
    }
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

      logger.info(`[TenMinVolume] Fetching from Dune query ${queryId} with start_time: ${startTime.toISOString()} for ${tokenAddresses.length} tokens`);

      const parameters: Record<string, any> = {
        start_time: startTime.toISOString().replace('T', ' ').replace('Z', ''),
      };

      // Format token list
      if (tokenAddresses.length > 0) {
        parameters.token_list = tokenAddresses.map(token => `'${token}'`).join(', ');
      } else {
        parameters.token_list = "'__ALL__'";
      }

      const result = await (this.duneService as any).executeQueryManually(queryId, parameters);

      if (!result || !result.rows) {
        logger.info('[TenMinVolume] No results from Dune');
        return;
      }

      const rows = result.rows;
      logger.info(`[TenMinVolume] Received ${rows.length} rows from Dune`);

      if (rows.length === 0) return;

      // Transform to records with extended fields
      const records: TenMinuteVolumeRecord[] = rows.map((row: any) => ({
        token: row.token,
        bucket: this.parseDuneBucket(row.bucket),
        base_volume: row.base_volume || '0',
        target_volume: row.target_volume || '0',
        buy_volume: row.buy_volume || '0',
        sell_volume: row.sell_volume || '0',
        high: row.high || '0',
        low: row.low || '0',
        average_price: row.average_price || '0',
        trade_count: parseInt(row.trade_count || '0'),
        usdc_fees: row.usdc_fees || '0',
        token_fees: row.token_fees || '0',
        token_fees_usdc: row.token_fees_usdc || '0',
        sell_volume_usdc: row.sell_volume_usdc || '0',
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
        // Regular refresh - batch upserts like historical mode
        const currentBucket = this.getCurrentBucketStart();
        const completeRecords = records.filter(r => new Date(r.bucket) < currentBucket);
        const incompleteRecords = records.filter(r => new Date(r.bucket) >= currentBucket);

        if (completeRecords.length > 0) {
          await this.databaseService.upsertTenMinuteVolumes(completeRecords, true);
        }
        if (incompleteRecords.length > 0) {
          await this.databaseService.upsertTenMinuteVolumes(incompleteRecords, false);
        }
      }

    } catch (error: any) {
      logger.error('[TenMinVolume] Error fetching from Dune', error);
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
      logger.info('[TenMinVolume] Service not ready, returning empty metrics');
      return new Map();
    }

    try {
      const dbMetrics = await this.databaseService.getRolling24hFromTenMinute(baseMintAddresses);

      const result = new Map<string, { base_volume_24h: number; target_volume_24h: number; high_24h: number; low_24h: number }>();
      for (const [token, metrics] of dbMetrics) {
        result.set(token, {
          base_volume_24h: parseFloat(metrics.base_volume_24h) || 0,
          target_volume_24h: parseFloat(metrics.target_volume_24h) || 0,
          high_24h: parseFloat(metrics.high_24h) || 0,
          low_24h: parseFloat(metrics.low_24h) || 0,
        });
      }

      logger.info(`[TenMinVolume] Returning rolling 24h metrics for ${result.size} tokens`);
      return result;
    } catch (error: any) {
      logger.error('[TenMinVolume] Error getting rolling 24h metrics', error);
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

