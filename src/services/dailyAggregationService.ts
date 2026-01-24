/**
 * Daily Aggregation Service
 * 
 * Extracts/rolls up daily metrics from daily_volumes table (which is populated by aggregate_hourly_to_daily DB function).
 * Provides comprehensive daily metrics: volume, price, fees, trade count, cumulative values.
 * Part of the aggregation pipeline: 10-min → hourly → daily
 * 
 * Schedule: Daily at 00:05 UTC (after day boundary)
 */

import { config } from '../config.js';
import { DuneService } from './duneService.js';
import { DatabaseService, DailyFeesVolumeRecord } from './databaseService.js';
import { FutarchyService } from './futarchyService.js';
import { logger } from '../utils/logger.js';

export class DailyAggregationService {
  private duneService: DuneService;
  private databaseService: DatabaseService;
  private futarchyService: FutarchyService;
  private initialized: boolean = false;
  private refreshTimer: NodeJS.Timeout | null = null;
  private isRefreshing: boolean = false;

  constructor(
    duneService: DuneService,
    databaseService: DatabaseService,
    futarchyService: FutarchyService
  ) {
    this.duneService = duneService;
    this.databaseService = databaseService;
    this.futarchyService = futarchyService;
  }

  /**
   * Initialize the service - check database for existing data
   */
  async initialize(): Promise<void> {
    if (!this.databaseService.isAvailable()) {
      logger.info('[DailyAggregation] Database not connected - service disabled');
      return;
    }

    logger.info('[DailyAggregation] Initializing daily aggregation service...');

    try {
      const recordCount = await this.databaseService.getFeesRecordCount();
      logger.info(`[DailyAggregation] Current record count in database: ${recordCount}`);

      if (recordCount > 0) {
        this.initialized = true;
        logger.info('[DailyAggregation] Service initialized with existing database records.');
      } else {
        logger.info('[DailyAggregation] No existing data - performing initial backfill...');
        await this.backfillFromStart();
        this.initialized = true;
        logger.info('[DailyAggregation] Initialization complete with backfill.');
      }
    } catch (error: any) {
      logger.error('[DailyAggregation] Error during initialization:', error);
      const recordCount = await this.databaseService.getFeesRecordCount();
      if (recordCount > 0) {
        this.initialized = true;
        logger.info('[DailyAggregation] Serving existing data despite initialization error.');
      }
    }
  }

  /**
   * Start the scheduled refresh process
   */
  start(): void {
    if (!this.databaseService.isAvailable()) {
      logger.info('[DailyAggregation] Service not starting - database not available');
      return;
    }

    logger.info('[DailyAggregation] Starting daily aggregation service...');
    this.refresh().catch(err => {
      logger.error('[DailyAggregation] Background refresh error', err);
    });

    this.scheduleDailyRefresh();
    logger.info('[DailyAggregation] Service started - will refresh daily at 00:05 UTC');
  }

  /**
   * Stop the service
   */
  stop(): void {
    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer);
      this.refreshTimer = null;
    }
    logger.info('[DailyAggregation] Service stopped');
  }

  /**
   * Schedule the next refresh at 00:05 UTC
   */
  private scheduleDailyRefresh(): void {
    const now = new Date();
    const nextRefresh = new Date(now);
    
    // Set to 00:05 UTC
    nextRefresh.setUTCHours(0, 5, 0, 0);
    
    // If we're past 00:05 today, schedule for tomorrow
    if (now >= nextRefresh) {
      nextRefresh.setUTCDate(nextRefresh.getUTCDate() + 1);
    }

    const msUntilRefresh = nextRefresh.getTime() - now.getTime();
    logger.info(`[DailyAggregation] Next refresh scheduled for ${nextRefresh.toISOString()} (in ${Math.round(msUntilRefresh / 60000)} minutes)`);

    this.refreshTimer = setTimeout(async () => {
      await this.refresh();
      // Re-schedule for the next day
      this.scheduleDailyRefresh();
    }, msUntilRefresh);
  }

  /**
   * Perform incremental refresh - extract data from daily_volumes table
   */
  async refresh(): Promise<void> {
    if (this.isRefreshing) {
      logger.info('[DailyAggregation] Refresh already in progress, skipping...');
      return;
    }

    this.isRefreshing = true;

    try {
      if (!this.databaseService.isAvailable()) {
        logger.info('[DailyAggregation] Database not available, skipping refresh');
        return;
      }

      const lastCompleteDate = await this.databaseService.getLatestFeesDate();
      
      const recordsUpserted = await this.extractFromDailyVolumes(lastCompleteDate || undefined);
      
      const today = new Date().toISOString().split('T')[0]!;
      await this.databaseService.markFeesDaysComplete(today);

      logger.info(`[DailyAggregation] Refresh completed successfully - ${recordsUpserted} records`);
    } catch (error: any) {
      logger.error('[DailyAggregation] Refresh error', error);
    } finally {
      this.isRefreshing = false;
    }
  }

  /**
   * Force a full refresh (useful for manual triggering)
   */
  async forceRefresh(): Promise<{ success: boolean; message: string; recordsUpserted?: number }> {
    if (this.isRefreshing) {
      return { success: false, message: 'Refresh already in progress' };
    }

    // Service now uses daily_volumes table instead of Dune

    this.isRefreshing = true;

    try {
      const lastCompleteDate = await this.databaseService.getLatestFeesDate();
      
      const recordsUpserted = await this.extractFromDailyVolumes(lastCompleteDate || undefined);
      
      const today = new Date().toISOString().split('T')[0]!;
      await this.databaseService.markFeesDaysComplete(today);

      this.initialized = true;
      return { 
        success: true, 
        message: `Refresh completed, extracted from daily_volumes starting from ${lastCompleteDate || 'beginning'}`,
        recordsUpserted 
      };
    } catch (error: any) {
      return { success: false, message: error.message };
    } finally {
      this.isRefreshing = false;
    }
  }

  /**
   * Backfill from the very beginning (2025-10-09)
   * Now extracts from daily_volumes instead of Dune
   */
  private async backfillFromStart(): Promise<void> {
    logger.info('[DailyAggregation] Starting full backfill from daily_volumes...');
    try {
      const recordsUpserted = await this.extractFromDailyVolumes('2025-10-09');
      logger.info(`[DailyAggregation] Backfill complete, upserted ${recordsUpserted} records`);
      
      const today = new Date().toISOString().split('T')[0]!;
      await this.databaseService.markFeesDaysComplete(today);
    } catch (error: any) {
      logger.error('[DailyAggregation] Backfill failed', error);
      throw error;
    }
  }

  /**
   * Extract fees data from daily_volumes table and store in daily_fees_volumes
   */
  private async extractFromDailyVolumes(startDate?: string): Promise<number> {
    if (!this.databaseService.isAvailable() || !this.databaseService.pool) {
      return 0;
    }

    try {
      // Query daily_volumes for records with fees and cumulative values
      let query = `
        SELECT 
          token,
          date,
          base_volume,
          target_volume,
          buy_volume,
          sell_volume,
          usdc_fees,
          token_fees,
          token_fees_usdc,
          sell_volume_usdc,
          cumulative_usdc_fees,
          cumulative_token_in_usdc_fees,
          cumulative_target_volume,
          cumulative_token_volume,
          high,
          average_price,
          low
        FROM daily_volumes
        WHERE usdc_fees IS NOT NULL
      `;
      
      const params: any[] = [];
      if (startDate) {
        query += ` AND date >= $1`;
        params.push(startDate);
      }

      query += ` ORDER BY token, date`;

      const result = await this.databaseService.pool.query(query, params);

      if (result.rows.length === 0) {
        logger.info('[DailyAggregation] No daily_volumes data available to extract');
        return 0;
      }

      // Transform to DailyFeesVolumeRecord format
      const records: DailyFeesVolumeRecord[] = result.rows.map((row: any) => ({
        token: row.token,
        trading_date: row.date,
        base_volume: String(row.base_volume || 0),
        target_volume: String(row.target_volume || 0),
        usdc_fees: String(row.usdc_fees || 0),
        token_fees_usdc: String(row.token_fees_usdc || 0),
        token_fees: String(row.token_fees || 0),
        buy_volume: String(row.buy_volume || 0),
        sell_volume: String(row.sell_volume || 0),
        sell_volume_usdc: String(row.sell_volume_usdc || 0),
        cumulative_usdc_fees: String(row.cumulative_usdc_fees || 0),
        cumulative_token_in_usdc_fees: String(row.cumulative_token_in_usdc_fees || 0),
        cumulative_target_volume: String(row.cumulative_target_volume || 0),
        cumulative_token_volume: String(row.cumulative_token_volume || 0),
        high: String(row.high || 0),
        average_price: String(row.average_price || 0),
        low: String(row.low || 0),
      }));

      const upserted = await this.databaseService.upsertDailyFeesVolumes(records, false);
      logger.info(`[DailyAggregation] Extracted and upserted ${upserted} records from daily_volumes`);
      
      return upserted;
    } catch (error: any) {
      logger.error('[DailyAggregation] Error extracting from daily_volumes', error);
      return 0;
    }
  }


  /**
   * Get daily fees volumes with optional filtering
   */
  async getDailyFeesVolumes(options?: {
    token?: string;
    startDate?: string;
    endDate?: string;
  }): Promise<DailyFeesVolumeRecord[]> {
    return this.databaseService.getDailyFeesVolumes(options);
  }

  /**
   * Check if service is ready
   */
  isReady(): boolean {
    return this.initialized && this.databaseService.isAvailable();
  }

  /**
   * Get service status
   */
  getStatus(): {
    initialized: boolean;
    databaseConnected: boolean;
    queryIdConfigured: boolean;
    isRefreshing: boolean;
  } {
    return {
      initialized: this.initialized,
      databaseConnected: this.databaseService.isAvailable(),
      queryIdConfigured: false, // Deprecated - now uses DB aggregation from daily_volumes
      isRefreshing: this.isRefreshing,
    };
  }
}
