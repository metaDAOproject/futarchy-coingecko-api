/**
 * MeteoraVolumeFetcherService
 * 
 * Fetches daily Meteora pool fees and volumes from Dune API per owner (mapped to token).
 * Stores data in PostgreSQL.
 * Cumulative values are calculated in the database, not from Dune.
 * 
 * Schedule: Daily at 00:00 UTC (midnight)
 */

import { config } from '../config.js';
import { DuneService } from './duneService.js';
import { DatabaseService, DailyMeteoraVolumeRecord } from './databaseService.js';
import { getTokenForOwner } from './meteoraService.js';
import { logger } from '../utils/logger.js';

export class MeteoraVolumeFetcherService {
  private duneService: DuneService;
  private databaseService: DatabaseService;
  private initialized: boolean = false;
  private refreshTimer: NodeJS.Timeout | null = null;
  private isRefreshing: boolean = false;

  constructor(
    duneService: DuneService,
    databaseService: DatabaseService
  ) {
    this.duneService = duneService;
    this.databaseService = databaseService;
  }

  /**
   * Initialize the service - check database for existing data
   */
  async initialize(): Promise<void> {
    if (!this.databaseService.isAvailable()) {
      logger.info('[MeteoraVolume] Database not connected - service disabled');
      return;
    }

    logger.info('[MeteoraVolume] Initializing daily Meteora volume service...');

    try {
      const recordCount = await this.databaseService.getMeteoraRecordCount();
      logger.info(`[MeteoraVolume] Current record count in database: ${recordCount}`);

      if (recordCount > 0) {
        this.initialized = true;
        logger.info('[MeteoraVolume] Service initialized with existing database records.');
      } else {
        logger.info('[MeteoraVolume] No existing data - performing initial backfill...');
        await this.backfillFromStart();
        this.initialized = true;
        logger.info('[MeteoraVolume] Initialization complete with backfill.');
      }
    } catch (error: any) {
      logger.error('[MeteoraVolume] Error during initialization:', error);
      // Still mark as initialized if we have existing data
      const recordCount = await this.databaseService.getMeteoraRecordCount();
      if (recordCount > 0) {
        this.initialized = true;
        logger.info('[MeteoraVolume] Serving existing data despite initialization error.');
      }
    }
  }

  /**
   * Start the scheduled refresh process
   */
  start(): void {
    if (!this.databaseService.isAvailable()) {
      logger.info('[MeteoraVolume] Service not starting - database not available');
      return;
    }

    logger.info('[MeteoraVolume] Starting daily Meteora volume service...');
    this.refresh().catch(err => {
      logger.error('[MeteoraVolume] Background refresh error', err);
    });

    this.scheduleDailyRefresh();
    logger.info('[MeteoraVolume] Service started - will refresh daily at 00:00 UTC');
  }

  /**
   * Stop the service
   */
  stop(): void {
    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer);
      this.refreshTimer = null;
    }
    logger.info('[MeteoraVolume] Service stopped');
  }

  /**
   * Schedule the next refresh at 00:00 UTC (midnight)
   */
  private scheduleDailyRefresh(): void {
    const now = new Date();
    const nextRefresh = new Date(now);
    
    // Set to 00:00 UTC (midnight)
    nextRefresh.setUTCHours(0, 0, 0, 0);
    
    // If we're past midnight today, schedule for tomorrow
    if (now >= nextRefresh) {
      nextRefresh.setUTCDate(nextRefresh.getUTCDate() + 1);
    }

    const msUntilRefresh = nextRefresh.getTime() - now.getTime();
    logger.info(`[MeteoraVolume] Next refresh scheduled for ${nextRefresh.toISOString()} (in ${Math.round(msUntilRefresh / 60000)} minutes)`);

    this.refreshTimer = setTimeout(async () => {
      await this.refresh();
      // Re-schedule for the next day
      this.scheduleDailyRefresh();
    }, msUntilRefresh);
  }

  /**
   * Perform incremental refresh - fetch data from Dune
   */
  async refresh(): Promise<void> {
    if (this.isRefreshing) {
      logger.info('[MeteoraVolume] Refresh already in progress, skipping...');
      return;
    }

    this.isRefreshing = true;

    try {
      if (!this.databaseService.isAvailable()) {
        logger.info('[MeteoraVolume] Database not available, skipping refresh');
        return;
      }

      const lastCompleteDate = await this.databaseService.getLatestMeteoraDate();
      
      const startDate = lastCompleteDate 
        ? this.addDays(lastCompleteDate, 1)
        : '2025-10-09';
      
      // Constrain to today for daily fetches
      const today = new Date().toISOString().split('T')[0]!;
      const recordsUpserted = await this.fetchAndStore(startDate, today);
      
      await this.databaseService.markMeteoraDaysComplete(today);

      logger.info(`[MeteoraVolume] Refresh completed successfully - ${recordsUpserted} records`);
    } catch (error: any) {
      logger.error('[MeteoraVolume] Refresh error', error);
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

    this.isRefreshing = true;

    try {
      const lastCompleteDate = await this.databaseService.getLatestMeteoraDate();
      const startDate = lastCompleteDate 
        ? this.addDays(lastCompleteDate, 1)
        : '2025-10-09';
      
      // Constrain to today for force refresh
      const today = new Date().toISOString().split('T')[0]!;
      const recordsUpserted = await this.fetchAndStore(startDate, today);
      
      await this.databaseService.markMeteoraDaysComplete(today);

      this.initialized = true;
      return { 
        success: true, 
        message: `Refresh completed, fetched from ${startDate}`,
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
   */
  private async backfillFromStart(): Promise<void> {
    logger.info('[MeteoraVolume] Starting full backfill from Dune...');
    try {
      // For full backfill, don't constrain end date - fetch all available data
      const recordsUpserted = await this.fetchAndStore('2025-10-09');
      logger.info(`[MeteoraVolume] Backfill complete, upserted ${recordsUpserted} records`);
      
      const today = new Date().toISOString().split('T')[0]!;
      await this.databaseService.markMeteoraDaysComplete(today);
    } catch (error: any) {
      logger.error('[MeteoraVolume] Backfill failed', error);
      throw error;
    }
  }

  /**
   * Public method to fetch data for a specific date range (for chunked backfills)
   */
  async fetchDateRange(startDate: string, endDate: string): Promise<number> {
    return this.fetchAndStore(startDate, endDate);
  }

  /**
   * Fetch data from Dune and store in database
   */
  private async fetchAndStore(startDate: string, endDate?: string): Promise<number> {
    if (!config.dune.meteoraVolumeQueryId) {
      throw new Error('No DUNE_METEORA_VOLUME_QUERY_ID configured');
    }

    const params: Record<string, any> = {
      start_date: startDate,
    };
    
    // Only include end_date if it's explicitly provided and not empty
    if (endDate && endDate.trim() !== '') {
      params.end_date = endDate;
    }

    logger.info(`[MeteoraVolume] Fetching from Dune query ${config.dune.meteoraVolumeQueryId}`);
    logger.info(`[MeteoraVolume] Parameters: start_date=${startDate}, end_date=${endDate || '(unconstrained)'}`);

    let result;
    try {
      result = await (this.duneService as any).executeQueryManually(
        config.dune.meteoraVolumeQueryId,
        params
      );
    } catch (duneError: any) {
      logger.error('[MeteoraVolume] Dune query execution failed', duneError);
      throw duneError;
    }

    if (!result) {
      logger.info('[MeteoraVolume] Dune returned null result');
      return 0;
    }

    if (!result.rows) {
      logger.info('[MeteoraVolume] Dune result has no rows property:', { preview: JSON.stringify(result).slice(0, 500) });
      return 0;
    }

    if (result.rows.length === 0) {
      logger.info('[MeteoraVolume] No data returned from Dune (empty rows array)');
      return 0;
    }

    logger.info(`[MeteoraVolume] Received ${result.rows.length} rows from Dune`);
    
    try {
      if (result.rows.length > 0 && result.rows[0]) {
        const sampleRow = result.rows[0] as unknown as Record<string, unknown>;
        logger.debug('[MeteoraVolume] Sample row fields:', { fields: Object.keys(sampleRow) });
        logger.debug('[MeteoraVolume] Sample row:', { row: JSON.stringify(sampleRow) });
      }
    } catch (logError: any) {
      logger.error('[MeteoraVolume] Error logging sample row', logError);
    }

    logger.info('[MeteoraVolume] Transforming rows to database records...');
    let records: DailyMeteoraVolumeRecord[];
    try {
      records = result.rows.map((row: any) => {
        let rawDate = row.day || row.date || '';
        let dateStr = '';
        
        if (rawDate instanceof Date) {
          dateStr = rawDate.toISOString().substring(0, 10);
        } else if (typeof rawDate === 'string') {
          dateStr = rawDate.substring(0, 10);
        }
        
        const owner = row.owner || '';
        const token = getTokenForOwner(owner);
        
        if (!token) {
          logger.warn(`[MeteoraVolume] No token mapping found for owner: ${owner}`);
        }
        
        // Convert scientific notation for all numeric fields
        const convertValue = (value: unknown): string => {
          if (value === null || value === undefined || value === '' || value === 0 || value === '0') {
            return '0';
          }
          if (typeof value === 'number') {
            if (isNaN(value) || !isFinite(value)) {
              return '0';
            }
            return value.toFixed(20).replace(/\.?0+$/, '');
          }
          if (typeof value === 'string') {
            if (value.includes('E') || value.includes('e')) {
              try {
                const num = parseFloat(value);
                if (isNaN(num)) {
                  return '0';
                }
                return num.toFixed(20).replace(/\.?0+$/, '');
              } catch {
                return value;
              }
            }
            return value;
          }
          return String(value);
        };
        
        // Calculate target_volume from buy_volume + sell_volume if not provided
        const buyVolume = parseFloat(convertValue(row.buy_volume || 0)) || 0;
        const sellVolume = parseFloat(convertValue(row.sell_volume || 0)) || 0;
        const targetVolume = buyVolume + sellVolume;
        
        return {
          token: token || owner, // Use owner as fallback if mapping not found
          date: dateStr,
          base_volume: convertValue(row.volume_usd_approx || 0),
          target_volume: String(targetVolume),
          trade_count: parseInt(String(row.num_swaps || 0)) || 0,
          buy_volume: convertValue(row.buy_volume || 0),
          sell_volume: convertValue(row.sell_volume || 0),
          usdc_fees: convertValue(row.lp_fee_usdc || 0),
          token_fees: convertValue(row.lp_fee_token || 0),
          token_fees_usdc: convertValue(row.lp_fee_token_usdc || 0),
          token_per_usdc: convertValue(row.token_per_usdc_raw || 0),
          average_price: convertValue(row.token_price_usdc || 0),
          ownership_share: convertValue(row.ownership_share || 0), // Default to 0 if not provided by query
          earned_fee_usdc: convertValue(row.earned_fee_usdc || 0),
          is_complete: false,
        };
      });
    } catch (transformError: any) {
      logger.error('[MeteoraVolume] Error transforming rows', transformError);
      throw transformError;
    }

    const validRecords = records.filter(r => r.token && r.date);
    const invalidCount = records.length - validRecords.length;
    
    logger.info(`[MeteoraVolume] Transformed ${records.length} rows, ${validRecords.length} valid, ${invalidCount} invalid`);
    
    if (invalidCount > 0 && records.length > 0) {
      const invalidSample = records.find(r => !r.token || !r.date);
      if (invalidSample) {
        logger.info('[MeteoraVolume] Sample invalid record:', { record: JSON.stringify(invalidSample) });
      }
    }
    
    if (validRecords.length === 0) {
      logger.info('[MeteoraVolume] No valid records to insert');
      return 0;
    }

    const today = new Date().toISOString().split('T')[0]!;
    const markComplete = true;

    logger.info(`[MeteoraVolume] Upserting ${validRecords.length} records to database...`);
    try {
      const upserted = await this.databaseService.upsertDailyMeteoraVolumes(validRecords, markComplete);
      logger.info(`[MeteoraVolume] Upsert complete: ${upserted} records`);
      return upserted;
    } catch (upsertError: any) {
      logger.error('[MeteoraVolume] Error upserting to database', upsertError);
      throw upsertError;
    }
  }

  /**
   * Helper to add days to a date string
   */
  private addDays(dateStr: string, days: number): string {
    const date = new Date(dateStr);
    date.setDate(date.getDate() + days);
    return date.toISOString().split('T')[0]!;
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
      queryIdConfigured: !!config.dune.meteoraVolumeQueryId,
      isRefreshing: this.isRefreshing,
    };
  }
}
