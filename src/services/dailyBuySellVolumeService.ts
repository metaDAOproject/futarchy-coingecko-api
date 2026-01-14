/**
 * Daily Buy/Sell Volume Service
 * 
 * Tracks daily buy and sell volumes per token with cumulative totals.
 * Fetches data from Dune once per day, stores in PostgreSQL.
 * Cumulative values are calculated on-the-fly from the database (not stored).
 * 
 * Schedule: Daily at 00:05 UTC (after day boundary)
 */

import { config } from '../config.js';
import { DuneService } from './duneService.js';
import { DatabaseService, DailyBuySellVolumeRecord, CumulativeVolumeData } from './databaseService.js';
import { FutarchyService } from './futarchyService.js';

export class DailyBuySellVolumeService {
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
      console.log('[BuySellVolume] Database not connected - service disabled');
      return;
    }

    if (!config.dune.dailyBuySellVolumeQueryId) {
      console.log('[BuySellVolume] No DUNE_DAILY_BUY_SELL_VOLUME_QUERY_ID configured - service disabled');
      return;
    }

    console.log('[BuySellVolume] Initializing daily buy/sell volume service...');

    try {
      const recordCount = await this.databaseService.getBuySellRecordCount();
      console.log(`[BuySellVolume] Current record count in database: ${recordCount}`);

      if (recordCount > 0) {
        this.initialized = true;
        console.log('[BuySellVolume] Service initialized with existing database records.');
      } else {
        // Perform initial backfill
        console.log('[BuySellVolume] No existing data - performing initial backfill...');
        await this.backfillFromStart();
        this.initialized = true;
        console.log('[BuySellVolume] Initialization complete with backfill.');
      }
    } catch (error: any) {
      console.error('[BuySellVolume] Error during initialization:', error.message);
      console.error('[BuySellVolume] Full error:', error.stack || error);
      // Still mark as initialized if we have existing data
      const recordCount = await this.databaseService.getBuySellRecordCount();
      if (recordCount > 0) {
        this.initialized = true;
        console.log('[BuySellVolume] Serving existing data despite initialization error.');
      }
    }
  }

  /**
   * Start the scheduled refresh process
   */
  start(): void {
    if (!this.databaseService.isAvailable() || !config.dune.dailyBuySellVolumeQueryId) {
      console.log('[BuySellVolume] Service not starting - missing database or query ID');
      return;
    }

    // Run initial background refresh
    console.log('[BuySellVolume] Starting daily buy/sell volume service...');
    this.refresh().catch(err => {
      console.error('[BuySellVolume] Background refresh error:', err.message);
    });

    // Schedule daily refresh at 00:05 UTC
    this.scheduleDailyRefresh();
    console.log('[BuySellVolume] Service started - will refresh daily at 00:05 UTC');
  }

  /**
   * Stop the service
   */
  stop(): void {
    if (this.refreshTimer) {
      clearTimeout(this.refreshTimer);
      this.refreshTimer = null;
    }
    console.log('[BuySellVolume] Service stopped');
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
    console.log(`[BuySellVolume] Next refresh scheduled for ${nextRefresh.toISOString()} (in ${Math.round(msUntilRefresh / 60000)} minutes)`);

    this.refreshTimer = setTimeout(async () => {
      await this.refresh();
      // Re-schedule for the next day
      this.scheduleDailyRefresh();
    }, msUntilRefresh);
  }

  /**
   * Perform incremental refresh - fetch only new/incomplete days
   */
  async refresh(): Promise<void> {
    if (this.isRefreshing) {
      console.log('[BuySellVolume] Refresh already in progress, skipping...');
      return;
    }

    if (config.devMode) {
      console.log('[BuySellVolume] DEV_MODE enabled - skipping Dune fetch');
      return;
    }

    this.isRefreshing = true;

    try {
      // Get the last complete date from the database
      const lastCompleteDate = await this.databaseService.getLatestBuySellDate();
      
      let startDate: string;
      if (lastCompleteDate) {
        // Fetch from the day after the last complete date (or re-fetch last day to be safe)
        startDate = lastCompleteDate;
        console.log(`[BuySellVolume] Last complete date: ${lastCompleteDate}, fetching from ${startDate}`);
      } else {
        // No data, start from the beginning
        startDate = '2025-10-09';
        console.log('[BuySellVolume] No existing data, starting from 2025-10-09');
      }

      await this.fetchAndStore(startDate);
      
      // Mark previous days as complete (today's data is still accumulating)
      const today = new Date().toISOString().split('T')[0]!;
      await this.databaseService.markBuySellDaysComplete(today);

      console.log('[BuySellVolume] Refresh completed successfully');
    } catch (error: any) {
      console.error('[BuySellVolume] Refresh error:', error.message);
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

    if (config.devMode) {
      return { success: false, message: 'DEV_MODE enabled - Dune fetches disabled' };
    }

    if (!config.dune.dailyBuySellVolumeQueryId) {
      return { success: false, message: 'No DUNE_DAILY_BUY_SELL_VOLUME_QUERY_ID configured' };
    }

    this.isRefreshing = true;

    try {
      const lastCompleteDate = await this.databaseService.getLatestBuySellDate();
      const startDate = lastCompleteDate || '2025-10-09';
      
      const recordsUpserted = await this.fetchAndStore(startDate);
      
      const today = new Date().toISOString().split('T')[0]!;
      await this.databaseService.markBuySellDaysComplete(today);

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
    console.log('[BuySellVolume] Starting full backfill from 2025-10-09...');
    try {
      const recordsUpserted = await this.fetchAndStore('2025-10-09');
      console.log(`[BuySellVolume] Backfill complete, upserted ${recordsUpserted} records`);
      
      // Mark all historical days as complete (except today)
      const today = new Date().toISOString().split('T')[0]!;
      await this.databaseService.markBuySellDaysComplete(today);
    } catch (error: any) {
      console.error(`[BuySellVolume] Backfill failed: ${error.message}`);
      throw error;
    }
  }

  /**
   * Fetch data from Dune and store in database
   */
  private async fetchAndStore(startDate: string): Promise<number> {
    if (!config.dune.dailyBuySellVolumeQueryId) {
      throw new Error('No DUNE_DAILY_BUY_SELL_VOLUME_QUERY_ID configured');
    }

    // Get active tokens (excluding excluded DAOs)
    const allDaos = await this.futarchyService.getAllDaos();
    const tokens = allDaos.map(dao => dao.baseMint.toString());
    
    // Use '__ALL__' sentinel if no specific tokens
    const tokenList = tokens.length > 0 
      ? tokens.map(t => `'${t}'`).join(',')
      : "'__ALL__'";

    console.log(`[BuySellVolume] Fetching from Dune query ${config.dune.dailyBuySellVolumeQueryId}`);
    console.log(`[BuySellVolume] Parameters: start_date=${startDate}, tokens=${tokens.length}`);

    let result;
    try {
      result = await this.duneService.executeQueryManually(
        config.dune.dailyBuySellVolumeQueryId,
        {
          start_date: startDate,
          token_list: tokenList,
        }
      );
    } catch (duneError: any) {
      console.error(`[BuySellVolume] Dune query execution failed: ${duneError.message}`);
      throw duneError;
    }

    if (!result) {
      console.log('[BuySellVolume] Dune returned null result');
      return 0;
    }

    if (!result.rows) {
      console.log('[BuySellVolume] Dune result has no rows property:', JSON.stringify(result).slice(0, 500));
      return 0;
    }

    if (result.rows.length === 0) {
      console.log('[BuySellVolume] No data returned from Dune (empty rows array)');
      return 0;
    }

    console.log(`[BuySellVolume] Received ${result.rows.length} rows from Dune`);
    
    // Log sample row for debugging field names
    try {
      if (result.rows.length > 0 && result.rows[0]) {
        const sampleRow = result.rows[0] as unknown as Record<string, unknown>;
        console.log('[BuySellVolume] Sample row fields:', Object.keys(sampleRow));
        console.log('[BuySellVolume] Sample row:', JSON.stringify(sampleRow));
      }
    } catch (logError: any) {
      console.error('[BuySellVolume] Error logging sample row:', logError.message);
    }

    // Transform Dune rows to database records
    console.log('[BuySellVolume] Transforming rows to database records...');
    let records: DailyBuySellVolumeRecord[];
    try {
      records = result.rows.map((row: any) => {
        // Parse date - handle both "2025-10-18T00:00:00Z" and "2025-10-18 00:00:00.000 UTC" formats
        let dateStr = row.date || row.trading_date || '';
        if (typeof dateStr === 'string' && dateStr.length > 0) {
          // Extract just the date part (YYYY-MM-DD)
          // First try splitting by T (ISO format), then by space (Dune format)
          const parts = dateStr.includes('T') ? dateStr.split('T') : dateStr.split(' ');
          dateStr = parts[0] || dateStr;
        }
        
        return {
          token: row.token,
          date: dateStr,
          base_volume: String(row.base_volume || 0),
          target_volume: String(row.target_volume || 0),
          buy_usdc_volume: String(row.buy_usdc_volume || 0),
          sell_token_volume: String(row.sell_token_volume || 0),
          high: String(row.high || 0),
          low: String(row.low || 0),
          trade_count: row.trade_count || 0,
        };
      });
    } catch (transformError: any) {
      console.error('[BuySellVolume] Error transforming rows:', transformError.message);
      throw transformError;
    }

    // Filter out invalid records
    const validRecords = records.filter(r => r.token && r.date);
    const invalidCount = records.length - validRecords.length;
    
    console.log(`[BuySellVolume] Transformed ${records.length} rows, ${validRecords.length} valid, ${invalidCount} invalid`);
    
    if (invalidCount > 0 && records.length > 0) {
      // Log a sample invalid record for debugging
      const invalidSample = records.find(r => !r.token || !r.date);
      if (invalidSample) {
        console.log('[BuySellVolume] Sample invalid record:', JSON.stringify(invalidSample));
      }
    }
    
    if (validRecords.length === 0) {
      console.log('[BuySellVolume] No valid records to insert');
      return 0;
    }

    // Check if we should mark historical data as complete
    const today = new Date().toISOString().split('T')[0]!;
    const markComplete = true; // We'll mark all fetched data as complete, then unmark today separately

    console.log(`[BuySellVolume] Upserting ${validRecords.length} records to database...`);
    try {
      const upserted = await this.databaseService.upsertDailyBuySellVolumes(validRecords, markComplete);
      console.log(`[BuySellVolume] Upsert complete: ${upserted} records`);
      return upserted;
    } catch (upsertError: any) {
      console.error('[BuySellVolume] Error upserting to database:', upsertError.message);
      throw upsertError;
    }
  }

  /**
   * Get cumulative volume data for a token (calculated from DB)
   */
  async getCumulativeVolumes(token?: string): Promise<CumulativeVolumeData[]> {
    return this.databaseService.getDailyBuySellVolumesWithCumulative(token);
  }

  /**
   * Get aggregate stats for all tokens
   */
  async getAggregates(tokens?: string[]) {
    return this.databaseService.getBuySellAggregates(tokens);
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
      queryIdConfigured: !!config.dune.dailyBuySellVolumeQueryId,
      isRefreshing: this.isRefreshing,
    };
  }
}
