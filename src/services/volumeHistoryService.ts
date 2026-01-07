import { DatabaseService, DailyVolumeRecord, TokenVolumeAggregate } from './databaseService.js';
import { DuneService, DuneAggregateVolumeResponse, DuneAggregateTokenVolume, DuneDailyVolume } from './duneService.js';
import { FutarchyService } from './futarchyService.js';
import { config } from '../config.js';

// The date when Futarchy trading data begins
const FUTARCHY_START_DATE = '2025-10-09';

export interface VolumeHistoryStatus {
  isInitialized: boolean;
  databaseConnected: boolean;
  latestDate: string | null;
  tokenCount: number;
  recordCount: number;
  lastSyncTime: Date | null;
  isRefreshing: boolean;
  incrementalQueryId: number | null;
  schedule: {
    dailyRefresh: string;
    queriesPerDay: number;
  };
}

export class VolumeHistoryService {
  private databaseService: DatabaseService;
  private duneService: DuneService;
  private futarchyService: FutarchyService;
  private isInitialized: boolean = false;
  private isRefreshing: boolean = false;
  private lastSyncTime: Date | null = null;
  private dailyTimer: ReturnType<typeof setTimeout> | null = null;
  
  // In-memory fallback when database is not available
  private inMemoryCache: Map<string, TokenVolumeAggregate> = new Map();

  constructor(databaseService: DatabaseService, duneService: DuneService, futarchyService: FutarchyService) {
    this.databaseService = databaseService;
    this.duneService = duneService;
    this.futarchyService = futarchyService;
  }

  /**
   * Initialize the service - connect to DB and do initial data load
   */
  async initialize(): Promise<void> {
    console.log('[VolumeHistory] Initializing service...');
    
    // Try to connect to database
    const dbConnected = await this.databaseService.initialize();
    
    if (dbConnected) {
      console.log('[VolumeHistory] Database connected, checking for existing data...');
      const recordCount = await this.databaseService.getRecordCount();
      const latestDate = await this.databaseService.getLatestDate();
      
      if (recordCount > 0) {
        console.log(`[VolumeHistory] Found ${recordCount} existing records, latest date: ${latestDate}`);
      } else {
        console.log('[VolumeHistory] No existing data, will perform full backfill on first refresh');
      }
    } else {
      console.log('[VolumeHistory] Database not available, will use in-memory cache');
    }

    this.isInitialized = true;
    console.log('[VolumeHistory] Service initialized');
  }

  /**
   * Start the scheduled daily refresh job
   * Runs at 00:01 UTC each day (1 query/day)
   */
  async start(): Promise<void> {
    if (!this.isInitialized) {
      await this.initialize();
    }

    console.log('[VolumeHistory] Starting scheduled daily refresh...');
    console.log('[VolumeHistory] - Daily refresh: 00:01 UTC (1 query/day)');
    
    // Do initial refresh/backfill
    await this.refresh();
    
    // Schedule daily refresh at 00:01 UTC
    this.scheduleDailyRefresh();

    console.log('[VolumeHistory] Scheduled daily refresh started');
  }

  /**
   * Schedule the next daily refresh at 00:01 UTC
   */
  private scheduleDailyRefresh(): void {
    const now = new Date();
    const tomorrow = new Date(now);
    tomorrow.setUTCDate(tomorrow.getUTCDate() + 1);
    tomorrow.setUTCHours(0, 1, 0, 0); // 00:01:00 UTC

    // If it's before 00:01 UTC today, schedule for today instead
    const todayMidnight = new Date(now);
    todayMidnight.setUTCHours(0, 1, 0, 0);
    
    let nextRun = tomorrow;
    if (now < todayMidnight) {
      nextRun = todayMidnight;
    }

    const msUntilNext = nextRun.getTime() - now.getTime();
    console.log(`[VolumeHistory] Next daily refresh at ${nextRun.toISOString()} (in ${Math.round(msUntilNext / 1000 / 60)} minutes)`);

    this.dailyTimer = setTimeout(async () => {
      console.log('[VolumeHistory] Daily refresh triggered at UTC midnight');
      await this.refresh();
      this.scheduleDailyRefresh(); // Schedule next day
    }, msUntilNext);
  }

  /**
   * Stop the scheduled daily refresh job
   */
  stop(): void {
    if (this.dailyTimer) {
      clearTimeout(this.dailyTimer);
      this.dailyTimer = null;
      console.log('[VolumeHistory] Scheduled daily refresh stopped');
    }
  }

  /**
   * Refresh data - fetch incremental updates from Dune
   */
  async refresh(tokenAddresses?: string[]): Promise<void> {
    if (this.isRefreshing) {
      console.log('[VolumeHistory] Refresh already in progress, skipping');
      return;
    }

    this.isRefreshing = true;
    const startTime = Date.now();
    console.log('[VolumeHistory] Starting data refresh...');

    try {
      // Determine the start date for fetching
      let startDate = FUTARCHY_START_DATE;
      
      if (this.databaseService.isAvailable()) {
        const latestDate = await this.databaseService.getLatestDate();
        if (latestDate) {
          // Go back 1 day to handle any partial data from the last sync
          const latestDateObj = new Date(latestDate);
          latestDateObj.setDate(latestDateObj.getDate() - 1);
          startDate = latestDateObj.toISOString().split('T')[0] as string;
          console.log(`[VolumeHistory] Incremental fetch from ${startDate} (latest in DB: ${latestDate})`);
        } else {
          console.log(`[VolumeHistory] Full backfill from ${startDate}`);
        }
      }

      // Fetch data from Dune using the incremental query
      const duneData = await this.fetchFromDune(startDate, tokenAddresses);
      
      if (duneData && duneData.tokens.length > 0) {
        // Convert Dune response to database records
        const records = this.convertToRecords(duneData);
        
        if (this.databaseService.isAvailable()) {
          // Upsert to database
          const upsertedCount = await this.databaseService.upsertDailyVolumes(records);
          console.log(`[VolumeHistory] Upserted ${upsertedCount} records to database`);
          
          // Update sync metadata
          await this.databaseService.setSyncMetadata('last_sync_time', new Date().toISOString());
        }
        
        // Also update in-memory cache
        this.updateInMemoryCache(duneData);
      } else {
        console.log('[VolumeHistory] No new data from Dune');
      }

      this.lastSyncTime = new Date();
      const duration = Date.now() - startTime;
      console.log(`[VolumeHistory] Refresh completed in ${duration}ms`);
    } catch (error: any) {
      console.error('[VolumeHistory] Error during refresh:', error.message);
    } finally {
      this.isRefreshing = false;
    }
  }

  /**
   * Fetch data from Dune using the incremental query
   * Only fetches tokens from active, non-excluded DAOs
   */
  private async fetchFromDune(startDate: string, tokenAddresses?: string[]): Promise<DuneAggregateVolumeResponse | null> {
    const incrementalQueryId = config.dune.incrementalVolumeQueryId;
    
    // Get valid tokens from FutarchyService (excludes EXCLUDED_DAOS) if not provided
    let validTokens = tokenAddresses;
    if (!validTokens || validTokens.length === 0) {
      const allDaos = await this.futarchyService.getAllDaos();
      validTokens = allDaos.map(dao => dao.baseMint.toString());
    }

    if (validTokens.length === 0) {
      console.warn('[VolumeHistory] No valid tokens to fetch');
      return null;
    }
    
    if (!incrementalQueryId) {
      console.log('[VolumeHistory] No incremental query ID configured, falling back to aggregate volume query');
      // Fall back to the existing aggregate volume query
      return await this.duneService.getAggregateVolume(validTokens, true);
    }

    try {
      console.log(`[VolumeHistory] Fetching from Dune query ${incrementalQueryId} with start_date: ${startDate} for ${validTokens.length} tokens`);
      
      // Build parameters with valid tokens (excludes EXCLUDED_DAOS)
      const parameters: Record<string, any> = {
        start_date: startDate,
        token_list: validTokens.map(token => `'${token}'`).join(', '),
      };

      // Execute the query manually
      const result = await this.duneService.executeQueryManually(incrementalQueryId, parameters);
      
      if (!result.rows || result.rows.length === 0) {
        return null;
      }

      // Convert raw Dune results to DuneAggregateVolumeResponse format
      return this.convertDuneResultsToResponse(result.rows);
    } catch (error: any) {
      console.error('[VolumeHistory] Error fetching from Dune:', error.message);
      return null;
    }
  }

  /**
   * Convert raw Dune query results to DuneAggregateVolumeResponse format
   */
  private convertDuneResultsToResponse(rows: any[]): DuneAggregateVolumeResponse {
    const tokenDataMap = new Map<string, {
      dailyData: DuneDailyVolume[];
      firstDate: string;
      lastDate: string;
      totalBaseVolume: number;
      totalTargetVolume: number;
      allTimeHigh: number;
      allTimeLow: number;
    }>();

    for (const row of rows) {
      const token = row.token;
      if (!token) continue;

      let tokenData = tokenDataMap.get(token);
      if (!tokenData) {
        tokenData = {
          dailyData: [],
          firstDate: '',
          lastDate: '',
          totalBaseVolume: 0,
          totalTargetVolume: 0,
          allTimeHigh: 0,
          allTimeLow: Infinity,
        };
        tokenDataMap.set(token, tokenData);
      }

      const dateStr = row.date || '';
      const baseVolume = parseFloat(row.base_volume) || 0;
      const targetVolume = parseFloat(row.target_volume) || 0;
      const high = parseFloat(row.high) || 0;
      const low = parseFloat(row.low) || Infinity;

      tokenData.dailyData.push({
        token,
        date: dateStr,
        base_volume: row.base_volume || '0',
        target_volume: row.target_volume || '0',
        high: row.high || '0',
        low: row.low || '0',
      });

      tokenData.totalBaseVolume += baseVolume;
      tokenData.totalTargetVolume += targetVolume;
      if (high > tokenData.allTimeHigh) tokenData.allTimeHigh = high;
      if (low > 0 && low < tokenData.allTimeLow) tokenData.allTimeLow = low;

      if (!tokenData.firstDate || dateStr < tokenData.firstDate) {
        tokenData.firstDate = dateStr;
      }
      if (!tokenData.lastDate || dateStr > tokenData.lastDate) {
        tokenData.lastDate = dateStr;
      }
    }

    const tokens: DuneAggregateTokenVolume[] = [];
    let totalTradingDays = 0;

    for (const [token, data] of tokenDataMap.entries()) {
      data.dailyData.sort((a, b) => a.date.localeCompare(b.date));

      tokens.push({
        token,
        first_trade_date: data.firstDate,
        last_trade_date: data.lastDate,
        total_base_volume: data.totalBaseVolume.toFixed(8),
        total_target_volume: data.totalTargetVolume.toFixed(8),
        all_time_high: data.allTimeHigh > 0 ? data.allTimeHigh.toFixed(12) : '0',
        all_time_low: data.allTimeLow < Infinity ? data.allTimeLow.toFixed(12) : '0',
        trading_days: data.dailyData.length,
        daily_data: data.dailyData,
      });

      totalTradingDays += data.dailyData.length;
    }

    tokens.sort((a, b) => parseFloat(b.total_base_volume) - parseFloat(a.total_base_volume));

    return {
      tokens,
      query_metadata: {
        since_start: true,
        token_count: tokens.length,
        total_trading_days: totalTradingDays,
        execution_time_millis: 0,
      },
    };
  }

  /**
   * Convert Dune response to database records
   */
  private convertToRecords(duneData: DuneAggregateVolumeResponse): DailyVolumeRecord[] {
    const records: DailyVolumeRecord[] = [];
    
    for (const tokenData of duneData.tokens) {
      for (const daily of tokenData.daily_data) {
        records.push({
          token: daily.token,
          date: daily.date,
          base_volume: daily.base_volume,
          target_volume: daily.target_volume,
          high: daily.high,
          low: daily.low,
        });
      }
    }
    
    return records;
  }

  /**
   * Update the in-memory cache with Dune data
   */
  private updateInMemoryCache(duneData: DuneAggregateVolumeResponse): void {
    for (const tokenData of duneData.tokens) {
      const existing = this.inMemoryCache.get(tokenData.token.toLowerCase());
      
      if (existing) {
        // Merge daily data
        const dateMap = new Map(existing.daily_data.map(d => [d.date, d]));
        for (const daily of tokenData.daily_data) {
          dateMap.set(daily.date, daily);
        }
        
        const mergedDailyData = Array.from(dateMap.values()).sort((a, b) => a.date.localeCompare(b.date));
        
        // Recalculate aggregates
        let totalBase = 0;
        let totalTarget = 0;
        let allTimeHigh = 0;
        let allTimeLow = Infinity;
        
        for (const daily of mergedDailyData) {
          totalBase += parseFloat(daily.base_volume) || 0;
          totalTarget += parseFloat(daily.target_volume) || 0;
          const high = parseFloat(daily.high) || 0;
          const low = parseFloat(daily.low) || Infinity;
          if (high > allTimeHigh) allTimeHigh = high;
          if (low > 0 && low < allTimeLow) allTimeLow = low;
        }
        
        this.inMemoryCache.set(tokenData.token.toLowerCase(), {
          token: tokenData.token,
          first_trade_date: mergedDailyData[0]?.date || tokenData.first_trade_date,
          last_trade_date: mergedDailyData[mergedDailyData.length - 1]?.date || tokenData.last_trade_date,
          total_base_volume: totalBase.toFixed(8),
          total_target_volume: totalTarget.toFixed(8),
          all_time_high: allTimeHigh.toFixed(12),
          all_time_low: allTimeLow < Infinity ? allTimeLow.toFixed(12) : '0',
          trading_days: mergedDailyData.length,
          daily_data: mergedDailyData,
        });
      } else {
        this.inMemoryCache.set(tokenData.token.toLowerCase(), tokenData);
      }
    }
  }

  /**
   * Get aggregate volume data for tokens
   * Returns from database if available, otherwise from in-memory cache or Dune
   */
  async getAggregateVolume(tokenAddresses?: string[]): Promise<DuneAggregateVolumeResponse> {
    // First, try to get from database
    if (this.databaseService.isAvailable()) {
      const dbData = await this.databaseService.getAggregatedVolumes(tokenAddresses);
      
      if (dbData.length > 0) {
        // Convert to DuneAggregateVolumeResponse format
        const tokens: DuneAggregateTokenVolume[] = dbData.map(data => ({
          token: data.token,
          first_trade_date: data.first_trade_date,
          last_trade_date: data.last_trade_date,
          total_base_volume: data.total_base_volume,
          total_target_volume: data.total_target_volume,
          all_time_high: data.all_time_high,
          all_time_low: data.all_time_low,
          trading_days: data.trading_days,
          daily_data: data.daily_data.map(d => ({
            token: d.token,
            date: d.date,
            base_volume: d.base_volume,
            target_volume: d.target_volume,
            high: d.high,
            low: d.low,
          })),
        }));

        return {
          tokens,
          query_metadata: {
            since_start: true,
            token_count: tokens.length,
            total_trading_days: tokens.reduce((sum, t) => sum + t.trading_days, 0),
            execution_time_millis: 0,
          },
        };
      }
    }

    // Fall back to in-memory cache
    if (this.inMemoryCache.size > 0) {
      let tokens: DuneAggregateTokenVolume[];
      
      if (tokenAddresses && tokenAddresses.length > 0) {
        tokens = tokenAddresses
          .map(addr => this.inMemoryCache.get(addr.toLowerCase()))
          .filter((t): t is TokenVolumeAggregate => t !== undefined)
          .map(data => ({
            ...data,
            daily_data: data.daily_data.map(d => ({
              token: d.token,
              date: d.date,
              base_volume: d.base_volume,
              target_volume: d.target_volume,
              high: d.high,
              low: d.low,
            })),
          }));
      } else {
        tokens = Array.from(this.inMemoryCache.values()).map(data => ({
          ...data,
          daily_data: data.daily_data.map(d => ({
            token: d.token,
            date: d.date,
            base_volume: d.base_volume,
            target_volume: d.target_volume,
            high: d.high,
            low: d.low,
          })),
        }));
      }

      return {
        tokens,
        query_metadata: {
          since_start: true,
          token_count: tokens.length,
          total_trading_days: tokens.reduce((sum, t) => sum + t.trading_days, 0),
          execution_time_millis: 0,
        },
      };
    }

    // No cached data - trigger a fetch (this shouldn't happen in normal operation)
    console.log('[VolumeHistory] No cached data available, fetching from Dune...');
    await this.refresh(tokenAddresses);
    
    // Return whatever we have now
    return this.getAggregateVolume(tokenAddresses);
  }

  /**
   * Get 24h volume data for tokens
   */
  async get24hVolumes(tokenAddresses?: string[]): Promise<Map<string, { base_volume: string; target_volume: string; high: string; low: string }>> {
    if (this.databaseService.isAvailable()) {
      return await this.databaseService.get24hVolumes(tokenAddresses);
    }

    // Fall back to in-memory cache - sum last 2 days
    const volumeMap = new Map<string, { base_volume: string; target_volume: string; high: string; low: string }>();
    const today = new Date().toISOString().split('T')[0]!;
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0]!;

    const tokensToCheck = tokenAddresses 
      ? tokenAddresses.map(t => t.toLowerCase())
      : Array.from(this.inMemoryCache.keys());

    for (const tokenLower of tokensToCheck) {
      const data = this.inMemoryCache.get(tokenLower);
      if (!data) continue;

      let baseVolume = 0;
      let targetVolume = 0;
      let high = 0;
      let low = Infinity;

      for (const daily of data.daily_data) {
        if (daily.date >= yesterday) {
          baseVolume += parseFloat(daily.base_volume) || 0;
          targetVolume += parseFloat(daily.target_volume) || 0;
          const dailyHigh = parseFloat(daily.high) || 0;
          const dailyLow = parseFloat(daily.low) || Infinity;
          if (dailyHigh > high) high = dailyHigh;
          if (dailyLow > 0 && dailyLow < low) low = dailyLow;
        }
      }

      volumeMap.set(tokenLower, {
        base_volume: baseVolume.toFixed(8),
        target_volume: targetVolume.toFixed(8),
        high: high.toFixed(12),
        low: low < Infinity ? low.toFixed(12) : '0',
      });
    }

    return volumeMap;
  }

  /**
   * Get service status
   */
  async getStatus(): Promise<VolumeHistoryStatus> {
    const latestDate = this.databaseService.isAvailable() 
      ? await this.databaseService.getLatestDate() 
      : null;
    const tokenCount = this.databaseService.isAvailable()
      ? await this.databaseService.getTokenCount()
      : this.inMemoryCache.size;
    const recordCount = this.databaseService.isAvailable()
      ? await this.databaseService.getRecordCount()
      : Array.from(this.inMemoryCache.values()).reduce((sum, t) => sum + t.trading_days, 0);

    return {
      isInitialized: this.isInitialized,
      databaseConnected: this.databaseService.isAvailable(),
      latestDate,
      tokenCount,
      recordCount,
      lastSyncTime: this.lastSyncTime,
      isRefreshing: this.isRefreshing,
      incrementalQueryId: config.dune.incrementalVolumeQueryId || null,
      schedule: {
        dailyRefresh: '00:01 UTC',
        queriesPerDay: 1,
      },
    };
  }

  /**
   * Force an immediate refresh
   */
  async forceRefresh(tokenAddresses?: string[]): Promise<void> {
    console.log('[VolumeHistory] Force refresh requested');
    await this.refresh(tokenAddresses);
  }
}

