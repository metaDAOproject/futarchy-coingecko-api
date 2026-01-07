import { DatabaseService, HourlyVolumeRecord, Rolling24hMetrics } from './databaseService.js';
import { DuneService } from './duneService.js';
import { FutarchyService } from './futarchyService.js';
import { config } from '../config.js';

// The timestamp when Futarchy trading data begins
const FUTARCHY_START_TIME = '2025-10-09 00:00:00';

export interface HourlyVolumeStatus {
  isInitialized: boolean;
  databaseConnected: boolean;
  latestHour: string | null;
  latestCompleteHour: string | null;
  tokenCount: number;
  recordCount: number;
  lastRefreshTime: Date | null;
  isRefreshing: boolean;
  hourlyQueryId: number | null;
  schedule: {
    hourlyRefresh: string;  // ":01 past each hour"
    tenMinRefresh: string;  // "every 10 minutes"
    queriesPerDay: number;  // 24 + 144 = 168
  };
}

export class HourlyVolumeService {
  private databaseService: DatabaseService;
  private duneService: DuneService;
  private futarchyService: FutarchyService;
  private _isInitialized: boolean = false;
  private isRefreshing: boolean = false;
  private lastRefreshTime: Date | null = null;
  
  // Scheduled timers
  private hourlyTimer: ReturnType<typeof setTimeout> | null = null;
  private tenMinTimer: ReturnType<typeof setTimeout> | null = null;

  // In-memory cache for when DB is unavailable
  private inMemoryHourlyCache: Map<string, HourlyVolumeRecord[]> = new Map();

  constructor(databaseService: DatabaseService, duneService: DuneService, futarchyService: FutarchyService) {
    this.databaseService = databaseService;
    this.duneService = duneService;
    this.futarchyService = futarchyService;
  }

  get isInitialized(): boolean {
    return this._isInitialized;
  }

  isDatabaseConnected(): boolean {
    return this.databaseService.isAvailable();
  }

  /**
   * Initialize the service
   */
  async initialize(): Promise<void> {
    console.log('[HourlyVolume] Initializing service...');

    if (this.databaseService.isAvailable()) {
      const recordCount = await this.databaseService.getHourlyRecordCount();
      const latestHour = await this.databaseService.getLatestHour();

      if (recordCount > 0) {
        console.log(`[HourlyVolume] Found ${recordCount} existing hourly records, latest: ${latestHour}`);
      } else {
        console.log('[HourlyVolume] No existing hourly data, will backfill on first refresh');
      }
    } else {
      console.log('[HourlyVolume] Database not available, will use in-memory cache');
    }

    this._isInitialized = true;
    console.log('[HourlyVolume] Service initialized');
  }

  /**
   * Start the scheduled refresh jobs
   * - Hourly: runs at :01 past each hour (fetches complete hours)
   * - 10-min: runs at :00, :10, :20, :30, :40, :50 (fetches current incomplete hour)
   */
  async start(): Promise<void> {
    if (!this._isInitialized) {
      await this.initialize();
    }

    // Check if we have data to serve immediately
    if (this.databaseService.isAvailable()) {
      const recordCount = await this.databaseService.getHourlyRecordCount();
      if (recordCount > 0) {
        console.log(`[HourlyVolume] Ready to serve ${recordCount} cached hourly records`);
      }
    }

    console.log('[HourlyVolume] Starting scheduled refresh jobs...');
    console.log('[HourlyVolume] - Hourly refresh: :01 past each hour (24 queries/day)');
    console.log('[HourlyVolume] - 10-min refresh: every 10 minutes (144 queries/day)');

    // Schedule hourly refresh (at :01 past each hour)
    this.scheduleHourlyRefresh();
    
    // Schedule 10-minute refresh (at :00, :10, :20, :30, :40, :50)
    this.scheduleTenMinRefresh();

    console.log('[HourlyVolume] Scheduled refresh jobs started');

    // Do initial refresh in background (non-blocking) if query ID is configured
    if (config.dune.hourlyVolumeQueryId) {
      console.log('[HourlyVolume] Starting background refresh...');
      this.refresh().catch(err => {
        console.error('[HourlyVolume] Background refresh failed:', err.message);
      });
    } else {
      console.log('[HourlyVolume] No DUNE_HOURLY_VOLUME_QUERY_ID - serving existing DB data only');
    }
  }

  /**
   * Schedule the next hourly refresh at :01 past the hour
   */
  private scheduleHourlyRefresh(): void {
    const now = new Date();
    const nextHour = new Date(now);
    nextHour.setHours(nextHour.getHours() + 1);
    nextHour.setMinutes(1, 0, 0); // :01:00

    // If we're past :01, schedule for next hour
    if (now.getMinutes() >= 1) {
      // Already past :01, next run is in the calculated time
    } else {
      // Before :01, run at :01 this hour
      nextHour.setHours(now.getHours());
    }

    const msUntilNext = nextHour.getTime() - now.getTime();
    console.log(`[HourlyVolume] Next hourly refresh at ${nextHour.toISOString()} (in ${Math.round(msUntilNext / 1000)}s)`);

    this.hourlyTimer = setTimeout(async () => {
      await this.refreshCompleteHours();
      this.scheduleHourlyRefresh(); // Schedule next
    }, msUntilNext);
  }

  /**
   * Schedule the next 10-minute refresh at the next 10-minute mark
   */
  private scheduleTenMinRefresh(): void {
    const now = new Date();
    const currentMinute = now.getMinutes();
    const nextTenMin = Math.ceil((currentMinute + 1) / 10) * 10;
    
    const nextRun = new Date(now);
    if (nextTenMin >= 60) {
      nextRun.setHours(nextRun.getHours() + 1);
      nextRun.setMinutes(0, 30, 0); // :00:30 of next hour (30 sec buffer)
    } else {
      nextRun.setMinutes(nextTenMin, 30, 0); // 30 sec buffer after the mark
    }

    const msUntilNext = nextRun.getTime() - now.getTime();
    console.log(`[HourlyVolume] Next 10-min refresh at ${nextRun.toISOString()} (in ${Math.round(msUntilNext / 1000)}s)`);

    this.tenMinTimer = setTimeout(async () => {
      await this.refreshCurrentHour();
      this.scheduleTenMinRefresh(); // Schedule next
    }, msUntilNext);
  }

  /**
   * Stop the scheduled refresh jobs
   */
  stop(): void {
    if (this.hourlyTimer) {
      clearTimeout(this.hourlyTimer);
      this.hourlyTimer = null;
    }
    if (this.tenMinTimer) {
      clearTimeout(this.tenMinTimer);
      this.tenMinTimer = null;
    }
    console.log('[HourlyVolume] Scheduled refresh jobs stopped');
  }

  /**
   * Refresh only complete hours (called at :01 past each hour)
   * This is efficient - only fetches the hour that just completed
   */
  private async refreshCompleteHours(): Promise<void> {
    if (this.isRefreshing) {
      console.log('[HourlyVolume] Refresh already in progress, skipping hourly refresh');
      return;
    }

    this.isRefreshing = true;
    console.log('[HourlyVolume] Hourly refresh - fetching last complete hour...');

    try {
      // Get the hour that just completed (current hour - 1)
      const lastCompleteHour = new Date();
      lastCompleteHour.setMinutes(0, 0, 0);
      lastCompleteHour.setHours(lastCompleteHour.getHours() - 1);
      
      const startTime = lastCompleteHour.toISOString().replace('T', ' ').replace('Z', '').slice(0, 19);
      const endTime = new Date(lastCompleteHour.getTime() + 60 * 60 * 1000 - 1).toISOString().replace('T', ' ').replace('Z', '').slice(0, 19);

      const hourlyData = await this.fetchHourlyFromDune(startTime);

      if (hourlyData && hourlyData.length > 0) {
        // All these records are for complete hours
        if (this.databaseService.isAvailable()) {
          await this.databaseService.upsertHourlyVolumes(hourlyData, true);
        }
        this.updateInMemoryCache(hourlyData);
        console.log(`[HourlyVolume] Hourly refresh complete - ${hourlyData.length} records`);
      }

      this.lastRefreshTime = new Date();
    } catch (error: any) {
      console.error('[HourlyVolume] Error in hourly refresh:', error.message);
    } finally {
      this.isRefreshing = false;
    }
  }

  /**
   * Refresh only the current incomplete hour (called every 10 minutes)
   */
  private async refreshCurrentHour(): Promise<void> {
    if (this.isRefreshing) {
      console.log('[HourlyVolume] Refresh already in progress, skipping 10-min refresh');
      return;
    }

    this.isRefreshing = true;
    console.log('[HourlyVolume] 10-min refresh - fetching current hour...');

    try {
      // Get data from the start of current hour
      const currentHourStart = new Date();
      currentHourStart.setMinutes(0, 0, 0);
      const startTime = currentHourStart.toISOString().replace('T', ' ').replace('Z', '').slice(0, 19);

      const hourlyData = await this.fetchHourlyFromDune(startTime);

      if (hourlyData && hourlyData.length > 0) {
        // These are incomplete hour records
        if (this.databaseService.isAvailable()) {
          await this.databaseService.upsertHourlyVolumes(hourlyData, false);
        }
        this.updateInMemoryCache(hourlyData);
        console.log(`[HourlyVolume] 10-min refresh complete - ${hourlyData.length} records`);
      }

      this.lastRefreshTime = new Date();
    } catch (error: any) {
      console.error('[HourlyVolume] Error in 10-min refresh:', error.message);
    } finally {
      this.isRefreshing = false;
    }
  }

  /**
   * Full refresh / backfill hourly data from Dune
   * Used for:
   * - Initial backfill when no data exists
   * - Manual force refresh
   * Strategy:
   * - If no data: backfill from FUTARCHY_START_TIME
   * - If has data: only fetch from last complete hour onwards
   */
  async refresh(): Promise<void> {
    if (this.isRefreshing) {
      console.log('[HourlyVolume] Refresh already in progress, skipping');
      return;
    }

    this.isRefreshing = true;
    const startTime = Date.now();
    console.log('[HourlyVolume] Starting full refresh/backfill...');

    try {
      // Determine start time for fetch
      let fetchStartTime = FUTARCHY_START_TIME;
      let isBackfill = true;

      if (this.databaseService.isAvailable()) {
        // Mark any old incomplete hours as complete (hour boundary passed)
        const currentHourStart = this.getCurrentHourStart();
        await this.databaseService.markHoursComplete(currentHourStart);

        // Get the latest complete hour
        const latestCompleteHour = await this.databaseService.getLatestCompleteHour();
        
        if (latestCompleteHour) {
          // Fetch from the hour after the latest complete hour
          // But go back 1 hour for safety overlap
          const latestDate = new Date(latestCompleteHour);
          fetchStartTime = latestDate.toISOString().replace('T', ' ').replace('Z', '').slice(0, 19);
          isBackfill = false;
          console.log(`[HourlyVolume] Incremental fetch from ${fetchStartTime}`);
        } else {
          console.log(`[HourlyVolume] Full backfill from ${fetchStartTime}`);
        }
      }

      // Fetch from Dune
      const hourlyData = await this.fetchHourlyFromDune(fetchStartTime);

      if (hourlyData && hourlyData.length > 0) {
        // Determine which records are complete (not current hour)
        const currentHourStart = this.getCurrentHourStart();
        const completeRecords = hourlyData.filter(r => r.hour < currentHourStart);
        const currentHourRecords = hourlyData.filter(r => r.hour >= currentHourStart);

        if (this.databaseService.isAvailable()) {
          // Upsert complete hours
          if (completeRecords.length > 0) {
            await this.databaseService.upsertHourlyVolumes(completeRecords, true);
          }
          // Upsert current hour (not complete)
          if (currentHourRecords.length > 0) {
            await this.databaseService.upsertHourlyVolumes(currentHourRecords, false);
          }

          // Note: Not pruning data - keeping all historical records
        }

        // Update in-memory cache
        this.updateInMemoryCache(hourlyData);

        console.log(`[HourlyVolume] Processed ${hourlyData.length} hourly records (${completeRecords.length} complete, ${currentHourRecords.length} current hour)`);
      } else {
        console.log('[HourlyVolume] No new hourly data from Dune');
      }

      this.lastRefreshTime = new Date();
      const duration = Date.now() - startTime;
      console.log(`[HourlyVolume] Refresh completed in ${duration}ms`);
    } catch (error: any) {
      console.error('[HourlyVolume] Error during refresh:', error.message);
    } finally {
      this.isRefreshing = false;
    }
  }

  /**
   * Fetch hourly data from Dune using the hourly query
   * Only fetches tokens from active, non-excluded DAOs
   */
  private async fetchHourlyFromDune(startTime: string): Promise<HourlyVolumeRecord[]> {
    const hourlyQueryId = config.dune.hourlyVolumeQueryId;

    if (!hourlyQueryId) {
      console.warn('[HourlyVolume] No hourly query ID configured (DUNE_HOURLY_VOLUME_QUERY_ID)');
      return [];
    }

    try {
      // Get valid tokens from FutarchyService (excludes EXCLUDED_DAOS)
      const allDaos = await this.futarchyService.getAllDaos();
      const validTokens = allDaos.map(dao => dao.baseMint.toString());
      
      if (validTokens.length === 0) {
        console.warn('[HourlyVolume] No valid tokens to fetch');
        return [];
      }

      console.log(`[HourlyVolume] Fetching from Dune query ${hourlyQueryId} with start_time: ${startTime} for ${validTokens.length} tokens`);

      // Build token list parameter (excludes EXCLUDED_DAOS)
      const tokenListParam = validTokens.map(token => `'${token}'`).join(', ');

      const parameters: Record<string, any> = {
        start_time: startTime,
        token_list: tokenListParam,
      };

      const result = await this.duneService.executeQueryManually(hourlyQueryId, parameters);

      if (!result.rows || result.rows.length === 0) {
        return [];
      }

      // Convert to HourlyVolumeRecord format
      const records: HourlyVolumeRecord[] = result.rows.map((row: any) => ({
        token: row.token,
        hour: this.normalizeHourTimestamp(row.hour),
        base_volume: row.base_volume || '0',
        target_volume: row.target_volume || '0',
        high: row.high || '0',
        low: row.low || '0',
        trade_count: parseInt(row.trade_count) || 0,
      }));

      console.log(`[HourlyVolume] Fetched ${records.length} hourly records from Dune`);
      return records;
    } catch (error: any) {
      console.error('[HourlyVolume] Error fetching from Dune:', error.message);
      return [];
    }
  }

  /**
   * Normalize hour timestamp to consistent ISO format
   */
  private normalizeHourTimestamp(hour: string): string {
    // Dune may return different formats, normalize to ISO
    const date = new Date(hour);
    return date.toISOString();
  }

  /**
   * Get the start of the current hour as ISO string
   */
  private getCurrentHourStart(): string {
    const now = new Date();
    now.setMinutes(0, 0, 0);
    return now.toISOString();
  }

  /**
   * Update in-memory cache with hourly data
   */
  private updateInMemoryCache(records: HourlyVolumeRecord[]): void {
    for (const record of records) {
      const tokenLower = record.token.toLowerCase();
      let tokenRecords = this.inMemoryHourlyCache.get(tokenLower);
      
      if (!tokenRecords) {
        tokenRecords = [];
        this.inMemoryHourlyCache.set(tokenLower, tokenRecords);
      }

      // Find and update or add
      const existingIndex = tokenRecords.findIndex(r => r.hour === record.hour);
      if (existingIndex >= 0) {
        tokenRecords[existingIndex] = record;
      } else {
        tokenRecords.push(record);
      }
    }

    // Prune old data from in-memory cache (keep 48 hours)
    const cutoffTime = new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString();
    for (const [token, records] of this.inMemoryHourlyCache.entries()) {
      const filtered = records.filter(r => r.hour >= cutoffTime);
      this.inMemoryHourlyCache.set(token, filtered);
    }
  }

  /**
   * Get rolling 24h metrics for tokens
   * Primary method for serving /api/tickers
   */
  async getRolling24hMetrics(tokens?: string[]): Promise<Map<string, Rolling24hMetrics>> {
    // Try database first
    if (this.databaseService.isAvailable()) {
      const dbMetrics = await this.databaseService.getRolling24hMetrics(tokens);
      if (dbMetrics.size > 0) {
        return dbMetrics;
      }
    }

    // Fall back to in-memory cache
    return this.calculateRolling24hFromCache(tokens);
  }

  /**
   * Calculate rolling 24h from in-memory cache
   */
  private calculateRolling24hFromCache(tokens?: string[]): Map<string, Rolling24hMetrics> {
    const metricsMap = new Map<string, Rolling24hMetrics>();
    const cutoffTime = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

    const tokensToCheck = tokens
      ? tokens.map(t => t.toLowerCase())
      : Array.from(this.inMemoryHourlyCache.keys());

    for (const tokenLower of tokensToCheck) {
      const records = this.inMemoryHourlyCache.get(tokenLower);
      if (!records) continue;

      const recent = records.filter(r => r.hour >= cutoffTime);
      if (recent.length === 0) continue;

      let baseVolume = 0;
      let targetVolume = 0;
      let high = 0;
      let low = Infinity;
      let tradeCount = 0;

      for (const record of recent) {
        baseVolume += parseFloat(record.base_volume) || 0;
        targetVolume += parseFloat(record.target_volume) || 0;
        const recordHigh = parseFloat(record.high) || 0;
        const recordLow = parseFloat(record.low) || Infinity;
        if (recordHigh > high) high = recordHigh;
        if (recordLow > 0 && recordLow < low) low = recordLow;
        tradeCount += record.trade_count || 0;
      }

      metricsMap.set(tokenLower, {
        token: tokenLower,
        base_volume_24h: baseVolume.toFixed(8),
        target_volume_24h: targetVolume.toFixed(8),
        high_24h: high > 0 ? high.toFixed(12) : '0',
        low_24h: low < Infinity ? low.toFixed(12) : '0',
        trade_count_24h: tradeCount,
      });
    }

    return metricsMap;
  }

  /**
   * Force an immediate refresh
   */
  async forceRefresh(): Promise<void> {
    console.log('[HourlyVolume] Force refresh requested');
    await this.refresh();
  }

  /**
   * Get service status
   */
  async getStatus(): Promise<HourlyVolumeStatus> {
    const latestHour = this.databaseService.isAvailable()
      ? await this.databaseService.getLatestHour()
      : null;
    const latestCompleteHour = this.databaseService.isAvailable()
      ? await this.databaseService.getLatestCompleteHour()
      : null;
    const tokenCount = this.databaseService.isAvailable()
      ? await this.databaseService.getHourlyTokenCount()
      : this.inMemoryHourlyCache.size;
    const recordCount = this.databaseService.isAvailable()
      ? await this.databaseService.getHourlyRecordCount()
      : Array.from(this.inMemoryHourlyCache.values()).reduce((sum, records) => sum + records.length, 0);

    return {
      isInitialized: this._isInitialized,
      databaseConnected: this.databaseService.isAvailable(),
      latestHour,
      latestCompleteHour,
      tokenCount,
      recordCount,
      lastRefreshTime: this.lastRefreshTime,
      isRefreshing: this.isRefreshing,
      hourlyQueryId: config.dune.hourlyVolumeQueryId || null,
      schedule: {
        hourlyRefresh: ':01 past each hour',
        tenMinRefresh: 'every 10 minutes (:00, :10, :20, :30, :40, :50)',
        queriesPerDay: 24 + 144, // 168 queries/day
      },
    };
  }
}

