import pg from 'pg';
import { config } from '../config.js';

const { Pool } = pg;

export interface DailyVolumeRecord {
  token: string;
  date: string; // YYYY-MM-DD
  base_volume: string;
  target_volume: string;
  high: string;
  low: string;
}

export interface HourlyVolumeRecord {
  token: string;
  hour: string; // ISO timestamp (YYYY-MM-DD HH:00:00)
  base_volume: string;
  target_volume: string;
  high: string;
  low: string;
  trade_count: number;
}

export interface TenMinuteVolumeRecord {
  token: string;
  bucket: string; // ISO timestamp (YYYY-MM-DD HH:M0:00 where M is 0,1,2,3,4,5)
  base_volume: string;
  target_volume: string;
  high: string;
  low: string;
  trade_count: number;
}

export interface DailyBuySellVolumeRecord {
  token: string;
  date: string; // YYYY-MM-DD
  base_volume: string;
  target_volume: string;
  buy_usdc_volume: string;
  sell_token_volume: string;
  high: string;
  low: string;
  trade_count: number;
}

export interface CumulativeVolumeData {
  token: string;
  date: string;
  base_volume: string;
  target_volume: string;
  buy_usdc_volume: string;
  sell_token_volume: string;
  cumulative_target_volume: string;
  cumulative_base_volume: string;
  cumulative_buy_usdc_volume: string;
  cumulative_sell_token_volume: string;
  high: string;
  low: string;
}

export interface Rolling24hMetrics {
  token: string;
  base_volume_24h: string;
  target_volume_24h: string;
  high_24h: string;
  low_24h: string;
  trade_count_24h: number;
}

export interface TokenVolumeAggregate {
  token: string;
  first_trade_date: string;
  last_trade_date: string;
  total_base_volume: string;
  total_target_volume: string;
  all_time_high: string;
  all_time_low: string;
  trading_days: number;
  daily_data: DailyVolumeRecord[];
}

export class DatabaseService {
  private pool: pg.Pool | null = null;
  private isConnected: boolean = false;

  constructor() {
    // Only initialize if database config is provided
    if (config.database.connectionString || config.database.host) {
      this.pool = new Pool({
        connectionString: config.database.connectionString,
        host: config.database.host,
        port: config.database.port,
        database: config.database.database,
        user: config.database.user,
        password: config.database.password,
        ssl: config.database.ssl ? { rejectUnauthorized: false } : false,
        max: 10, // Max connections in pool
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 10000,
      });
    }
  }

  /**
   * Initialize the database connection and create tables if needed
   */
  async initialize(): Promise<boolean> {
    if (!this.pool) {
      console.log('[Database] No database configuration provided, volume history will use in-memory cache only');
      return false;
    }

    try {
      // Test connection
      const client = await this.pool.connect();
      console.log('[Database] Connected to PostgreSQL');
      client.release();

      // Create tables
      await this.createTables();
      this.isConnected = true;
      return true;
    } catch (error: any) {
      console.error('[Database] Failed to connect:', error.message);
      this.isConnected = false;
      return false;
    }
  }

  /**
   * Create required tables if they don't exist
   */
  private async createTables(): Promise<void> {
    if (!this.pool) return;

    const createTableSQL = `
      CREATE TABLE IF NOT EXISTS daily_volumes (
        id SERIAL PRIMARY KEY,
        token VARCHAR(64) NOT NULL,
        date DATE NOT NULL,
        base_volume NUMERIC(40, 12) NOT NULL,
        target_volume NUMERIC(40, 12) NOT NULL,
        high NUMERIC(40, 12) NOT NULL,
        low NUMERIC(40, 12) NOT NULL,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(token, date)
      );

      CREATE INDEX IF NOT EXISTS idx_daily_volumes_token ON daily_volumes(token);
      CREATE INDEX IF NOT EXISTS idx_daily_volumes_date ON daily_volumes(date);
      CREATE INDEX IF NOT EXISTS idx_daily_volumes_token_date ON daily_volumes(token, date);

      -- Hourly volumes table for hourly aggregates
      CREATE TABLE IF NOT EXISTS hourly_volumes (
        id SERIAL PRIMARY KEY,
        token VARCHAR(64) NOT NULL,
        hour TIMESTAMPTZ NOT NULL,
        base_volume NUMERIC(40, 12) NOT NULL,
        target_volume NUMERIC(40, 12) NOT NULL,
        high NUMERIC(40, 12) NOT NULL,
        low NUMERIC(40, 12) NOT NULL,
        trade_count INT NOT NULL DEFAULT 0,
        is_complete BOOLEAN NOT NULL DEFAULT false,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(token, hour)
      );

      CREATE INDEX IF NOT EXISTS idx_hourly_volumes_token ON hourly_volumes(token);
      CREATE INDEX IF NOT EXISTS idx_hourly_volumes_hour ON hourly_volumes(hour);
      CREATE INDEX IF NOT EXISTS idx_hourly_volumes_token_hour ON hourly_volumes(token, hour);
      CREATE INDEX IF NOT EXISTS idx_hourly_volumes_recent ON hourly_volumes(hour DESC);

      -- 10-minute volumes table for accurate rolling 24h calculations
      CREATE TABLE IF NOT EXISTS ten_minute_volumes (
        id SERIAL PRIMARY KEY,
        token VARCHAR(64) NOT NULL,
        bucket TIMESTAMPTZ NOT NULL,
        base_volume NUMERIC(40, 12) NOT NULL,
        target_volume NUMERIC(40, 12) NOT NULL,
        high NUMERIC(40, 12) NOT NULL,
        low NUMERIC(40, 12) NOT NULL,
        trade_count INT NOT NULL DEFAULT 0,
        is_complete BOOLEAN NOT NULL DEFAULT false,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(token, bucket)
      );

      CREATE INDEX IF NOT EXISTS idx_ten_minute_volumes_token ON ten_minute_volumes(token);
      CREATE INDEX IF NOT EXISTS idx_ten_minute_volumes_bucket ON ten_minute_volumes(bucket);
      CREATE INDEX IF NOT EXISTS idx_ten_minute_volumes_token_bucket ON ten_minute_volumes(token, bucket);
      CREATE INDEX IF NOT EXISTS idx_ten_minute_volumes_recent ON ten_minute_volumes(bucket DESC);

      -- Metadata table to track sync status
      CREATE TABLE IF NOT EXISTS sync_metadata (
        key VARCHAR(64) PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
      );

      -- Daily buy/sell volumes table for tracking directional volume
      CREATE TABLE IF NOT EXISTS daily_buy_sell_volumes (
        id SERIAL PRIMARY KEY,
        token VARCHAR(64) NOT NULL,
        date DATE NOT NULL,
        base_volume NUMERIC(40, 12) NOT NULL DEFAULT 0,
        target_volume NUMERIC(40, 12) NOT NULL DEFAULT 0,
        buy_usdc_volume NUMERIC(40, 12) NOT NULL DEFAULT 0,
        sell_token_volume NUMERIC(40, 12) NOT NULL DEFAULT 0,
        high NUMERIC(40, 12) NOT NULL DEFAULT 0,
        low NUMERIC(40, 12) NOT NULL DEFAULT 0,
        trade_count INT NOT NULL DEFAULT 0,
        is_complete BOOLEAN NOT NULL DEFAULT false,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(token, date)
      );

      CREATE INDEX IF NOT EXISTS idx_daily_buy_sell_volumes_token ON daily_buy_sell_volumes(token);
      CREATE INDEX IF NOT EXISTS idx_daily_buy_sell_volumes_date ON daily_buy_sell_volumes(date);
      CREATE INDEX IF NOT EXISTS idx_daily_buy_sell_volumes_token_date ON daily_buy_sell_volumes(token, date);

      -- Metrics history table for storing periodic snapshots of system metrics
      CREATE TABLE IF NOT EXISTS metrics_history (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        metric_name VARCHAR(128) NOT NULL,
        metric_value NUMERIC(40, 12) NOT NULL,
        labels JSONB DEFAULT '{}',
        UNIQUE(timestamp, metric_name, labels)
      );

      CREATE INDEX IF NOT EXISTS idx_metrics_history_timestamp ON metrics_history(timestamp DESC);
      CREATE INDEX IF NOT EXISTS idx_metrics_history_name ON metrics_history(metric_name);
      CREATE INDEX IF NOT EXISTS idx_metrics_history_name_time ON metrics_history(metric_name, timestamp DESC);

      -- Service health snapshots for historical analysis
      CREATE TABLE IF NOT EXISTS service_health_snapshots (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        service_name VARCHAR(64) NOT NULL,
        is_healthy BOOLEAN NOT NULL,
        last_refresh_time TIMESTAMPTZ,
        record_count INT,
        error_message TEXT,
        metadata JSONB DEFAULT '{}'
      );

      CREATE INDEX IF NOT EXISTS idx_service_health_timestamp ON service_health_snapshots(timestamp DESC);
      CREATE INDEX IF NOT EXISTS idx_service_health_service ON service_health_snapshots(service_name);
      CREATE INDEX IF NOT EXISTS idx_service_health_service_time ON service_health_snapshots(service_name, timestamp DESC);
    `;

    await this.pool.query(createTableSQL);
    console.log('[Database] Tables created/verified');
  }

  /**
   * Check if database is available
   */
  isAvailable(): boolean {
    return this.isConnected && this.pool !== null;
  }

  /**
   * Get the latest date we have data for (across all tokens)
   */
  async getLatestDate(): Promise<string | null> {
    if (!this.pool || !this.isConnected) return null;

    try {
      const result = await this.pool.query(
        'SELECT MAX(date) as latest_date FROM daily_volumes'
      );
      return result.rows[0]?.latest_date?.toISOString().split('T')[0] || null;
    } catch (error: any) {
      console.error('[Database] Error getting latest date:', error.message);
      return null;
    }
  }

  /**
   * Get the latest date for a specific token
   */
  async getLatestDateForToken(token: string): Promise<string | null> {
    if (!this.pool || !this.isConnected) return null;

    try {
      const result = await this.pool.query(
        'SELECT MAX(date) as latest_date FROM daily_volumes WHERE LOWER(token) = LOWER($1)',
        [token]
      );
      return result.rows[0]?.latest_date?.toISOString().split('T')[0] || null;
    } catch (error: any) {
      console.error('[Database] Error getting latest date for token:', error.message);
      return null;
    }
  }

  /**
   * Upsert daily volume records using batched inserts for performance
   */
  async upsertDailyVolumes(records: DailyVolumeRecord[]): Promise<number> {
    if (!this.pool || !this.isConnected || records.length === 0) return 0;

    const BATCH_SIZE = 500; // Insert 500 records per batch
    let totalUpserted = 0;

    try {
      const client = await this.pool.connect();

      try {
        await client.query('BEGIN');

        // Process in batches
        for (let i = 0; i < records.length; i += BATCH_SIZE) {
          const batch = records.slice(i, i + BATCH_SIZE);
          
          // Build multi-value INSERT statement
          const values: any[] = [];
          const valuePlaceholders: string[] = [];
          
          batch.forEach((record, idx) => {
            const offset = idx * 6; // 6 parameters per record
            valuePlaceholders.push(
              `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, CURRENT_TIMESTAMP)`
            );
            values.push(
              record.token,
              record.date,
              record.base_volume,
              record.target_volume,
              record.high,
              record.low
            );
          });

          const batchSQL = `
            INSERT INTO daily_volumes (token, date, base_volume, target_volume, high, low, updated_at)
            VALUES ${valuePlaceholders.join(', ')}
            ON CONFLICT (token, date) 
            DO UPDATE SET 
              base_volume = EXCLUDED.base_volume,
              target_volume = EXCLUDED.target_volume,
              high = EXCLUDED.high,
              low = EXCLUDED.low,
              updated_at = CURRENT_TIMESTAMP
          `;

          await client.query(batchSQL, values);
          totalUpserted += batch.length;
          
          // Log progress for large batches
          if (records.length > BATCH_SIZE) {
            console.log(`[Database] Daily volume batch progress: ${totalUpserted}/${records.length}`);
          }
        }

        await client.query('COMMIT');
        console.log(`[Database] Upserted ${totalUpserted} daily volume records`);
        return totalUpserted;
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }
    } catch (error: any) {
      console.error('[Database] Error upserting daily volumes:', error.message);
      return 0;
    }
  }

  /**
   * Get all daily volumes for a token
   */
  async getDailyVolumesForToken(token: string): Promise<DailyVolumeRecord[]> {
    if (!this.pool || !this.isConnected) return [];

    try {
      const result = await this.pool.query(
        `SELECT token, date::text, 
                base_volume::text, target_volume::text, 
                high::text, low::text
         FROM daily_volumes 
         WHERE LOWER(token) = LOWER($1) 
         ORDER BY date ASC`,
        [token]
      );
      return result.rows;
    } catch (error: any) {
      console.error('[Database] Error getting daily volumes for token:', error.message);
      return [];
    }
  }

  /**
   * Get daily volumes for multiple tokens
   */
  async getDailyVolumesForTokens(tokens: string[]): Promise<Map<string, DailyVolumeRecord[]>> {
    if (!this.pool || !this.isConnected || tokens.length === 0) {
      return new Map();
    }

    try {
      const placeholders = tokens.map((_, i) => `LOWER($${i + 1})`).join(', ');
      const result = await this.pool.query(
        `SELECT token, date::text, 
                base_volume::text, target_volume::text, 
                high::text, low::text
         FROM daily_volumes 
         WHERE LOWER(token) IN (${placeholders})
         ORDER BY token, date ASC`,
        tokens.map(t => t.toLowerCase())
      );

      const tokenMap = new Map<string, DailyVolumeRecord[]>();
      for (const row of result.rows) {
        const tokenLower = row.token.toLowerCase();
        if (!tokenMap.has(tokenLower)) {
          tokenMap.set(tokenLower, []);
        }
        tokenMap.get(tokenLower)!.push(row);
      }
      return tokenMap;
    } catch (error: any) {
      console.error('[Database] Error getting daily volumes for tokens:', error.message);
      return new Map();
    }
  }

  /**
   * Get aggregated volume data for all tokens (for API responses)
   */
  async getAggregatedVolumes(tokens?: string[]): Promise<TokenVolumeAggregate[]> {
    if (!this.pool || !this.isConnected) return [];

    try {
      let whereClause = '';
      let params: string[] = [];

      if (tokens && tokens.length > 0) {
        const placeholders = tokens.map((_, i) => `LOWER($${i + 1})`).join(', ');
        whereClause = `WHERE LOWER(token) IN (${placeholders})`;
        params = tokens.map(t => t.toLowerCase());
      }

      // Get aggregates
      const aggregateResult = await this.pool.query(
        `SELECT 
          token,
          MIN(date)::text as first_trade_date,
          MAX(date)::text as last_trade_date,
          SUM(base_volume)::text as total_base_volume,
          SUM(target_volume)::text as total_target_volume,
          MAX(high)::text as all_time_high,
          MIN(CASE WHEN low > 0 THEN low END)::text as all_time_low,
          COUNT(*)::int as trading_days
         FROM daily_volumes
         ${whereClause}
         GROUP BY token
         ORDER BY SUM(base_volume) DESC`,
        params
      );

      // Get daily data for each token
      const dailyDataMap = await this.getDailyVolumesForTokens(
        aggregateResult.rows.map(r => r.token)
      );

      return aggregateResult.rows.map(row => ({
        token: row.token,
        first_trade_date: row.first_trade_date,
        last_trade_date: row.last_trade_date,
        total_base_volume: row.total_base_volume || '0',
        total_target_volume: row.total_target_volume || '0',
        all_time_high: row.all_time_high || '0',
        all_time_low: row.all_time_low || '0',
        trading_days: row.trading_days,
        daily_data: dailyDataMap.get(row.token.toLowerCase()) || [],
      }));
    } catch (error: any) {
      console.error('[Database] Error getting aggregated volumes:', error.message);
      return [];
    }
  }

  /**
   * Get 24h rolling volume data (last 24 hours from current time)
   * This queries data from today and yesterday to cover the 24h window
   */
  async get24hVolumes(tokens?: string[]): Promise<Map<string, { base_volume: string; target_volume: string; high: string; low: string }>> {
    if (!this.pool || !this.isConnected) return new Map();

    try {
      // Get today and yesterday's date
      const today = new Date().toISOString().split('T')[0];
      const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().split('T')[0];

      let whereClause = 'WHERE date >= $1';
      let params: any[] = [yesterday];

      if (tokens && tokens.length > 0) {
        const placeholders = tokens.map((_, i) => `LOWER($${i + 2})`).join(', ');
        whereClause += ` AND LOWER(token) IN (${placeholders})`;
        params = [yesterday, ...tokens.map(t => t.toLowerCase())];
      }

      const result = await this.pool.query(
        `SELECT 
          token,
          SUM(base_volume)::text as base_volume,
          SUM(target_volume)::text as target_volume,
          MAX(high)::text as high,
          MIN(CASE WHEN low > 0 THEN low END)::text as low
         FROM daily_volumes
         ${whereClause}
         GROUP BY token`,
        params
      );

      const volumeMap = new Map();
      for (const row of result.rows) {
        volumeMap.set(row.token.toLowerCase(), {
          base_volume: row.base_volume || '0',
          target_volume: row.target_volume || '0',
          high: row.high || '0',
          low: row.low || '0',
        });
      }
      return volumeMap;
    } catch (error: any) {
      console.error('[Database] Error getting 24h volumes:', error.message);
      return new Map();
    }
  }

  /**
   * Set sync metadata
   */
  async setSyncMetadata(key: string, value: string): Promise<void> {
    if (!this.pool || !this.isConnected) return;

    try {
      await this.pool.query(
        `INSERT INTO sync_metadata (key, value, updated_at)
         VALUES ($1, $2, CURRENT_TIMESTAMP)
         ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = CURRENT_TIMESTAMP`,
        [key, value]
      );
    } catch (error: any) {
      console.error('[Database] Error setting sync metadata:', error.message);
    }
  }

  /**
   * Get sync metadata
   */
  async getSyncMetadata(key: string): Promise<string | null> {
    if (!this.pool || !this.isConnected) return null;

    try {
      const result = await this.pool.query(
        'SELECT value FROM sync_metadata WHERE key = $1',
        [key]
      );
      return result.rows[0]?.value || null;
    } catch (error: any) {
      console.error('[Database] Error getting sync metadata:', error.message);
      return null;
    }
  }

  /**
   * Get total daily volume record count
   */
  async getRecordCount(): Promise<number> {
    return this.getDailyRecordCount();
  }

  /**
   * Get total daily volume record count
   */
  async getDailyRecordCount(): Promise<number> {
    if (!this.pool || !this.isConnected) return 0;

    try {
      const result = await this.pool.query('SELECT COUNT(*) as count FROM daily_volumes');
      return parseInt(result.rows[0]?.count || '0');
    } catch (error: any) {
      console.error('[Database] Error getting daily record count:', error.message);
      return 0;
    }
  }

  /**
   * Get unique token count
   */
  async getTokenCount(): Promise<number> {
    if (!this.pool || !this.isConnected) return 0;

    try {
      const result = await this.pool.query('SELECT COUNT(DISTINCT token) as count FROM daily_volumes');
      return parseInt(result.rows[0]?.count || '0');
    } catch (error: any) {
      console.error('[Database] Error getting token count:', error.message);
      return 0;
    }
  }

  // ============================================
  // HOURLY VOLUME METHODS
  // ============================================

  /**
   * Get the latest hour we have data for (across all tokens)
   */
  async getLatestHour(): Promise<string | null> {
    if (!this.pool || !this.isConnected) return null;

    try {
      const result = await this.pool.query(
        'SELECT MAX(hour) as latest_hour FROM hourly_volumes'
      );
      return result.rows[0]?.latest_hour?.toISOString() || null;
    } catch (error: any) {
      console.error('[Database] Error getting latest hour:', error.message);
      return null;
    }
  }

  /**
   * Get the latest complete hour (where is_complete = true)
   */
  async getLatestCompleteHour(): Promise<string | null> {
    if (!this.pool || !this.isConnected) return null;

    try {
      const result = await this.pool.query(
        'SELECT MAX(hour) as latest_hour FROM hourly_volumes WHERE is_complete = true'
      );
      return result.rows[0]?.latest_hour?.toISOString() || null;
    } catch (error: any) {
      console.error('[Database] Error getting latest complete hour:', error.message);
      return null;
    }
  }

  /**
   * Upsert hourly volume records using batched inserts for performance
   * @param records Array of hourly volume records
   * @param markComplete If true, marks these hours as complete (for historical data)
   */
  async upsertHourlyVolumes(records: HourlyVolumeRecord[], markComplete: boolean = false): Promise<number> {
    if (!this.pool || !this.isConnected || records.length === 0) return 0;

    const BATCH_SIZE = 500; // Insert 500 records per batch
    let totalUpserted = 0;

    try {
      const client = await this.pool.connect();

      try {
        await client.query('BEGIN');

        // Process in batches
        for (let i = 0; i < records.length; i += BATCH_SIZE) {
          const batch = records.slice(i, i + BATCH_SIZE);
          
          // Build multi-value INSERT statement
          const values: any[] = [];
          const valuePlaceholders: string[] = [];
          
          batch.forEach((record, idx) => {
            const offset = idx * 7; // 7 parameters per record
            valuePlaceholders.push(
              `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, ${markComplete}, CURRENT_TIMESTAMP)`
            );
            values.push(
              record.token,
              record.hour,
              record.base_volume,
              record.target_volume,
              record.high,
              record.low,
              record.trade_count || 0
            );
          });

          const batchSQL = `
            INSERT INTO hourly_volumes (token, hour, base_volume, target_volume, high, low, trade_count, is_complete, updated_at)
            VALUES ${valuePlaceholders.join(', ')}
            ON CONFLICT (token, hour) 
            DO UPDATE SET 
              base_volume = EXCLUDED.base_volume,
              target_volume = EXCLUDED.target_volume,
              high = EXCLUDED.high,
              low = EXCLUDED.low,
              trade_count = EXCLUDED.trade_count,
              is_complete = CASE WHEN EXCLUDED.is_complete THEN true ELSE hourly_volumes.is_complete END,
              updated_at = CURRENT_TIMESTAMP
          `;

          await client.query(batchSQL, values);
          totalUpserted += batch.length;
          
          // Log progress for large batches
          if (records.length > BATCH_SIZE) {
            console.log(`[Database] Hourly volume batch progress: ${totalUpserted}/${records.length}`);
          }
        }

        await client.query('COMMIT');
        console.log(`[Database] Upserted ${totalUpserted} hourly volume records (complete: ${markComplete})`);
        return totalUpserted;
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }
    } catch (error: any) {
      console.error('[Database] Error upserting hourly volumes:', error.message);
      return 0;
    }
  }

  /**
   * Mark hours as complete (called when hour boundary passes)
   */
  async markHoursComplete(beforeHour: string): Promise<void> {
    if (!this.pool || !this.isConnected) return;

    try {
      await this.pool.query(
        `UPDATE hourly_volumes SET is_complete = true, updated_at = CURRENT_TIMESTAMP
         WHERE hour < $1 AND is_complete = false`,
        [beforeHour]
      );
      console.log(`[Database] Marked hours before ${beforeHour} as complete`);
    } catch (error: any) {
      console.error('[Database] Error marking hours complete:', error.message);
    }
  }

  /**
   * Get rolling 24h metrics for all tokens from hourly data
   * This is the primary method for serving /api/tickers
   */
  async getRolling24hMetrics(tokens?: string[]): Promise<Map<string, Rolling24hMetrics>> {
    if (!this.pool || !this.isConnected) return new Map();

    try {
      // Get data from the last 24 hours
      const cutoffTime = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

      let whereClause = 'WHERE hour >= $1';
      let params: any[] = [cutoffTime];

      if (tokens && tokens.length > 0) {
        const placeholders = tokens.map((_, i) => `LOWER($${i + 2})`).join(', ');
        whereClause += ` AND LOWER(token) IN (${placeholders})`;
        params = [cutoffTime, ...tokens.map(t => t.toLowerCase())];
      }

      const result = await this.pool.query(
        `SELECT 
          token,
          SUM(base_volume)::text as base_volume_24h,
          SUM(target_volume)::text as target_volume_24h,
          MAX(high)::text as high_24h,
          MIN(CASE WHEN low > 0 THEN low END)::text as low_24h,
          SUM(trade_count)::int as trade_count_24h
         FROM hourly_volumes
         ${whereClause}
         GROUP BY token`,
        params
      );

      const metricsMap = new Map<string, Rolling24hMetrics>();
      for (const row of result.rows) {
        metricsMap.set(row.token.toLowerCase(), {
          token: row.token,
          base_volume_24h: row.base_volume_24h || '0',
          target_volume_24h: row.target_volume_24h || '0',
          high_24h: row.high_24h || '0',
          low_24h: row.low_24h || '0',
          trade_count_24h: row.trade_count_24h || 0,
        });
      }
      return metricsMap;
    } catch (error: any) {
      console.error('[Database] Error getting rolling 24h metrics:', error.message);
      return new Map();
    }
  }

  /**
   * Get hourly data for a specific time range
   */
  async getHourlyVolumes(startHour: string, endHour?: string, tokens?: string[]): Promise<HourlyVolumeRecord[]> {
    if (!this.pool || !this.isConnected) return [];

    try {
      let whereClause = 'WHERE hour >= $1';
      let params: any[] = [startHour];
      let paramIndex = 2;

      if (endHour) {
        whereClause += ` AND hour <= $${paramIndex}`;
        params.push(endHour);
        paramIndex++;
      }

      if (tokens && tokens.length > 0) {
        const placeholders = tokens.map((_, i) => `LOWER($${paramIndex + i})`).join(', ');
        whereClause += ` AND LOWER(token) IN (${placeholders})`;
        params = [...params, ...tokens.map(t => t.toLowerCase())];
      }

      const result = await this.pool.query(
        `SELECT 
          token,
          hour::text,
          base_volume::text,
          target_volume::text,
          high::text,
          low::text,
          trade_count
         FROM hourly_volumes
         ${whereClause}
         ORDER BY token, hour ASC`,
        params
      );

      return result.rows;
    } catch (error: any) {
      console.error('[Database] Error getting hourly volumes:', error.message);
      return [];
    }
  }

  /**
   * Get hourly record count
   */
  async getHourlyRecordCount(): Promise<number> {
    if (!this.pool || !this.isConnected) return 0;

    try {
      const result = await this.pool.query('SELECT COUNT(*) as count FROM hourly_volumes');
      return parseInt(result.rows[0]?.count || '0');
    } catch (error: any) {
      console.error('[Database] Error getting hourly record count:', error.message);
      return 0;
    }
  }

  /**
   * Get unique token count from hourly table
   */
  async getHourlyTokenCount(): Promise<number> {
    if (!this.pool || !this.isConnected) return 0;

    try {
      const result = await this.pool.query('SELECT COUNT(DISTINCT token) as count FROM hourly_volumes');
      return parseInt(result.rows[0]?.count || '0');
    } catch (error: any) {
      console.error('[Database] Error getting hourly token count:', error.message);
      return 0;
    }
  }

  /**
   * Delete old hourly data to prevent table from growing indefinitely
   * Keeps data for the specified number of hours
   */
  async pruneOldHourlyData(keepHours: number = 48): Promise<number> {
    if (!this.pool || !this.isConnected) return 0;

    try {
      const cutoffTime = new Date(Date.now() - keepHours * 60 * 60 * 1000).toISOString();
      const result = await this.pool.query(
        'DELETE FROM hourly_volumes WHERE hour < $1 RETURNING id',
        [cutoffTime]
      );
      const deletedCount = result.rowCount || 0;
      if (deletedCount > 0) {
        console.log(`[Database] Pruned ${deletedCount} hourly records older than ${keepHours} hours`);
      }
      return deletedCount;
    } catch (error: any) {
      console.error('[Database] Error pruning old hourly data:', error.message);
      return 0;
    }
  }

  // ============================================
  // 10-MINUTE VOLUME METHODS
  // ============================================

  /**
   * Upsert 10-minute volume records using batched inserts
   */
  async upsertTenMinuteVolumes(records: TenMinuteVolumeRecord[], markComplete: boolean = false): Promise<number> {
    if (!this.pool || !this.isConnected || records.length === 0) return 0;

    const BATCH_SIZE = 500;
    let totalUpserted = 0;

    try {
      const client = await this.pool.connect();

      try {
        await client.query('BEGIN');

        for (let i = 0; i < records.length; i += BATCH_SIZE) {
          const batch = records.slice(i, i + BATCH_SIZE);
          
          const values: any[] = [];
          const valuePlaceholders: string[] = [];
          
          batch.forEach((record, idx) => {
            const offset = idx * 7;
            valuePlaceholders.push(
              `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, ${markComplete}, CURRENT_TIMESTAMP)`
            );
            values.push(
              record.token,
              record.bucket,
              record.base_volume,
              record.target_volume,
              record.high,
              record.low,
              record.trade_count || 0
            );
          });

          const batchSQL = `
            INSERT INTO ten_minute_volumes (token, bucket, base_volume, target_volume, high, low, trade_count, is_complete, updated_at)
            VALUES ${valuePlaceholders.join(', ')}
            ON CONFLICT (token, bucket) 
            DO UPDATE SET 
              base_volume = EXCLUDED.base_volume,
              target_volume = EXCLUDED.target_volume,
              high = EXCLUDED.high,
              low = EXCLUDED.low,
              trade_count = EXCLUDED.trade_count,
              is_complete = CASE WHEN EXCLUDED.is_complete THEN true ELSE ten_minute_volumes.is_complete END,
              updated_at = CURRENT_TIMESTAMP
          `;

          await client.query(batchSQL, values);
          totalUpserted += batch.length;
          
          if (records.length > BATCH_SIZE) {
            console.log(`[Database] 10-min volume batch progress: ${totalUpserted}/${records.length}`);
          }
        }

        await client.query('COMMIT');
        console.log(`[Database] Upserted ${totalUpserted} 10-minute volume records (complete: ${markComplete})`);
        return totalUpserted;
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }
    } catch (error: any) {
      console.error('[Database] Error upserting 10-minute volumes:', error.message);
      return 0;
    }
  }

  /**
   * Mark 10-minute buckets as complete
   */
  async markTenMinuteBucketsComplete(beforeBucket: string): Promise<void> {
    if (!this.pool || !this.isConnected) return;

    try {
      await this.pool.query(
        `UPDATE ten_minute_volumes SET is_complete = true, updated_at = CURRENT_TIMESTAMP
         WHERE bucket < $1 AND is_complete = false`,
        [beforeBucket]
      );
    } catch (error: any) {
      console.error('[Database] Error marking 10-min buckets complete:', error.message);
    }
  }

  /**
   * Get rolling 24h metrics from 10-minute data (most accurate)
   * This sums the last 144 ten-minute buckets for each token
   */
  async getRolling24hFromTenMinute(tokens?: string[]): Promise<Map<string, Rolling24hMetrics>> {
    if (!this.pool || !this.isConnected) return new Map();

    try {
      const cutoffTime = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

      let whereClause = 'WHERE bucket >= $1';
      let params: any[] = [cutoffTime];

      if (tokens && tokens.length > 0) {
        const placeholders = tokens.map((_, i) => `LOWER($${i + 2})`).join(', ');
        whereClause += ` AND LOWER(token) IN (${placeholders})`;
        params = [cutoffTime, ...tokens.map(t => t.toLowerCase())];
      }

      const result = await this.pool.query(
        `SELECT 
          token,
          SUM(base_volume)::text as base_volume_24h,
          SUM(target_volume)::text as target_volume_24h,
          MAX(high)::text as high_24h,
          MIN(CASE WHEN low > 0 THEN low END)::text as low_24h,
          SUM(trade_count)::int as trade_count_24h
         FROM ten_minute_volumes
         ${whereClause}
         GROUP BY token`,
        params
      );

      const metricsMap = new Map<string, Rolling24hMetrics>();
      for (const row of result.rows) {
        metricsMap.set(row.token.toLowerCase(), {
          token: row.token,
          base_volume_24h: row.base_volume_24h || '0',
          target_volume_24h: row.target_volume_24h || '0',
          high_24h: row.high_24h || '0',
          low_24h: row.low_24h || '0',
          trade_count_24h: row.trade_count_24h || 0,
        });
      }
      return metricsMap;
    } catch (error: any) {
      console.error('[Database] Error getting rolling 24h from 10-min data:', error.message);
      return new Map();
    }
  }

  /**
   * Get latest 10-minute bucket
   */
  async getLatestTenMinuteBucket(): Promise<string | null> {
    if (!this.pool || !this.isConnected) return null;

    try {
      const result = await this.pool.query(
        'SELECT MAX(bucket) as latest_bucket FROM ten_minute_volumes'
      );
      return result.rows[0]?.latest_bucket?.toISOString() || null;
    } catch (error: any) {
      console.error('[Database] Error getting latest 10-min bucket:', error.message);
      return null;
    }
  }

  /**
   * Get 10-minute record count
   */
  async getTenMinuteRecordCount(): Promise<number> {
    if (!this.pool || !this.isConnected) return 0;

    try {
      const result = await this.pool.query('SELECT COUNT(*) as count FROM ten_minute_volumes');
      return parseInt(result.rows[0]?.count || '0');
    } catch (error: any) {
      console.error('[Database] Error getting 10-min record count:', error.message);
      return 0;
    }
  }

  /**
   * Prune old 10-minute data (keep 25 hours for safety margin)
   */
  async pruneOldTenMinuteData(keepHours: number = 25): Promise<number> {
    if (!this.pool || !this.isConnected) return 0;

    try {
      const cutoffTime = new Date(Date.now() - keepHours * 60 * 60 * 1000).toISOString();
      const result = await this.pool.query(
        'DELETE FROM ten_minute_volumes WHERE bucket < $1 RETURNING id',
        [cutoffTime]
      );
      const deletedCount = result.rowCount || 0;
      if (deletedCount > 0) {
        console.log(`[Database] Pruned ${deletedCount} 10-minute records older than ${keepHours} hours`);
      }
      return deletedCount;
    } catch (error: any) {
      console.error('[Database] Error pruning old 10-min data:', error.message);
      return 0;
    }
  }

  // ============================================
  // DAILY BUY/SELL VOLUME METHODS
  // ============================================

  /**
   * Get the latest date we have buy/sell data for
   */
  async getLatestBuySellDate(): Promise<string | null> {
    if (!this.pool || !this.isConnected) return null;

    try {
      const result = await this.pool.query(
        'SELECT MAX(date) as latest_date FROM daily_buy_sell_volumes WHERE is_complete = true'
      );
      return result.rows[0]?.latest_date?.toISOString().split('T')[0] || null;
    } catch (error: any) {
      console.error('[Database] Error getting latest buy/sell date:', error.message);
      return null;
    }
  }

  /**
   * Upsert daily buy/sell volume records using batched inserts
   * @param records Array of daily buy/sell volume records
   * @param markComplete If true, marks these days as complete (for historical data)
   */
  async upsertDailyBuySellVolumes(records: DailyBuySellVolumeRecord[], markComplete: boolean = false): Promise<number> {
    if (!this.pool || !this.isConnected || records.length === 0) return 0;

    const BATCH_SIZE = 500;
    let totalUpserted = 0;

    try {
      const client = await this.pool.connect();

      try {
        await client.query('BEGIN');

        for (let i = 0; i < records.length; i += BATCH_SIZE) {
          const batch = records.slice(i, i + BATCH_SIZE);
          
          const values: any[] = [];
          const valuePlaceholders: string[] = [];
          
          batch.forEach((record, idx) => {
            const offset = idx * 9; // 9 parameters per record
            valuePlaceholders.push(
              `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, ${markComplete}, CURRENT_TIMESTAMP)`
            );
            values.push(
              record.token,
              record.date,
              record.base_volume,
              record.target_volume,
              record.buy_usdc_volume,
              record.sell_token_volume,
              record.high,
              record.low,
              record.trade_count || 0
            );
          });

          const batchSQL = `
            INSERT INTO daily_buy_sell_volumes (token, date, base_volume, target_volume, buy_usdc_volume, sell_token_volume, high, low, trade_count, is_complete, updated_at)
            VALUES ${valuePlaceholders.join(', ')}
            ON CONFLICT (token, date) 
            DO UPDATE SET 
              base_volume = EXCLUDED.base_volume,
              target_volume = EXCLUDED.target_volume,
              buy_usdc_volume = EXCLUDED.buy_usdc_volume,
              sell_token_volume = EXCLUDED.sell_token_volume,
              high = EXCLUDED.high,
              low = EXCLUDED.low,
              trade_count = EXCLUDED.trade_count,
              is_complete = CASE WHEN EXCLUDED.is_complete THEN true ELSE daily_buy_sell_volumes.is_complete END,
              updated_at = CURRENT_TIMESTAMP
          `;

          await client.query(batchSQL, values);
          totalUpserted += batch.length;
          
          if (records.length > BATCH_SIZE) {
            console.log(`[Database] Buy/sell volume batch progress: ${totalUpserted}/${records.length}`);
          }
        }

        await client.query('COMMIT');
        console.log(`[Database] Upserted ${totalUpserted} daily buy/sell volume records (complete: ${markComplete})`);
        return totalUpserted;
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }
    } catch (error: any) {
      console.error('[Database] Error upserting daily buy/sell volumes:', error.message);
      return 0;
    }
  }

  /**
   * Get daily buy/sell volumes with cumulative totals calculated from DB
   * Cumulative values are computed on-the-fly using window functions
   */
  async getDailyBuySellVolumesWithCumulative(token?: string): Promise<CumulativeVolumeData[]> {
    if (!this.pool || !this.isConnected) return [];

    try {
      let whereClause = '';
      let params: any[] = [];

      if (token) {
        whereClause = 'WHERE LOWER(token) = LOWER($1)';
        params = [token];
      }

      const result = await this.pool.query(
        `SELECT 
          token,
          date::text,
          base_volume::text,
          target_volume::text,
          buy_usdc_volume::text,
          sell_token_volume::text,
          SUM(target_volume) OVER (
            PARTITION BY token
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          )::text AS cumulative_target_volume,
          SUM(base_volume) OVER (
            PARTITION BY token
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          )::text AS cumulative_base_volume,
          SUM(buy_usdc_volume) OVER (
            PARTITION BY token
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          )::text AS cumulative_buy_usdc_volume,
          SUM(sell_token_volume) OVER (
            PARTITION BY token
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          )::text AS cumulative_sell_token_volume,
          high::text,
          low::text
         FROM daily_buy_sell_volumes
         ${whereClause}
         ORDER BY token, date ASC`,
        params
      );

      return result.rows;
    } catch (error: any) {
      console.error('[Database] Error getting cumulative volumes:', error.message);
      return [];
    }
  }

  /**
   * Get aggregated buy/sell stats for all tokens
   */
  async getBuySellAggregates(tokens?: string[]): Promise<Map<string, {
    total_buy_usdc: string;
    total_sell_token: string;
    total_base_volume: string;
    total_target_volume: string;
    first_date: string;
    last_date: string;
    trading_days: number;
  }>> {
    if (!this.pool || !this.isConnected) return new Map();

    try {
      let whereClause = '';
      let params: any[] = [];

      if (tokens && tokens.length > 0) {
        const placeholders = tokens.map((_, i) => `LOWER($${i + 1})`).join(', ');
        whereClause = `WHERE LOWER(token) IN (${placeholders})`;
        params = tokens.map(t => t.toLowerCase());
      }

      const result = await this.pool.query(
        `SELECT 
          token,
          SUM(buy_usdc_volume)::text AS total_buy_usdc,
          SUM(sell_token_volume)::text AS total_sell_token,
          SUM(base_volume)::text AS total_base_volume,
          SUM(target_volume)::text AS total_target_volume,
          MIN(date)::text AS first_date,
          MAX(date)::text AS last_date,
          COUNT(*)::int AS trading_days
         FROM daily_buy_sell_volumes
         ${whereClause}
         GROUP BY token
         ORDER BY SUM(target_volume) DESC`,
        params
      );

      const aggregates = new Map();
      for (const row of result.rows) {
        aggregates.set(row.token.toLowerCase(), {
          total_buy_usdc: row.total_buy_usdc || '0',
          total_sell_token: row.total_sell_token || '0',
          total_base_volume: row.total_base_volume || '0',
          total_target_volume: row.total_target_volume || '0',
          first_date: row.first_date,
          last_date: row.last_date,
          trading_days: row.trading_days || 0,
        });
      }
      return aggregates;
    } catch (error: any) {
      console.error('[Database] Error getting buy/sell aggregates:', error.message);
      return new Map();
    }
  }

  /**
   * Get buy/sell volume record count
   */
  async getBuySellRecordCount(): Promise<number> {
    if (!this.pool || !this.isConnected) return 0;

    try {
      const result = await this.pool.query('SELECT COUNT(*) as count FROM daily_buy_sell_volumes');
      return parseInt(result.rows[0]?.count || '0');
    } catch (error: any) {
      console.error('[Database] Error getting buy/sell record count:', error.message);
      return 0;
    }
  }

  /**
   * Mark days as complete (called when day boundary passes)
   */
  async markBuySellDaysComplete(beforeDate: string): Promise<void> {
    if (!this.pool || !this.isConnected) return;

    try {
      await this.pool.query(
        `UPDATE daily_buy_sell_volumes SET is_complete = true, updated_at = CURRENT_TIMESTAMP
         WHERE date < $1 AND is_complete = false`,
        [beforeDate]
      );
      console.log(`[Database] Marked buy/sell days before ${beforeDate} as complete`);
    } catch (error: any) {
      console.error('[Database] Error marking buy/sell days complete:', error.message);
    }
  }

  // ============================================
  // METRICS HISTORY METHODS
  // ============================================

  /**
   * Insert a metrics snapshot
   */
  async insertMetric(metricName: string, value: number, labels: Record<string, string> = {}): Promise<void> {
    if (!this.pool || !this.isConnected) return;

    try {
      await this.pool.query(
        `INSERT INTO metrics_history (metric_name, metric_value, labels)
         VALUES ($1, $2, $3)
         ON CONFLICT (timestamp, metric_name, labels) DO UPDATE SET metric_value = $2`,
        [metricName, value, JSON.stringify(labels)]
      );
    } catch (error: any) {
      // Silently ignore metrics insert errors to not affect main operations
      console.error('[Database] Error inserting metric:', error.message);
    }
  }

  /**
   * Insert multiple metrics at once
   */
  async insertMetricsBatch(metrics: Array<{ name: string; value: number; labels?: Record<string, string> }>): Promise<void> {
    if (!this.pool || !this.isConnected || metrics.length === 0) return;

    try {
      const client = await this.pool.connect();
      try {
        await client.query('BEGIN');
        
        for (const metric of metrics) {
          await client.query(
            `INSERT INTO metrics_history (metric_name, metric_value, labels)
             VALUES ($1, $2, $3)
             ON CONFLICT DO NOTHING`,
            [metric.name, metric.value, JSON.stringify(metric.labels || {})]
          );
        }
        
        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }
    } catch (error: any) {
      console.error('[Database] Error inserting metrics batch:', error.message);
    }
  }

  /**
   * Insert a service health snapshot
   */
  async insertServiceHealthSnapshot(
    serviceName: string,
    isHealthy: boolean,
    lastRefreshTime?: Date,
    recordCount?: number,
    errorMessage?: string,
    metadata?: Record<string, any>
  ): Promise<void> {
    if (!this.pool || !this.isConnected) return;

    try {
      await this.pool.query(
        `INSERT INTO service_health_snapshots 
         (service_name, is_healthy, last_refresh_time, record_count, error_message, metadata)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          serviceName,
          isHealthy,
          lastRefreshTime || null,
          recordCount || null,
          errorMessage || null,
          JSON.stringify(metadata || {})
        ]
      );
    } catch (error: any) {
      console.error('[Database] Error inserting service health snapshot:', error.message);
    }
  }

  /**
   * Get recent metrics for a specific metric name
   */
  async getRecentMetrics(
    metricName: string,
    hours: number = 24,
    labels?: Record<string, string>
  ): Promise<Array<{ timestamp: string; value: number; labels: Record<string, string> }>> {
    if (!this.pool || !this.isConnected) return [];

    try {
      const cutoff = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
      
      let query = `
        SELECT timestamp::text, metric_value::numeric as value, labels
        FROM metrics_history
        WHERE metric_name = $1 AND timestamp >= $2
      `;
      const params: any[] = [metricName, cutoff];

      if (labels && Object.keys(labels).length > 0) {
        query += ' AND labels @> $3';
        params.push(JSON.stringify(labels));
      }

      query += ' ORDER BY timestamp DESC LIMIT 1000';

      const result = await this.pool.query(query, params);
      return result.rows.map(row => ({
        timestamp: row.timestamp,
        value: parseFloat(row.value),
        labels: row.labels,
      }));
    } catch (error: any) {
      console.error('[Database] Error getting recent metrics:', error.message);
      return [];
    }
  }

  /**
   * Get service health history
   */
  async getServiceHealthHistory(
    serviceName?: string,
    hours: number = 24
  ): Promise<Array<{
    timestamp: string;
    service_name: string;
    is_healthy: boolean;
    last_refresh_time: string | null;
    record_count: number | null;
    error_message: string | null;
    metadata: Record<string, any>;
  }>> {
    if (!this.pool || !this.isConnected) return [];

    try {
      const cutoff = new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
      
      let query = `
        SELECT 
          timestamp::text,
          service_name,
          is_healthy,
          last_refresh_time::text,
          record_count,
          error_message,
          metadata
        FROM service_health_snapshots
        WHERE timestamp >= $1
      `;
      const params: any[] = [cutoff];

      if (serviceName) {
        query += ' AND service_name = $2';
        params.push(serviceName);
      }

      query += ' ORDER BY timestamp DESC LIMIT 1000';

      const result = await this.pool.query(query, params);
      return result.rows;
    } catch (error: any) {
      console.error('[Database] Error getting service health history:', error.message);
      return [];
    }
  }

  /**
   * Prune old metrics data (keep last N days)
   */
  async pruneOldMetrics(keepDays: number = 30): Promise<{ metricsDeleted: number; healthDeleted: number }> {
    if (!this.pool || !this.isConnected) return { metricsDeleted: 0, healthDeleted: 0 };

    try {
      const cutoff = new Date(Date.now() - keepDays * 24 * 60 * 60 * 1000).toISOString();
      
      const metricsResult = await this.pool.query(
        'DELETE FROM metrics_history WHERE timestamp < $1 RETURNING id',
        [cutoff]
      );
      
      const healthResult = await this.pool.query(
        'DELETE FROM service_health_snapshots WHERE timestamp < $1 RETURNING id',
        [cutoff]
      );

      const metricsDeleted = metricsResult.rowCount || 0;
      const healthDeleted = healthResult.rowCount || 0;

      if (metricsDeleted > 0 || healthDeleted > 0) {
        console.log(`[Database] Pruned ${metricsDeleted} metrics and ${healthDeleted} health snapshots older than ${keepDays} days`);
      }

      return { metricsDeleted, healthDeleted };
    } catch (error: any) {
      console.error('[Database] Error pruning old metrics:', error.message);
      return { metricsDeleted: 0, healthDeleted: 0 };
    }
  }

  /**
   * Close the database connection
   */
  async close(): Promise<void> {
    if (this.pool) {
      await this.pool.end();
      this.isConnected = false;
      console.log('[Database] Connection closed');
    }
  }
}

