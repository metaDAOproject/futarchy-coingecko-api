import { config } from '../config.js';
import { PublicKey } from '@solana/web3.js';

export interface DunePoolMetrics {
  pool_id: string;
  base_volume_24h: string;
  target_volume_24h: string;
  high_24h: string;
  low_24h: string;
}

export interface DuneQueryResult {
  rows: DunePoolMetrics[];
  metadata: {
    column_names: string[];
    result_set_bytes: number;
    total_row_count: number;
    datapoint_count: number;
    pending_time_millis: number;
    execution_time_millis: number;
  };
}

export interface DuneQuery {
  query_id?: number;
  name: string;
  query_sql: string;
  parameters?: Array<{
    key: string;
    type: string;
    value?: any;
  }>;
}

// Daily volume data for a single token on a specific date
export interface DuneDailyVolume {
  token: string;
  date: string;
  base_volume: string;
  target_volume: string;
  high: string;
  low: string;
}

// Aggregate volume data for a single token (totals across all dates)
export interface DuneAggregateTokenVolume {
  token: string;
  first_trade_date: string;
  last_trade_date: string;
  total_base_volume: string;
  total_target_volume: string;
  all_time_high: string;
  all_time_low: string;
  trading_days: number;
  daily_data: DuneDailyVolume[];
}

// Complete aggregate volume response
export interface DuneAggregateVolumeResponse {
  tokens: DuneAggregateTokenVolume[];
  query_metadata: {
    since_start: boolean;
    token_count: number;
    total_trading_days: number;
    execution_time_millis: number;
  };
}

export class DuneService {
  private apiKey: string;
  private queryId?: number;
  private aggregateVolumeQueryId?: number;
  private baseUrl: string = 'https://api.dune.com/api/v1';
  private cache: Map<string, { data: DunePoolMetrics | null; timestamp: number }>;
  private batchCache: Map<string, { data: Map<string, DunePoolMetrics>; timestamp: number }>;
  private aggregateVolumeCache: Map<string, { data: DuneAggregateVolumeResponse; timestamp: number }>;
  private cacheTTL: number = 60000; // 1 minute cache for individual pools
  private batchCacheTTL: number = 300000; // 5 minutes cache for batch queries (Dune queries are heavy)
  private aggregateVolumeCacheTTL: number = 600000; // 10 minutes cache for aggregate volume (heavy historical query)
  private fetchTimeout: number = 240000; // 4 minutes timeout for Dune API calls

  constructor() {
    this.apiKey = config.dune.apiKey;
    this.queryId = config.dune.queryId;
    this.aggregateVolumeQueryId = config.dune.aggregateVolumeQueryId;
    this.cache = new Map();
    this.batchCache = new Map();
    this.aggregateVolumeCache = new Map();
    // Allow configurable cache TTLs from environment
    if (process.env.DUNE_CACHE_TTL) {
      this.cacheTTL = parseInt(process.env.DUNE_CACHE_TTL) * 1000; // Convert seconds to milliseconds
    }
    if (process.env.DUNE_BATCH_CACHE_TTL) {
      this.batchCacheTTL = parseInt(process.env.DUNE_BATCH_CACHE_TTL) * 1000; // Convert seconds to milliseconds
    }
    if (process.env.DUNE_AGGREGATE_VOLUME_CACHE_TTL) {
      this.aggregateVolumeCacheTTL = parseInt(process.env.DUNE_AGGREGATE_VOLUME_CACHE_TTL) * 1000;
    }
    if (process.env.DUNE_FETCH_TIMEOUT) {
      this.fetchTimeout = parseInt(process.env.DUNE_FETCH_TIMEOUT) * 1000; // Convert seconds to milliseconds
    }
  }

  /**
   * Fetch with timeout wrapper
   * @param url The URL to fetch
   * @param options Fetch options
   * @returns Response promise
   */
  private async fetchWithTimeout(url: string, options: RequestInit = {}): Promise<Response> {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.fetchTimeout);
    
    try {
      const response = await fetch(url, {
        ...options,
        signal: controller.signal,
      });
      return response;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  /**
   * Convert scientific notation to regular decimal notation
   * Handles string, number, and other types from Dune API
   * @param value Value that may be in scientific notation (e.g., "3.2259955052399996E5" or 322599.55)
   * @returns String in regular decimal notation (e.g., "322599.55052399996")
   */
  private convertScientificNotation(value: unknown): string {
    // Handle null, undefined, empty values
    if (value === null || value === undefined || value === '' || value === 0 || value === '0') {
      return '0';
    }
    
    // Handle number type directly
    if (typeof value === 'number') {
      if (isNaN(value) || !isFinite(value)) {
        return '0';
      }
      // Use toFixed with enough precision, then remove trailing zeros
      return value.toFixed(20).replace(/\.?0+$/, '');
    }
    
    // Handle string type
    if (typeof value === 'string') {
      // Check if it contains scientific notation (E or e)
      if (value.includes('E') || value.includes('e')) {
        try {
          const num = parseFloat(value);
          if (isNaN(num)) {
            return '0';
          }
          return num.toFixed(20).replace(/\.?0+$/, '');
        } catch (error) {
          console.warn(`[Dune] Failed to convert scientific notation: ${value}`, error);
          return value;
        }
      }
      return value;
    }
    
    // Handle any other type (object, boolean, etc.) - convert to string first
    try {
      const strValue = String(value);
      if (strValue === '[object Object]' || strValue === 'undefined' || strValue === 'null') {
        return '0';
      }
      const num = parseFloat(strValue);
      if (isNaN(num) || !isFinite(num)) {
        return '0';
      }
      return num.toFixed(20).replace(/\.?0+$/, '');
    } catch {
      return '0';
    }
  }

  /**
   * Generate the SQL query for fetching 24h metrics
   * @param tokenAddresses Optional list of token addresses to filter by
   * @returns The generated SQL query string
   */
  generate24hMetricsQuery(tokenAddresses?: string[]): string {
    // Build token filter condition - need to use the full CASE expression, not the alias
    let tokenFilter = '';
    if (tokenAddresses && tokenAddresses.length > 0) {
      // Escape single quotes in token addresses and create SQL IN clause
      const escapedTokens = tokenAddresses.map(token => `'${token.replace(/'/g, "''")}'`).join(', ');
      // Use the full CASE expression since we can't reference the alias in WHERE clause
      tokenFilter = `AND (
        CASE
          WHEN LENGTH(data) = 406 THEN to_base58(SUBSTR(data, 279, 32))
          WHEN LENGTH(data) = 670 THEN to_base58(SUBSTR(data, 543, 32))
        END
      ) IN (${escapedTokens})`;
    }

    return `
WITH futswap AS (
    SELECT
        block_time,
        tx_signer,
        tx_id,
        data,
        CASE
            WHEN to_hex(SUBSTR(data, 105, 1)) = '00' THEN 'buy'
            WHEN to_hex(SUBSTR(data, 105, 1)) = '01' THEN 'sell'
        END AS swap_type,
        from_big_endian_64(reverse(SUBSTR(data, 106, 8))) / 1e6 AS input_amount,
        from_big_endian_64(reverse(SUBSTR(data, 114, 8))) / 1e6 AS output_amount,
        CASE
            WHEN LENGTH(data) = 406 THEN to_base58(SUBSTR(data, 279, 32))
            WHEN LENGTH(data) = 670 THEN to_base58(SUBSTR(data, 543, 32))
        END AS token,
        CASE
            WHEN LENGTH(data) = 406 THEN to_base58(SUBSTR(data, 311, 32))
            WHEN LENGTH(data) = 670 THEN to_base58(SUBSTR(data, 575, 32))
        END AS quote_mint,
        account_arguments[1] AS dao_address
    FROM solana.instruction_calls
    WHERE executing_account = 'FUTARELBfJfQ8RDGhg1wdhddq1odMAJUePHFuBYfUxKq'
      AND inner_executing_account = 'FUTARELBfJfQ8RDGhg1wdhddq1odMAJUePHFuBYfUxKq'
      AND account_arguments[1] = 'DGEympSS4qLvdr9r3uGHTfACdN8snShk4iGdJtZPxuBC'
      AND cardinality(account_arguments) = 1
      AND is_inner = true
      AND tx_success = true
      AND CAST(data AS VARCHAR) LIKE '0xe445a52e51cb9a1d%'
      AND LENGTH(data) >= 300
      AND array_join(log_messages, ' ') LIKE '%SpotSwap%'
      AND block_time >= current_timestamp - interval '24' hour
      ${tokenFilter}
),
swaps_with_price AS (
    SELECT
        block_time,
        tx_id,
        dao_address,
        token,
        quote_mint,
        swap_type,
        input_amount,
        output_amount,
        CASE
            WHEN input_amount > 0 THEN output_amount / input_amount
            ELSE NULL
        END AS price
    FROM futswap
    WHERE swap_type IN ('buy', 'sell')
      AND input_amount > 0
      AND output_amount > 0
)
SELECT
    token AS pool_id,
    CAST(SUM(CASE WHEN swap_type = 'buy' THEN input_amount ELSE 0 END) +
         SUM(CASE WHEN swap_type = 'sell' THEN output_amount ELSE 0 END) AS VARCHAR) AS base_volume_24h,
    CAST(SUM(CASE WHEN swap_type = 'buy' THEN output_amount ELSE 0 END) +
         SUM(CASE WHEN swap_type = 'sell' THEN input_amount ELSE 0 END) AS VARCHAR) AS target_volume_24h,
    CAST(MAX(price) AS VARCHAR) AS high_24h,
    CAST(MIN(price) AS VARCHAR) AS low_24h
FROM swaps_with_price
WHERE price IS NOT NULL
  AND price > 0
GROUP BY token
ORDER BY base_volume_24h DESC;
`.trim();
  }

  /**
   * Create or update a Dune query
   * @param query The query definition
   * @returns The created/updated query ID
   */
  async createOrUpdateQuery(query: DuneQuery): Promise<number> {
    const url = query.query_id
      ? `${this.baseUrl}/query/${query.query_id}`
      : `${this.baseUrl}/query`;

    const method = query.query_id ? 'PATCH' : 'POST';

    const response = await this.fetchWithTimeout(url, {
      method,
      headers: {
        'X-Dune-API-Key': this.apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        name: query.name,
        query_sql: query.query_sql,
        parameters: query.parameters || [],
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Dune API error: ${response.status} ${response.statusText} - ${errorText}`);
    }

    const result = await response.json() as any;
    return result.query_id || result.query?.query_id;
  }

  /**
   * Execute a raw SQL query using fullQuery parameter (if queryId supports it)
   * or by creating a temporary query
   * Based on DefiLlama's approach: https://github.com/DefiLlama/dimension-adapters/blob/master/helpers/dune.ts
   * @param sqlQuery The SQL query to execute
   * @param queryName Optional name for the temporary query
   * @returns The query results
   */
  async executeRawQuery(sqlQuery: string, queryName: string = 'Temporary Query'): Promise<DuneQueryResult> {
    try {
      // If we have a queryId configured, try using fullQuery parameter first
      // This is more efficient and works with free tier
      // Based on DefiLlama's pattern where they use a specific query ID that accepts fullQuery
      if (this.queryId) {
        try {
          console.log(`[Dune] Attempting to use fullQuery with query ID ${this.queryId}`);
          console.log(`[Dune] SQL query length: ${sqlQuery.length} characters`);
          console.log(`[Dune] SQL query preview (first 300 chars):\n${sqlQuery.substring(0, 300)}...`);
          
          // Try executing with fullQuery parameter
          // Note: This requires a query that's configured to accept fullQuery parameter
          const executeResult = await this.executeQuery(this.queryId, {
            fullQuery: sqlQuery,
          });
          
          console.log(`[Dune] Query execution started, execution_id: ${executeResult.execution_id}`);
          // Wait for results
          const queryResult = await this.getQueryResults(executeResult.execution_id);
          console.log(`[Dune] Query completed successfully`);
          return queryResult;
        } catch (error: any) {
          // If fullQuery doesn't work, provide helpful error message
          if (error.message?.includes('403') || error.message?.includes('Forbidden')) {
            throw new Error(
              'Dune query execution failed. This usually means:\n' +
              '1. The query ID does not accept fullQuery parameter\n' +
              '2. You need to create a query in Dune that accepts a fullQuery parameter\n' +
              '3. See DUNE_QUERY_SETUP.md for instructions'
            );
          }
          throw error;
        }
      }

      // Without a queryId that accepts fullQuery, we cannot execute raw SQL on free tier
      // Query creation requires a paid plan
      throw new Error(
        'Cannot execute raw SQL query without a Dune query ID that accepts fullQuery parameter.\n' +
        'Query creation requires a paid Dune plan.\n\n' +
        'To fix this:\n' +
        '1. Create a query in Dune Analytics (free)\n' +
        '2. Add a parameter named "fullQuery" (type: Text)\n' +
        '3. In the query SQL, use: {{fullQuery}}\n' +
        '4. Set DUNE_QUERY_ID in your .env file to that query ID\n\n' +
        'See DUNE_QUERY_SETUP.md for detailed instructions.'
      );
    } catch (error: any) {
      console.error('[Dune] Error executing raw query:', error);
      console.error('[Dune] Error details:', error.message);
      if (error.stack) {
        console.error('[Dune] Stack trace:', error.stack);
      }
      
      // Better error messages based on DefiLlama's error handling
      if (error.message?.includes('401') || error.message?.includes('invalid')) {
        throw new Error('Dune API Key is invalid - check your DUNE_API_KEY environment variable');
      }
      
      if (error.message?.includes('403') || error.message?.includes('paid plan')) {
        throw new Error(
          'Dune query management requires a paid plan. ' +
          'Please set up a query that accepts fullQuery parameter instead. ' +
          'See the error message above for instructions.'
        );
      }
      
      throw error;
    }
  }

  /**
   * Execute a Dune query and get execution ID
   * Based on DefiLlama's implementation: https://github.com/DefiLlama/dimension-adapters/blob/master/helpers/dune.ts
   * @param queryId The Dune query ID
   * @param parameters Optional query parameters (e.g., token list, fullQuery)
   */
  private async executeQuery(
    queryId: number,
    parameters?: Record<string, any>
  ): Promise<{ execution_id: string }> {
    const executeUrl = `${this.baseUrl}/query/${queryId}/execute`;
    
    const requestBody: any = {};
    if (parameters && Object.keys(parameters).length > 0) {
      // Ensure all parameters are strings (Dune expects string parameters)
      const stringParams: Record<string, string> = {};
      for (const [key, value] of Object.entries(parameters)) {
        stringParams[key] = String(value);
      }
      requestBody.query_parameters = stringParams;
      console.log('[Dune] Sending parameters:', JSON.stringify(stringParams));
    }
    
    const response = await this.fetchWithTimeout(executeUrl, {
      method: 'POST',
      headers: {
        'X-Dune-API-Key': this.apiKey,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(requestBody),
    });

    if (!response.ok) {
      const errorText = await response.text();
      if (response.status === 401) {
        throw new Error('Dune API Key is invalid');
      }
      throw new Error(`Dune API error: ${response.status} ${response.statusText} - ${errorText}`);
    }

    const result = await response.json() as any;
    
    if (result?.execution_id) {
      return { execution_id: result.execution_id };
    }
    
    // DefiLlama pattern: log the query if it fails
    if (parameters?.fullQuery) {
      console.log(`Dune query: ${parameters.fullQuery}`);
    } else {
      console.log('Dune parameters', parameters);
    }
    
    throw new Error(`Error query data: ${JSON.stringify(result)}`);
  }

  /**
   * Get the execution status
   */
  private async getExecutionStatus(executionId: string): Promise<any> {
    const statusUrl = `${this.baseUrl}/execution/${executionId}/status`;
    
    const response = await this.fetchWithTimeout(statusUrl, {
      method: 'GET',
      headers: {
        'X-Dune-API-Key': this.apiKey,
      },
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Dune API error: ${response.status} ${response.statusText} - ${errorText}`);
    }

    const result = await response.json();
    return result;
  }

  /**
   * Get the execution results
   * Based on DefiLlama's implementation: https://github.com/DefiLlama/dimension-adapters/blob/master/helpers/dune.ts
   */
  private async getQueryResults(executionId: string, limit: number = 100000): Promise<DuneQueryResult> {
    const resultsUrl = `${this.baseUrl}/execution/${executionId}/results?limit=${limit}`;
    
    let attempts = 0;
    const maxAttempts = 120; // Wait up to 2 minutes (parameterized queries can take longer)
    
    while (attempts < maxAttempts) {
      // Check execution status first
      const status = await this.getExecutionStatus(executionId);
      
      if (status.state === 'QUERY_STATE_COMPLETED') {
        // Query completed, fetch results
        const response = await this.fetchWithTimeout(resultsUrl, {
          method: 'GET',
          headers: {
            'X-Dune-API-Key': this.apiKey,
          },
        });

        if (!response.ok) {
          const errorText = await response.text();
          throw new Error(`Dune API error: ${response.status} ${response.statusText} - ${errorText}`);
        }

        const result = await response.json() as any;
        
        // Parse result based on DefiLlama's format
        // Result structure: { result: { rows, metadata: { column_names, column_types, ... } } }
        if (result.result) {
          return {
            rows: result.result.rows || [],
            metadata: {
              column_names: result.result.metadata?.column_names || [],
              result_set_bytes: result.result.metadata?.result_set_bytes || 0,
              total_row_count: result.result.metadata?.total_row_count || result.result.rows?.length || 0,
              datapoint_count: result.result.metadata?.datapoint_count || 0,
              pending_time_millis: result.result.metadata?.pending_time_millis || 0,
              execution_time_millis: result.result.metadata?.execution_time_millis || 0,
            },
          };
        }
        
        // Fallback to direct result format
        return result as DuneQueryResult;
      } else if (status.state === 'QUERY_STATE_FAILED') {
        // Status already contains error info, no need to fetch again
        const errorObj = (status as any).error || {};
        const errorMessage = typeof errorObj === 'string' 
          ? errorObj 
          : errorObj.message || errorObj.type || status.state;
        const errorDetails = errorObj.metadata 
          ? `Line ${errorObj.metadata.line}, Column ${errorObj.metadata.column}: ${errorMessage}`
          : '';
        
        console.error('[Dune] Query failed with state:', status.state);
        console.error('[Dune] Error type:', errorObj.type);
        console.error('[Dune] Error message:', errorMessage);
        if (errorDetails) {
          console.error('[Dune] Error details:', errorDetails);
        }
        console.error('[Dune] Full status response:', JSON.stringify(status, null, 2));
        
        const fullErrorMessage = errorDetails || errorMessage;
        throw new Error(`Dune query failed: ${fullErrorMessage}`);
      }
      
      // Query is still running, wait and retry with random delay (like DefiLlama)
      const delay = Math.floor(Math.random() * 3) + 2; // 2-4 seconds
      await new Promise(resolve => setTimeout(resolve, delay * 1000));
      attempts++;
    }
    
    throw new Error('Dune query execution timeout');
  }

  /**
   * Get latest results for a query without executing (if results are recent)
   * Based on DefiLlama's implementation
   */
  private async getLatestResults(queryId: number, maxAgeHours: number = 3): Promise<DuneQueryResult | null> {
    try {
      const response = await this.fetchWithTimeout(`${this.baseUrl}/query/${queryId}/results`, {
        method: 'GET',
        headers: {
          'X-Dune-API-Key': this.apiKey,
        },
      });

      if (!response.ok) {
        return null;
      }

      const result = await response.json() as any;
      const submittedAt = result.submitted_at;
      
      if (!submittedAt) {
        return null;
      }

      const submittedAtTimestamp = Math.trunc(new Date(submittedAt).getTime() / 1000);
      const nowTimestamp = Math.trunc(Date.now() / 1000);
      const diff = nowTimestamp - submittedAtTimestamp;
      
      // If results are older than maxAgeHours, return null to trigger new execution
      if (diff >= maxAgeHours * 60 * 60) {
        return null;
      }

      // Parse result format
      if (result.result) {
        return {
          rows: result.result.rows || [],
          metadata: {
            column_names: result.result.metadata?.column_names || [],
            result_set_bytes: result.result.metadata?.result_set_bytes || 0,
            total_row_count: result.result.metadata?.total_row_count || result.result.rows?.length || 0,
            datapoint_count: result.result.metadata?.datapoint_count || 0,
            pending_time_millis: result.result.metadata?.pending_time_millis || 0,
            execution_time_millis: result.result.metadata?.execution_time_millis || 0,
          },
        };
      }

      return null;
    } catch (error) {
      // If we can't get latest results, return null to trigger new execution
      return null;
    }
  }

  /**
   * Fetch 24h metrics for a specific pool
   */
  async getPoolMetrics24h(poolId: string | PublicKey): Promise<DunePoolMetrics | null> {
    const poolIdStr = typeof poolId === 'string' ? poolId : poolId.toString();
    const cacheKey = `pool_metrics_24h_${poolIdStr}`;
    
    // Check cache
    const cached = this.cache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < this.cacheTTL) {
      return cached.data;
    }

    if (!this.queryId) {
      console.warn('Dune query ID not configured, skipping 24h metrics');
      return null;
    }

    try {
      // Execute the query
      const executeResult = await this.executeQuery(this.queryId);
      
      // Wait for results using the execution_id
      const queryResult = await this.getQueryResults(executeResult.execution_id);

      // Find metrics for this specific pool
      const poolMetrics = queryResult.rows?.find(
        (row: any) => row.pool_id?.toLowerCase() === poolIdStr.toLowerCase()
      );

      if (!poolMetrics) {
        // Cache null result to avoid repeated queries for pools without data
        this.cache.set(cacheKey, { data: null, timestamp: Date.now() });
        return null;
      }

      const metrics: DunePoolMetrics = {
        pool_id: poolMetrics.pool_id || poolIdStr,
        base_volume_24h: this.convertScientificNotation(poolMetrics.base_volume_24h) || '0',
        target_volume_24h: this.convertScientificNotation(poolMetrics.target_volume_24h) || '0',
        high_24h: this.convertScientificNotation(poolMetrics.high_24h) || '0',
        low_24h: this.convertScientificNotation(poolMetrics.low_24h) || '0',
      };

      this.cache.set(cacheKey, { data: metrics, timestamp: Date.now() });
      return metrics;
    } catch (error) {
      console.error(`Error fetching Dune metrics for pool ${poolIdStr}:`, error);
      // Cache null result on error to avoid repeated failures
      this.cache.set(cacheKey, { data: null, timestamp: Date.now() });
      return null;
    }
  }

  /**
   * Fetch 24h metrics for all pools in a batch
   * This is more efficient than calling getPoolMetrics24h for each pool
   * @param tokenAddresses Optional list of token addresses to filter by
   */
  async getAllPoolsMetrics24h(tokenAddresses?: string[]): Promise<Map<string, DunePoolMetrics>> {
    if (!this.apiKey) {
      console.warn('[Dune] API key not configured, skipping 24h metrics');
      return new Map();
    }

    if (!this.queryId) {
      console.warn('[Dune] Query ID not configured - need DUNE_QUERY_ID to use parameterized queries');
      return new Map();
    }

    // Create cache key based on token list (sorted for consistency)
    const cacheKey = tokenAddresses && tokenAddresses.length > 0
      ? `batch_metrics_${tokenAddresses.sort().join(',')}`
      : 'batch_metrics_all';
    
    // Check batch cache
    const cached = this.batchCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < this.batchCacheTTL) {
      console.log(`[Dune] Using cached batch metrics (age: ${Math.round((Date.now() - cached.timestamp) / 1000)}s)`);
      return new Map(cached.data); // Return a copy to prevent mutations
    }

    try {
      // Build query parameters
      const parameters: Record<string, any> = {};
      
      if (tokenAddresses && tokenAddresses.length > 0) {
        // Format tokens with single quotes around each token, comma-separated
        // Format: 'token1','token2','token3' (as Dune expects for IN clause)
        parameters.token_list = tokenAddresses.map(token => `'${token}'`).join(',');
        console.log('[Dune] Using parameterized query with', tokenAddresses.length, 'tokens');
        console.log('[Dune] Token list parameter value:', parameters.token_list.substring(0, 150) + '...');
        console.log('[Dune] Full token list:', tokenAddresses);
      } else {
        // Empty string means query all tokens
        parameters.token_list = '';
        console.log('[Dune] Using parameterized query for all tokens');
      }

      console.log('[Dune] Executing parameterized query with parameters:', JSON.stringify(parameters));
      // Execute the query with parameters
      const executeResult = await this.executeQuery(this.queryId, parameters);
      console.log(`[Dune] Query execution started, execution_id: ${executeResult.execution_id}`);
      
      // Wait for results
      const queryResult = await this.getQueryResults(executeResult.execution_id);

      console.log('[Dune] Query executed successfully');
      console.log('[Dune] Result metadata:', {
        total_rows: queryResult.metadata?.total_row_count || queryResult.rows?.length || 0,
        column_names: queryResult.metadata?.column_names || [],
        execution_time_ms: queryResult.metadata?.execution_time_millis || 0,
      });

      const metricsMap = new Map<string, DunePoolMetrics>();
      
      if (queryResult.rows && queryResult.rows.length > 0) {
        console.log('[Dune] Processing', queryResult.rows.length, 'rows');
        for (const row of queryResult.rows) {
          // Query returns 'token' field (baseMint address), not 'pool_id'
          // Row is from Dune API, so it may have 'token' or 'pool_id' field
          const rowAny = row as any;
          const tokenAddress = rowAny.token || rowAny.pool_id;
          if (tokenAddress) {
            console.log(`[Dune] Found metrics for token: ${tokenAddress}`, {
              base_volume: rowAny.base_volume_24h,
              target_volume: rowAny.target_volume_24h,
              high: rowAny.high_24h,
              low: rowAny.low_24h,
            });
            metricsMap.set(tokenAddress.toLowerCase(), {
              pool_id: tokenAddress, // Store token address, will be mapped to DAO in server
              base_volume_24h: this.convertScientificNotation(rowAny.base_volume_24h) || '0',
              target_volume_24h: this.convertScientificNotation(rowAny.target_volume_24h) || '0',
              high_24h: this.convertScientificNotation(rowAny.high_24h) || '0',
              low_24h: this.convertScientificNotation(rowAny.low_24h) || '0',
            });
            
            // Also cache individual results
            const cacheKey = `pool_metrics_24h_${tokenAddress}`;
            this.cache.set(cacheKey, { 
              data: metricsMap.get(tokenAddress.toLowerCase())!, 
              timestamp: Date.now() 
            });
          } else {
            console.warn('[Dune] Row missing token/pool_id:', row);
          }
        }
        console.log('[Dune] Created metrics map with', metricsMap.size, 'entries');
      } else {
        console.warn('[Dune] Query returned no rows');
        console.warn('[Dune] This could mean:');
        console.warn('  - No transactions in the last 24 hours for these tokens');
        console.warn('  - Token addresses don\'t match the query filter');
        console.warn('  - Query SQL has an issue');
        if (tokenAddresses && tokenAddresses.length > 0) {
          console.warn('[Dune] Query was filtering by tokens:', tokenAddresses.slice(0, 3), '...');
        }
      }

      // Cache the batch results
      this.batchCache.set(cacheKey, { data: metricsMap, timestamp: Date.now() });
      console.log(`[Dune] Cached batch metrics for ${metricsMap.size} pools`);
      
      return metricsMap;
    } catch (error: any) {
      console.error('[Dune] Error fetching Dune metrics for all pools:', error);
      console.error('[Dune] Error message:', error.message);
      console.error('[Dune] Error stack:', error.stack);
      return new Map();
    }
  }

  /**
   * Manually execute a Dune query with custom parameters
   * Useful for testing or one-off queries
   * @param queryId The Dune query ID to execute
   * @param parameters Optional query parameters
   * @returns The raw query results
   */
  async executeQueryManually(
    queryId: number,
    parameters?: Record<string, any>
  ): Promise<DuneQueryResult> {
    try {
      // Execute the query with parameters
      const executeResult = await this.executeQuery(queryId, parameters);
      
      // Wait for results using the execution_id
      const queryResult = await this.getQueryResults(executeResult.execution_id);
      
      return queryResult;
    } catch (error) {
      console.error(`Error executing Dune query ${queryId}:`, error);
      throw error;
    }
  }

  /**
   * Manually execute a generated SQL query
   * @param tokenAddresses Optional list of token addresses to filter by
   * @returns The raw query results
   */
  async executeGeneratedQuery(tokenAddresses?: string[]): Promise<DuneQueryResult> {
    const sqlQuery = this.generate24hMetricsQuery(tokenAddresses);
    return await this.executeRawQuery(sqlQuery, 'Futarchy AMM 24h Metrics');
  }

  /**
   * Fetch aggregate volume data with daily breakdown for tokens
   * Uses Dune query 6422948 which returns daily volume data since start or last 24h
   * @param tokenAddresses List of token addresses to filter by
   * @param sinceStart Whether to fetch all historical data (true) or just last 24h (false)
   * @returns Aggregate volume data with daily breakdown per token
   */
  async getAggregateVolume(
    tokenAddresses: string[],
    sinceStart: boolean = true
  ): Promise<DuneAggregateVolumeResponse> {
    if (!this.apiKey) {
      throw new Error('Dune API key not configured');
    }

    if (!this.aggregateVolumeQueryId) {
      throw new Error('Aggregate volume query ID not configured');
    }

    // Create cache key based on token list and sinceStart flag
    const cacheKey = `aggregate_volume_${sinceStart ? 'all' : '24h'}_${tokenAddresses.sort().join(',')}`;
    
    // Check cache
    const cached = this.aggregateVolumeCache.get(cacheKey);
    if (cached && Date.now() - cached.timestamp < this.aggregateVolumeCacheTTL) {
      console.log(`[Dune] Using cached aggregate volume (age: ${Math.round((Date.now() - cached.timestamp) / 1000)}s)`);
      return cached.data;
    }

    try {
      // Build query parameters
      const parameters: Record<string, any> = {};
      
      if (tokenAddresses && tokenAddresses.length > 0) {
        // Format tokens with single quotes around each token, comma-separated
        // The Dune query expects format: 'token1', 'token2', 'token3'
        parameters.token_list = tokenAddresses.map(token => `'${token}'`).join(', ');
        console.log('[Dune] Aggregate volume query with', tokenAddresses.length, 'tokens, since_start:', sinceStart);
      } else {
        parameters.token_list = '';
        console.log('[Dune] Aggregate volume query for all tokens, since_start:', sinceStart);
      }

      console.log('[Dune] Executing aggregate volume query ID:', this.aggregateVolumeQueryId);
      
      // Execute the query with parameters
      const executeResult = await this.executeQuery(this.aggregateVolumeQueryId, parameters);
      console.log(`[Dune] Aggregate volume query started, execution_id: ${executeResult.execution_id}`);
      
      // Wait for results
      const queryResult = await this.getQueryResults(executeResult.execution_id);
      console.log('[Dune] Aggregate volume query completed, rows:', queryResult.rows?.length || 0);

      // Process results into structured format
      // Group rows by token and aggregate
      const tokenDataMap = new Map<string, {
        dailyData: DuneDailyVolume[];
        firstDate: string;
        lastDate: string;
        totalBaseVolume: number;
        totalTargetVolume: number;
        allTimeHigh: number;
        allTimeLow: number;
      }>();

      if (queryResult.rows && queryResult.rows.length > 0) {
        for (const row of queryResult.rows) {
          const rowAny = row as any;
          const token = rowAny.token || rowAny.pool_id;
          
          if (!token) continue;

          // Get or initialize token data
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

          // Parse the date - handle various date formats from Dune
          const dateStr = rowAny.date || rowAny.trading_date || rowAny.block_date || '';
          const baseVolume = parseFloat(this.convertScientificNotation(rowAny.base_volume)) || 0;
          const targetVolume = parseFloat(this.convertScientificNotation(rowAny.target_volume)) || 0;
          const high = parseFloat(this.convertScientificNotation(rowAny.high)) || 0;
          const low = parseFloat(this.convertScientificNotation(rowAny.low)) || Infinity;

          // Add daily data point
          tokenData.dailyData.push({
            token,
            date: dateStr,
            base_volume: this.convertScientificNotation(rowAny.base_volume) || '0',
            target_volume: this.convertScientificNotation(rowAny.target_volume) || '0',
            high: this.convertScientificNotation(rowAny.high) || '0',
            low: this.convertScientificNotation(rowAny.low) || '0',
          });

          // Update aggregates
          tokenData.totalBaseVolume += baseVolume;
          tokenData.totalTargetVolume += targetVolume;
          if (high > tokenData.allTimeHigh) tokenData.allTimeHigh = high;
          if (low > 0 && low < tokenData.allTimeLow) tokenData.allTimeLow = low;
          
          // Track first and last dates
          if (!tokenData.firstDate || dateStr < tokenData.firstDate) {
            tokenData.firstDate = dateStr;
          }
          if (!tokenData.lastDate || dateStr > tokenData.lastDate) {
            tokenData.lastDate = dateStr;
          }
        }
      }

      // Build response
      const tokens: DuneAggregateTokenVolume[] = [];
      let totalTradingDays = 0;

      for (const [token, data] of tokenDataMap.entries()) {
        // Sort daily data by date
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

      // Sort tokens by total volume descending
      tokens.sort((a, b) => parseFloat(b.total_base_volume) - parseFloat(a.total_base_volume));

      const response: DuneAggregateVolumeResponse = {
        tokens,
        query_metadata: {
          since_start: sinceStart,
          token_count: tokens.length,
          total_trading_days: totalTradingDays,
          execution_time_millis: queryResult.metadata?.execution_time_millis || 0,
        },
      };

      // Cache the response
      this.aggregateVolumeCache.set(cacheKey, { data: response, timestamp: Date.now() });
      console.log(`[Dune] Cached aggregate volume for ${tokens.length} tokens`);

      return response;
    } catch (error: any) {
      console.error('[Dune] Error fetching aggregate volume:', error);
      throw error;
    }
  }

  /**
   * Get the aggregate volume query ID
   */
  getAggregateVolumeQueryId(): number | undefined {
    return this.aggregateVolumeQueryId;
  }
}


