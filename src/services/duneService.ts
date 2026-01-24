import { config } from '../config.js';
import { PublicKey } from '@solana/web3.js';
import { withRetryAndTimeout, isTransientError, createRetryLogger } from '../utils/resilience.js';
import { logger } from '../utils/logger.js';

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
  // Deprecated query IDs removed - now using DB aggregation from 10-minute data
  private baseUrl: string = 'https://api.dune.com/api/v1';
  private cache: Map<string, { data: DunePoolMetrics | null; timestamp: number }>;
  private batchCache: Map<string, { data: Map<string, DunePoolMetrics>; timestamp: number }>;
  private aggregateVolumeCache: Map<string, { data: DuneAggregateVolumeResponse; timestamp: number }>;
  private cacheTTL: number = 60000; // 1 minute cache for individual pools
  private batchCacheTTL: number = 300000; // 5 minutes cache for batch queries (Dune queries are heavy)
  private aggregateVolumeCacheTTL: number = 600000; // 10 minutes cache for aggregate volume (heavy historical query)
  private fetchTimeout: number = 600000; // 10 minutes timeout for Dune API calls
  private devMode: boolean;

  constructor() {
    this.apiKey = config.dune.apiKey;
    // Deprecated query IDs removed - services now use DB aggregation
    this.cache = new Map();
    this.batchCache = new Map();
    this.aggregateVolumeCache = new Map();
    this.devMode = config.devMode;
    
    if (this.devMode) {
      logger.info('[Dune] ⚠️  DEV_MODE enabled - external Dune API calls are disabled');
    }
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
   * Check if dev mode is enabled
   */
  isDevMode(): boolean {
    return this.devMode;
  }

  /**
   * Fetch with timeout and retry wrapper.
   * Retries on transient errors (network issues, rate limits, 5xx).
   * @param url The URL to fetch
   * @param options Fetch options
   * @returns Response promise
   */
  private async fetchWithTimeout(url: string, options: RequestInit = {}): Promise<Response> {
    return withRetryAndTimeout(
      async () => {
        const response = await fetch(url, options);
        // Throw on transient HTTP errors so they get retried
        if (response.status === 429 || response.status >= 500) {
          const errorText = await response.text();
          const error = new Error(`Dune API error: ${response.status} - ${errorText}`);
          (error as any).status = response.status;
          throw error;
        }
        return response;
      },
      {
        timeoutMs: this.fetchTimeout,
        timeoutMessage: `Dune API request timed out after ${this.fetchTimeout}ms`,
        maxRetries: 2,
        initialDelayMs: 1000,
        maxDelayMs: 5000,
        isRetryable: isTransientError,
        onRetry: createRetryLogger('[Dune]'),
      }
    );
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
          logger.warn(`[Dune] Failed to convert scientific notation: ${value}`, { error: String(error) });
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
      logger.info('[Dune] Sending parameters:', { params: stringParams });
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
      logger.info(`Dune query: ${parameters.fullQuery}`);
    } else {
      logger.info('Dune parameters', { parameters });
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
   * Fetch all paginated results from a URL
   * Dune API returns next_uri when there are more results
   */
  private async fetchAllPaginatedResults(initialUrl: string): Promise<{ rows: any[], metadata: any }> {
    const allRows: any[] = [];
    let currentUrl: string | null = initialUrl;
    let metadata: any = null;
    let pageCount = 0;
    
    while (currentUrl) {
      pageCount++;
      logger.info(`[Dune] Fetching page ${pageCount}...`);
      
      const response = await this.fetchWithTimeout(currentUrl, {
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
      
      // Extract rows from this page
      const pageRows = result.result?.rows || result.rows || [];
      allRows.push(...pageRows);
      
      // Store metadata from first page
      if (!metadata && result.result?.metadata) {
        metadata = result.result.metadata;
      }
      
      logger.info(`[Dune] Page ${pageCount}: ${pageRows.length} rows (total: ${allRows.length})`);
      
      // Check for next page
      if (result.next_uri) {
        // next_uri can be relative or absolute - handle both cases
        if (result.next_uri.startsWith('http')) {
          currentUrl = result.next_uri;
        } else {
          currentUrl = `https://api.dune.com${result.next_uri}`;
        }
      } else {
        currentUrl = null;
      }
    }
    
    if (pageCount > 1) {
      logger.info(`[Dune] Fetched ${pageCount} pages, total rows: ${allRows.length}`);
    }
    
    return { rows: allRows, metadata };
  }

  /**
   * Get the execution results
   * Based on DefiLlama's implementation: https://github.com/DefiLlama/dimension-adapters/blob/master/helpers/dune.ts
   */
  private async getQueryResults(executionId: string, limit: number = 100000): Promise<DuneQueryResult> {
    const resultsUrl = `${this.baseUrl}/execution/${executionId}/results?limit=${limit}`;
    
    let attempts = 0;
    const maxAttempts = 180; // Wait up to ~10 minutes (parameterized queries can take longer)
    
    while (attempts < maxAttempts) {
      // Check execution status first
      const status = await this.getExecutionStatus(executionId);
      
      if (status.state === 'QUERY_STATE_COMPLETED') {
        // Query completed, fetch all paginated results
        const { rows, metadata } = await this.fetchAllPaginatedResults(resultsUrl);
        
        return {
          rows,
          metadata: {
            column_names: metadata?.column_names || [],
            result_set_bytes: metadata?.result_set_bytes || 0,
            total_row_count: metadata?.total_row_count || rows.length,
            datapoint_count: metadata?.datapoint_count || 0,
            pending_time_millis: metadata?.pending_time_millis || 0,
            execution_time_millis: metadata?.execution_time_millis || 0,
          },
        };
      } else if (status.state === 'QUERY_STATE_FAILED') {
        // Status already contains error info, no need to fetch again
        const errorObj = (status as any).error || {};
        const errorMessage = typeof errorObj === 'string' 
          ? errorObj 
          : errorObj.message || errorObj.type || status.state;
        const errorDetails = errorObj.metadata 
          ? `Line ${errorObj.metadata.line}, Column ${errorObj.metadata.column}: ${errorMessage}`
          : '';
        
        logger.error('[Dune] Query failed with state:', undefined, { state: status.state });
        logger.error('[Dune] Error type:', undefined, { type: errorObj.type });
        logger.error('[Dune] Error message:', undefined, { errorMessage });
        if (errorDetails) {
          logger.error('[Dune] Error details:', undefined, { errorDetails });
        }
        logger.error('[Dune] Full status response:', undefined, { status });
        
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
   * Now with pagination support
   */
  private async getLatestResults(queryId: number, maxAgeHours: number = 3): Promise<DuneQueryResult | null> {
    try {
      const initialUrl = `${this.baseUrl}/query/${queryId}/results`;
      const response = await this.fetchWithTimeout(initialUrl, {
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

      // Parse result format and handle pagination
      if (result.result) {
        const allRows = [...(result.result.rows || [])];
        const metadata = result.result.metadata;
        
        // Check for more pages
        let nextUri = result.next_uri;
        let pageCount = 1;
        
        while (nextUri) {
          pageCount++;
          logger.info(`[Dune] Fetching latest results page ${pageCount}...`);
          
          // next_uri can be relative or absolute - handle both cases
          const nextUrl = nextUri.startsWith('http') ? nextUri : `https://api.dune.com${nextUri}`;
          const nextResponse = await this.fetchWithTimeout(nextUrl, {
            method: 'GET',
            headers: {
              'X-Dune-API-Key': this.apiKey,
            },
          });
          
          if (!nextResponse.ok) {
            break;
          }
          
          const nextResult = await nextResponse.json() as any;
          const pageRows = nextResult.result?.rows || nextResult.rows || [];
          allRows.push(...pageRows);
          
          logger.info(`[Dune] Page ${pageCount}: ${pageRows.length} rows (total: ${allRows.length})`);
          
          nextUri = nextResult.next_uri;
        }
        
        if (pageCount > 1) {
          logger.info(`[Dune] Fetched ${pageCount} pages of latest results, total rows: ${allRows.length}`);
        }
        
        return {
          rows: allRows,
          metadata: {
            column_names: metadata?.column_names || [],
            result_set_bytes: metadata?.result_set_bytes || 0,
            total_row_count: metadata?.total_row_count || allRows.length,
            datapoint_count: metadata?.datapoint_count || 0,
            pending_time_millis: metadata?.pending_time_millis || 0,
            execution_time_millis: metadata?.execution_time_millis || 0,
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
    // Skip external calls in dev mode
    if (this.devMode) {
      logger.info(`[Dune] DEV_MODE: Skipping query ${queryId} execution`);
      return {
        rows: [],
        metadata: {
          column_names: [],
          result_set_bytes: 0,
          total_row_count: 0,
          datapoint_count: 0,
          pending_time_millis: 0,
          execution_time_millis: 0,
        }
      };
    }

    try {
      // Execute the query with parameters
      const executeResult = await this.executeQuery(queryId, parameters);
      
      // Wait for results using the execution_id
      const queryResult = await this.getQueryResults(executeResult.execution_id);
      
      return queryResult;
    } catch (error) {
      logger.error(`Error executing Dune query ${queryId}:`, error);
      throw error;
    }
  }

}


