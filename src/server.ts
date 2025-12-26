import express, { type Request, type Response, type NextFunction } from 'express';
import { FutarchyService } from './services/futarchyService.js';
import { PriceService } from './services/priceService.js';
import { DuneService } from './services/duneService.js';
import { SolanaService } from './services/solanaService.js';
import { LaunchpadService } from './services/launchpadService.js';
import { config } from './config.js';
import type { CoinGeckoTicker } from './types/coingecko.js';
import { PublicKey } from '@solana/web3.js';
import BN from 'bn.js';

const app = express();

// Lazy initialization of services to avoid loading at module import time
// This allows tests to mock services before they're accessed
let futarchyServiceInstance: FutarchyService | null = null;
let priceServiceInstance: PriceService | null = null;
let duneServiceInstance: DuneService | null = null;
let solanaServiceInstance: SolanaService | null = null;
let launchpadServiceInstance: LaunchpadService | null = null;

function getFutarchyService(): FutarchyService {
  if (!futarchyServiceInstance) {
    futarchyServiceInstance = new FutarchyService();
  }
  return futarchyServiceInstance;
}

function getPriceService(): PriceService {
  if (!priceServiceInstance) {
    priceServiceInstance = new PriceService();
  }
  return priceServiceInstance;
}

function getDuneService(): DuneService | null {
  // Only require API key - queryId is optional since we can create temporary queries
  if (!duneServiceInstance && config.dune.apiKey) {
    duneServiceInstance = new DuneService();
  }
  return duneServiceInstance;
}

function getSolanaService(): SolanaService {
  if (!solanaServiceInstance) {
    solanaServiceInstance = new SolanaService();
  }
  return solanaServiceInstance;
}

function getLaunchpadService(): LaunchpadService {
  if (!launchpadServiceInstance) {
    launchpadServiceInstance = new LaunchpadService();
  }
  return launchpadServiceInstance;
}

// Export for testing purposes
export function setFutarchyService(service: FutarchyService | null): void {
  futarchyServiceInstance = service;
}

export function setPriceService(service: PriceService | null): void {
  priceServiceInstance = service;
}

export function setDuneService(service: DuneService | null): void {
  duneServiceInstance = service;
}

export function setSolanaService(service: SolanaService | null): void {
  solanaServiceInstance = service;
}

export function setLaunchpadService(service: LaunchpadService | null): void {
  launchpadServiceInstance = service;
}

// Middleware
app.use(express.json());
app.use((req: Request, res: Response, next: NextFunction) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  next();
});

// Rate limiting
const rateLimitMap = new Map<string, { count: number; resetTime: number }>();

function rateLimit(req: Request, res: Response, next: NextFunction) {
  const ip = req.ip || 'unknown';
  const now = Date.now();
  const limit = rateLimitMap.get(ip);

  if (!limit || now > limit.resetTime) {
    rateLimitMap.set(ip, {
      count: 1,
      resetTime: now + config.server.rateLimit.windowMs,
    });
    return next();
  }

  if (limit.count >= config.server.rateLimit.maxRequests) {
    return res.status(429).json({ error: 'Too many requests' });
  }

  limit.count++;
  next();
}

app.use(rateLimit);

// CoinGecko Endpoint 1: /tickers
app.get('/api/tickers', async (req: Request, res: Response) => {
  try {
    const futarchyService = getFutarchyService();
    const priceService = getPriceService();
    const duneService = getDuneService();
    
    // Fetch all DAOs with their pool data
    const allDaos = await futarchyService.getAllDaos();
    
    // Collect baseMint addresses from all DAOs to pass to Dune query
    const baseMintAddresses = allDaos.map(dao => dao.baseMint.toString());
    
    // Create a map from baseMint (token) to DAO address for lookup
    const tokenToDaoMap = new Map<string, string>();
    for (const dao of allDaos) {
      tokenToDaoMap.set(dao.baseMint.toString().toLowerCase(), dao.daoAddress.toString().toLowerCase());
    }
    
    // Fetch 24h metrics from Dune for all pools in batch (if Dune is configured)
    // Note: Dune returns metrics keyed by token (baseMint), we need to map to DAO address
    let duneMetricsMap = new Map<string, { base_volume_24h: string; target_volume_24h: string; high_24h: string; low_24h: string }>();
    if (duneService) {
      try {
        console.log(`[Dune] Fetching metrics for ${baseMintAddresses.length} tokens:`, baseMintAddresses.slice(0, 5), baseMintAddresses.length > 5 ? '...' : '');
        // Pass token addresses to filter the query
        // Dune returns metrics keyed by token (baseMint), not DAO address
        const allDuneMetrics = await duneService.getAllPoolsMetrics24h(baseMintAddresses);
        console.log(`[Dune] Received ${allDuneMetrics.size} pool metrics from Dune`);
        
        if (allDuneMetrics.size > 0) {
          console.log('[Dune] Token addresses found:', Array.from(allDuneMetrics.keys()).slice(0, 5));
        } else {
          console.warn('[Dune] No metrics returned from query - this could mean:');
          console.warn('  - No transactions in the last 24 hours');
          console.warn('  - Query returned no matching pools');
          console.warn('  - Query execution failed silently');
        }
        
        // Map from token (baseMint) to DAO address
        for (const [tokenAddress, metrics] of allDuneMetrics.entries()) {
          const daoAddress = tokenToDaoMap.get(tokenAddress.toLowerCase());
          if (daoAddress) {
            console.log(`[Dune] Mapping token ${tokenAddress} to DAO ${daoAddress}`);
            duneMetricsMap.set(daoAddress, {
              base_volume_24h: metrics.base_volume_24h,
              target_volume_24h: metrics.target_volume_24h,
              high_24h: metrics.high_24h,
              low_24h: metrics.low_24h,
            });
          } else {
            console.warn(`[Dune] No DAO found for token ${tokenAddress}`);
          }
        }
      } catch (error: any) {
        console.error('[Dune] Error fetching Dune metrics:', error);
        console.error('[Dune] Error details:', error.message);
        if (error.stack) {
          console.error('[Dune] Stack trace:', error.stack);
        }
        // Continue without Dune data if it fails
      }
    } else {
      console.warn('[Dune] Dune service not configured - set DUNE_API_KEY in environment');
    }
    
    // Generate tickers for all DAOs
    const tickers: CoinGeckoTicker[] = [];
    
    for (const daoData of allDaos) {
      try {
        const { 
          daoAddress, 
          baseMint, 
          quoteMint, 
          baseDecimals, 
          quoteDecimals, 
          baseSymbol,
          baseName,
          quoteSymbol,
          quoteName,
          poolData 
        } = daoData;
        const tickerId = `${baseMint.toString()}_${quoteMint.toString()}`;
        const poolId = daoAddress.toString();
        
        // Calculate price and metrics with validation
        const lastPrice = priceService.calculatePrice(
          poolData.baseReserves,
          poolData.quoteReserves,
          baseDecimals,
          quoteDecimals
        );
        
        // Skip if price calculation failed
        if (!lastPrice) {
          continue;
        }
        
        const priceNum = parseFloat(lastPrice);
        const spread = priceService.calculateSpread(priceNum);
        
        // Skip if spread calculation failed
        if (!spread) {
          continue;
        }
        
        const liquidityUsd = priceService.calculateLiquidityUSD(
          poolData.quoteReserves,
          quoteDecimals
        );
        
        // Skip if liquidity calculation failed
        if (!liquidityUsd) {
          continue;
        }

        // Get 24h metrics from Dune if available, otherwise fallback to fee-based calculation
        const duneMetrics = duneMetricsMap.get(poolId.toLowerCase());
        let baseVolume: string;
        let targetVolume: string;
        let high24h: string | undefined;
        let low24h: string | undefined;

        if (duneMetrics) {
          // Use Dune data for base volume, high, and low
          baseVolume = duneMetrics.base_volume_24h;
          // Calculate target_volume from base_volume * last_price to ensure consistency
          const baseVolumeNum = parseFloat(baseVolume);
          const lastPriceNum = parseFloat(lastPrice);
          if (isFinite(baseVolumeNum) && isFinite(lastPriceNum) && baseVolumeNum > 0 && lastPriceNum > 0) {
            targetVolume = (baseVolumeNum * lastPriceNum).toFixed(12);
          } else {
            // Fallback to Dune's target_volume if calculation fails
            targetVolume = duneMetrics.target_volume_24h;
          }
          high24h = duneMetrics.high_24h !== '0' ? duneMetrics.high_24h : undefined;
          low24h = duneMetrics.low_24h !== '0' ? duneMetrics.low_24h : undefined;
        } else {
          // Only log once per request, not for every pool
          if (tickers.length === 0 && duneService) {
            console.log(`[Dune] No metrics found for pool ${poolId}, calculating volumes from protocol fees`);
            console.log(`[Dune] Looking for pool_id: ${poolId.toLowerCase()}`);
            console.log(`[Dune] Available pool IDs in map:`, Array.from(duneMetricsMap.keys()).slice(0, 10));
          }
          // Fallback: Calculate volumes from protocol fees
          const volumeData = priceService.calculateVolumeFromFees(
            poolData.baseProtocolFees,
            poolData.quoteProtocolFees,
            baseDecimals,
            quoteDecimals,
            config.fees.protocolFeeRate
          );

          if (volumeData) {
            baseVolume = volumeData.baseVolume;
            targetVolume = volumeData.targetVolume;
          } else {
            // Fallback: estimate volume from reserves (old method)
            const baseReservesNum = poolData.baseReserves.toNumber();
            const quoteReservesNum = poolData.quoteReserves.toNumber();
            
            if (!isFinite(baseReservesNum) || !isFinite(quoteReservesNum)) {
              continue;
            }
            
            baseVolume = (baseReservesNum * 0.01 / Math.pow(10, baseDecimals)).toFixed(8);
            targetVolume = (quoteReservesNum * 0.01 / Math.pow(10, quoteDecimals)).toFixed(8);
          }
        }

        // Final validation - ensure no NaN values
        if (isNaN(parseFloat(baseVolume)) || isNaN(parseFloat(targetVolume))) {
          continue;
        }

        const ticker: CoinGeckoTicker = {
          ticker_id: tickerId,
          base_currency: baseMint.toString(),
          target_currency: quoteMint.toString(),
          base_symbol: baseSymbol,
          base_name: baseName,
          target_symbol: quoteSymbol,
          target_name: quoteName,
          pool_id: poolId,
          last_price: lastPrice,
          base_volume: baseVolume,
          target_volume: targetVolume,
          liquidity_in_usd: liquidityUsd,
          bid: spread.bid,
          ask: spread.ask,
        };

        // Add high and low if available from Dune
        if (high24h) {
          ticker.high_24h = high24h;
        }
        if (low24h) {
          ticker.low_24h = low24h;
        }

        // Add treasury USDC AUM and vault address if available
        if (daoData.treasuryUsdcAum) {
          ticker.treasury_usdc_aum = daoData.treasuryUsdcAum;
        }
        if (daoData.treasuryVaultAddress) {
          ticker.treasury_vault_address = daoData.treasuryVaultAddress;
        }

        tickers.push(ticker);
      } catch (error) {
        console.error(`Error generating ticker for DAO ${daoData.daoAddress.toString()}:`, error);
        // Continue processing other DAOs
      }
    }

    res.json(tickers);
  } catch (error) {
    console.error('Error in /api/tickers:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Health check
app.get('/health', (req: Request, res: Response) => {
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
  });
});

// Token Supply Endpoints

// Get complete supply info for a token
app.get('/api/supply/:mintAddress', async (req: Request, res: Response) => {
  try {
    const mintAddress = req.params.mintAddress;
    const solanaService = getSolanaService();
    const launchpadService = getLaunchpadService();

    if (!mintAddress || !solanaService.isValidPublicKey(mintAddress)) {
      return res.status(400).json({
        error: 'Invalid mint address',
        message: 'The provided address is not a valid Solana public key',
      });
    }

    // Get complete token allocation breakdown (team, futarchyAMM, meteora)
    const allocation = await launchpadService.getTokenAllocationBreakdown(
      new PublicKey(mintAddress)
    );

    const supplyInfo = await solanaService.getSupplyInfo(mintAddress, {
      teamPerformancePackage: {
        amount: allocation.teamPerformancePackage.amount,
        address: allocation.teamPerformancePackage.address?.toString(),
      },
      futarchyAmmLiquidity: {
        amount: allocation.futarchyAmmLiquidity.amount,
        vaultAddress: allocation.futarchyAmmLiquidity.vaultAddress?.toString(),
      },
      meteoraLpLiquidity: {
        amount: allocation.meteoraLpLiquidity.amount,
        poolAddress: allocation.meteoraLpLiquidity.poolAddress?.toString(),
        vaultAddress: allocation.meteoraLpLiquidity.vaultAddress?.toString(),
      },
      daoAddress: allocation.daoAddress?.toString(),
      launchAddress: allocation.launchAddress?.toString(),
    });

    res.json({
      result: supplyInfo.totalSupply,
      data: supplyInfo,
    });
  } catch (error: any) {
    console.error('Error in /api/supply/:mintAddress:', error);
    res.status(500).json({
      error: 'Failed to fetch supply info',
      message: error.message || 'Internal server error',
    });
  }
});

// Get total supply for a token
app.get('/api/supply/:mintAddress/total', async (req: Request, res: Response) => {
  try {
    const mintAddress = req.params.mintAddress;
    const solanaService = getSolanaService();

    if (!mintAddress || !solanaService.isValidPublicKey(mintAddress)) {
      return res.status(400).json({
        error: 'Invalid mint address',
        message: 'The provided address is not a valid Solana public key',
      });
    }

    const totalSupply = await solanaService.getTotalSupply(mintAddress);

    res.json({
      result: totalSupply,
    });
  } catch (error: any) {
    console.error('Error in /api/supply/:mintAddress/total:', error);
    res.status(500).json({
      error: 'Failed to fetch total supply',
      message: error.message || 'Internal server error',
    });
  }
});

// Get circulating supply for a token
app.get('/api/supply/:mintAddress/circulating', async (req: Request, res: Response) => {
  try {
    const mintAddress = req.params.mintAddress;
    const solanaService = getSolanaService();
    const launchpadService = getLaunchpadService();

    if (!mintAddress || !solanaService.isValidPublicKey(mintAddress)) {
      return res.status(400).json({
        error: 'Invalid mint address',
        message: 'The provided address is not a valid Solana public key',
      });
    }

    // Get complete token allocation breakdown
    const allocation = await launchpadService.getTokenAllocationBreakdown(
      new PublicKey(mintAddress)
    );

    // Only team performance package is excluded from circulating supply
    // Liquidity (futarchyAMM and meteora) IS considered circulating
    const lockedAmount = allocation.teamPerformancePackage.amount;

    const circulatingSupply = await solanaService.getCirculatingSupply(mintAddress, lockedAmount);

    // Include allocation addresses in response
    const response: { 
      result: string; 
      allocation?: {
        teamPerformancePackageAddress?: string;
        futarchyAmmVaultAddress?: string;
        meteoraPoolAddress?: string;
        meteoraVaultAddress?: string;
        daoAddress?: string;
        launchAddress?: string;
      };
    } = {
      result: circulatingSupply,
    };
    
    // Add allocation details if any are present
    if (allocation.teamPerformancePackage.address || 
        allocation.futarchyAmmLiquidity.vaultAddress || 
        allocation.meteoraLpLiquidity.poolAddress) {
      response.allocation = {
        teamPerformancePackageAddress: allocation.teamPerformancePackage.address?.toString(),
        futarchyAmmVaultAddress: allocation.futarchyAmmLiquidity.vaultAddress?.toString(),
        meteoraPoolAddress: allocation.meteoraLpLiquidity.poolAddress?.toString(),
        meteoraVaultAddress: allocation.meteoraLpLiquidity.vaultAddress?.toString(),
        daoAddress: allocation.daoAddress?.toString(),
        launchAddress: allocation.launchAddress?.toString(),
      };
    }

    res.json(response);
  } catch (error: any) {
    console.error('Error in /api/supply/:mintAddress/circulating:', error);
    res.status(500).json({
      error: 'Failed to fetch circulating supply',
      message: error.message || 'Internal server error',
    });
  }
});

// Manual Dune query execution endpoint
app.post('/api/dune/execute', async (req: Request, res: Response) => {
  try {
    const duneService = getDuneService();
    if (!duneService) {
      return res.status(400).json({ error: 'Dune service not configured' });
    }

    const { queryId, parameters, tokenAddresses, sqlQuery } = req.body;
    
    let result: any;
    
    if (sqlQuery) {
      // Execute raw SQL query
      result = await duneService.executeRawQuery(sqlQuery, 'Manual Query');
    } else if (tokenAddresses) {
      // Execute generated query with token filter
      result = await duneService.executeGeneratedQuery(tokenAddresses);
    } else if (queryId) {
      // Execute existing query by ID
      result = await duneService.executeQueryManually(queryId, parameters);
    } else {
      return res.status(400).json({ 
        error: 'Either queryId, tokenAddresses, or sqlQuery must be provided' 
      });
    }

    res.json(result);
  } catch (error: any) {
    console.error('Error executing Dune query:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// Aggregate Volume Endpoint - Daily volume data with totals for all DAO tokens
app.get('/api/volume/aggregate', async (req: Request, res: Response) => {
  try {
    const futarchyService = getFutarchyService();
    const duneService = getDuneService();
    
    if (!duneService) {
      return res.status(400).json({ 
        error: 'Dune service not configured',
        message: 'DUNE_API_KEY environment variable is required'
      });
    }

    // Automatically fetch all DAO tokens from the futarchy protocol
    const allDaos = await futarchyService.getAllDaos();
    const tokenAddresses = allDaos.map(dao => dao.baseMint.toString());

    if (tokenAddresses.length === 0) {
      return res.status(404).json({
        error: 'No DAOs found',
        message: 'No active DAOs were discovered in the Futarchy protocol'
      });
    }

    console.log(`[Volume] Fetching aggregate volume for ${tokenAddresses.length} DAO tokens since launch`);

    // Always fetch full history since launch
    const aggregateData = await duneService.getAggregateVolume(tokenAddresses, true);

    res.json(aggregateData);
  } catch (error: any) {
    console.error('Error in /api/volume/aggregate:', error);
    res.status(500).json({ 
      error: 'Failed to fetch aggregate volume',
      message: error.message || 'Internal server error'
    });
  }
});

// Get generated SQL query endpoint
app.get('/api/dune/query', (req: Request, res: Response) => {
  try {
    const duneService = getDuneService();
    if (!duneService) {
      return res.status(400).json({ error: 'Dune service not configured' });
    }

    const tokenAddresses = req.query.tokens 
      ? (req.query.tokens as string).split(',').map(t => t.trim())
      : undefined;

    const sqlQuery = duneService.generate24hMetricsQuery(tokenAddresses);
    
    res.json({
      sql: sqlQuery,
      tokenAddresses: tokenAddresses || 'all',
    });
  } catch (error: any) {
    console.error('Error generating query:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// Debug endpoint to test Dune query with current DAOs
app.get('/api/dune/debug', async (req: Request, res: Response) => {
  try {
    const futarchyService = getFutarchyService();
    const duneService = getDuneService();
    
    if (!duneService) {
      return res.status(400).json({ error: 'Dune service not configured' });
    }

    // Fetch all DAOs
    const allDaos = await futarchyService.getAllDaos();
    const baseMintAddresses = allDaos.map(dao => dao.baseMint.toString());
    const daoAddresses = allDaos.map(dao => dao.daoAddress.toString());

    // Build parameters as the service does
    const parameters: Record<string, any> = {};
    if (baseMintAddresses && baseMintAddresses.length > 0) {
      parameters.token_list = baseMintAddresses.join(',');
    } else {
      parameters.token_list = '';
    }

    // Try to execute with parameterized query
    let executionResult: any = null;
    let error: any = null;
    let queryResults: any = null;
    try {
      if (config.dune.queryId) {
        // Use parameterized query
        const executeResult = await (duneService as any).executeQuery(config.dune.queryId, parameters);
        console.log('[Debug] Execution ID:', executeResult.execution_id);
        queryResults = await (duneService as any).getQueryResults(executeResult.execution_id);
        executionResult = { 
          execution_id: executeResult.execution_id, 
          success: true,
          rows: queryResults.rows,
          metadata: queryResults.metadata,
        };
      }
    } catch (e: any) {
      error = {
        message: e.message,
        stack: e.stack,
      };
    }

    res.json({
      config: {
        hasApiKey: !!config.dune.apiKey,
        hasQueryId: !!config.dune.queryId,
        queryId: config.dune.queryId,
      },
      daos: {
        count: allDaos.length,
        baseMintAddresses: baseMintAddresses.slice(0, 10),
        daoAddresses: daoAddresses.slice(0, 10),
      },
      parameters: {
        token_list: parameters.token_list,
        token_list_length: parameters.token_list.length,
        token_count: baseMintAddresses.length,
        token_list_preview: parameters.token_list.substring(0, 200),
      },
      execution: error ? {
        error,
      } : {
        success: true,
        execution_id: executionResult?.execution_id,
        rowsReturned: queryResults?.rows?.length || 0,
        metadata: queryResults?.metadata || {},
        sampleRows: queryResults?.rows?.slice(0, 3) || [],
        allTokens: queryResults?.rows?.map((r: any) => r.token || r.pool_id) || [],
      },
    });
  } catch (error: any) {
    console.error('Error in debug endpoint:', error);
    res.status(500).json({ error: error.message || 'Internal server error' });
  }
});

// Root endpoint
app.get('/', (req: Request, res: Response) => {
  res.json({
    name: 'Futarchy AMM - CoinGecko API',
    version: '1.0.0',
    documentation: 'https://docs.coingecko.com/reference/exchanges-list',
    endpoints: {
      tickers: '/api/tickers - Returns all DAO tickers with pricing and volume',
      supply: '/api/supply/:mintAddress - Returns complete supply breakdown with allocation details',
      supply_total: '/api/supply/:mintAddress/total - Returns total supply only',
      supply_circulating: '/api/supply/:mintAddress/circulating - Returns circulating supply (excludes team performance package)',
      volume_aggregate: '/api/volume/aggregate - Returns aggregate volume with daily breakdown for all DAO tokens since launch',
      health: '/health',
    },
    dex: {
      fork_type: config.dex.forkType,
      factory_address: config.dex.factoryAddress,
      router_address: config.dex.routerAddress,
    },
    supplyBreakdown: {
      description: 'For launchpad tokens, supply is broken down into:',
      circulatingSupply: 'Total supply minus team performance package (liquidity IS circulating)',
      teamPerformancePackage: 'Locked tokens allocated to the team (price-based unlock) - NOT circulating',
      futarchyAmmLiquidity: 'Tokens in the internal FutarchyAMM for spot trading - IS circulating',
      meteoraLpLiquidity: 'Tokens in the external Meteora DAMM pool (POL) - IS circulating',
    },
    note: 'This API automatically discovers and aggregates all DAOs from the Futarchy protocol.',
  });
});

// Error handling
app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

// Start server
app.listen(config.server.port, () => {
  console.log(`✅ CoinGecko API running on port ${config.server.port}`);
  console.log(`📊 Tickers: http://localhost:${config.server.port}/api/tickers`);
  console.log(`📈 Health: http://localhost:${config.server.port}/health`);
});

export default app;