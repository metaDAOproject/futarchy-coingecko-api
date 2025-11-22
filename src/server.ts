import express, { type Request, type Response, type NextFunction } from 'express';
import { FutarchyService } from './services/futarchyService.js';
import { PriceService } from './services/priceService.js';
import { config } from './config.js';
import type { CoinGeckoTicker, CoinGeckoOrderbook } from './types/coingecko.js';

const app = express();

// Lazy initialization of services to avoid loading at module import time
// This allows tests to mock services before they're accessed
let futarchyServiceInstance: FutarchyService | null = null;
let priceServiceInstance: PriceService | null = null;

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

// Export for testing purposes
export function setFutarchyService(service: FutarchyService | null): void {
  futarchyServiceInstance = service;
}

export function setPriceService(service: PriceService | null): void {
  priceServiceInstance = service;
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
    
    // Fetch all DAOs with their pool data
    const allDaos = await futarchyService.getAllDaos();
    
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

        // Calculate volumes from protocol fees
        const volumeData = priceService.calculateVolumeFromFees(
          poolData.baseProtocolFees,
          poolData.quoteProtocolFees,
          baseDecimals,
          quoteDecimals,
          config.fees.protocolFeeRate
        );

        // Fallback to estimated volume if fee-based calculation fails
        let baseVolume: string;
        let targetVolume: string;
        
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

        // Final validation - ensure no NaN values
        if (isNaN(parseFloat(baseVolume)) || isNaN(parseFloat(targetVolume))) {
          continue;
        }

        tickers.push({
          ticker_id: tickerId,
          base_currency: baseMint.toString(),
          target_currency: quoteMint.toString(),
          base_symbol: baseSymbol,
          base_name: baseName,
          target_symbol: quoteSymbol,
          target_name: quoteName,
          pool_id: daoAddress.toString(),
          last_price: lastPrice,
          base_volume: baseVolume,
          target_volume: targetVolume,
          liquidity_in_usd: liquidityUsd,
          bid: spread.bid,
          ask: spread.ask,
        });
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

// CoinGecko Endpoint 2: /orderbook
app.get('/api/orderbook', async (req: Request, res: Response) => {
  try {
    const { ticker_id, depth = '100' } = req.query;

    if (!ticker_id) {
      return res.status(400).json({ error: 'ticker_id parameter is required' });
    }

    // Parse ticker_id to extract base and quote mints
    const [baseMintStr, quoteMintStr] = (ticker_id as string).split('_');
    if (!baseMintStr || !quoteMintStr) {
      return res.status(400).json({ error: 'Invalid ticker_id format. Expected: BASE_MINT_QUOTE_MINT' });
    }

    const futarchyService = getFutarchyService();
    const priceService = getPriceService();
    
    // Find the DAO that matches this ticker_id
    const allDaos = await futarchyService.getAllDaos();
    const daoData = allDaos.find(
      (dao) => 
        dao.baseMint.toString() === baseMintStr && 
        dao.quoteMint.toString() === quoteMintStr
    );

    if (!daoData) {
      return res.status(404).json({ error: 'Ticker not found' });
    }

    const depthLevels = Math.min(parseInt(depth as string) / 2, 100);

    const orderbookData = priceService.calculateOrderbookDepth(
      daoData.poolData.baseReserves,
      daoData.poolData.quoteReserves,
      depthLevels,
      daoData.baseDecimals
    );

    if (!orderbookData) {
      return res.status(400).json({ error: 'Invalid pool data - cannot calculate orderbook' });
    }

    const orderbook: CoinGeckoOrderbook = {
      ticker_id: ticker_id as string,
      timestamp: Date.now().toString(),
      bids: orderbookData.bids,
      asks: orderbookData.asks,
    };

    res.json(orderbook);
  } catch (error) {
    console.error('Error in /api/orderbook:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// CoinGecko Endpoint 3: /historical_trades (Optional)
app.get('/api/historical_trades', async (req: Request, res: Response) => {
  res.json({
    buy: [],
    sell: [],
    note: 'Historical trades require transaction monitoring - not yet implemented',
  });
});

// Health check
app.get('/health', (req: Request, res: Response) => {
  res.json({
    status: 'healthy',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
  });
});

// Root endpoint
app.get('/', (req: Request, res: Response) => {
  res.json({
    name: 'Futarchy AMM - CoinGecko API',
    version: '1.0.0',
    documentation: 'https://docs.coingecko.com/reference/exchanges-list',
    endpoints: {
      tickers: '/api/tickers - Returns all DAO tickers',
      orderbook: '/api/orderbook?ticker_id={BASE_MINT_QUOTE_MINT}&depth={DEPTH}',
      historical_trades: '/api/historical_trades?ticker_id={TICKER_ID}',
      health: '/health',
    },
    note: 'This API automatically discovers and aggregates all DAOs from the Futarchy protocol',
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