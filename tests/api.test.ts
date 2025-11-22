import request from 'supertest';
import BN from 'bn.js';
import { FutarchyService } from '../src/services/futarchyService.js';
import { PriceService } from '../src/services/priceService.js';
import { setFutarchyService, setPriceService } from '../src/server.js';
import app from '../src/server.js';

// Mock services
const mockFutarchyService = {
  getPoolData: jest.fn().mockResolvedValue({
    baseReserves: new BN('1000000000000'),
    quoteReserves: new BN('50000000000'),
    baseProtocolFees: new BN('0'),
    quoteProtocolFees: new BN('0'),
  }),
  getTotalLiquidity: jest.fn().mockResolvedValue(new BN('100000000000')),
} as unknown as FutarchyService;

const mockPriceService = {
  calculatePrice: jest.fn((baseReserves: BN, quoteReserves: BN) => {
    return '0.05';
  }),
  calculateSpread: jest.fn((price: number) => ({
    bid: '0.04975',
    ask: '0.05025',
  })),
  calculateLiquidityUSD: jest.fn((quoteReserves: BN, decimals: number) => {
    return '100000.00';
  }),
  calculateOrderbookDepth: jest.fn((baseReserves: BN, quoteReserves: BN, depthLevels: number) => ({
    bids: [['0.04975', '100.0']],
    asks: [['0.05025', '100.0']],
  })),
} as unknown as PriceService;

describe('CoinGecko API', () => {
  beforeAll(() => {
    // Set mocks before any routes are accessed
    setFutarchyService(mockFutarchyService);
    setPriceService(mockPriceService);
  });

  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('GET /api/tickers', () => {
    it('should return tickers array', async () => {
      const response = await request(app).get('/api/tickers');
      expect(response.status).toBe(200);
      expect(Array.isArray(response.body)).toBe(true);
      expect(response.body[0]).toHaveProperty('ticker_id');
      expect(response.body[0]).toHaveProperty('last_price');
    });
  });

  describe('GET /api/orderbook', () => {
    it('should return orderbook with ticker_id', async () => {
      const response = await request(app)
        .get('/api/orderbook')
        .query({ ticker_id: 'TEST_USDC' });
      expect(response.status).toBe(200);
      expect(response.body).toHaveProperty('bids');
      expect(response.body).toHaveProperty('asks');
    });

    it('should return 400 without ticker_id', async () => {
      const response = await request(app).get('/api/orderbook');
      expect(response.status).toBe(400);
    });
  });
});