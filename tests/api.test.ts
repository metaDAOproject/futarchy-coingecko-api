import request from 'supertest';
import BN from 'bn.js';
import { PublicKey } from '@solana/web3.js';
import { FutarchyService, DaoTickerData } from '../src/services/futarchyService.js';
import { PriceService } from '../src/services/priceService.js';
import { setFutarchyService, setPriceService } from '../src/server.js';
import app from '../src/server.js';

// Mock DAO data
const mockBaseMint = new PublicKey('SoLo9oxzLDpcq1dpqAgMwgce5WqkRDtNXK7EPnbmeta');
const mockQuoteMint = new PublicKey('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');
const mockDaoAddress = new PublicKey('5FPGRzY9ArJFwY2Hp2y2eqMzVewyWCBox7esmpuZfCvE');

const mockDaoTickerData: DaoTickerData = {
  daoAddress: mockDaoAddress,
  baseMint: mockBaseMint,
  quoteMint: mockQuoteMint,
  baseDecimals: 9,
  quoteDecimals: 6,
  baseSymbol: 'TEST',
  baseName: 'Test Token',
  quoteSymbol: 'USDC',
  quoteName: 'USD Coin',
  poolData: {
    baseReserves: new BN('1000000000000'),
    quoteReserves: new BN('50000000000'),
    baseProtocolFees: new BN('100000000'),
    quoteProtocolFees: new BN('5000000'),
  },
};

// Mock services
const mockFutarchyService = {
  getAllDaos: jest.fn().mockResolvedValue([mockDaoTickerData]),
  getPoolData: jest.fn().mockResolvedValue({
    baseReserves: new BN('1000000000000'),
    quoteReserves: new BN('50000000000'),
    baseProtocolFees: new BN('100000000'),
    quoteProtocolFees: new BN('5000000'),
  }),
  getTotalLiquidity: jest.fn().mockResolvedValue(new BN('100000000000')),
} as unknown as FutarchyService;

const mockPriceService = {
  calculatePrice: jest.fn((baseReserves: BN, quoteReserves: BN, baseDecimals: number, quoteDecimals: number) => {
    return '0.05';
  }),
  calculateSpread: jest.fn((price: number) => ({
    bid: '0.04975',
    ask: '0.05025',
  })),
  calculateLiquidityUSD: jest.fn((quoteReserves: BN, decimals: number) => {
    return '100000.00';
  }),
  calculateVolumeFromFees: jest.fn((baseFees: BN, quoteFees: BN, baseDecimals: number, quoteDecimals: number, feeRate: number) => {
    return {
      baseVolume: '40.00000000',
      targetVolume: '2.00000000',
    };
  }),
  calculateOrderbookDepth: jest.fn((baseReserves: BN, quoteReserves: BN, depthLevels: number, baseDecimals: number) => ({
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
      expect(response.body.length).toBeGreaterThan(0);
      
      const ticker = response.body[0];
      expect(ticker).toHaveProperty('ticker_id');
      expect(ticker).toHaveProperty('base_currency');
      expect(ticker).toHaveProperty('target_currency');
      expect(ticker).toHaveProperty('base_symbol');
      expect(ticker).toHaveProperty('base_name');
      expect(ticker).toHaveProperty('target_symbol');
      expect(ticker).toHaveProperty('target_name');
      expect(ticker).toHaveProperty('pool_id');
      expect(ticker).toHaveProperty('last_price');
      expect(ticker).toHaveProperty('base_volume');
      expect(ticker).toHaveProperty('target_volume');
      expect(ticker).toHaveProperty('liquidity_in_usd');
      expect(ticker).toHaveProperty('bid');
      expect(ticker).toHaveProperty('ask');
      
      // Should not have high/low (removed)
      expect(ticker).not.toHaveProperty('high');
      expect(ticker).not.toHaveProperty('low');
    });

    it('should call getAllDaos', async () => {
      await request(app).get('/api/tickers');
      expect(mockFutarchyService.getAllDaos).toHaveBeenCalled();
    });

    it('should calculate volume from fees', async () => {
      await request(app).get('/api/tickers');
      expect(mockPriceService.calculateVolumeFromFees).toHaveBeenCalled();
    });
  });

  describe('GET /api/orderbook', () => {
    it('should return orderbook with valid ticker_id', async () => {
      const tickerId = `${mockBaseMint.toString()}_${mockQuoteMint.toString()}`;
      const response = await request(app)
        .get('/api/orderbook')
        .query({ ticker_id: tickerId });
      expect(response.status).toBe(200);
      expect(response.body).toHaveProperty('ticker_id', tickerId);
      expect(response.body).toHaveProperty('timestamp');
      expect(response.body).toHaveProperty('bids');
      expect(response.body).toHaveProperty('asks');
      expect(Array.isArray(response.body.bids)).toBe(true);
      expect(Array.isArray(response.body.asks)).toBe(true);
    });

    it('should return 400 without ticker_id', async () => {
      const response = await request(app).get('/api/orderbook');
      expect(response.status).toBe(400);
      expect(response.body).toHaveProperty('error');
    });

    it('should return 400 with invalid ticker_id format', async () => {
      const response = await request(app)
        .get('/api/orderbook')
        .query({ ticker_id: 'INVALID' });
      expect(response.status).toBe(400);
      expect(response.body).toHaveProperty('error');
    });

    it('should return 404 for non-existent ticker', async () => {
      const response = await request(app)
        .get('/api/orderbook')
        .query({ ticker_id: '11111111111111111111111111111111_22222222222222222222222222222222' });
      expect(response.status).toBe(404);
      expect(response.body).toHaveProperty('error');
    });

    it('should call getAllDaos to find the DAO', async () => {
      const tickerId = `${mockBaseMint.toString()}_${mockQuoteMint.toString()}`;
      await request(app)
        .get('/api/orderbook')
        .query({ ticker_id: tickerId });
      expect(mockFutarchyService.getAllDaos).toHaveBeenCalled();
    });
  });

  describe('GET /health', () => {
    it('should return health status', async () => {
      const response = await request(app).get('/health');
      expect(response.status).toBe(200);
      expect(response.body).toHaveProperty('status', 'healthy');
      expect(response.body).toHaveProperty('timestamp');
      expect(response.body).toHaveProperty('uptime');
    });
  });

  describe('GET /', () => {
    it('should return API information', async () => {
      const response = await request(app).get('/');
      expect(response.status).toBe(200);
      expect(response.body).toHaveProperty('name');
      expect(response.body).toHaveProperty('version');
      expect(response.body).toHaveProperty('endpoints');
    });
  });
});