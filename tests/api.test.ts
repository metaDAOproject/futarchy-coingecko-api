import request from 'supertest';
import BN from 'bn.js';
import { PublicKey } from '@solana/web3.js';
import { createApp, type Services } from '../src/app.js';
import type { FutarchyService, DaoTickerData } from '../src/services/futarchyService.js';
import type { PriceService } from '../src/services/priceService.js';
import type { DatabaseService } from '../src/services/databaseService.js';

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
  calculatePrice: jest.fn(() => '0.05'),
  calculateSpread: jest.fn(() => ({
    bid: '0.04975',
    ask: '0.05025',
  })),
  calculateLiquidityUSD: jest.fn(() => '100000.00'),
  calculateVolumeFromFees: jest.fn(() => ({
    baseVolume: '40.00000000',
    targetVolume: '2.00000000',
  })),
} as unknown as PriceService;

const mockDatabaseService = {
  isAvailable: jest.fn().mockReturnValue(true),
  getFirstTradeDates: jest.fn().mockResolvedValue(new Map()),
} as unknown as DatabaseService;

function createMockServices(): Services {
  return {
    futarchyService: mockFutarchyService,
    priceService: mockPriceService,
    databaseService: mockDatabaseService,
    duneService: null,
    duneCacheService: null,
    solanaService: undefined,
    launchpadService: undefined,
    hourlyAggregationService: null,
    tenMinuteVolumeFetcherService: null,
    dailyAggregationService: null,
    meteoraVolumeFetcherService: null,
  };
}

describe('CoinGecko API', () => {
  const app = createApp({ services: createMockServices() });

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

    it('should return zero volume when no metrics available', async () => {
      const response = await request(app).get('/api/tickers');
      expect(response.status).toBe(200);
      if (response.body.length > 0) {
        expect(response.body[0].base_volume).toBe('0');
        expect(response.body[0].target_volume).toBe('0');
      }
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
