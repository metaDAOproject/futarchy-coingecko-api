import { describe, it, expect, afterEach, setSystemTime } from 'bun:test';
import BN from 'bn.js';
import { PublicKey } from '@solana/web3.js';
import { FutarchyService, type DaoTickerData } from '../../src/services/futarchyService.js';
import { config } from '../../src/config.js';

const DAO = new PublicKey('5FPGRzY9ArJFwY2Hp2y2eqMzVewyWCBox7esmpuZfCvE');
const BASE = new PublicKey('SoLo9oxzLDpcq1dpqAgMwgce5WqkRDtNXK7EPnbmeta');
const USDC = new PublicKey('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');

afterEach(() => setSystemTime());

describe('FutarchyService.getAllDaos', () => {
  it('serves DAOs whose pool reserves exceed 2^53', async () => {
    const svc = new FutarchyService();
    (svc as any).client = {
      futarchy: {
        account: {
          dao: {
            all: async () => [{
              publicKey: DAO,
              account: {
                baseMint: BASE,
                quoteMint: USDC,
                amm: { state: { futarchy: { spot: { pool: {
                  baseReserves: new BN('20000000000000000'), // > 2^53
                  quoteReserves: new BN('1000000000000'),
                } } } } },
              },
            }],
          },
        },
      },
    };
    (svc as any).connection = { getMultipleAccountsInfo: async (keys: unknown[]) => keys.map(() => null) };
    (svc as any).getTokenDecimals = async (mint: PublicKey) => (mint.equals(USDC) ? 6 : 9);
    (svc as any).getTokenMetadata = async () => null;

    const daos = await svc.getAllDaos();

    expect(daos.map((d) => d.daoAddress.toBase58())).toEqual([DAO.toBase58()]);
    expect(daos[0]!.poolData.baseReserves.toString()).toBe('20000000000000000');
  });

  it('serves the previous snapshot while refreshing, up to the max-stale cap', async () => {
    const now = Date.now();
    setSystemTime(new Date(now));
    const svc = new FutarchyService();
    let scans = 0;
    let fail = false;
    (svc as any).fetchAllDaos = async () => {
      scans++;
      if (fail) throw new Error('RPC down');
      return [{ daoAddress: DAO, scan: scans } as unknown as DaoTickerData];
    };
    const scanOf = (daos: DaoTickerData[]) => (daos[0] as any).scan;

    expect(scanOf(await svc.getAllDaos())).toBe(1);

    // Past the TTL: the old snapshot is returned immediately; a refresh runs behind it.
    setSystemTime(new Date(now + config.cache.tickersTTL + 1));
    expect(scanOf(await svc.getAllDaos())).toBe(1);
    await Bun.sleep(0);
    expect(scans).toBe(2);
    expect(scanOf(await svc.getAllDaos())).toBe(2);

    // RPC down: keep serving the last good snapshot while it's within the cap...
    fail = true;
    const refreshedAt = now + config.cache.tickersTTL + 1;
    setSystemTime(new Date(refreshedAt + config.cache.tickersTTL + 1));
    expect(scanOf(await svc.getAllDaos())).toBe(2);

    // ...and fail once it's older than the cap, rather than serve stale prices.
    setSystemTime(new Date(refreshedAt + config.cache.tickersMaxStale + 1));
    await expect(svc.getAllDaos()).rejects.toThrow('RPC down');
  });
});
