import { describe, it, expect } from 'bun:test';
import BN from 'bn.js';
import { SolanaService } from '../../src/services/solanaService.js';

const MINT = 'SoLo9oxzLDpcq1dpqAgMwgce5WqkRDtNXK7EPnbmeta';

describe('SolanaService.getSupplyInfo', () => {
  it('reads the mint once and adds no cache entries as allocation balances change', async () => {
    const svc = new SolanaService();
    let mintReads = 0;
    (svc as any).withRetry = async () => { mintReads++; return { supply: 1_000_000_000n, decimals: 6 }; };
    const allocation = (ammAmount: number) => ({
      teamPerformancePackage: { amount: new BN(100_000_000) },
      futarchyAmmLiquidity: { amount: new BN(ammAmount) },
      meteoraLpLiquidity: { amount: new BN(0) },
    });

    // The AMM balance moves with every trade; each poll sees a new value.
    const results = [];
    for (const amm of [1, 2, 3, 4, 5]) results.push(await svc.getSupplyInfo(MINT, allocation(amm)));

    expect(mintReads).toBe(1);
    expect((svc as any).cache.size).toBe(1);
    expect(results.every((r) => r.totalSupply === '1000' && r.circulatingSupply === '900')).toBe(true);
  });
});
