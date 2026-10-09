import { describe, it, expect } from 'bun:test';
import BN from 'bn.js';
import { SolanaService } from '../../src/services/solanaService.js';

const MINT = 'SoLo9oxzLDpcq1dpqAgMwgce5WqkRDtNXK7EPnbmeta';

describe('SolanaService.getSupplyInfo', () => {
  it('reads the mint once and adds no cache entries as allocation balances change, while each result reflects its own balances', async () => {
    const svc = new SolanaService();
    let mintReads = 0;
    (svc as any).withRetry = async () => { mintReads++; return { supply: 1_000_000_000n, decimals: 6 }; };

    // Balances move between polls: the team package (subtracted) and the AMM
    // vault (reported, not subtracted) differ on every call.
    for (const n of [1, 2, 3, 4, 5]) {
      const result = await svc.getSupplyInfo(MINT, {
        teamPerformancePackage: { amount: new BN(n * 100_000_000) }, // n * 100 tokens
        futarchyAmmLiquidity: { amount: new BN(n * 1_000_000) },     // n tokens
        meteoraLpLiquidity: { amount: new BN(0) },
      });

      expect(result.totalSupply).toBe('1000');
      expect(result.circulatingSupply).toBe(String(1000 - n * 100));
      expect(result.allocation?.futarchyAmmLiquidity?.amount).toBe(String(n));
    }

    expect(mintReads).toBe(1);
    expect((svc as any).cache.size).toBe(1);
  });
});
