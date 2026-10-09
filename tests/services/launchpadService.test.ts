/**
 * Regression tests for the getTokenAllocationBreakdown error contract.
 *
 * An empty breakdown (zero locked allocations) is ONLY correct when the token
 * genuinely has no launch / an incomplete launch. An infrastructure failure
 * must REJECT — a swallowed error here would zero out every locked allocation
 * and serve circulating supply = total supply during any RPC blip (the exact
 * bug this suite pins down).
 */

import { describe, it, expect } from 'bun:test';
import { PublicKey } from '@solana/web3.js';
import BN from 'bn.js';
import { LaunchpadService } from '../../src/services/launchpadService.js';

const MINT = new PublicKey('SoLo9oxzLDpcq1dpqAgMwgce5WqkRDtNXK7EPnbmeta');

describe('LaunchpadService.getTokenAllocationBreakdown', () => {
  it('propagates infrastructure failures instead of returning an empty breakdown', async () => {
    const svc = new LaunchpadService();
    (svc as any).getLaunchByBaseMint = async () => {
      throw new Error('RPC connection refused');
    };

    await expect(svc.getTokenAllocationBreakdown(MINT)).rejects.toThrow('RPC connection refused');
  });

  it('returns an empty breakdown when the token genuinely has no launch', async () => {
    const svc = new LaunchpadService();
    (svc as any).getLaunchByBaseMint = async () => null;

    const breakdown = await svc.getTokenAllocationBreakdown(MINT);

    expect(breakdown.teamPerformancePackage.amount.isZero()).toBe(true);
    expect(breakdown.futarchyAmmLiquidity.amount.isZero()).toBe(true);
    expect(breakdown.meteoraLpLiquidity.amount.isZero()).toBe(true);
    expect(breakdown.totalNonCirculating.isZero()).toBe(true);
  });

  it('returns an empty breakdown (with launch metadata) for an incomplete launch', async () => {
    const svc = new LaunchpadService();
    const launchAddress = new PublicKey('5FPGRzY9ArJFwY2Hp2y2eqMzVewyWCBox7esmpuZfCvE');
    (svc as any).getLaunchByBaseMint = async () => ({
      launchAddress,
      baseMint: MINT,
      version: 'v0.7',
      dao: undefined,
    });

    const breakdown = await svc.getTokenAllocationBreakdown(MINT);

    expect(breakdown.version).toBe('v0.7');
    expect(breakdown.launchAddress?.equals(launchAddress)).toBe(true);
    expect(breakdown.totalNonCirculating.isZero()).toBe(true);
  });
});

describe('LaunchpadService.getLiveLaunches', () => {
  const LAUNCH = new PublicKey('5FPGRzY9ArJFwY2Hp2y2eqMzVewyWCBox7esmpuZfCvE');
  const USDC = new PublicKey('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');

  function fakeProgram(launches: any[], fundingRecords: any[] = [], calls = { count: 0 }) {
    return {
      account: {
        launch: { all: async () => { calls.count++; return launches; } },
        fundingRecord: { all: async () => fundingRecords },
      },
    };
  }

  function serviceWith(programs: { v06?: any; v07?: any; v08?: any }) {
    const svc = new LaunchpadService();
    (svc as any).clientV06 = { launchpad: programs.v06 ?? fakeProgram([]) };
    (svc as any).clientV07 = { launchpad: programs.v07 ?? fakeProgram([]) };
    (svc as any).clientV08 = { launchpad: programs.v08 ?? fakeProgram([]) };
    (svc as any).getMintDecimals = async () => 6;
    return svc;
  }

  const liveLaunch = {
    publicKey: LAUNCH,
    account: {
      state: { live: {} },
      baseMint: MINT,
      quoteMint: USDC,
      minimumRaiseAmount: new BN('500000000000'),
      unixTimestampStarted: new BN(1_760_000_000),
      secondsForLaunch: 4 * 24 * 60 * 60,
    },
  };

  it('aggregates funding records for live launches only', async () => {
    const completed = { ...liveLaunch, account: { ...liveLaunch.account, state: { complete: {} } } };
    const svc = serviceWith({
      v07: fakeProgram([liveLaunch, completed], [
        { account: { committedAmount: new BN('1500000') } },
        { account: { committedAmount: new BN('250000000') } },
        { account: { committedAmount: new BN(0) } },
      ]),
    });

    const { launches } = await svc.getLiveLaunches();

    expect(launches).toEqual([{
      launchAddress: LAUNCH.toBase58(),
      version: 'v0.7',
      baseMint: MINT.toBase58(),
      quoteMint: USDC.toBase58(),
      quoteDecimals: 6,
      committerCount: 2,
      totalCommitted: '251.5',
      totalCommittedRaw: '251500000',
      minimumRaise: '500000',
      minimumRaiseRaw: '500000000000',
      closeTime: 1_760_000_000 + 4 * 24 * 60 * 60,
    }]);
  });

  it('serves the cached snapshot within the TTL', async () => {
    const calls = { count: 0 };
    const svc = serviceWith({ v08: fakeProgram([liveLaunch], [], calls) });

    const first = await svc.getLiveLaunches();
    const second = await svc.getLiveLaunches();

    expect(second).toBe(first);
    expect(calls.count).toBe(1);
  });

  it('propagates RPC failures instead of serving an empty list, and does not cache them', async () => {
    let fail = true;
    const svc = serviceWith({
      v06: {
        account: {
          launch: { all: async () => { if (fail) throw new Error('RPC connection refused'); return []; } },
          fundingRecord: { all: async () => [] },
        },
      },
    });

    await expect(svc.getLiveLaunches()).rejects.toThrow('RPC connection refused');
    fail = false;
    expect((await svc.getLiveLaunches()).launches).toEqual([]);
  });
});
