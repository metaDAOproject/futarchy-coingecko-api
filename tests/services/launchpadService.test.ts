/**
 * Regression tests for the getTokenAllocationBreakdown error contract.
 *
 * An empty breakdown (zero locked allocations) is ONLY correct when the token
 * genuinely has no launch / an incomplete launch. An infrastructure failure
 * must REJECT — a swallowed error here would zero out every locked allocation
 * and serve circulating supply = total supply during any RPC blip (the exact
 * bug this suite pins down).
 */

import { describe, it, expect, setSystemTime, afterEach } from 'bun:test';
import { Keypair, PublicKey } from '@solana/web3.js';
import { ACCOUNT_SIZE, AccountLayout, TOKEN_PROGRAM_ID, getAssociatedTokenAddressSync } from '@solana/spl-token';
import BN from 'bn.js';
import { LaunchpadService } from '../../src/services/launchpadService.js';
import { config } from '../../src/config.js';

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

  function fakeProgram(launches: any[], fundingRecords: any[] = [], calls = { launch: 0, fundingRecord: 0 }) {
    return {
      account: {
        launch: { all: async () => { calls.launch++; return launches; } },
        fundingRecord: { all: async () => { calls.fundingRecord++; return fundingRecords; } },
      },
    };
  }

  afterEach(() => setSystemTime());

  function serviceWith(programs: { v06?: any; v07?: any; v08?: any }) {
    const svc = new LaunchpadService();
    (svc as any).clientV06 = { launchpad: programs.v06 ?? fakeProgram([]) };
    (svc as any).clientV07 = { launchpad: programs.v07 ?? fakeProgram([]) };
    (svc as any).clientV08 = { launchpad: programs.v08 ?? fakeProgram([]) };
    (svc as any).getMintDecimals = async () => 6;
    return svc;
  }

  const STARTED = Math.floor(Date.now() / 1000) - 60 * 60;
  const DURATION = 4 * 24 * 60 * 60;
  const liveLaunch = {
    publicKey: LAUNCH,
    account: {
      state: { live: {} },
      baseMint: MINT,
      quoteMint: USDC,
      minimumRaiseAmount: new BN('500000000000'),
      unixTimestampStarted: new BN(STARTED),
      secondsForLaunch: DURATION,
    },
  };

  it('aggregates funding records for open live launches only', async () => {
    const completed = { ...liveLaunch, account: { ...liveLaunch.account, state: { complete: {} } } };
    // Still `live` on-chain, but its close time passed and nobody called closeLaunch.
    const pastClose = {
      ...liveLaunch,
      account: { ...liveLaunch.account, unixTimestampStarted: new BN(STARTED - 2 * DURATION) },
    };
    const calls = { launch: 0, fundingRecord: 0 };
    const svc = serviceWith({
      v07: fakeProgram([liveLaunch, completed, pastClose], [
        { account: { committedAmount: new BN('1500000') } },
        { account: { committedAmount: new BN('250000000') } },
        { account: { committedAmount: new BN(0) } },
      ], calls),
    });

    const { launches } = await svc.getLiveLaunches();

    expect(launches).toEqual([{
      launchAddress: LAUNCH.toBase58(),
      version: 'v0.7',
      tokenAddress: MINT.toBase58(),
      quoteMint: USDC.toBase58(),
      quoteDecimals: 6,
      committerCount: 2,
      totalCommitted: '251.5',
      totalCommittedRaw: '251500000',
      minimumRaise: '500000',
      minimumRaiseRaw: '500000000000',
      closeTime: STARTED + DURATION,
    }]);
    // Funding records are read for the open launch only, not the expired one.
    expect(calls.fundingRecord).toBe(1);
  });

  it('shares one scan per TTL window and drops launches the moment they close', async () => {
    const now = Date.now();
    setSystemTime(new Date(now));
    const closingSoon = {
      ...liveLaunch,
      account: {
        ...liveLaunch.account,
        unixTimestampStarted: new BN(Math.floor(now / 1000) - DURATION + 60),
      },
    };
    const calls = { launch: 0, fundingRecord: 0 };
    const svc = serviceWith({ v08: fakeProgram([closingSoon], [], calls) });

    // Concurrent callers share the in-flight scan.
    const [a, b] = await Promise.all([svc.getLiveLaunches(), svc.getLiveLaunches()]);
    expect(a.launches).toHaveLength(1);
    expect(b).toEqual(a);
    expect(calls.launch).toBe(1);

    // Within the TTL but past the close time: served from cache, launch filtered out.
    setSystemTime(new Date(now + 120_000));
    expect((await svc.getLiveLaunches()).launches).toEqual([]);
    expect(calls.launch).toBe(1);

    // After the TTL: rescanned.
    setSystemTime(new Date(now + config.cache.liveLaunchesTTL + 1));
    await svc.getLiveLaunches();
    expect(calls.launch).toBe(2);
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

describe('LaunchpadService batched allocation reads', () => {
  const USDC = new PublicKey('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');
  const DAO = Keypair.generate().publicKey;
  const AMM_VAULT = Keypair.generate().publicKey;
  const SQUADS_VAULT = Keypair.generate().publicKey;
  const LAUNCH = new PublicKey('5FPGRzY9ArJFwY2Hp2y2eqMzVewyWCBox7esmpuZfCvE');

  const tokenAccount = (owner: PublicKey, amount: bigint) => {
    const data = Buffer.alloc(ACCOUNT_SIZE);
    AccountLayout.encode({
      mint: MINT, owner, amount, delegateOption: 0, delegate: PublicKey.default, state: 1,
      isNativeOption: 0, isNative: 0n, delegatedAmount: 0n, closeAuthorityOption: 0, closeAuthority: PublicKey.default,
    }, data);
    return { data, owner: TOKEN_PROGRAM_ID, lamports: 1, executable: false };
  };

  function completedLaunchService(balances: (keys: PublicKey[]) => Promise<unknown[]>) {
    const svc = new LaunchpadService();
    const calls = { daoFetches: 0, balanceReads: 0 };
    (svc as any).getLaunchByBaseMint = async () => ({
      launchAddress: LAUNCH, baseMint: MINT, version: 'v0.7', dao: DAO,
      performancePackageTokenAmount: new BN(0),
    });
    (svc as any).futarchyClient = {
      fetchDao: async () => { calls.daoFetches++; return { quoteMint: USDC, amm: { ammBaseVault: AMM_VAULT }, squadsMultisigVault: SQUADS_VAULT }; },
    };
    (svc as any).connection = {
      getMultipleAccountsInfo: async (keys: PublicKey[]) => { calls.balanceReads++; return balances(keys); },
    };
    return { svc, calls };
  }

  it('reads every balance in one call, fetches the DAO once, and treats absent accounts as 0', async () => {
    const treasuryAta = getAssociatedTokenAddressSync(MINT, SQUADS_VAULT, true);
    const { svc, calls } = completedLaunchService(async (keys) => keys.map((key) => {
      if (key.equals(AMM_VAULT)) return tokenAccount(AMM_VAULT, 5_000n);
      if (key.equals(treasuryAta)) return tokenAccount(SQUADS_VAULT, 7_000n);
      return null; // performance package and Meteora vault absent
    }));

    const breakdown = await svc.getTokenAllocationBreakdown(MINT);

    expect(calls).toEqual({ daoFetches: 1, balanceReads: 1 });
    expect(breakdown.futarchyAmmLiquidity.amount.toString()).toBe('5000');
    expect(breakdown.futarchyAmmLiquidity.vaultAddress?.equals(AMM_VAULT)).toBe(true);
    expect(breakdown.daoTreasuryTokens.amount.toString()).toBe('7000');
    expect(breakdown.daoTreasuryTokens.vaultAddress?.equals(SQUADS_VAULT)).toBe(true);
    // Absent accounts: 0, and (as before) no pool/vault address reported for them.
    expect(breakdown.teamPerformancePackage.amount.isZero()).toBe(true);
    expect(breakdown.teamPerformancePackage.address).toBeDefined();
    expect(breakdown.meteoraLpLiquidity).toEqual({ amount: new BN(0) });
    expect(breakdown.totalNonCirculating.toString()).toBe('7000');
  });

  it('rejects instead of reading zeros when the balance read fails', async () => {
    const { svc } = completedLaunchService(async () => { throw new Error('RPC connection refused'); });

    await expect(svc.getTokenAllocationBreakdown(MINT)).rejects.toThrow('RPC connection refused');
  });

  it('shares one load between concurrent requests for a mint', async () => {
    const { svc, calls } = completedLaunchService(async (keys) => keys.map(() => null));

    await Promise.all([1, 2, 3, 4, 5].map(() => svc.getTokenAllocationBreakdown(MINT)));

    expect(calls).toEqual({ daoFetches: 1, balanceReads: 1 });
  });

  it('reads both launch program versions in one call and caches "no launch"', async () => {
    const svc = new LaunchpadService();
    let reads = 0;
    (svc as any).connection = {
      getMultipleAccountsInfo: async (keys: PublicKey[]) => { reads++; expect(keys).toHaveLength(2); return [null, null]; },
    };

    expect(await svc.getLaunchByBaseMint(MINT)).toBeNull();
    expect(await svc.getLaunchByBaseMint(MINT)).toBeNull();
    expect(reads).toBe(1);
  });
});
