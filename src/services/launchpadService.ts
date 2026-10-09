import { Connection, PublicKey, Keypair, type AccountInfo } from '@solana/web3.js';
import { AnchorProvider, Wallet } from '@coral-xyz/anchor';
import {
  LaunchpadClient as LaunchpadClientV06,
  getLaunchSignerAddr,
} from "@metadaoproject/programs/launchpad/v0.6";
import {
  LaunchpadClient as LaunchpadClientV07,
} from "@metadaoproject/programs/launchpad/v0.7";
import {
  LaunchpadClient as LaunchpadClientV08,
} from "@metadaoproject/programs/launchpad/v0.8";
import { FutarchyClient } from "@metadaoproject/programs/futarchy/v0.6";
import { getPerformancePackageAddr } from "@metadaoproject/programs/price_based_performance_package/v0.6";
import {
  PRICE_BASED_PERFORMANCE_PACKAGE_PROGRAM_ID,
  DAMM_V2_PROGRAM_ID,
  LAUNCHPAD_V0_6_MAINNET_METEORA_CONFIG as MAINNET_METEORA_CONFIG_V06,
  LAUNCHPAD_V0_7_MAINNET_METEORA_CONFIG as MAINNET_METEORA_CONFIG_V07,
} from "@metadaoproject/programs";
import { getAssociatedTokenAddressSync, getMint, unpackAccount } from '@solana/spl-token';
import { isTokenAccountAbsent } from '../utils/solanaErrors.js';
import { config } from '../config.js';
import { createSolanaConnection } from '../utils/solanaConnection.js';
import { TtlCache } from '../utils/ttlCache.js';
import BN from 'bn.js';
import { logger } from '../utils/logger.js';

// Launchpad version detection
export type LaunchpadVersion = 'v0.6' | 'v0.7';

/**
 * Additional token recipient allocation (v0.7+ only)
 */
export interface AdditionalTokenAllocation {
  recipient: PublicKey;
  amount: BN;
  claimed: boolean;
  tokenAccountAddress?: PublicKey;
}

/**
 * Complete token allocation breakdown for launchpad tokens
 */
export interface TokenAllocationBreakdown {
  // Launchpad version used
  version: LaunchpadVersion;
  // Team Performance Package - locked tokens for the team
  teamPerformancePackage: {
    amount: BN;
    address?: PublicKey;
  };
  // FutarchyAMM Liquidity - tokens in the internal Futarchy AMM for spot trading
  futarchyAmmLiquidity: {
    amount: BN;
    vaultAddress?: PublicKey;
  };
  // Meteora LP Position - tokens in the external Meteora DAMM pool
  meteoraLpLiquidity: {
    amount: BN;
    poolAddress?: PublicKey;
    vaultAddress?: PublicKey;
  };
  // Additional token recipient (v0.7+ only) - not in circulating supply
  additionalTokenAllocation?: AdditionalTokenAllocation;
  // DAO treasury tokens - base tokens held in the DAO's squads vault (not circulating)
  daoTreasuryTokens: {
    amount: BN;
    vaultAddress?: PublicKey;
  };
  // DAO address (if launch completed)
  daoAddress?: PublicKey;
  // Launch address
  launchAddress?: PublicKey;
  // Total non-circulating supply (performance package + additional tokens if unclaimed + DAO treasury)
  totalNonCirculating: BN;
}

export interface LaunchData {
  launchAddress: PublicKey;
  baseMint: PublicKey;
  performancePackageGrantee: PublicKey;
  performancePackageTokenAmount: BN;
  state: LaunchState;
  dao?: PublicKey;
  // v0.7+ fields
  version: LaunchpadVersion;
  additionalTokensAmount?: BN;
  additionalTokensRecipient?: PublicKey;
  additionalTokensClaimed?: boolean;
}

/**
 * A launch that is currently accepting commitments (on-chain state `live`).
 * Amounts are in the quote mint (usually USDC); `*Raw` fields are base units.
 */
export interface LiveLaunch {
  launchAddress: string;
  version: LaunchpadVersion | 'v0.8';
  /** Mint of the token being launched (the launch's `baseMint`). */
  tokenAddress: string;
  quoteMint: string;
  quoteDecimals: number;
  /** Number of funding records with a non-zero commitment (one record per funder). */
  committerCount: number;
  /** Sum of `committedAmount` across the launch's funding records. */
  totalCommitted: string;
  totalCommittedRaw: string;
  /** On-chain `minimumRaiseAmount`. */
  minimumRaise: string;
  minimumRaiseRaw: string;
  /** Unix seconds: `unixTimestampStarted + secondsForLaunch`. */
  closeTime: number;
}

export interface LiveLaunchesSnapshot {
  updatedAt: string;
  launches: LiveLaunch[];
}

// FundingRecord layout: 8-byte discriminator, pdaBump (u8), funder (Pubkey),
// then launch (Pubkey) — identical in launchpad v0.6, v0.7 and v0.8.
const FUNDING_RECORD_LAUNCH_OFFSET = 8 + 1 + 32;

function formatUnits(raw: BN, decimals: number): string {
  if (decimals === 0) return raw.toString();
  const digits = raw.toString().padStart(decimals + 1, '0');
  const whole = digits.slice(0, -decimals);
  const fraction = digits.slice(-decimals).replace(/0+$/, '');
  return fraction ? `${whole}.${fraction}` : whole;
}

/**
 * Balance of an already-fetched SPL token account, with the same semantics
 * as `getAccount`: null when the account is genuinely absent (missing, or
 * not owned by the token program) — a 0 balance is then correct. A
 * malformed account throws.
 */
function tokenBalance(address: PublicKey, info: AccountInfo<Buffer> | null): BN | null {
  try {
    return new BN(unpackAccount(address, info).amount.toString());
  } catch (error) {
    if (isTokenAccountAbsent(error)) return null;
    throw error;
  }
}

export type LaunchState =
  | { initialized: Record<string, never> }
  | { active: Record<string, never> }
  | { closed: Record<string, never> }
  | { completed: Record<string, never> }
  | { cancelled: Record<string, never> };

export class LaunchpadService {
  private connection: Connection;
  private clientV06: LaunchpadClientV06;
  private clientV07: LaunchpadClientV07;
  private clientV08: LaunchpadClientV08;
  private futarchyClient: FutarchyClient;
  // Keyed by caller-supplied mints, so bounded.
  private cache = new TtlCache(10_000);
  private liveLaunches: { snapshot: LiveLaunchesSnapshot; fetchedAt: number } | null = null;
  private liveLaunchesInFlight: Promise<LiveLaunchesSnapshot> | null = null;
  private mintDecimals = new Map<string, Promise<number>>();

  constructor() {
    this.connection = createSolanaConnection();
    
    // Create a dummy wallet for read-only operations
    let wallet: Wallet;
    try {
      wallet = Wallet.local();
    } catch (error) {
      // If ANCHOR_WALLET is not set, create a dummy wallet for read-only operations
      const dummyKeypair = Keypair.generate();
      wallet = new Wallet(dummyKeypair);
    }
    
    const provider = new AnchorProvider(this.connection, wallet, {
      commitment: 'confirmed',
    });
    this.clientV06 = LaunchpadClientV06.createClient({ provider });
    this.clientV07 = LaunchpadClientV07.createClient({ provider });
    this.clientV08 = LaunchpadClientV08.createClient({ provider });
    this.futarchyClient = FutarchyClient.createClient({ provider });
  }


  /**
   * Get the Launch PDA address for a given base mint (v0.6 program)
   */
  getLaunchAddressV06(baseMint: PublicKey): PublicKey {
    return this.clientV06.getLaunchAddress({ baseMint });
  }

  /**
   * Get the Launch PDA address for a given base mint (v0.7 program)
   */
  getLaunchAddressV07(baseMint: PublicKey): PublicKey {
    return this.clientV07.getLaunchAddress({ baseMint });
  }


  /**
   * The launch for a token's base mint, from the v0.7 or v0.6 launchpad (v0.7,
   * the newer program, wins if both exist). Both launch PDAs are read in one
   * RPC call. "No launch" is cached like a found launch; an RPC failure
   * throws and is not cached — a silent null would make the allocation
   * breakdown treat a launched token as un-launched (zero locked allocations,
   * circulating = total supply).
   */
  getLaunchByBaseMint(baseMint: PublicKey): Promise<LaunchData | null> {
    return this.cache.getOrLoad(`launch_by_mint_${baseMint.toString()}`, config.cache.tickersTTL * 10, async () => {
      const launchAddressV07 = this.getLaunchAddressV07(baseMint);
      const launchAddressV06 = this.getLaunchAddressV06(baseMint);
      const [infoV07, infoV06] = await this.connection.getMultipleAccountsInfo([launchAddressV07, launchAddressV06]);

      let found: { launchAccount: any; version: LaunchpadVersion; launchAddress: PublicKey } | null = null;
      if (infoV07) {
        found = { launchAccount: await this.clientV07.deserializeLaunch(infoV07), version: 'v0.7', launchAddress: launchAddressV07 };
      } else if (infoV06) {
        found = { launchAccount: await this.clientV06.deserializeLaunch(infoV06), version: 'v0.6', launchAddress: launchAddressV06 };
      }
      if (!found) {
        logger.debug(`[Launchpad] No launch found for mint ${baseMint.toString()}`);
        return null;
      }

      const { launchAccount, version, launchAddress } = found;
      logger.debug(`[Launchpad] Found ${version} launch ${launchAddress.toString()} for mint ${baseMint.toString()}`);
      const launchData: LaunchData = {
        launchAddress,
        baseMint: launchAccount.baseMint,
        performancePackageGrantee: launchAccount.performancePackageGrantee,
        performancePackageTokenAmount: new BN(launchAccount.performancePackageTokenAmount.toString()),
        state: launchAccount.state as LaunchState,
        dao: launchAccount.dao || undefined,
        version,
        // v0.7 specific fields
        additionalTokensAmount: launchAccount.additionalTokensAmount
          ? new BN(launchAccount.additionalTokensAmount.toString())
          : undefined,
        additionalTokensRecipient: launchAccount.additionalTokensRecipient || undefined,
        additionalTokensClaimed: launchAccount.additionalTokensClaimed || undefined,
      };
      return launchData;
    });
  }

  /**
   * Derive the performance package address for a given launch (v0.6 style).
   * The createKey used during completeLaunch is the launch signer.
   */
  getPerformancePackageAddressV06(launchAddress: PublicKey): PublicKey {
    const [launchSigner] = getLaunchSignerAddr(
      this.clientV06.getProgramId(),
      launchAddress
    );
    const [performancePackageAddress] = getPerformancePackageAddr({
      programId: PRICE_BASED_PERFORMANCE_PACKAGE_PROGRAM_ID,
      createKey: launchSigner,
    });
    return performancePackageAddress;
  }

  /**
   * Derive the performance package address for a given launch (v0.7 style).
   * Uses the launch-specific PDA derivation.
   */
  getPerformancePackageAddressV07(launchAddress: PublicKey): PublicKey {
    return this.clientV07.getLaunchPerformancePackageAddress({ launch: launchAddress });
  }

  /**
   * Get performance package address for a launch, detecting version automatically.
   */
  getPerformancePackageAddress(launchAddress: PublicKey, version: LaunchpadVersion = 'v0.6'): PublicKey {
    if (version === 'v0.7') {
      return this.getPerformancePackageAddressV07(launchAddress);
    }
    return this.getPerformancePackageAddressV06(launchAddress);
  }

  /**
   * Get the appropriate Meteora config for a given launchpad version.
   * v0.6 and v0.7 use different Meteora configs.
   */
  getMeteoraConfig(version: LaunchpadVersion): PublicKey {
    return version === 'v0.7' ? MAINNET_METEORA_CONFIG_V07 : MAINNET_METEORA_CONFIG_V06;
  }

  /**
   * Derive the Meteora DAMM v2 pool address for a token pair.
   * Seeds: ["pool", config, larger_mint, smaller_mint]
   * Token order: DESCENDING (larger first, smaller second) - per SDK's getFirstKey/getSecondKey
   * 
   * @param baseMint - The base token mint
   * @param quoteMint - The quote token mint
   * @param version - The launchpad version (determines which Meteora config to use)
   */
  getMeteoraPoolAddress(baseMint: PublicKey, quoteMint: PublicKey, version: LaunchpadVersion = 'v0.6'): PublicKey {
    // Sort mints - Meteora uses DESCENDING order (larger first, smaller second)
    const buf1 = baseMint.toBuffer();
    const buf2 = quoteMint.toBuffer();
    const comparison = Buffer.compare(buf1, buf2);
    
    // getFirstKey: if buf1 > buf2, return buf1, else return buf2 (the larger one)
    // getSecondKey: if buf1 > buf2, return buf2, else return buf1 (the smaller one)
    const firstKey = comparison === 1 ? baseMint : quoteMint;
    const secondKey = comparison === 1 ? quoteMint : baseMint;
    
    const meteoraConfig = this.getMeteoraConfig(version);
    
    const [poolAddress] = PublicKey.findProgramAddressSync(
      [
        Buffer.from("pool"),
        meteoraConfig.toBuffer(),
        firstKey.toBuffer(),
        secondKey.toBuffer(),
      ],
      DAMM_V2_PROGRAM_ID
    );
    return poolAddress;
  }

  /**
   * Get the Meteora DAMM v2 pool's token vault for a given mint.
   * Seeds: ["token_vault", tokenMint, pool] - per SDK's derivation
   */
  getMeteoraPoolVault(poolAddress: PublicKey, tokenMint: PublicKey): PublicKey {
    // Meteora DAMM v2 vault PDA derivation - note: tokenMint comes BEFORE pool
    const [vaultAddress] = PublicKey.findProgramAddressSync(
      [
        Buffer.from("token_vault"),
        tokenMint.toBuffer(),
        poolAddress.toBuffer(),
      ],
      DAMM_V2_PROGRAM_ID
    );
    return vaultAddress;
  }

  /**
   * Get the complete token allocation breakdown for a launchpad token.
   * This provides a complete picture of where all tokens are allocated:
   * - Team Performance Package (locked)
   * - FutarchyAMM Liquidity (internal AMM)
   * - Meteora LP Liquidity (external DEX)
   * - Additional Token Allocation (v0.7+ only, not in circulating supply until claimed)
   *
   * Circulating Supply = Total - Team - FutarchyAMM - Meteora - AdditionalTokens (if unclaimed)
   *
   * Cached per mint; concurrent requests for one mint share a single load.
   */
  getTokenAllocationBreakdown(baseMint: PublicKey): Promise<TokenAllocationBreakdown> {
    return this.cache.getOrLoad(
      `allocation_${baseMint.toString()}`,
      config.cache.tickersTTL * 5,
      () => this.loadTokenAllocationBreakdown(baseMint),
    );
  }

  private async loadTokenAllocationBreakdown(baseMint: PublicKey): Promise<TokenAllocationBreakdown> {
    const emptyBreakdown: TokenAllocationBreakdown = {
      version: 'v0.6',
      teamPerformancePackage: { amount: new BN(0) },
      futarchyAmmLiquidity: { amount: new BN(0) },
      meteoraLpLiquidity: { amount: new BN(0) },
      daoTreasuryTokens: { amount: new BN(0) },
      totalNonCirculating: new BN(0),
    };

    // No catch-all below: an empty breakdown is returned ONLY for the two
    // genuinely-empty cases (token never launched / launch not completed).
    // Every infrastructure failure (RPC, network, timeout) propagates to the
    // caller — swallowing it here would zero out every locked allocation and
    // serve circulating supply = total supply on a transient outage, which is
    // the exact mispricing the isTokenAccountAbsent contract exists to prevent.
    const launch = await this.getLaunchByBaseMint(baseMint);
    if (!launch) {
      // Token was not launched via launchpad
      return emptyBreakdown;
    }

    if (!launch.dao) {
      // Launch not yet completed
      return {
        ...emptyBreakdown,
        version: launch.version,
        launchAddress: launch.launchAddress,
      };
    }

    // The DAO gives the quote mint, the AMM base vault and the treasury vault.
    const dao = await this.futarchyClient.fetchDao(launch.dao);
    const quoteMint: PublicKey | undefined = dao?.quoteMint;

    // Every token account whose live balance feeds the breakdown. We use live
    // balances rather than configured amounts because tokens may have been
    // unlocked/claimed (e.g. ZKFG, Loyal), making configured amounts stale.
    const performancePackageAddress = this.getPerformancePackageAddress(launch.launchAddress, launch.version);
    const performancePackageAta = getAssociatedTokenAddressSync(baseMint, performancePackageAddress, true);
    const ammBaseVault: PublicKey | undefined = dao?.amm.ammBaseVault;
    const meteoraPool = quoteMint ? this.getMeteoraPoolAddress(baseMint, quoteMint, launch.version) : undefined;
    const meteoraVault = meteoraPool ? this.getMeteoraPoolVault(meteoraPool, baseMint) : undefined;
    const treasuryVault = dao && (dao as any).squadsMultisigVault
      ? new PublicKey((dao as any).squadsMultisigVault)
      : undefined;
    const treasuryAta = treasuryVault ? getAssociatedTokenAddressSync(baseMint, treasuryVault, true) : undefined;

    // One read for all balances. An RPC failure throws (→ 5xx); only a
    // genuinely absent account reads as 0 (see tokenBalance).
    const keys = [performancePackageAta, ammBaseVault, meteoraVault, treasuryAta].filter(
      (key): key is PublicKey => key !== undefined
    );
    const infos = await this.connection.getMultipleAccountsInfo(keys);
    const balanceOf = (key: PublicKey | undefined): BN | null =>
      key ? tokenBalance(key, infos[keys.indexOf(key)] ?? null) : null;

    const performancePackageLockedAmount = balanceOf(performancePackageAta) ?? new BN(0);

    const ammAmount = balanceOf(ammBaseVault);
    const futarchyAmm = ammAmount ? { amount: ammAmount, vaultAddress: ammBaseVault } : { amount: new BN(0) };

    const meteoraAmount = balanceOf(meteoraVault);
    const meteoraLp: { amount: BN; poolAddress?: PublicKey; vaultAddress?: PublicKey } = meteoraAmount
      ? { amount: meteoraAmount, poolAddress: meteoraPool, vaultAddress: meteoraVault }
      : { amount: new BN(0) };

    const treasuryAmount = balanceOf(treasuryAta);
    const daoTreasuryTokens: { amount: BN; vaultAddress?: PublicKey } = treasuryAmount
      ? { amount: treasuryAmount, vaultAddress: treasuryVault }
      : { amount: new BN(0) };

    // Handle additional token allocation (v0.7+ only)
    let additionalTokenAllocation: AdditionalTokenAllocation | undefined;
    if (launch.version === 'v0.7' && launch.additionalTokensRecipient && launch.additionalTokensAmount) {
      // Get the token account address for the additional tokens recipient
      let tokenAccountAddress: PublicKey | undefined;
      try {
        tokenAccountAddress = getAssociatedTokenAddressSync(baseMint, launch.additionalTokensRecipient);
      } catch (error) {
        logger.warn(`[Launchpad] Could not derive additional tokens account for ${launch.additionalTokensRecipient.toString()}`);
      }

      additionalTokenAllocation = {
        recipient: launch.additionalTokensRecipient,
        amount: launch.additionalTokensAmount,
        claimed: launch.additionalTokensClaimed || false,
        tokenAccountAddress,
      };
    }

    // Calculate total non-circulating supply using live on-chain balance
    let totalNonCirculating = performancePackageLockedAmount;

    // Add additional tokens if not yet claimed (they're still locked)
    if (additionalTokenAllocation && !additionalTokenAllocation.claimed) {
      totalNonCirculating = totalNonCirculating.add(additionalTokenAllocation.amount);
    }

    // Add DAO treasury tokens (protocol-controlled, not circulating)
    totalNonCirculating = totalNonCirculating.add(daoTreasuryTokens.amount);

    return {
      version: launch.version,
      teamPerformancePackage: {
        amount: performancePackageLockedAmount,
        address: performancePackageAddress,
      },
      futarchyAmmLiquidity: futarchyAmm,
      meteoraLpLiquidity: meteoraLp,
      additionalTokenAllocation,
      daoTreasuryTokens,
      daoAddress: launch.dao,
      launchAddress: launch.launchAddress,
      totalNonCirculating,
    };
  }

  /**
   * Launches still accepting commitments across launchpad v0.6/v0.7/v0.8:
   * on-chain state `live` and close time not yet passed. Launches past their
   * close time stay `live` on-chain until someone calls closeLaunch (many
   * never are), so they are excluded at serve time — a launch drops out the
   * moment it closes, even within a cache window.
   *
   * Served from a snapshot refreshed at most once per
   * `config.cache.liveLaunchesTTL`; concurrent requests during a refresh share
   * one scan. A failed refresh is not cached and propagates (→ 5xx) rather
   * than serving an empty list.
   */
  async getLiveLaunches(): Promise<LiveLaunchesSnapshot> {
    const snapshot = await this.getLiveLaunchesSnapshot();
    const nowSeconds = Date.now() / 1000;
    return {
      updatedAt: snapshot.updatedAt,
      launches: snapshot.launches.filter((launch) => launch.closeTime > nowSeconds),
    };
  }

  private async getLiveLaunchesSnapshot(): Promise<LiveLaunchesSnapshot> {
    if (this.liveLaunches && Date.now() - this.liveLaunches.fetchedAt < config.cache.liveLaunchesTTL) {
      return this.liveLaunches.snapshot;
    }
    if (!this.liveLaunchesInFlight) {
      this.liveLaunchesInFlight = this.fetchLiveLaunches()
        .then((snapshot) => {
          this.liveLaunches = { snapshot, fetchedAt: Date.now() };
          return snapshot;
        })
        .finally(() => {
          this.liveLaunchesInFlight = null;
        });
    }
    return this.liveLaunchesInFlight;
  }

  private async fetchLiveLaunches(): Promise<LiveLaunchesSnapshot> {
    const programs = [
      { version: 'v0.6', program: this.clientV06.launchpad },
      { version: 'v0.7', program: this.clientV07.launchpad },
      { version: 'v0.8', program: this.clientV08.launchpad },
    ] as const;

    const nowSeconds = Date.now() / 1000;
    const perProgram = await Promise.all(programs.map(async ({ version, program }) => {
      const launches = await program.account.launch.all();

      // Keep `live` launches whose close time hasn't passed. Expired launches
      // stay `live` on-chain until closeLaunch is called (many never are) and
      // would always be filtered at serve time, so don't read their funding
      // records at all.
      const open: Array<{ publicKey: PublicKey; account: (typeof launches)[number]['account']; closeTime: number }> = [];
      for (const { publicKey, account } of launches) {
        if (!('live' in account.state)) continue;
        // A launch only enters `live` via startLaunch, which sets this timestamp.
        if (!account.unixTimestampStarted) {
          throw new Error(`Live ${version} launch ${publicKey.toBase58()} has no start timestamp`);
        }
        const closeTime = account.unixTimestampStarted.toNumber() + account.secondsForLaunch;
        if (closeTime > nowSeconds) open.push({ publicKey, account, closeTime });
      }

      return Promise.all(open.map(async ({ publicKey, account, closeTime }): Promise<LiveLaunch> => {
        const [fundingRecords, quoteDecimals] = await Promise.all([
          program.account.fundingRecord.all([
            { memcmp: { offset: FUNDING_RECORD_LAUNCH_OFFSET, bytes: publicKey.toBase58() } },
          ]),
          this.getMintDecimals(account.quoteMint),
        ]);

        let totalCommitted = new BN(0);
        let committerCount = 0;
        for (const { account: record } of fundingRecords) {
          if (record.committedAmount.isZero()) continue;
          totalCommitted = totalCommitted.add(record.committedAmount);
          committerCount++;
        }

        return {
          launchAddress: publicKey.toBase58(),
          version,
          tokenAddress: account.baseMint.toBase58(),
          quoteMint: account.quoteMint.toBase58(),
          quoteDecimals,
          committerCount,
          totalCommitted: formatUnits(totalCommitted, quoteDecimals),
          totalCommittedRaw: totalCommitted.toString(),
          minimumRaise: formatUnits(account.minimumRaiseAmount, quoteDecimals),
          minimumRaiseRaw: account.minimumRaiseAmount.toString(),
          closeTime,
        };
      }));
    }));

    const launches = perProgram.flat().sort((a, b) => a.closeTime - b.closeTime);
    logger.info(`[Launchpad] Refreshed live launches: ${launches.length} live`);
    return { updatedAt: new Date().toISOString(), launches };
  }

  // Caches the pending lookup so launches sharing a quote mint (usually USDC)
  // make one getMint call. A failed lookup is evicted so the next refresh retries.
  private getMintDecimals(mint: PublicKey): Promise<number> {
    const key = mint.toBase58();
    let decimals = this.mintDecimals.get(key);
    if (!decimals) {
      decimals = getMint(this.connection, mint).then((info) => info.decimals);
      decimals.catch(() => this.mintDecimals.delete(key));
      this.mintDecimals.set(key, decimals);
    }
    return decimals;
  }
}

export default LaunchpadService;

