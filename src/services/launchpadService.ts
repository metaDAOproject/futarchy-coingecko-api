import { Connection, PublicKey, Keypair } from '@solana/web3.js';
import { AnchorProvider, Wallet, Program } from '@coral-xyz/anchor';
import { 
  LaunchpadClient, 
  FutarchyClient, 
  getLaunchSignerAddr, 
  getPerformancePackageAddr, 
  PRICE_BASED_PERFORMANCE_PACKAGE_PROGRAM_ID,
  DAMM_V2_PROGRAM_ID,
  MAINNET_METEORA_CONFIG,
} from "@metadaoproject/futarchy/v0.6";
import { getAccount } from '@solana/spl-token';
import { config } from '../config.js';
import BN from 'bn.js';

/**
 * Complete token allocation breakdown for launchpad tokens
 */
export interface TokenAllocationBreakdown {
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
  // DAO address (if launch completed)
  daoAddress?: PublicKey;
  // Launch address
  launchAddress?: PublicKey;
}

export interface LaunchData {
  launchAddress: PublicKey;
  baseMint: PublicKey;
  performancePackageGrantee: PublicKey;
  performancePackageTokenAmount: BN;
  state: LaunchState;
  dao?: PublicKey;
}

export type LaunchState = 
  | { initialized: Record<string, never> }
  | { active: Record<string, never> }
  | { closed: Record<string, never> }
  | { completed: Record<string, never> }
  | { cancelled: Record<string, never> };

export interface PerformancePackageData {
  performancePackageAddress: PublicKey;
  totalTokenAmount: BN;
  alreadyUnlockedAmount: BN;
  recipient: PublicKey;
  tokenMint: PublicKey;
  state: PerformancePackageState;
}

export type PerformancePackageState = 
  | { locked: Record<string, never> }
  | { unlocking: { startAggregator: BN; startTimestamp: BN } }
  | { unlocked: Record<string, never> };

export class LaunchpadService {
  private connection: Connection;
  private client: LaunchpadClient;
  private futarchyClient: FutarchyClient;
  private cache: Map<string, { data: any; timestamp: number }>;

  constructor() {
    this.connection = new Connection(config.solana.rpcUrl, 'confirmed');
    
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
    this.client = LaunchpadClient.createClient({ provider });
    this.futarchyClient = FutarchyClient.createClient({ provider });
    this.cache = new Map();
  }

  private getCached<T>(key: string, ttl: number): T | null {
    const cached = this.cache.get(key);
    if (cached && Date.now() - cached.timestamp < ttl) {
      return cached.data as T;
    }
    return null;
  }

  private setCache(key: string, data: any): void {
    this.cache.set(key, { data, timestamp: Date.now() });
  }

  /**
   * Get the Launch PDA address for a given base mint
   */
  getLaunchAddress(baseMint: PublicKey): PublicKey {
    return this.client.getLaunchAddress({ baseMint });
  }

  /**
   * Fetch a Launch account by its address
   */
  async getLaunch(launchAddress: PublicKey): Promise<LaunchData | null> {
    const cacheKey = `launch_${launchAddress.toString()}`;
    const cached = this.getCached<LaunchData>(cacheKey, config.cache.tickersTTL * 10);
    if (cached) return cached;

    try {
      const launch = await this.client.fetchLaunch(launchAddress);
      if (!launch) {
        return null;
      }

      const launchData: LaunchData = {
        launchAddress,
        baseMint: launch.baseMint,
        performancePackageGrantee: launch.performancePackageGrantee,
        performancePackageTokenAmount: new BN(launch.performancePackageTokenAmount.toString()),
        state: launch.state as LaunchState,
        dao: launch.dao || undefined,
      };

      this.setCache(cacheKey, launchData);
      return launchData;
    } catch (error) {
      console.error(`Error fetching launch ${launchAddress.toString()}:`, error);
      return null;
    }
  }

  /**
   * Fetch a Launch account by the token's base mint address
   */
  async getLaunchByBaseMint(baseMint: PublicKey): Promise<LaunchData | null> {
    const launchAddress = this.getLaunchAddress(baseMint);
    return this.getLaunch(launchAddress);
  }

  /**
   * Fetch all Launch accounts
   */
  async getAllLaunches(): Promise<LaunchData[]> {
    const cacheKey = 'all_launches';
    const cached = this.getCached<LaunchData[]>(cacheKey, config.cache.tickersTTL);
    if (cached) return cached;

    try {
      // Fetch all launch accounts from the program
      const launchAccounts = await this.client.launchpad.account.launch.all();
      
      const launches: LaunchData[] = [];
      for (const account of launchAccounts) {
        const launch = account.account;
        launches.push({
          launchAddress: account.publicKey,
          baseMint: launch.baseMint,
          performancePackageGrantee: launch.performancePackageGrantee,
          performancePackageTokenAmount: new BN(launch.performancePackageTokenAmount.toString()),
          state: launch.state as LaunchState,
          dao: launch.dao || undefined,
        });
      }

      this.setCache(cacheKey, launches);
      return launches;
    } catch (error) {
      console.error('Error fetching all launches:', error);
      return [];
    }
  }

  /**
   * Fetch a PerformancePackage account by its address
   */
  async getPerformancePackage(performancePackageAddress: PublicKey): Promise<PerformancePackageData | null> {
    const cacheKey = `performance_package_${performancePackageAddress.toString()}`;
    const cached = this.getCached<PerformancePackageData>(cacheKey, config.cache.tickersTTL * 10);
    if (cached) return cached;

    try {
      const pkg = await this.client.priceBasedUnlock.getPerformancePackage(performancePackageAddress);
      if (!pkg) {
        return null;
      }

      const packageData: PerformancePackageData = {
        performancePackageAddress,
        totalTokenAmount: pkg.totalTokenAmount,
        alreadyUnlockedAmount: pkg.alreadyUnlockedAmount,
        recipient: pkg.recipient,
        tokenMint: pkg.tokenMint,
        state: pkg.state as PerformancePackageState,
      };

      this.setCache(cacheKey, packageData);
      return packageData;
    } catch (error) {
      console.error(`Error fetching performance package ${performancePackageAddress.toString()}:`, error);
      return null;
    }
  }

  /**
   * Derive the performance package address for a given launch.
   * The createKey used during completeLaunch is the launch signer.
   */
  getPerformancePackageAddress(launchAddress: PublicKey): PublicKey {
    const [launchSigner] = getLaunchSignerAddr(
      this.client.getProgramId(),
      launchAddress
    );
    const [performancePackageAddress] = getPerformancePackageAddr({
      programId: PRICE_BASED_PERFORMANCE_PACKAGE_PROGRAM_ID,
      createKey: launchSigner,
    });
    return performancePackageAddress;
  }

  /**
   * Derive the Meteora DAMM v2 pool address for a token pair.
   * Seeds: ["pool", config, larger_mint, smaller_mint]
   * Token order: DESCENDING (larger first, smaller second) - per SDK's getFirstKey/getSecondKey
   */
  getMeteoraPoolAddress(baseMint: PublicKey, quoteMint: PublicKey): PublicKey {
    // Sort mints - Meteora uses DESCENDING order (larger first, smaller second)
    const buf1 = baseMint.toBuffer();
    const buf2 = quoteMint.toBuffer();
    const comparison = Buffer.compare(buf1, buf2);
    
    // getFirstKey: if buf1 > buf2, return buf1, else return buf2 (the larger one)
    // getSecondKey: if buf1 > buf2, return buf2, else return buf1 (the smaller one)
    const firstKey = comparison === 1 ? baseMint : quoteMint;
    const secondKey = comparison === 1 ? quoteMint : baseMint;
    
    const [poolAddress] = PublicKey.findProgramAddressSync(
      [
        Buffer.from("pool"),
        MAINNET_METEORA_CONFIG.toBuffer(),
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
   * Get the FutarchyAMM liquidity for a DAO.
   * This is the base token balance in the DAO's embedded AMM base vault.
   */
  async getFutarchyAmmLiquidity(daoAddress: PublicKey): Promise<{
    amount: BN;
    vaultAddress?: PublicKey;
  }> {
    try {
      const dao = await this.futarchyClient.fetchDao(daoAddress);
      if (!dao) {
        return { amount: new BN(0) };
      }

      const vaultAddress = dao.amm.ammBaseVault;
      const tokenAccount = await getAccount(this.connection, vaultAddress);
      const amount = new BN(tokenAccount.amount.toString());

      return { amount, vaultAddress };
    } catch (error) {
      console.error(`Error fetching FutarchyAMM liquidity for DAO ${daoAddress.toString()}:`, error);
      return { amount: new BN(0) };
    }
  }

  /**
   * Get the Meteora LP liquidity for a token pair.
   * This is the base token balance in the Meteora pool's vault.
   * 
   * Note: The pool address derivation follows Meteora DAMM v2 seeds.
   * If the derived pool doesn't exist, we try to find the vault directly.
   */
  async getMeteoraLpLiquidity(baseMint: PublicKey, quoteMint: PublicKey): Promise<{
    amount: BN;
    poolAddress?: PublicKey;
    vaultAddress?: PublicKey;
  }> {
    try {
      const poolAddress = this.getMeteoraPoolAddress(baseMint, quoteMint);
      const vaultAddress = this.getMeteoraPoolVault(poolAddress, baseMint);
      
      console.log(`[Meteora] Checking pool ${poolAddress.toString()} vault ${vaultAddress.toString()} for ${baseMint.toString()}`);
      
      const tokenAccount = await getAccount(this.connection, vaultAddress);
      const amount = new BN(tokenAccount.amount.toString());

      console.log(`[Meteora] Found ${amount.toString()} tokens in Meteora pool`);
      return { amount, poolAddress, vaultAddress };
    } catch (error: any) {
      // Pool might not exist or vault might be empty - log more details
      console.log(`[Meteora] Pool lookup failed for ${baseMint.toString()}: ${error.message || 'Unknown error'}`);
      
      // Try alternative: look for the pool with different config or derivation
      // This can be enhanced later when we know the exact Meteora pool structure
      return { amount: new BN(0) };
    }
  }

  /**
   * Get the complete token allocation breakdown for a launchpad token.
   * This provides a complete picture of where all tokens are allocated:
   * - Team Performance Package (locked)
   * - FutarchyAMM Liquidity (internal AMM)
   * - Meteora LP Liquidity (external DEX)
   * 
   * Circulating Supply = Total - Team - FutarchyAMM - Meteora
   */
  async getTokenAllocationBreakdown(baseMint: PublicKey): Promise<TokenAllocationBreakdown> {
    const cacheKey = `allocation_${baseMint.toString()}`;
    const cached = this.getCached<TokenAllocationBreakdown>(cacheKey, config.cache.tickersTTL * 5);
    if (cached) return cached;

    const emptyBreakdown: TokenAllocationBreakdown = {
      teamPerformancePackage: { amount: new BN(0) },
      futarchyAmmLiquidity: { amount: new BN(0) },
      meteoraLpLiquidity: { amount: new BN(0) },
    };

    try {
      const launch = await this.getLaunchByBaseMint(baseMint);
      if (!launch) {
        // Token was not launched via launchpad
        return emptyBreakdown;
      }

      if (!launch.dao) {
        // Launch not yet completed
        return {
          ...emptyBreakdown,
          launchAddress: launch.launchAddress,
        };
      }

      // Get DAO to find quote mint
      const dao = await this.futarchyClient.fetchDao(launch.dao);
      const quoteMint = dao?.quoteMint;

      // Derive the performance package address
      const performancePackageAddress = this.getPerformancePackageAddress(launch.launchAddress);

      // Get FutarchyAMM liquidity
      const futarchyAmm = await this.getFutarchyAmmLiquidity(launch.dao);

      // Get Meteora LP liquidity (if quote mint is available)
      let meteoraLp: { amount: BN; poolAddress?: PublicKey; vaultAddress?: PublicKey } = { amount: new BN(0) };
      if (quoteMint) {
        meteoraLp = await this.getMeteoraLpLiquidity(baseMint, quoteMint);
      }

      const breakdown: TokenAllocationBreakdown = {
        teamPerformancePackage: {
          amount: launch.performancePackageTokenAmount,
          address: performancePackageAddress,
        },
        futarchyAmmLiquidity: {
          amount: futarchyAmm.amount,
          vaultAddress: futarchyAmm.vaultAddress,
        },
        meteoraLpLiquidity: {
          amount: meteoraLp.amount,
          poolAddress: meteoraLp.poolAddress,
          vaultAddress: meteoraLp.vaultAddress,
        },
        daoAddress: launch.dao,
        launchAddress: launch.launchAddress,
      };

      this.setCache(cacheKey, breakdown);
      return breakdown;
    } catch (error) {
      console.error(`Error getting token allocation breakdown for ${baseMint.toString()}:`, error);
      return emptyBreakdown;
    }
  }

  /**
   * @deprecated Use getTokenAllocationBreakdown instead
   * Get the amount of tokens locked in performance packages for a given token mint.
   */
  async getLockedPerformancePackageInfo(baseMint: PublicKey): Promise<{
    lockedAmount: BN;
    performancePackageAddress?: PublicKey;
    polAmount?: BN;
    ammBaseVault?: PublicKey;
  }> {
    const breakdown = await this.getTokenAllocationBreakdown(baseMint);
    return {
      lockedAmount: breakdown.teamPerformancePackage.amount,
      performancePackageAddress: breakdown.teamPerformancePackage.address,
      polAmount: breakdown.futarchyAmmLiquidity.amount.add(breakdown.meteoraLpLiquidity.amount),
      ammBaseVault: breakdown.futarchyAmmLiquidity.vaultAddress,
    };
  }

  /**
   * @deprecated Use getLockedPerformancePackageInfo instead
   * Get the amount of tokens locked in performance packages for a given token mint.
   */
  async getLockedPerformancePackageAmount(baseMint: PublicKey): Promise<BN> {
    const info = await this.getLockedPerformancePackageInfo(baseMint);
    return info.lockedAmount;
  }

  /**
   * Build a map of baseMint -> lockedAmount for all launched tokens
   * More efficient when you need to check multiple tokens
   */
  async buildLockedAmountsMap(): Promise<Map<string, BN>> {
    const cacheKey = 'locked_amounts_map';
    const cached = this.getCached<Map<string, BN>>(cacheKey, config.cache.tickersTTL);
    if (cached) return cached;

    const lockedAmountsMap = new Map<string, BN>();
    
    try {
      const allLaunches = await this.getAllLaunches();
      
      for (const launch of allLaunches) {
        // Only include completed launches (those with a DAO set)
        if (launch.dao) {
          lockedAmountsMap.set(
            launch.baseMint.toString(),
            launch.performancePackageTokenAmount
          );
        }
      }

      this.setCache(cacheKey, lockedAmountsMap);
      return lockedAmountsMap;
    } catch (error) {
      console.error('Error building locked amounts map:', error);
      return lockedAmountsMap;
    }
  }
}

export default LaunchpadService;

