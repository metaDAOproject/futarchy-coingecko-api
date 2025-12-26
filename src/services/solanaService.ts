import { Connection, PublicKey } from '@solana/web3.js';
import { getMint } from '@solana/spl-token';
import { config } from '../config.js';
import BN from 'bn.js';

export interface TokenSupplyInfo {
  mint: string;
  totalSupply: string;
  circulatingSupply: string;
  decimals: number;
  rawTotalSupply: string;
  // Detailed breakdown of non-circulating tokens
  allocation?: {
    // Team performance package (locked tokens)
    teamPerformancePackage?: {
      amount: string;
      address?: string;
    };
    // FutarchyAMM liquidity (internal AMM for spot trading)
    futarchyAmmLiquidity?: {
      amount: string;
      vaultAddress?: string;
    };
    // Meteora LP position (external DEX liquidity)
    meteoraLpLiquidity?: {
      amount: string;
      poolAddress?: string;
      vaultAddress?: string;
    };
    // DAO address
    daoAddress?: string;
    // Launch address
    launchAddress?: string;
  };
}

export interface TokenAllocationInput {
  teamPerformancePackage: {
    amount: BN;
    address?: string;
  };
  futarchyAmmLiquidity: {
    amount: BN;
    vaultAddress?: string;
  };
  meteoraLpLiquidity: {
    amount: BN;
    poolAddress?: string;
    vaultAddress?: string;
  };
  daoAddress?: string;
  launchAddress?: string;
}

export class SolanaService {
  private connection: Connection;
  private cache: Map<string, { data: any; timestamp: number }>;

  constructor() {
    this.connection = new Connection(config.solana.rpcUrl, 'confirmed');
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
   * Validate if a string is a valid Solana public key
   */
  isValidPublicKey(address: string): boolean {
    try {
      new PublicKey(address);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Get the total supply of a token
   * @param mintAddress - The mint address of the token
   * @returns Promise<string> - The total supply with proper decimals
   */
  async getTotalSupply(mintAddress: string): Promise<string> {
    const cacheKey = `total_supply_${mintAddress}`;
    const cached = this.getCached<string>(cacheKey, config.cache.tickersTTL);
    if (cached !== null) return cached;

    try {
      const mintPubkey = new PublicKey(mintAddress);
      const mintInfo = await getMint(this.connection, mintPubkey);
      const supply = Number(mintInfo.supply);
      const decimals = mintInfo.decimals;

      const totalSupply = supply / Math.pow(10, decimals);
      const result = totalSupply.toString();

      this.setCache(cacheKey, result);
      return result;
    } catch (error) {
      console.error(`Error fetching total supply for ${mintAddress}:`, error);
      throw new Error(`Failed to fetch total supply for token: ${mintAddress}`);
    }
  }

  /**
   * Get the circulating supply of a token
   * Circulating supply = Total supply - Locked amounts (performance packages, etc.)
   * 
   * @param mintAddress - The mint address of the token
   * @param lockedAmount - Optional BN of locked tokens to subtract (e.g., performance package)
   * @returns Promise<string> - The circulating supply with proper decimals
   */
  async getCirculatingSupply(mintAddress: string, lockedAmount?: BN): Promise<string> {
    const lockedKey = lockedAmount ? lockedAmount.toString() : 'none';
    const cacheKey = `circulating_supply_${mintAddress}_${lockedKey}`;
    const cached = this.getCached<string>(cacheKey, config.cache.tickersTTL);
    if (cached !== null) return cached;

    try {
      const mintPubkey = new PublicKey(mintAddress);
      const mintInfo = await getMint(this.connection, mintPubkey);
      let supply = new BN(mintInfo.supply.toString());
      const decimals = mintInfo.decimals;

      // Subtract locked amounts (performance package tokens, etc.)
      if (lockedAmount && lockedAmount.gt(new BN(0))) {
        supply = supply.sub(lockedAmount);
        // Ensure we don't go negative
        if (supply.isNeg()) {
          supply = new BN(0);
        }
      }

      const circulatingSupply = Number(supply.toString()) / Math.pow(10, decimals);
      const result = circulatingSupply.toString();

      this.setCache(cacheKey, result);
      return result;
    } catch (error) {
      console.error(`Error fetching circulating supply for ${mintAddress}:`, error);
      throw new Error(`Failed to fetch circulating supply for token: ${mintAddress}`);
    }
  }

  /**
   * Get complete supply information for a token with detailed allocation breakdown
   * @param mintAddress - The mint address of the token
   * @param allocation - Optional token allocation breakdown (team, futarchyAMM, meteora)
   * @returns Promise<TokenSupplyInfo> - Complete supply information with allocation details
   */
  async getSupplyInfo(mintAddress: string, allocation?: TokenAllocationInput): Promise<TokenSupplyInfo> {
    const cacheKey = allocation 
      ? `supply_info_${mintAddress}_${allocation.teamPerformancePackage.amount}_${allocation.futarchyAmmLiquidity.amount}_${allocation.meteoraLpLiquidity.amount}`
      : `supply_info_${mintAddress}_none`;
    const cached = this.getCached<TokenSupplyInfo>(cacheKey, config.cache.tickersTTL);
    if (cached !== null) return cached;

    try {
      const mintPubkey = new PublicKey(mintAddress);
      const mintInfo = await getMint(this.connection, mintPubkey);
      const rawSupply = mintInfo.supply.toString();
      const totalSupplyBN = new BN(mintInfo.supply.toString());
      const decimals = mintInfo.decimals;
      const divisor = Math.pow(10, decimals);

      const totalSupplyWithDecimals = Number(totalSupplyBN.toString()) / divisor;

      // Calculate circulating supply by subtracting all non-circulating allocations
      let circulatingSupplyBN = totalSupplyBN;
      let allocationDetails: TokenSupplyInfo['allocation'] | undefined;

      if (allocation) {
        const teamAmount = allocation.teamPerformancePackage.amount;
        const futarchyAmount = allocation.futarchyAmmLiquidity.amount;
        const meteoraAmount = allocation.meteoraLpLiquidity.amount;

        // Only subtract team performance package from circulating supply
        // Liquidity (futarchyAMM and meteora) IS considered circulating
        if (teamAmount.gt(new BN(0))) {
          circulatingSupplyBN = circulatingSupplyBN.sub(teamAmount);
        }

        // Ensure we don't go negative
        if (circulatingSupplyBN.isNeg()) {
          circulatingSupplyBN = new BN(0);
        }

        // Build allocation details for response (include all for transparency)
        allocationDetails = {
          teamPerformancePackage: teamAmount.gt(new BN(0)) ? {
            amount: (Number(teamAmount.toString()) / divisor).toString(),
            address: allocation.teamPerformancePackage.address,
          } : undefined,
          futarchyAmmLiquidity: futarchyAmount.gt(new BN(0)) ? {
            amount: (Number(futarchyAmount.toString()) / divisor).toString(),
            vaultAddress: allocation.futarchyAmmLiquidity.vaultAddress,
          } : undefined,
          meteoraLpLiquidity: meteoraAmount.gt(new BN(0)) ? {
            amount: (Number(meteoraAmount.toString()) / divisor).toString(),
            poolAddress: allocation.meteoraLpLiquidity.poolAddress,
            vaultAddress: allocation.meteoraLpLiquidity.vaultAddress,
          } : undefined,
          daoAddress: allocation.daoAddress,
          launchAddress: allocation.launchAddress,
        };
      }
      
      const circulatingSupplyWithDecimals = Number(circulatingSupplyBN.toString()) / divisor;

      const result: TokenSupplyInfo = {
        mint: mintAddress,
        totalSupply: totalSupplyWithDecimals.toString(),
        circulatingSupply: circulatingSupplyWithDecimals.toString(),
        decimals,
        rawTotalSupply: rawSupply,
        allocation: allocationDetails,
      };

      this.setCache(cacheKey, result);
      return result;
    } catch (error) {
      console.error(`Error fetching supply info for ${mintAddress}:`, error);
      throw new Error(`Failed to fetch supply info for token: ${mintAddress}`);
    }
  }
}

export default SolanaService;
