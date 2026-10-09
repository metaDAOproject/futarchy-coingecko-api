import { Connection, PublicKey } from '@solana/web3.js';
import { getMint } from '@solana/spl-token';
import { config } from '../config.js';
import { createSolanaConnection } from '../utils/solanaConnection.js';
import { TtlCache } from '../utils/ttlCache.js';
import BN from 'bn.js';
import { retry, isTransientError, createRetryLogger } from '../utils/resilience.js';
import { logger } from '../utils/logger.js';

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
    // Additional token allocation (v0.7+ only) - not in circulating supply
    additionalTokenAllocation?: {
      amount: string;
      recipient: string;
      claimed: boolean;
      tokenAccountAddress?: string;
    };
    // Initial token allocation that has been claimed and IS in circulating supply
    initialTokenAllocation?: {
      amount: string;
      claimed: boolean;
    };
    // DAO treasury tokens - base tokens held in the squads vault (not circulating)
    daoTreasuryTokens?: {
      amount: string;
      vaultAddress?: string;
    };
    // DAO address
    daoAddress?: string;
    // Launch address
    launchAddress?: string;
    // Launchpad version
    version?: string;
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
  // Additional token allocation (v0.7+ only)
  additionalTokenAllocation?: {
    amount: BN;
    recipient: string;
    claimed: boolean;
    tokenAccountAddress?: string;
  };
  // DAO treasury tokens - base tokens held in the squads vault
  daoTreasuryTokens?: {
    amount: BN;
    vaultAddress?: string;
  };
  daoAddress?: string;
  launchAddress?: string;
  version?: string;
}

export class SolanaService {
  private connection: Connection;
  // Keyed by caller-supplied mints, so bounded.
  private cache = new TtlCache(10_000);

  constructor() {
    this.connection = createSolanaConnection();
  }

  /**
   * The mint's supply and decimals, cached per mint for `tickersTTL`.
   * Concurrent requests for one mint share a single RPC read. Supply figures
   * are derived from this on every call (cheap arithmetic), so nothing is
   * cached under keys that change whenever balances move.
   */
  private getMintInfo(mintAddress: string): Promise<{ supply: bigint; decimals: number }> {
    return this.cache.getOrLoad(`mint_${mintAddress}`, config.cache.tickersTTL, async () => {
      const mint = await this.withRetry(() => getMint(this.connection, new PublicKey(mintAddress)));
      return { supply: mint.supply, decimals: mint.decimals };
    });
  }

  /**
   * Retry wrapper for RPC calls with exponential backoff.
   */
  private async withRetry<T>(fn: () => Promise<T>): Promise<T> {
    return retry(fn, {
      maxRetries: 3,
      initialDelayMs: 1000,
      maxDelayMs: 5000,
      isRetryable: isTransientError,
      onRetry: createRetryLogger('[Solana]'),
    });
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
    try {
      const mintInfo = await this.getMintInfo(mintAddress);
      const supply = Number(mintInfo.supply);
      const decimals = mintInfo.decimals;

      const totalSupply = supply / Math.pow(10, decimals);
      return totalSupply.toString();
    } catch (error) {
      logger.error(`Error fetching total supply for ${mintAddress}:`, error);
      throw new Error(`Failed to fetch total supply for token: ${mintAddress}`);
    }
  }

  /**
   * Get complete supply information for a token with detailed allocation breakdown
   * @param mintAddress - The mint address of the token
   * @param allocation - Optional token allocation breakdown (team, futarchyAMM, meteora)
   * @returns Promise<TokenSupplyInfo> - Complete supply information with allocation details
   */
  async getSupplyInfo(mintAddress: string, allocation?: TokenAllocationInput): Promise<TokenSupplyInfo> {
    try {
      const mintInfo = await this.getMintInfo(mintAddress);
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

        // Subtract team performance package from circulating supply (always locked)
        if (teamAmount.gt(new BN(0))) {
          circulatingSupplyBN = circulatingSupplyBN.sub(teamAmount);
        }

        // Subtract additional token allocation from circulating supply (v0.7+ only)
        // These tokens go to a specific recipient, not into general circulation
        if (allocation.additionalTokenAllocation && 
            allocation.additionalTokenAllocation.amount.gt(new BN(0))) {
          circulatingSupplyBN = circulatingSupplyBN.sub(allocation.additionalTokenAllocation.amount);
        }

        // Subtract DAO treasury tokens (held in squads vault, protocol-controlled)
        if (allocation.daoTreasuryTokens && 
            allocation.daoTreasuryTokens.amount.gt(new BN(0))) {
          circulatingSupplyBN = circulatingSupplyBN.sub(allocation.daoTreasuryTokens.amount);
        }

        // Special case: RNGR token has an initial token allocation that IS in circulation
        // This is a claimed portion from the additional token recipient that should be added back
        const RNGR_MINT = 'RNGRtJMbCveqCp7AC6U95KmrdKecFckaJZiWbPGmeta';
        const RNGR_INITIAL_ALLOCATION = new BN(192187500000); // 192,187.5 tokens with 6 decimals
        let initialTokenAllocationDetails: { amount: string; claimed: boolean } | undefined;
        
        if (mintAddress === RNGR_MINT) {
          circulatingSupplyBN = circulatingSupplyBN.add(RNGR_INITIAL_ALLOCATION);
          initialTokenAllocationDetails = {
            amount: (Number(RNGR_INITIAL_ALLOCATION.toString()) / divisor).toString(),
            claimed: true,
          };
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
          // Include additional token allocation if present (v0.7+ only)
          additionalTokenAllocation: allocation.additionalTokenAllocation ? {
            amount: (Number(allocation.additionalTokenAllocation.amount.toString()) / divisor).toString(),
            recipient: allocation.additionalTokenAllocation.recipient,
            claimed: allocation.additionalTokenAllocation.claimed,
            tokenAccountAddress: allocation.additionalTokenAllocation.tokenAccountAddress,
          } : undefined,
          // Include initial token allocation if present (special cases)
          initialTokenAllocation: initialTokenAllocationDetails,
          // Include DAO treasury tokens if present
          daoTreasuryTokens: allocation.daoTreasuryTokens && allocation.daoTreasuryTokens.amount.gt(new BN(0)) ? {
            amount: (Number(allocation.daoTreasuryTokens.amount.toString()) / divisor).toString(),
            vaultAddress: allocation.daoTreasuryTokens.vaultAddress,
          } : undefined,
          daoAddress: allocation.daoAddress,
          launchAddress: allocation.launchAddress,
          version: allocation.version,
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

      return result;
    } catch (error) {
      logger.error(`Error fetching supply info for ${mintAddress}:`, error);
      throw new Error(`Failed to fetch supply info for token: ${mintAddress}`);
    }
  }
}

export default SolanaService;
