import { Connection, PublicKey } from '@solana/web3.js';
import { getMint } from '@solana/spl-token';
import { config } from '../config.js';

export interface TokenSupplyInfo {
  mint: string;
  totalSupply: string;
  circulatingSupply: string;
  decimals: number;
  rawTotalSupply: string;
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
   * For now, this returns the same as total supply since we don't have
   * information about locked/vested tokens. This can be extended to
   * subtract known locked addresses.
   * 
   * @param mintAddress - The mint address of the token
   * @param excludeAddresses - Optional array of addresses to exclude from circulating supply
   * @returns Promise<string> - The circulating supply with proper decimals
   */
  async getCirculatingSupply(mintAddress: string, excludeAddresses?: string[]): Promise<string> {
    const cacheKey = `circulating_supply_${mintAddress}_${excludeAddresses?.join(',') || 'none'}`;
    const cached = this.getCached<string>(cacheKey, config.cache.tickersTTL);
    if (cached !== null) return cached;

    try {
      const mintPubkey = new PublicKey(mintAddress);
      const mintInfo = await getMint(this.connection, mintPubkey);
      let supply = Number(mintInfo.supply);
      const decimals = mintInfo.decimals;

      // If exclude addresses are provided, we would subtract their balances
      // This is a placeholder for future implementation
      // For now, circulating = total (conservative approach)

      const circulatingSupply = supply / Math.pow(10, decimals);
      const result = circulatingSupply.toString();

      this.setCache(cacheKey, result);
      return result;
    } catch (error) {
      console.error(`Error fetching circulating supply for ${mintAddress}:`, error);
      throw new Error(`Failed to fetch circulating supply for token: ${mintAddress}`);
    }
  }

  /**
   * Get complete supply information for a token
   * @param mintAddress - The mint address of the token
   * @returns Promise<TokenSupplyInfo> - Complete supply information
   */
  async getSupplyInfo(mintAddress: string): Promise<TokenSupplyInfo> {
    const cacheKey = `supply_info_${mintAddress}`;
    const cached = this.getCached<TokenSupplyInfo>(cacheKey, config.cache.tickersTTL);
    if (cached !== null) return cached;

    try {
      const mintPubkey = new PublicKey(mintAddress);
      const mintInfo = await getMint(this.connection, mintPubkey);
      const rawSupply = mintInfo.supply.toString();
      const supply = Number(mintInfo.supply);
      const decimals = mintInfo.decimals;

      const supplyWithDecimals = supply / Math.pow(10, decimals);

      const result: TokenSupplyInfo = {
        mint: mintAddress,
        totalSupply: supplyWithDecimals.toString(),
        circulatingSupply: supplyWithDecimals.toString(), // Same for now
        decimals,
        rawTotalSupply: rawSupply,
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
