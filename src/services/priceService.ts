import BN from 'bn.js';

export class PriceService {
  calculatePrice(
    baseReserves: BN, 
    quoteReserves: BN, 
    baseDecimals: number, 
    quoteDecimals: number
  ): string | null {
    // Validate inputs
    if (!baseReserves || !quoteReserves) {
      return null;
    }

    const baseReservesNum = baseReserves.toNumber();
    const quoteReservesNum = quoteReserves.toNumber();

    // Check for zero or invalid reserves
    if (baseReservesNum === 0 || quoteReservesNum === 0 || 
        !isFinite(baseReservesNum) || !isFinite(quoteReservesNum) ||
        isNaN(baseReservesNum) || isNaN(quoteReservesNum)) {
      return null;
    }

    // Price = quoteReserves / baseReserves, adjusted for decimals
    const decimalAdjustment = Math.pow(
      10,
      baseDecimals - quoteDecimals
    );
    
    const price = (quoteReservesNum / baseReservesNum) * decimalAdjustment;
    
    // Validate calculated price
    if (!isFinite(price) || isNaN(price) || price <= 0) {
      return null;
    }
    
    return price.toFixed(12);
  }

  calculateSpread(price: number, spreadBps: number = 50): { bid: string; ask: string } | null {
    // Validate price
    if (!isFinite(price) || isNaN(price) || price <= 0) {
      return null;
    }

    // spreadBps = 50 means 0.5%
    const spreadPercent = spreadBps / 10000;
    const bid = price * (1 - spreadPercent);
    const ask = price * (1 + spreadPercent);
    
    // Validate results
    if (!isFinite(bid) || !isFinite(ask) || isNaN(bid) || isNaN(ask)) {
      return null;
    }
    
    return {
      bid: bid.toFixed(12),
      ask: ask.toFixed(12),
    };
  }

  calculateLiquidityUSD(quoteReserves: BN, quoteDecimals: number): string | null {
    if (!quoteReserves) {
      return null;
    }

    const quoteReservesNum = quoteReserves.toNumber();
    
    // Validate reserves
    if (!isFinite(quoteReservesNum) || isNaN(quoteReservesNum) || quoteReservesNum < 0) {
      return null;
    }

    // For stablecoin pairs, liquidity = 2 * quoteReserves
    const liquidity = (quoteReservesNum * 2) / Math.pow(10, quoteDecimals);
    
    // Validate result
    if (!isFinite(liquidity) || isNaN(liquidity) || liquidity < 0) {
      return null;
    }
    
    return liquidity.toFixed(2);
  }

  calculateVolumeFromFees(
    baseProtocolFees: BN,
    quoteProtocolFees: BN,
    baseDecimals: number,
    quoteDecimals: number,
    feeRate: number
  ): { baseVolume: string; targetVolume: string } | null {
    if (feeRate <= 0) {
      return null;
    }

    // Convert BN to numbers, handling null/undefined
    // Check if BN exists and is not zero before converting
    const baseFeesNum = (baseProtocolFees && !baseProtocolFees.isZero()) 
      ? baseProtocolFees.toNumber() 
      : 0;
    const quoteFeesNum = (quoteProtocolFees && !quoteProtocolFees.isZero())
      ? quoteProtocolFees.toNumber()
      : 0;

    // Validate fees - at least one must have a value
    if ((!isFinite(baseFeesNum) || isNaN(baseFeesNum) || baseFeesNum < 0) &&
        (!isFinite(quoteFeesNum) || isNaN(quoteFeesNum) || quoteFeesNum < 0)) {
      return null;
    }

    // If both fees are zero, return null
    if (baseFeesNum === 0 && quoteFeesNum === 0) {
      return null;
    }

    // Volume = Fees / Fee Rate
    // Adjust for decimals
    // Calculate volume for whichever fees exist (can be zero if that fee doesn't exist)
    const baseVolume = baseFeesNum > 0 
      ? (baseFeesNum / feeRate) / Math.pow(10, baseDecimals)
      : 0;
    
    const targetVolume = quoteFeesNum > 0
      ? (quoteFeesNum / feeRate) / Math.pow(10, quoteDecimals)
      : 0;

    // Validate results - check if calculated values are valid
    const baseVolumeValid = baseVolume === 0 || (isFinite(baseVolume) && !isNaN(baseVolume) && baseVolume >= 0);
    const targetVolumeValid = targetVolume === 0 || (isFinite(targetVolume) && !isNaN(targetVolume) && targetVolume >= 0);

    if (!baseVolumeValid || !targetVolumeValid) {
      return null;
    }

    return {
      baseVolume: baseVolume.toFixed(8),
      targetVolume: targetVolume.toFixed(8),
    };
  }

  // Calculate constant product orderbook depth
  calculateOrderbookDepth(
    baseReserves: BN,
    quoteReserves: BN,
    depthLevels: number,
    baseDecimals: number
  ): {
    bids: [string, string][];
    asks: [string, string][];
  } | null {
    // Validate inputs
    if (!baseReserves || !quoteReserves) {
      return null;
    }

    const baseReservesNum = baseReserves.toNumber();
    const quoteReservesNum = quoteReserves.toNumber();

    // Check for zero or invalid reserves
    if (baseReservesNum === 0 || quoteReservesNum === 0 || 
        !isFinite(baseReservesNum) || !isFinite(quoteReservesNum) ||
        isNaN(baseReservesNum) || isNaN(quoteReservesNum)) {
      return null;
    }

    const k = baseReserves.mul(quoteReserves);
    const bids: [string, string][] = [];
    const asks: [string, string][] = [];

    // Calculate bids (buying base with quote)
    for (let i = 1; i <= depthLevels; i++) {
      try {
        const newQuoteReserves = quoteReserves.muln(100 + i).divn(100);
        const newBaseReserves = k.div(newQuoteReserves);
        
        const newQuoteReservesNum = newQuoteReserves.toNumber();
        const newBaseReservesNum = newBaseReserves.toNumber();
        
        if (newBaseReservesNum === 0 || !isFinite(newQuoteReservesNum) || !isFinite(newBaseReservesNum) ||
            isNaN(newQuoteReservesNum) || isNaN(newBaseReservesNum)) {
          continue;
        }
        
        const price = newQuoteReservesNum / newBaseReservesNum;
        const amount = baseReserves.sub(newBaseReserves).toNumber() / Math.pow(10, baseDecimals);
        
        if (amount > 0 && isFinite(price) && !isNaN(price) && isFinite(amount) && !isNaN(amount)) {
          bids.push([price.toFixed(12), amount.toFixed(8)]);
        }
      } catch (error) {
        // Skip this level if calculation fails
        continue;
      }
    }

    // Calculate asks (selling base for quote)
    for (let i = 1; i <= depthLevels; i++) {
      try {
        const newBaseReserves = baseReserves.muln(100 + i).divn(100);
        const newQuoteReserves = k.div(newBaseReserves);
        
        const newBaseReservesNum = newBaseReserves.toNumber();
        const newQuoteReservesNum = newQuoteReserves.toNumber();
        
        if (newBaseReservesNum === 0 || !isFinite(newBaseReservesNum) || !isFinite(newQuoteReservesNum) ||
            isNaN(newBaseReservesNum) || isNaN(newQuoteReservesNum)) {
          continue;
        }
        
        const price = newQuoteReservesNum / newBaseReservesNum;
        const amount = newBaseReserves.sub(baseReserves).toNumber() / Math.pow(10, baseDecimals);
        
        if (amount > 0 && isFinite(price) && !isNaN(price) && isFinite(amount) && !isNaN(amount)) {
          asks.push([price.toFixed(12), amount.toFixed(8)]);
        }
      } catch (error) {
        // Skip this level if calculation fails
        continue;
      }
    }

    return {
      bids: bids.reverse(), // Highest bid first
      asks, // Lowest ask first
    };
  }
}