export interface CoinGeckoTicker {
  ticker_id: string;
  base_currency: string;
  target_currency: string;
  base_symbol?: string;
  base_name?: string;
  target_symbol?: string;
  target_name?: string;
  pool_id: string;
  last_price: string;
  base_volume: string;
  target_volume: string;
  liquidity_in_usd: string;
  bid: string;
  ask: string;
  high_24h?: string;
  low_24h?: string;
}

export interface CoinGeckoHistoricalTrade {
  trade_id: number;
  price: string;
  base_volume: string;
  target_volume: string;
  trade_timestamp: string;
  type: 'buy' | 'sell';
}

export interface CoinGeckoHistoricalTrades {
  buy: CoinGeckoHistoricalTrade[];
  sell: CoinGeckoHistoricalTrade[];
}