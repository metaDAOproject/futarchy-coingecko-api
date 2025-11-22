import { PublicKey } from '@solana/web3.js';
import dotenv from 'dotenv';

dotenv.config();

export const config = {
  solana: {
    rpcUrl: process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com',
    wsUrl: process.env.SOLANA_WS_URL || 'wss://api.mainnet-beta.solana.com',
  },
  dao: {
    publicKey: new PublicKey(process.env.DAO_PUBLIC_KEY || ''),
    baseMint: new PublicKey(process.env.BASE_MINT || ''),
    quoteMint: new PublicKey(process.env.QUOTE_MINT || 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'),
  },
  tokens: {
    baseDecimals: parseInt(process.env.BASE_DECIMALS || '9'),
    quoteDecimals: parseInt(process.env.QUOTE_DECIMALS || '6'),
    baseSymbol: process.env.BASE_SYMBOL || 'META',
    quoteSymbol: process.env.QUOTE_SYMBOL || 'USDC',
  },
  server: {
    port: parseInt(process.env.PORT || '3000'),
    rateLimit: {
      windowMs: 60000, // 1 minute
      maxRequests: 60, // 60 requests per minute
    },
  },
  cache: {
    tickersTTL: 10000, // 10 seconds
  },
  dex: {
    forkType: process.env.DEX_FORK_TYPE || 'Custom',
    factoryAddress: process.env.FACTORY_ADDRESS || '',
    routerAddress: process.env.ROUTER_ADDRESS || '',
  },
  excludedDaos: (process.env.EXCLUDED_DAOS || '')
    .split(',')
    .map(addr => addr.trim())
    .filter(addr => addr.length > 0)
    .map(addr => new PublicKey(addr)),
  fees: {
    // Protocol fee rate (e.g., 0.0025 = 0.25%)
    protocolFeeRate: parseFloat(process.env.PROTOCOL_FEE_RATE || '0.0025'),
  },
};