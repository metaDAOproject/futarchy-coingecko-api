# Futarchy CoinGecko DEX API

A CoinGecko-compatible DEX API for the Futarchy protocol that automatically discovers and aggregates all DAOs, providing real-time pricing and trading information.

## Public API Documentation

**Base URL:** `https://your-api-domain.com`

### Mandatory Endpoints

#### Tickers

**Endpoint:** `GET /api/tickers`

Returns all DAO tickers with pricing and volume information. Automatically discovers all DAOs from the Futarchy protocol.

**Response:**
```json
[
  {
    "ticker_id": "ZKFHiLAfAFMTcDAuCtjNW54VzpERvoe7PBF9mYgmeta_EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "base_currency": "ZKFHiLAfAFMTcDAuCtjNW54VzpERvoe7PBF9mYgmeta",
    "target_currency": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "base_symbol": "ZKFG",
    "base_name": "ZKFG",
    "target_symbol": "USDC",
    "target_name": "USD Coin",
    "pool_id": "5FPGRzY9ArJFwY2Hp2y2eqMzVewyWCBox7esmpuZfCvE",
    "last_price": "0.081340728222",
    "base_volume": "30024.81040000",
    "target_volume": "2441.23456789",
    "liquidity_in_usd": "180138.45",
    "bid": "0.080934024581",
    "ask": "0.081747431863"
  }
]
```

**Fields:**
- `ticker_id`: Format `{BASE_MINT}_{QUOTE_MINT}`
- `base_currency`: Base token mint address
- `target_currency`: Quote token mint address (usually USDC)
- `base_symbol`: Base token symbol (if available)
- `base_name`: Base token name (if available)
- `target_symbol`: Quote token symbol (if available)
- `target_name`: Quote token name (if available)
- `pool_id`: DAO address
- `last_price`: Current price (quote/base)
- `base_volume`: Trading volume in base token (calculated from protocol fees)
- `target_volume`: Trading volume in quote token (calculated from protocol fees)
- `liquidity_in_usd`: Total liquidity in USD
- `bid`: Best bid price
- `ask`: Best ask price

**Example:**
```bash
curl https://your-api-domain.com/api/tickers
```

## DEX Information

### DEX Fork Type

The Futarchy protocol uses a custom AMM implementation. You may refer to the [list of DEX Forks currently supported by GeckoTerminal](https://docs.geckoterminal.com/api-reference/dex-forks) for reference.

**Supported DEX Fork Types:**
- Algebra
- Algebra Integral
- Balancer V2
- Balancer V3
- Bluemove
- Camelot V3
- Cetus
- Curve
- Dedust
- Ekubo
- Iziswap
- Jediswap
- Kyberswap
- Kyberswap Elastic
- Liquidswap
- Maverick V2
- Orca
- Quickswap V3
- Raydum CLMM
- Raydium
- Solidly V2
- Solidly V3
- Sparrowswap
- Ston.fi
- Ston.fi V2
- Surge Protocol
- Traderjoe V2
- Uniswap V2
- Uniswap V3
- Uniswap V4
- Velocore V2

**Note:** Futarchy uses a custom AMM implementation. Configure the fork type via the `DEX_FORK_TYPE` environment variable.

### Factory Address

**Configuration:** Set via `FACTORY_ADDRESS` environment variable

The factory address for the Futarchy protocol program. This is the program ID that creates and manages DAO instances.

**Example:**
```env
FACTORY_ADDRESS=YOUR_FACTORY_PROGRAM_ID
```

### Router Address

**Configuration:** Set via `ROUTER_ADDRESS` environment variable

The router address for executing swaps on the Futarchy protocol.

**Example:**
```env
ROUTER_ADDRESS=YOUR_ROUTER_PROGRAM_ID
```

## Installation

```bash
# Install dependencies
bun install

# Build the project
bun run build

# Start the server
bun start
```

For development with hot reload:

```bash
bun run dev
```

## Configuration

Create a `.env` file in the root directory (see `example.env` for reference):

```env
# Solana Configuration
SOLANA_RPC_URL=https://api.mainnet-beta.solana.com
SOLANA_WS_URL=wss://api.mainnet-beta.solana.com

# Server Configuration
PORT=3000
NODE_ENV=production

# DEX Configuration
DEX_FORK_TYPE=Custom
FACTORY_ADDRESS=YOUR_FACTORY_PROGRAM_ID
ROUTER_ADDRESS=YOUR_ROUTER_PROGRAM_ID

# Protocol Fee Rate (default: 0.0025 = 0.25%)
PROTOCOL_FEE_RATE=0.0025

# Excluded DAOs (comma-separated list of PublicKeys)
EXCLUDED_DAOS=DAO1_PUBLIC_KEY,DAO2_PUBLIC_KEY
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SOLANA_RPC_URL` | Solana RPC endpoint URL | `https://api.mainnet-beta.solana.com` |
| `SOLANA_WS_URL` | Solana WebSocket endpoint URL | `wss://api.mainnet-beta.solana.com` |
| `PORT` | Server port | `3000` |
| `DEX_FORK_TYPE` | DEX fork type identifier | `Custom` |
| `FACTORY_ADDRESS` | Factory program address | (empty) |
| `ROUTER_ADDRESS` | Router program address | (empty) |
| `PROTOCOL_FEE_RATE` | Protocol fee rate for volume calculation | `0.0025` (0.25%) |
| `EXCLUDED_DAOS` | Comma-separated list of DAO addresses to exclude | (empty) |

## Additional Endpoints

### GET `/`

Get API information and available endpoints.

**Response:**
```json
{
  "name": "Futarchy AMM - CoinGecko API",
  "version": "1.0.0",
  "documentation": "https://docs.coingecko.com/reference/exchanges-list",
  "endpoints": {
    "tickers": "/api/tickers - Returns all DAO tickers",
    "historical_trades": "/api/historical_trades?ticker_id={TICKER_ID}",
    "health": "/health"
  },
  "dex": {
    "fork_type": "Custom",
    "factory_address": "YOUR_FACTORY_PROGRAM_ID",
    "router_address": "YOUR_ROUTER_PROGRAM_ID"
  },
  "note": "This API automatically discovers and aggregates all DAOs from the Futarchy protocol"
}
```

### GET `/api/historical_trades`

Returns historical trades (currently not implemented).

**Query Parameters:**
- `ticker_id` (optional): Format `{BASE_MINT}_{QUOTE_MINT}`

**Response:**
```json
{
  "buy": [],
  "sell": [],
  "note": "Historical trades require transaction monitoring - not yet implemented"
}
```

### GET `/health`

Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T00:00:00.000Z",
  "uptime": 3600.5
}
```

## Data Sources

### Price Calculation
- Prices are calculated from spot pool reserves only (not conditional/futarchy pools)
- Formula: `price = (quoteReserves / baseReserves) * 10^(baseDecimals - quoteDecimals)`

### Volume Calculation
- Volume is calculated from accumulated protocol fees
- Formula: `volume = protocolFees / feeRate`
- Uses the protocol fee rate configured in `PROTOCOL_FEE_RATE` (default: 0.25%)

### Liquidity Calculation
- Liquidity is calculated as: `2 * quoteReserves` (for stablecoin pairs)

## Rate Limiting

The API implements rate limiting:
- **60 requests per minute** per IP address
- Returns `429 Too Many Requests` when limit is exceeded

## Error Handling

### Standard Error Response
```json
{
  "error": "Error message"
}
```

### Common Error Codes
- `400`: Bad Request (missing/invalid parameters)
- `404`: Not Found (ticker/DAO not found)
- `429`: Too Many Requests (rate limit exceeded)
- `500`: Internal Server Error

## Caching

The API implements intelligent caching:
- **Tickers**: 10 seconds TTL
- **Token Metadata**: 100 seconds TTL (longer cache for static data)

## Development

### Running Tests

```bash
bun test
```

### Project Structure

```
src/
  ├── config.ts           # Configuration and environment variables
  ├── server.ts          # Express server and routes
  ├── services/
  │   ├── futarchyService.ts  # DAO and pool data fetching
  │   └── priceService.ts     # Price and volume calculations
  └── types/
      └── coingecko.ts   # TypeScript interfaces
```

## Notes

- The API automatically discovers all DAOs from the Futarchy protocol
- Only DAOs with valid pools (non-zero reserves) are included
- Prices are only calculated from spot pools, not conditional markets
- Volume is calculated from protocol fees, providing accurate trading volume
- Token metadata (symbols/names) is fetched from on-chain Metaplex Token Metadata
- DAOs can be excluded via the `EXCLUDED_DAOS` environment variable

## License

Private project
