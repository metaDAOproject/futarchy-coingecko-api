import { PublicKey } from '@solana/web3.js';

// Numeric env vars are validated at startup: a typo (e.g. "30s") must fail
// fast, not become NaN — a NaN TTL silently disables caching, a NaN port or
// rate limit breaks serving.
function intEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer, got "${raw}"`);
  }
  return value;
}

function fractionEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0 || value >= 1) {
    throw new Error(`${name} must be a number in [0, 1), got "${raw}"`);
  }
  return value;
}

export const config = {
  solana: {
    rpcUrl: process.env.RPCPOOL_RPC_URL || process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com',
    // Per-request RPC timeout (ms). Generous enough for getProgramAccounts scans.
    rpcTimeoutMs: intEnv('RPC_TIMEOUT_MS', 20000),
  },
  server: {
    port: intEnv('PORT', 3000),
    // Request timeout in milliseconds (default: 5 minutes)
    requestTimeout: intEnv('SERVER_REQUEST_TIMEOUT', 300000),
    // Keep-alive timeout in milliseconds (default: 5 minutes)
    keepAliveTimeout: intEnv('SERVER_KEEP_ALIVE_TIMEOUT', 300000),
    // Number of reverse-proxy hops in front of this process. Express uses it to
    // resolve the real client IP from X-Forwarded-For for per-IP rate limiting.
    // 0 = no proxy (req.ip is the socket peer). Use the exact hop count — a
    // blanket "trust everything" would let clients spoof their IP via XFF.
    trustProxyHops: intEnv('TRUST_PROXY_HOPS', 0),
    // `Cache-Control: private, max-age=N` on successful data responses, so
    // clients can reuse a response for N seconds. Errors and health/metrics are no-store.
    cacheMaxAgeSeconds: intEnv('CACHE_CONTROL_MAX_AGE', 30),
    // On SIGTERM, report not-ready and keep serving this long (ms) so the load
    // balancer stops routing here before the listener closes. Kubernetes
    // removes a terminating pod from its endpoints right away; this covers the
    // propagation to the ingress. Keep drain + 15s close budget under the
    // platform's termination grace period (Kubernetes default 30s).
    shutdownDrainMs: intEnv('SHUTDOWN_DRAIN_MS', 10000),
    rateLimit: {
      windowMs: 60000, // 1 minute
      maxRequests: 60, // 60 requests per minute
    },
    trustedApiKeys: new Set<string>(
      (process.env.TRUSTED_API_KEYS || '')
        .split(',')
        .map(k => k.trim())
        .filter(Boolean)
    ),
    trustedRateLimit: {
      windowMs: 60_000,
      maxRequests: intEnv('TRUSTED_RATE_LIMIT_MAX', 600),
    },
  },
  cache: {
    // TTL for blockchain data cache in milliseconds (default: 55 seconds).
    // Consumers (CoinGecko/DexScreener pollers) read about once per minute, so a
    // sub-minute TTL keeps every poll fresher than its cadence while cutting the
    // full DAO RPC scan from ~6x/minute to ~1x/minute.
    // Lower = more real-time prices but more RPC calls.
    tickersTTL: intEnv('CACHE_TICKERS_TTL', 55000),
    // Once the DAO snapshot is older than tickersTTL it is refreshed in the
    // background while the previous snapshot keeps being served, up to this
    // age (ms). Past it, requests wait for a fresh scan and fail (5xx) if the
    // RPC is down, rather than serve prices older than this.
    tickersMaxStale: intEnv('CACHE_TICKERS_MAX_STALE', 300000),
    // TTL for the /api/launches/live snapshot (default: 5 minutes). Each refresh
    // scans every launch account plus the funding records of each live launch.
    liveLaunchesTTL: intEnv('CACHE_LIVE_LAUNCHES_TTL', 300000),
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
    // Protocol fee rate (0.005 = 0.5%); used to report fee bps on DexScreener routes.
    protocolFeeRate: fractionEnv('PROTOCOL_FEE_RATE', 0.005),
  },
  coinmarketcap: {
    // Optional allowlist of base-mint addresses exposed on the CoinMarketCap
    // routes. Empty (the default) means "serve every discovered DAO", matching
    // the CoinGecko/DexScreener adapters. When set, ONLY these base mints appear
    // — this is how we map our tokens onto CMC's expected asset ids without
    // leaking test/never-listed DAOs into the CMC feed.
    //
    // Each entry is validated as a Solana pubkey at startup (like EXCLUDED_DAOS):
    // a typo throws here — fail fast — rather than silently filtering every pair
    // and serving an empty /cmc feed that a poller would read as "delisted". The
    // normalized base58 form is stored so lookups match baseMint.toString().
    allowedMints: new Set<string>(
      (process.env.CMC_ALLOWED_MINTS || '')
        .split(',')
        .map(m => m.trim())
        .filter(Boolean)
        .map(m => new PublicKey(m).toString())
    ),
  },
  alerts: {
    webhookUrl: process.env.ALERT_WEBHOOK_URL || 'https://telegram-webhook-relay.themetadao-org.workers.dev',
    webhookSecret: process.env.ALERT_WEBHOOK_SECRET || '',
  },
  externalDatabase: {
    // Read-only connection to the served ETL DB — the ONLY database this API uses.
    // (The old app DB is fully removed; any future write goes to the prod DB.)
    connectionString: process.env.DATABASE_PG_URL || '',
    ssl: process.env.DATABASE_PG_SSL === 'true',
    // PEM CA certificate (the cert content, not a path) for verifying a server
    // signed by a private CA. With SSL on and no CA cert, system CAs are used.
    caCert: process.env.DATABASE_PG_CA_CERT || '',
    // Explicit opt-out of TLS server verification (legacy/self-signed setups).
    // Encrypts but does NOT authenticate the server — set only as a stopgap.
    sslNoVerify: process.env.DATABASE_PG_SSL_NO_VERIFY === 'true',
  },
  heartbeat: {
    // Background self-check cadence (served DB connectivity, data freshness,
    // contract drift). 0 disables the heartbeat entirely.
    intervalMs: intEnv('HEARTBEAT_INTERVAL_MS', 60000),
    // Alert when the newest user_pool swap is older than this (seconds).
    // 0 disables the staleness alert (connectivity/contract alerts remain).
    maxDataAgeSeconds: intEnv('HEARTBEAT_MAX_DATA_AGE_SECONDS', 21600),
    // Run the served-data contract check every Nth heartbeat tick.
    contractCheckEveryTicks: 10,
  },
};
