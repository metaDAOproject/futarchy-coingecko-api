import { Router, type Request, type Response, type NextFunction } from 'express';
import { createHealthRouter } from './health.js';
import { createMetricsRouter } from './metrics.js';
import { createCoinGeckoRouter } from './coingecko.js';
import { createCoinMarketCapRouter } from './coinmarketcap.js';

import { createSupplyRouter } from './supply.js';
import { createMarketRouter } from './market.js';
import { createLaunchesRouter } from './launches.js';

import { createDexScreenerRouter } from './dexscreener.js';
import { createRootRouter } from './root.js';
import type { ServiceGetters } from './types.js';

export type { ServiceGetters } from './types.js';

export interface ApiDeprecation {
  /** When the version was deprecated (RFC 9745 `Deprecation` header). */
  since: Date;
  /** When it stops being served (RFC 8594 `Sunset` header). */
  sunset?: Date;
  /** Prefix of the replacement version, e.g. '/v2' (`Link: rel="successor-version"`). */
  successor: string;
}

interface ApiVersion {
  prefix: string;
  createRoutes: (services: ServiceGetters) => Router;
  deprecation?: ApiDeprecation;
}

/** The v1 API contract: every partner-facing data route. */
function createV1Routes(services: ServiceGetters): Router {
  const router = Router();
  router.use(createCoinGeckoRouter(services));
  router.use(createCoinMarketCapRouter(services));
  router.use(createSupplyRouter(services));
  router.use(createMarketRouter(services));
  router.use(createLaunchesRouter(services));
  router.use(createDexScreenerRouter(services));
  return router;
}

/**
 * Every served API version, mounted at its URL prefix (`/v1/api/tickers`, ...).
 *
 * - Ship a breaking change: add `{ prefix: '/v2', createRoutes: createV2Routes }`
 *   and leave v1 untouched.
 * - Deprecate a version: set `deprecation` on it. Every response under its
 *   prefix then carries `Deprecation`, `Sunset` and `Link: rel="successor-version"`
 *   headers so clients can discover the migration.
 * - Retire it: remove the entry after its sunset date.
 */
const API_VERSIONS: ApiVersion[] = [
  { prefix: '/v1', createRoutes: createV1Routes },
];

/**
 * The pre-versioning unversioned paths (`/api/tickers`, `/cmc/summary`, ...)
 * that existing partner integrations (CoinGecko, CMC, DexScreener, Jupiter)
 * are configured with. Frozen as an alias of v1; deprecate it here once
 * partners have moved to versioned URLs.
 */
const UNVERSIONED_ALIAS: { version: string; deprecation?: ApiDeprecation } = { version: '/v1' };

const toHttpDate = (date: Date): string => date.toUTCString();

/**
 * Advertise a version's deprecation (RFC 9745 / RFC 8594). `prefix` is the
 * deprecated version's mount path ('' for the unversioned alias).
 */
export function deprecationHeaders(prefix: string, deprecation: ApiDeprecation) {
  return (req: Request, res: Response, next: NextFunction): void => {
    res.setHeader('Deprecation', `@${Math.floor(deprecation.since.getTime() / 1000)}`);
    if (deprecation.sunset) res.setHeader('Sunset', toHttpDate(deprecation.sunset));
    const successorUrl = deprecation.successor + req.originalUrl.slice(prefix.length);
    res.setHeader('Link', `<${successorUrl}>; rel="successor-version"`);
    next();
  };
}

/** All versioned API routes plus the unversioned legacy alias. */
export function createApiRoutes(services: ServiceGetters): Router {
  const router = Router();
  const mount = (prefix: string, routes: Router, deprecation?: ApiDeprecation) => {
    const path = prefix || '/';
    if (deprecation) router.use(path, deprecationHeaders(prefix, deprecation), routes);
    else router.use(path, routes);
  };

  // Each version's router is built once; the unversioned alias mounts the same
  // instance, so the two paths share in-router state (e.g. DexScreener caches).
  const routersByPrefix = new Map<string, Router>();
  for (const version of API_VERSIONS) {
    const routes = version.createRoutes(services);
    routersByPrefix.set(version.prefix, routes);
    mount(version.prefix, routes, version.deprecation);
  }

  const aliased = routersByPrefix.get(UNVERSIONED_ALIAS.version);
  if (!aliased) throw new Error(`Unversioned alias targets unknown API version ${UNVERSIONED_ALIAS.version}`);
  mount('', aliased, UNVERSIONED_ALIAS.deprecation);

  return router;
}

/** Operational endpoints: health, metrics, API index. Not versioned. */
export function createInfraRoutes(services: ServiceGetters): Router {
  const router = Router();
  router.use(createHealthRouter(services));
  router.use(createMetricsRouter(services));
  router.use(createRootRouter(services));
  return router;
}
