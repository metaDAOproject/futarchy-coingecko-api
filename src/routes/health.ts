import { Router, type Request, type Response } from 'express';
import { logger } from '../utils/logger.js';
import type { ServiceGetters } from './types.js';
import type { ServedDataFreshness } from '../services/externalDatabaseService.js';
import { withTimeout } from '../utils/resilience.js';

// Below the recommended 3s probe timeout, so a hung DB is reported as a 503
// by the API rather than as a probe timeout.
const READINESS_DB_TIMEOUT_MS = 2_000;

/**
 * Container probes for the orchestrator (Northflank / Kubernetes). Mounted
 * before the rate limiter and request metrics: probes arrive from inside the
 * container (all from 127.0.0.1, one shared rate-limit bucket), and a 429 on
 * the liveness probe would restart a healthy container.
 *
 * - startup:   200 once the served DB connected at boot. If it never did, the
 *              API would otherwise run degraded forever (the DB health-check
 *              loop that recovers connections only starts after a successful
 *              initial connect), so failing here gets the container restarted.
 * - readiness: 200 while the served DB answers `SELECT 1` within 2s; 503
 *              takes this container out of the load balancer.
 * - liveness:  200 whenever the event loop can answer. No dependency checks,
 *              so a DB or RPC outage never triggers a restart loop.
 */
export function createProbeRouter(services: ServiceGetters): Router {
  const router = Router();
  const { getExternalDatabaseService } = services;

  router.get('/health/startup', (_req: Request, res: Response) => {
    if (getExternalDatabaseService()?.isAvailable()) {
      res.json({ status: 'ok' });
    } else {
      res.status(503).json({ status: 'starting', message: 'Served database not connected' });
    }
  });

  router.get('/health/ready', async (req: Request, res: Response) => {
    const externalDatabaseService = getExternalDatabaseService();
    try {
      if (!externalDatabaseService?.isAvailable()) {
        throw new Error('Served database not connected');
      }
      await withTimeout(externalDatabaseService.query('SELECT 1'), {
        timeoutMs: READINESS_DB_TIMEOUT_MS,
        timeoutMessage: `Served database ping timed out after ${READINESS_DB_TIMEOUT_MS}ms`,
      });
      res.json({ status: 'ok' });
    } catch (error) {
      logger.warn('Readiness probe failed', {
        requestId: req.requestId,
        error: error instanceof Error ? error.message : String(error),
      });
      res.status(503).json({ status: 'unavailable', message: 'Served database not reachable' });
    }
  });

  router.get('/health/live', (_req: Request, res: Response) => {
    res.json({ status: 'ok' });
  });

  return router;
}

export function createHealthRouter(services: ServiceGetters): Router {
  const router = Router();
  const { getExternalDatabaseService } = services;

  // Liveness: is the process up? Never touches a dependency.
  router.get('/health', (req: Request, res: Response) => {
    res.json({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
    });
  });

  // Readiness / comprehensive health. The served (external) ETL DB is the
  // API's only data dependency, so health is modeled on its three axes:
  // connectivity, data contract, and data freshness.
  router.get('/api/health', async (req: Request, res: Response) => {
    const externalDatabaseService = getExternalDatabaseService();
    const connected = !!externalDatabaseService?.isAvailable();

    const servedDataContract = connected
      ? await externalDatabaseService!.checkServedDataContract()
      : {
          ok: false,
          checkedAt: new Date().toISOString(),
          missing: ['connection'],
        };

    let freshness: ServedDataFreshness | null = null;
    let freshnessError = false;
    if (connected) {
      try {
        freshness = await externalDatabaseService!.getServedDataFreshness();
      } catch (error) {
        freshnessError = true;
        logger.error('Health: served DB freshness check failed', error, { requestId: req.requestId });
      }
    }

    const health: Record<string, any> = {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      uptime: process.uptime(),
      servedDatabase: {
        connected,
        servedDataContract,
        freshness,
      },
    };

    if (!connected) {
      health.status = 'degraded';
      health.message = 'Served (external) indexer database not connected';
    } else if (!servedDataContract.ok) {
      health.status = 'degraded';
      health.message = 'Served ETL contract check failed';
    } else if (freshnessError) {
      health.status = 'degraded';
      health.message = 'Served DB freshness check failed';
    }

    res.json(health);
  });

  return router;
}
