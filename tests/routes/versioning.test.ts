import { describe, it, expect } from 'bun:test';
import express from 'express';
import request from 'supertest';
import { createTestApp } from '../helpers/testApp.js';
import { deprecationHeaders } from '../../src/routes/index.js';
import type { LaunchpadService } from '../../src/services/launchpadService.js';

const launchpadService = {
  getLiveLaunches: async () => ({ updatedAt: '2026-10-09T00:00:00.000Z', launches: [] }),
} as unknown as LaunchpadService;
const app = createTestApp({ launchpadService });

describe('API versioning', () => {
  it('serves every data route under /v1 identically to its unversioned alias', async () => {
    for (const path of [
      '/api/launches/live',
      '/api/supply/So11111111111111111111111111111111111111112/total',
      '/cmc/assets',
      '/api/market-data?startDate=2026-01-01&endDate=2026-01-31',
    ]) {
      const unversioned = await request(app).get(path);
      const versioned = await request(app).get(`/v1${path}`);

      expect(unversioned.status).toBe(200);
      expect(versioned.status).toBe(unversioned.status);
      expect(versioned.body).toEqual(unversioned.body);
    }
  });

  it('keeps health, probe and metrics endpoints unversioned', async () => {
    expect((await request(app).get('/health/live')).status).toBe(200);
    expect((await request(app).get('/v1/health/live')).status).toBe(404);
    expect((await request(app).get('/v1/api/health')).status).toBe(404);
  });

  it('does not mark the current version or the alias as deprecated', async () => {
    for (const path of ['/v1/api/launches/live', '/api/launches/live']) {
      const res = await request(app).get(path);
      expect(res.headers.deprecation).toBeUndefined();
      expect(res.headers.sunset).toBeUndefined();
    }
  });

  it('advertises deprecation, sunset and the successor URL for a deprecated version', async () => {
    const deprecation = {
      since: new Date('2026-11-01T00:00:00Z'),
      sunset: new Date('2027-05-01T00:00:00Z'),
      successor: '/v2',
    };
    const versioned = express();
    versioned.use('/v1', deprecationHeaders('/v1', deprecation), (_req, res) => { res.json({}); });
    const unversioned = express();
    unversioned.use(deprecationHeaders('', deprecation), (_req, res) => { res.json({}); });

    const v1 = await request(versioned).get('/v1/api/tickers?x=1');
    expect(v1.headers.deprecation).toBe('@1793491200');
    expect(v1.headers.sunset).toBe('Sat, 01 May 2027 00:00:00 GMT');
    expect(v1.headers.link).toBe('</v2/api/tickers?x=1>; rel="successor-version"');

    const legacy = await request(unversioned).get('/cmc/summary');
    expect(legacy.headers.link).toBe('</v2/cmc/summary>; rel="successor-version"');
  });
});
