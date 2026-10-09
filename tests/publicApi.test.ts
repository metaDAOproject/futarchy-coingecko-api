import { describe, it, expect } from 'bun:test';
import request from 'supertest';
import { createTestApp } from './helpers/testApp.js';
import type { LaunchpadService } from '../src/services/launchpadService.js';

const launchpadService = {
  getLiveLaunches: async () => ({ updatedAt: '2026-10-09T00:00:00.000Z', launches: [] }),
} as unknown as LaunchpadService;
const app = createTestApp({ launchpadService });

describe('Public API surface', () => {
  it('answers unknown routes with a JSON 404, never cached', async () => {
    const res = await request(app).get('/no/such/route');

    expect(res.status).toBe(404);
    expect(res.body).toMatchObject({ error: 'Not found', code: 'NOT_FOUND' });
    expect(res.headers['cache-control']).toBe('no-store');
  });

  it('marks successful data responses cacheable, and errors and health no-store', async () => {
    const data = await request(app).get('/api/launches/live');
    const badRequest = await request(app).get('/api/supply/not-a-mint/total');
    const health = await request(app).get('/health');

    expect(data.status).toBe(200);
    expect(data.headers['cache-control']).toBe('public, max-age=30');
    expect(badRequest.status).toBe(400);
    expect(badRequest.headers['cache-control']).toBe('no-store');
    expect(health.headers['cache-control']).toBe('no-store');
  });

  it('rejects a repeated query parameter with 400 instead of failing on an array', async () => {
    const res = await request(app).get('/dexscreener/asset?id=a&id=b');

    expect(res.status).toBe(400);
    expect(res.body.code).toBe('INVALID_QUERY_PARAMETER');
  });

  it('answers CORS preflight directly, allowing the API key header', async () => {
    const res = await request(app)
      .options('/api/tickers')
      .set('Origin', 'https://example.com')
      .set('Access-Control-Request-Headers', 'x-api-key');

    expect(res.status).toBe(204);
    expect(res.headers['access-control-allow-origin']).toBe('*');
    expect(res.headers['access-control-allow-headers']).toContain('X-API-Key');
  });

  it('sends security headers and hides the framework', async () => {
    const res = await request(app).get('/health');

    expect(res.headers['x-content-type-options']).toBe('nosniff');
    expect(res.headers['x-powered-by']).toBeUndefined();
  });

  it('echoes a safe X-Request-Id and replaces an unsafe one', async () => {
    const safe = await request(app).get('/health').set('X-Request-Id', 'abc-123');
    const unsafe = await request(app).get('/health').set('X-Request-Id', 'a'.repeat(500));

    expect(safe.headers['x-request-id']).toBe('abc-123');
    expect(unsafe.headers['x-request-id']).not.toBe('a'.repeat(500));
  });
});
