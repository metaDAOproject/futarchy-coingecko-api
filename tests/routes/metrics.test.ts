import { describe, it, expect } from 'bun:test';
import request from 'supertest';
import { createTestApp } from '../helpers/testApp.js';

const app = createTestApp();

describe('Metrics Routes', () => {
  describe('GET /metrics', () => {
    it('should return Prometheus metrics', async () => {
      const response = await request(app).get('/metrics');

      expect(response.status).toBe(200);
      expect(response.headers['content-type']).toContain('text/plain');
    });

    it('should include standard metrics', async () => {
      const response = await request(app).get('/metrics');

      expect(response.status).toBe(200);
      // Prometheus format includes HELP and TYPE comments
      expect(response.text).toContain('# HELP');
      expect(response.text).toContain('# TYPE');
    });

    it('labels unmatched paths as one series, so path scans cannot grow metrics without bound', async () => {
      await request(app).get('/wp-admin/scan-xyz-123');
      const response = await request(app).get('/metrics');

      expect(response.text).toContain('path="unmatched"');
      expect(response.text).not.toContain('scan-xyz');
    });

    it('labels matched requests by route pattern, never by the requested URL', async () => {
      await request(app).get('/api/supply/aaa111/total'); // matches :mintAddress, then 400s
      const response = await request(app).get('/metrics');

      expect(response.text).toContain('path="/api/supply/:mintAddress/total"');
      expect(response.text).not.toContain('aaa111');
    });

    it('should expose the served DB connectivity gauge', async () => {
      const response = await request(app).get('/metrics');

      expect(response.text).toContain('futarchy_served_db_connected 1');
    });
  });
});
