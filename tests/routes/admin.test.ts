import { describe, it, expect } from 'bun:test';
import request from 'supertest';
import { createTestApp } from '../helpers/testApp.js';

const app = createTestApp();

describe('Admin Routes', () => {
  describe('GET /api/cache/status', () => {
    it('should return cache status or not configured error', async () => {
      const response = await request(app).get('/api/cache/status');
      
      // Either returns status (200) or not configured (400)
      expect([200, 400]).toContain(response.status);
      
      if (response.status === 200) {
        expect(response.body).toHaveProperty('lastUpdated');
        expect(response.body).toHaveProperty('isRefreshing');
        expect(response.body).toHaveProperty('poolMetricsCount');
        expect(response.body).toHaveProperty('cacheAgeSeconds');
        expect(response.body).toHaveProperty('isInitialized');
      } else {
        expect(response.body.error).toBe('Dune cache service not configured');
      }
    });
  });

  describe('POST /api/cache/refresh', () => {
    it('should start refresh or return not configured', async () => {
      const response = await request(app).post('/api/cache/refresh');
      
      // Either starts refresh, already in progress, or not configured
      expect([200, 400, 409]).toContain(response.status);
    });
  });

  describe('GET /api/ten-minute-volume/status', () => {
    it('should return status or not configured error', async () => {
      const response = await request(app).get('/api/ten-minute-volume/status');
      
      expect([200, 400]).toContain(response.status);
      
      if (response.status === 200) {
        expect(response.body).toHaveProperty('isInitialized');
        expect(response.body).toHaveProperty('isRunning');
        expect(response.body).toHaveProperty('databaseConnected');
      }
    });
  });


});
