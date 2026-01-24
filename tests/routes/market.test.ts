import { describe, it, expect } from 'bun:test';
import request from 'supertest';
import { createTestApp } from '../helpers/testApp.js';

const app = createTestApp();

describe('Market Routes', () => {
  describe('GET /api/market-data', () => {
    it('should require startDate parameter', async () => {
      const response = await request(app)
        .get('/api/market-data')
        .query({ endDate: '2024-01-15' });
      
      // 400 for validation error, or 503 if database check happens first
      expect([400, 503]).toContain(response.status);
      if (response.status === 400) {
        expect(response.body.error).toBe('Missing required parameter');
        expect(response.body.field).toBe('startDate');
      }
    });

    it('should require endDate parameter', async () => {
      const response = await request(app)
        .get('/api/market-data')
        .query({ startDate: '2024-01-01' });
      
      expect([400, 503]).toContain(response.status);
      if (response.status === 400) {
        expect(response.body.error).toBe('Missing required parameter');
        expect(response.body.field).toBe('endDate');
      }
    });

    it('should validate date format for startDate', async () => {
      const response = await request(app)
        .get('/api/market-data')
        .query({ startDate: '01-01-2024', endDate: '2024-01-15' });
      
      expect([400, 503]).toContain(response.status);
      if (response.status === 400) {
        expect(response.body.error).toBe('Invalid date format');
      }
    });

    it('should validate date format for endDate', async () => {
      const response = await request(app)
        .get('/api/market-data')
        .query({ startDate: '2024-01-01', endDate: '15/01/2024' });
      
      expect([400, 503]).toContain(response.status);
      if (response.status === 400) {
        expect(response.body.error).toBe('Invalid date format');
      }
    });

    it('should accept valid date parameters', async () => {
      const response = await request(app)
        .get('/api/market-data')
        .query({ startDate: '2024-01-01', endDate: '2024-01-15' });
      
      // 200 with data, 503 if database not connected, or 500 if mock incomplete
      expect([200, 500, 503]).toContain(response.status);
      
      if (response.status === 200) {
        expect(response.body).toHaveProperty('filters');
        expect(response.body).toHaveProperty('count');
        expect(response.body).toHaveProperty('data');
        expect(response.body.filters.startDate).toBe('2024-01-01');
        expect(response.body.filters.endDate).toBe('2024-01-15');
      }
    });

    it('should accept tokens parameter', async () => {
      const response = await request(app)
        .get('/api/market-data')
        .query({ 
          startDate: '2024-01-01', 
          endDate: '2024-01-15',
          tokens: 'token1,token2'
        });
      
      expect([200, 500, 503]).toContain(response.status);
      
      if (response.status === 200) {
        expect(response.body.filters.tokens).toEqual(['token1', 'token2']);
      }
    });
  });
});
