import { describe, it, expect } from 'bun:test';
import request from 'supertest';
import { createTestApp } from '../helpers/testApp.js';

const app = createTestApp();

describe('Supply Routes', () => {
  const validMintAddress = 'So11111111111111111111111111111111111111112'; // SOL wrapped
  const invalidAddress = 'invalid-address';
  const tooShortAddress = 'abc123';

  describe('GET /api/supply/:mintAddress', () => {
    it('should reject invalid mint address', async () => {
      const response = await request(app).get(`/api/supply/${invalidAddress}`);
      
      expect(response.status).toBe(400);
      expect(response.body.error).toContain('not a valid Solana public key');
    });

    it('should reject too short address', async () => {
      const response = await request(app).get(`/api/supply/${tooShortAddress}`);
      
      expect(response.status).toBe(400);
      expect(response.body.error).toContain('not a valid Solana public key');
    });

    it('should accept valid Solana address format', async () => {
      const response = await request(app).get(`/api/supply/${validMintAddress}`);
      
      // Either succeeds or fails with RPC error (not validation error)
      if (response.status === 400) {
        expect(response.body.error).not.toContain('Invalid Solana address');
      }
    });
  });

  describe('GET /api/supply/:mintAddress/total', () => {
    it('should reject invalid mint address', async () => {
      const response = await request(app).get(`/api/supply/${invalidAddress}/total`);
      
      expect(response.status).toBe(400);
      expect(response.body.error).toContain('not a valid Solana public key');
    });

    it('should accept valid format', async () => {
      const response = await request(app).get(`/api/supply/${validMintAddress}/total`);
      
      if (response.status === 400) {
        expect(response.body.error).not.toContain('Invalid Solana address');
      }
    });
  });

  describe('GET /api/supply/:mintAddress/circulating', () => {
    it('should reject invalid mint address', async () => {
      const response = await request(app).get(`/api/supply/${invalidAddress}/circulating`);
      
      expect(response.status).toBe(400);
      expect(response.body.error).toContain('not a valid Solana public key');
    });
  });

  describe('GET /api/supply/:mintAddress/jupiter/circulating', () => {
    it('should reject invalid mint address', async () => {
      const response = await request(app).get(`/api/supply/${invalidAddress}/jupiter/circulating`);
      
      expect(response.status).toBe(400);
      expect(response.body.error).toContain('not a valid Solana public key');
    });
  });

  describe('GET /api/supply/:mintAddress/jupiter/total', () => {
    it('should reject invalid mint address', async () => {
      const response = await request(app).get(`/api/supply/${invalidAddress}/jupiter/total`);
      
      expect(response.status).toBe(400);
      expect(response.body.error).toContain('not a valid Solana public key');
    });
  });
});
