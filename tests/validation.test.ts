import { describe, it, expect } from 'bun:test';
import {
  parseJsonParam,
  parseIntParam,
  parseDateParam,
  parseCommaSeparatedList,
  parseSolanaAddress,
  parseRequiredString,
} from '../src/utils/validation.js';

describe('Validation Helpers', () => {
  describe('parseJsonParam', () => {
    it('should return undefined for empty input', () => {
      const result = parseJsonParam(undefined, 'test');
      expect(result.success).toBe(true);
      if (result.success) expect(result.value).toBeUndefined();
    });

    it('should parse valid JSON', () => {
      const result = parseJsonParam('{"key": "value"}', 'test');
      expect(result.success).toBe(true);
      if (result.success) expect(result.value).toEqual({ key: 'value' });
    });

    it('should return error for invalid JSON', () => {
      const result = parseJsonParam('{invalid}', 'labels');
      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.error).toBe('Invalid JSON');
        expect(result.error.field).toBe('labels');
      }
    });
  });

  describe('parseIntParam', () => {
    it('should return default for empty input', () => {
      const result = parseIntParam(undefined, 'hours', { defaultValue: 24 });
      expect(result.success).toBe(true);
      if (result.success) expect(result.value).toBe(24);
    });

    it('should parse valid integer', () => {
      const result = parseIntParam('48', 'hours', { defaultValue: 24 });
      expect(result.success).toBe(true);
      if (result.success) expect(result.value).toBe(48);
    });

    it('should reject non-integer', () => {
      const result = parseIntParam('abc', 'hours', { defaultValue: 24 });
      expect(result.success).toBe(false);
      if (!result.success) expect(result.error.error).toBe('Invalid integer');
    });

    it('should enforce min bound', () => {
      const result = parseIntParam('0', 'hours', { defaultValue: 24, min: 1 });
      expect(result.success).toBe(false);
      if (!result.success) expect(result.error.error).toBe('Value too small');
    });

    it('should enforce max bound', () => {
      const result = parseIntParam('200', 'hours', { defaultValue: 24, max: 168 });
      expect(result.success).toBe(false);
      if (!result.success) expect(result.error.error).toBe('Value too large');
    });
  });

  describe('parseDateParam', () => {
    it('should return error for missing required date', () => {
      const result = parseDateParam(undefined, 'startDate', { required: true });
      expect(result.success).toBe(false);
      if (!result.success) expect(result.error.error).toBe('Missing required parameter');
    });

    it('should return undefined for optional empty date', () => {
      const result = parseDateParam(undefined, 'startDate', { required: false });
      expect(result.success).toBe(true);
      if (result.success) expect(result.value).toBeUndefined();
    });

    it('should parse valid date', () => {
      const result = parseDateParam('2024-01-15', 'startDate', { required: true });
      expect(result.success).toBe(true);
      if (result.success) expect(result.value).toBe('2024-01-15');
    });

    it('should reject invalid format', () => {
      const result = parseDateParam('01-15-2024', 'startDate', { required: true });
      expect(result.success).toBe(false);
      if (!result.success) expect(result.error.error).toBe('Invalid date format');
    });

    it('should reject invalid date', () => {
      const result = parseDateParam('2024-13-45', 'startDate', { required: true });
      expect(result.success).toBe(false);
    });
  });

  describe('parseCommaSeparatedList', () => {
    it('should return undefined for empty input', () => {
      const result = parseCommaSeparatedList(undefined, 'tokens');
      expect(result.success).toBe(true);
      if (result.success) expect(result.value).toBeUndefined();
    });

    it('should parse comma-separated values', () => {
      const result = parseCommaSeparatedList('a, b, c', 'tokens');
      expect(result.success).toBe(true);
      if (result.success) expect(result.value).toEqual(['a', 'b', 'c']);
    });

    it('should filter empty items', () => {
      const result = parseCommaSeparatedList('a,,b,  ,c', 'tokens');
      expect(result.success).toBe(true);
      if (result.success) expect(result.value).toEqual(['a', 'b', 'c']);
    });
  });

  describe('parseSolanaAddress', () => {
    it('should reject empty input', () => {
      const result = parseSolanaAddress(undefined, 'mintAddress');
      expect(result.success).toBe(false);
      if (!result.success) expect(result.error.error).toBe('Missing required parameter');
    });

    it('should accept valid Solana address', () => {
      const result = parseSolanaAddress('So11111111111111111111111111111111111111112', 'mintAddress');
      expect(result.success).toBe(true);
      if (result.success) expect(result.value).toBe('So11111111111111111111111111111111111111112');
    });

    it('should reject invalid characters', () => {
      const result = parseSolanaAddress('invalid0Oaddress', 'mintAddress');
      expect(result.success).toBe(false);
      if (!result.success) expect(result.error.error).toBe('Invalid Solana address');
    });

    it('should reject too short address', () => {
      const result = parseSolanaAddress('abc123', 'mintAddress');
      expect(result.success).toBe(false);
    });
  });

  describe('parseRequiredString', () => {
    it('should reject empty input', () => {
      const result = parseRequiredString(undefined, 'name');
      expect(result.success).toBe(false);
      if (!result.success) expect(result.error.error).toBe('Missing required parameter');
    });

    it('should accept valid string', () => {
      const result = parseRequiredString('test', 'name');
      expect(result.success).toBe(true);
      if (result.success) expect(result.value).toBe('test');
    });

    it('should trim whitespace', () => {
      const result = parseRequiredString('  test  ', 'name');
      expect(result.success).toBe(true);
      if (result.success) expect(result.value).toBe('test');
    });
  });
});
