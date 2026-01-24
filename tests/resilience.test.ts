import { describe, it, expect, mock } from 'bun:test';
import {
  withTimeout,
  retry,
  withRetryAndTimeout,
  isTransientError,
  TimeoutError,
  RetryExhaustedError,
} from '../src/utils/resilience.js';

describe('Resilience Helpers', () => {
  describe('withTimeout', () => {
    it('should resolve if promise completes before timeout', async () => {
      const result = await withTimeout(
        Promise.resolve('success'),
        { timeoutMs: 1000 }
      );
      expect(result).toBe('success');
    });

    it('should reject with TimeoutError if promise exceeds timeout', async () => {
      const slowPromise = new Promise(resolve => setTimeout(() => resolve('done'), 200));
      
      try {
        await withTimeout(slowPromise, { timeoutMs: 50 });
        expect(true).toBe(false); // Should not reach here
      } catch (error) {
        expect(error).toBeInstanceOf(TimeoutError);
        expect((error as TimeoutError).timeoutMs).toBe(50);
      }
    });

    it('should use custom timeout message', async () => {
      const slowPromise = new Promise(resolve => setTimeout(() => resolve('done'), 200));
      
      try {
        await withTimeout(slowPromise, { 
          timeoutMs: 50, 
          timeoutMessage: 'Custom timeout message' 
        });
      } catch (error) {
        expect((error as Error).message).toBe('Custom timeout message');
      }
    });

    it('should propagate errors from the promise', async () => {
      const failingPromise = Promise.reject(new Error('Original error'));
      
      try {
        await withTimeout(failingPromise, { timeoutMs: 1000 });
      } catch (error) {
        expect((error as Error).message).toBe('Original error');
      }
    });
  });

  describe('retry', () => {
    it('should succeed on first attempt if no error', async () => {
      const fn = mock(() => Promise.resolve('success'));
      
      const result = await retry(fn, { maxRetries: 3 });
      
      expect(result).toBe('success');
      expect(fn).toHaveBeenCalledTimes(1);
    });

    it('should retry on failure and succeed', async () => {
      let attempts = 0;
      const fn = mock(() => {
        attempts++;
        if (attempts < 3) {
          return Promise.reject(new Error('Temporary failure'));
        }
        return Promise.resolve('success');
      });
      
      const result = await retry(fn, { 
        maxRetries: 3, 
        initialDelayMs: 10 
      });
      
      expect(result).toBe('success');
      expect(fn).toHaveBeenCalledTimes(3);
    });

    it('should throw after exhausting retries', async () => {
      const fn = mock(() => Promise.reject(new Error('Persistent failure')));
      
      try {
        await retry(fn, { maxRetries: 2, initialDelayMs: 10 });
        expect(true).toBe(false);
      } catch (error) {
        expect((error as Error).message).toBe('Persistent failure');
        expect(fn).toHaveBeenCalledTimes(3); // initial + 2 retries
      }
    });

    it('should respect isRetryable predicate', async () => {
      const fn = mock(() => Promise.reject(new Error('Non-retryable')));
      
      try {
        await retry(fn, { 
          maxRetries: 3, 
          isRetryable: () => false 
        });
      } catch (error) {
        expect(fn).toHaveBeenCalledTimes(1); // No retries
      }
    });

    it('should call onRetry callback', async () => {
      let retryCount = 0;
      let attempts = 0;
      const fn = mock(() => {
        attempts++;
        if (attempts < 2) {
          return Promise.reject(new Error('Fail'));
        }
        return Promise.resolve('success');
      });
      
      await retry(fn, { 
        maxRetries: 3, 
        initialDelayMs: 10,
        onRetry: (attempt) => { retryCount = attempt; }
      });
      
      expect(retryCount).toBe(1);
    });
  });

  describe('withRetryAndTimeout', () => {
    it('should succeed with both timeout and retry', async () => {
      const fn = mock(() => Promise.resolve('success'));
      
      const result = await withRetryAndTimeout(fn, {
        timeoutMs: 1000,
        maxRetries: 2,
      });
      
      expect(result).toBe('success');
    });

    it('should retry on timeout', async () => {
      let attempts = 0;
      const fn = mock(() => {
        attempts++;
        if (attempts < 2) {
          return new Promise(resolve => setTimeout(() => resolve('slow'), 200));
        }
        return Promise.resolve('fast');
      });
      
      const result = await withRetryAndTimeout(fn, {
        timeoutMs: 50,
        maxRetries: 2,
        initialDelayMs: 10,
      });
      
      expect(result).toBe('fast');
      expect(fn).toHaveBeenCalledTimes(2);
    });
  });

  describe('isTransientError', () => {
    it('should return true for TimeoutError', () => {
      expect(isTransientError(new TimeoutError('timeout', 1000))).toBe(true);
    });

    it('should return true for network errors', () => {
      expect(isTransientError(new Error('ECONNRESET'))).toBe(true);
      expect(isTransientError(new Error('socket hang up'))).toBe(true);
      expect(isTransientError(new Error('fetch failed'))).toBe(true);
    });

    it('should return true for rate limit errors', () => {
      expect(isTransientError(new Error('rate limit exceeded'))).toBe(true);
      expect(isTransientError(new Error('429 Too Many Requests'))).toBe(true);
    });

    it('should return true for temporary server errors', () => {
      expect(isTransientError(new Error('502 Bad Gateway'))).toBe(true);
      expect(isTransientError(new Error('503 Service Unavailable'))).toBe(true);
    });

    it('should return false for non-transient errors', () => {
      expect(isTransientError(new Error('Invalid input'))).toBe(false);
      expect(isTransientError(new Error('Not found'))).toBe(false);
    });

    it('should check status property on objects', () => {
      expect(isTransientError({ status: 429 })).toBe(true);
      expect(isTransientError({ status: 503 })).toBe(true);
      expect(isTransientError({ status: 404 })).toBe(false);
    });
  });
});
