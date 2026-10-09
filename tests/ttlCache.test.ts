import { describe, it, expect } from 'bun:test';
import { TtlCache } from '../src/utils/ttlCache.js';

describe('TtlCache', () => {
  it('stays within maxEntries, evicting the oldest write', () => {
    const cache = new TtlCache(3);
    for (const key of ['a', 'b', 'c', 'd']) cache.set(key, key);

    expect(cache.size).toBe(3);
    expect(cache.get('a', 60_000)).toBeUndefined();
    expect(cache.get('d', 60_000)).toBe('d');
  });

  it('shares one in-flight load per key, caches null, and does not cache failures', async () => {
    const cache = new TtlCache(10);
    let loads = 0;
    const load = async () => { loads++; return null; };

    const results = await Promise.all([cache.getOrLoad('k', 60_000, load), cache.getOrLoad('k', 60_000, load)]);
    expect(results).toEqual([null, null]);
    expect(loads).toBe(1);
    await cache.getOrLoad('k', 60_000, load); // cached null is a hit
    expect(loads).toBe(1);

    await expect(cache.getOrLoad('bad', 60_000, async () => { throw new Error('RPC down'); })).rejects.toThrow('RPC down');
    expect(await cache.getOrLoad('bad', 60_000, async () => 'ok')).toBe('ok');
  });
});
