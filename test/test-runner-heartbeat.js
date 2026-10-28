// -----------------------------------------------------------------------------
// test/test-runner-heartbeat.js
//
// Unit tests for lib/runner-heartbeat.js: each runner keeps its id in a
// sorted set scored by the time it last checked in, so live runners can be
// counted without trusting a counter that hard-killed pods never decrement.
// Redis, the clock and the timer API are all injected.
// -----------------------------------------------------------------------------
'use strict';

const assert = require('assert');
const { createRunnerHeartbeat, countLiveRunners } = require('../lib/runner-heartbeat');

// Minimal sorted-set redis fake: key -> Map(member -> score).
function makeFakeRedis() {
  const zsets = new Map();
  const ttls = new Map();
  const zset = (k) => { if (!zsets.has(k)) zsets.set(k, new Map()); return zsets.get(k); };
  return {
    zsets,
    ttls,
    zAdd: async (k, { score, value }) => { zset(k).set(value, score); return 1; },
    zRem: async (k, member) => zset(k).delete(member) ? 1 : 0,
    zCount: async (k, min, max) => {
      const lo = Number(min);
      const hi = max === '+inf' ? Infinity : Number(max);
      return [...zset(k).values()].filter(s => s >= lo && s <= hi).length;
    },
    expire: async (k, ttl) => { ttls.set(k, ttl); return 1; },
  };
}

function makeFakeTimers() {
  let nextId = 1;
  const active = new Map();
  return {
    active,
    setInterval: (fn, ms) => { const id = nextId++; active.set(id, { fn, ms }); return id; },
    clearInterval: (id) => { active.delete(id); },
    tick: async () => { for (const { fn } of active.values()) await fn(); },
  };
}

function makeHeartbeat(redis, timers, clock) {
  return createRunnerHeartbeat({
    redis,
    key: 'exec:runners_heartbeat',
    runnerId: 'runner-1',
    intervalMs: 30000,
    ttlSec: 3600,
    now: () => clock.now,
    setInterval: timers.setInterval,
    clearInterval: timers.clearInterval,
  });
}

describe('runner heartbeat', function () {

  it('checks in immediately on start, with the current time and a TTL', async function () {
    const redis = makeFakeRedis();
    const clock = { now: 1000 };
    const heartbeat = makeHeartbeat(redis, makeFakeTimers(), clock);

    await heartbeat.start();

    assert.strictEqual(redis.zsets.get('exec:runners_heartbeat').get('runner-1'), 1000);
    assert.strictEqual(redis.ttls.get('exec:runners_heartbeat'), 3600);
  });

  it('checks in again on every interval tick', async function () {
    const redis = makeFakeRedis();
    const timers = makeFakeTimers();
    const clock = { now: 1000 };
    const heartbeat = makeHeartbeat(redis, timers, clock);

    await heartbeat.start();
    clock.now = 31000;
    await timers.tick();

    assert.strictEqual([...timers.active.values()][0].ms, 30000);
    assert.strictEqual(redis.zsets.get('exec:runners_heartbeat').get('runner-1'), 31000);
  });

  it('stop clears the timer and removes the runner', async function () {
    const redis = makeFakeRedis();
    const timers = makeFakeTimers();
    const heartbeat = makeHeartbeat(redis, timers, { now: 1000 });

    await heartbeat.start();
    await heartbeat.stop();

    assert.strictEqual(timers.active.size, 0);
    assert.strictEqual(redis.zsets.get('exec:runners_heartbeat').has('runner-1'), false);
  });

  it('never throws when redis fails', async function () {
    const failing = {
      zAdd: async () => { throw new Error('down'); },
      zRem: async () => { throw new Error('down'); },
      expire: async () => { throw new Error('down'); },
    };
    const timers = makeFakeTimers();
    const heartbeat = makeHeartbeat(failing, timers, { now: 1000 });

    await heartbeat.start();
    await timers.tick();
    await heartbeat.stop();
  });

  it('countLiveRunners counts only runners seen within the window', async function () {
    const redis = makeFakeRedis();
    await redis.zAdd('exec:runners_heartbeat', { score: 5000, value: 'stale' });
    await redis.zAdd('exec:runners_heartbeat', { score: 95000, value: 'fresh' });
    await redis.zAdd('exec:runners_heartbeat', { score: 100000, value: 'fresher' });

    const live = await countLiveRunners(redis, 'exec:runners_heartbeat', 90000, () => 100000);

    assert.strictEqual(live, 2);
  });
});
