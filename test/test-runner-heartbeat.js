// -----------------------------------------------------------------------------
// test/test-runner-heartbeat.js
//
// Unit tests for lib/runner-heartbeat.js: each runner keeps its id in a
// sorted set scored by the time it last checked in, so live runners can be
// counted without trusting a counter that hard-killed pods never decrement.
// The same beat refreshes every claim the runner holds, so a claim lives
// exactly as long as its runner keeps beating. Redis, the clock and the
// timer API are all injected.
// -----------------------------------------------------------------------------
'use strict';

const assert = require('assert');
const { createRunnerHeartbeat, countLiveRunners } = require('../lib/runner-heartbeat');

// Minimal redis fake: sorted sets plus plain keys that expire on the
// injected clock, so a test can tell whether a claim outlived its TTL.
// multi() queues commands and runs them in order on exec, like a pipeline.
function makeFakeRedis(clock = { now: 0 }) {
  const zsets = new Map();
  const ttls = new Map();
  const keys = new Map(); // key -> expireAt (ms) or Infinity
  const execs = [];       // command names of each exec'd pipeline
  const zset = (k) => { if (!zsets.has(k)) zsets.set(k, new Map()); return zsets.get(k); };
  const alive = (k) => {
    if (keys.has(k) && keys.get(k) <= clock.now) keys.delete(k);
    return keys.has(k) || zsets.has(k);
  };
  const redis = {
    zsets,
    ttls,
    execs,
    setKey: (k, ttlSec) => { keys.set(k, clock.now + ttlSec * 1000); },
    exists: async (k) => (alive(k) ? 1 : 0),
    zAdd: async (k, { score, value }) => { zset(k).set(value, score); return 1; },
    zRem: async (k, member) => zset(k).delete(member) ? 1 : 0,
    zCount: async (k, min, max) => {
      const lo = Number(min);
      const hi = max === '+inf' ? Infinity : Number(max);
      return [...zset(k).values()].filter(s => s >= lo && s <= hi).length;
    },
    expire: async (k, ttl) => {
      ttls.set(k, ttl);
      if (!alive(k)) return 0;
      if (keys.has(k)) keys.set(k, clock.now + ttl * 1000);
      return 1;
    },
    multi: () => {
      const queued = [];
      const chain = {};
      for (const name of ['zAdd', 'expire']) {
        chain[name] = (...args) => { queued.push([name, args]); return chain; };
      }
      chain.exec = async () => {
        execs.push(queued.map(([name, args]) => [name, args[0]]));
        const results = [];
        for (const [name, args] of queued) results.push(await redis[name](...args));
        return results;
      };
      return chain;
    },
  };
  return redis;
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

function makeHeartbeat(redis, timers, clock, heldClaims = () => []) {
  return createRunnerHeartbeat({
    redis,
    key: 'exec:runners_heartbeat',
    runnerId: 'runner-1',
    intervalMs: 30000,
    ttlSec: 3600,
    heldClaims,
    claimTtlSec: 180,
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
    const down = async () => { throw new Error('down'); };
    const failing = {
      zAdd: down,
      zRem: down,
      expire: down,
      multi: () => {
        const chain = { zAdd: () => chain, expire: () => chain, exec: down };
        return chain;
      },
    };
    const timers = makeFakeTimers();
    const heartbeat = makeHeartbeat(failing, timers, { now: 1000 });

    await heartbeat.start();
    await timers.tick();
    await heartbeat.stop();
  });

  it('refreshes every held claim in the same pipeline as the check-in', async function () {
    const redis = makeFakeRedis();
    const heartbeat = makeHeartbeat(redis, makeFakeTimers(), { now: 1000 }, () => ['claim-a', 'claim-b']);

    await heartbeat.start();

    assert.deepStrictEqual(redis.execs, [[
      ['zAdd', 'exec:runners_heartbeat'],
      ['expire', 'exec:runners_heartbeat'],
      ['expire', 'claim-a'],
      ['expire', 'claim-b'],
    ]], 'one round-trip per beat');
    assert.strictEqual(redis.ttls.get('claim-a'), 180);
    assert.strictEqual(redis.ttls.get('claim-b'), 180);
  });

  it('keeps a held claim alive past its TTL while the runner beats', async function () {
    // A 5-minute test with a 3-minute claim: the 30 s beats keep it held.
    const clock = { now: 0 };
    const redis = makeFakeRedis(clock);
    const timers = makeFakeTimers();
    let held = ['claim-slow'];
    const heartbeat = makeHeartbeat(redis, timers, clock, () => held);

    redis.setKey('claim-slow', 180);
    await heartbeat.start();
    for (clock.now = 30000; clock.now <= 300000; clock.now += 30000) {
      await timers.tick();
    }
    assert.strictEqual(await redis.exists('claim-slow'), 1, 'claim still held after 300 s');

    // Released (or the runner died): no more refreshes, so it expires.
    held = [];
    clock.now += 181000;
    await timers.tick();
    assert.strictEqual(await redis.exists('claim-slow'), 0, 'claim expires once nobody refreshes it');
  });

  it('reads the held claims afresh on every beat', async function () {
    const redis = makeFakeRedis();
    const timers = makeFakeTimers();
    let held = [];
    const heartbeat = makeHeartbeat(redis, timers, { now: 1000 }, () => held);

    await heartbeat.start();
    held = ['claim-later'];
    await timers.tick();

    assert.deepStrictEqual(redis.execs.map(cmds => cmds.map(c => c[1])), [
      ['exec:runners_heartbeat', 'exec:runners_heartbeat'],
      ['exec:runners_heartbeat', 'exec:runners_heartbeat', 'claim-later'],
    ]);
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
