// -----------------------------------------------------------------------------
// test/test-preemption.js
//
// Verifies the preemption-resilience behaviour:
//   1. beforeEach claim uses MOCHA_DISTRIBUTED_CLAIM_EXPIRATION_TIME (short).
//   2. afterEach promotes the claim key to MOCHA_DISTRIBUTED_EXPIRATION_TIME
//      (long, the result-list lifetime) as a tombstone.
//   3. When another runner owns the claim the test is skipped and no
//      tombstone is written.
//   4. While a test runs the keepalive refreshes the claim TTL.
//   5. SIGTERM releases the in-flight claim with DEL when we own it.
// -----------------------------------------------------------------------------
'use strict';

const assert = require('assert');
const Mocha  = require('mocha/lib/mocha');
const Suite  = require('mocha/lib/suite');
const Test   = require('mocha/lib/test');

// -----------------------------------------------------------------------------
// Mock redis client that records every call
// -----------------------------------------------------------------------------
const redisResolved = require.resolve('redis');

function makeMockClient(opts = {}) {
  const claimOwners = opts.claimOwners || {}; // testKey -> existing owner
  const calls = [];
  const client = {
    calls,
    on:      () => {},
    connect: async () => {},
    quit:    async () => { calls.push(['quit']); },
    expire:  async (k, ttl) => { calls.push(['expire', k, ttl]); return 1; },
    get:     async (k) => {
      calls.push(['get', k]);
      return claimOwners[k] || process.env.MOCHA_DISTRIBUTED_RUNNER_ID;
    },
    del:     async (k) => { calls.push(['del', k]); return 1; },
    multi:   () => {
      const cmds = [];
      const chain = {
        set:    (...a) => { cmds.push(['set',    ...a]); return chain; },
        get:    (...a) => { cmds.push(['get',    ...a]); return chain; },
        rPush:  (...a) => { cmds.push(['rPush',  ...a]); return chain; },
        expire: (...a) => { cmds.push(['expire', ...a]); return chain; },
        incr:   (...a) => { cmds.push(['incr',   ...a]); return chain; },
        exec: async () => {
          calls.push(['multi', cmds.slice()]);
          // beforeEach pipeline: SET NX + GET
          if (cmds[0] && cmds[0][0] === 'set') {
            const testKey = cmds[0][1];
            const owner = claimOwners[testKey] ||
                          process.env.MOCHA_DISTRIBUTED_RUNNER_ID;
            return [null, owner];
          }
          // afterEach pipeline: rPush + expire + incr + expire
          return cmds.map(() => 1);
        }
      };
      return chain;
    }
  };
  return client;
}

function injectMockRedis(client) {
  require.cache[redisResolved] = {
    id:       redisResolved,
    filename: redisResolved,
    loaded:   true,
    exports:  { createClient: () => client },
  };
}

function restoreRedis() {
  delete require.cache[redisResolved];
}

function loadFreshLib() {
  const libPath = require.resolve('../index.js');
  delete require.cache[libPath];
  return require('../index.js');
}

function clearLib() {
  delete require.cache[require.resolve('../index.js')];
  // The lib registers SIGTERM/SIGINT listeners on load; remove ours so
  // each describe starts fresh and the outer test process is not affected.
  process.removeAllListeners('SIGTERM');
  process.removeAllListeners('SIGINT');
}

function findSetCmd(calls) {
  for (const c of calls) {
    if (c[0] !== 'multi') continue;
    const set = c[1].find(cmd => cmd[0] === 'set');
    if (set) return set;
  }
  return null;
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------
const LONG_TTL  = `${7 * 24 * 3600}`;
const SHORT_TTL = `${10 * 60}`;

describe('mocha-distributed preemption resilience', function () {

  // ---------------------------------------------------------------------------
  describe('claim TTL split + tombstone on completion', function () {
    let client, lib;

    before(function () {
      client = makeMockClient();
      injectMockRedis(client);
      process.env.MOCHA_DISTRIBUTED              = 'redis://mock';
      process.env.MOCHA_DISTRIBUTED_EXECUTION_ID = 'pre-exec-ttl';
      process.env.MOCHA_DISTRIBUTED_RUNNER_ID    = 'runner-ttl';
      delete process.env.MOCHA_DISTRIBUTED_CLAIM_EXPIRATION_TIME;
      delete process.env.MOCHA_DISTRIBUTED_EXPIRATION_TIME;
      lib = loadFreshLib();
    });

    after(function () { restoreRedis(); clearLib(); });

    it('SETs claim with short TTL and tombstones with long TTL', async function () {
      this.timeout(10000);

      const m = new Mocha({ reporter: 'min' });
      m.rootHooks(lib.mochaHooks);
      m.globalSetup([lib.mochaGlobalSetup]);
      m.globalTeardown([lib.mochaGlobalTeardown]);

      const suite = Suite.create(m.suite, 'ttl-suite');
      suite.addTest(new Test('passes', function () {}));

      await new Promise(resolve => m.run(resolve));

      const setCmd = findSetCmd(client.calls);
      assert.ok(setCmd, 'SET NX was recorded');
      const [, testKey, runnerId, setOpts] = setCmd;
      assert.strictEqual(runnerId, 'runner-ttl');
      assert.strictEqual(setOpts.NX, true, 'SET NX flag set');
      assert.strictEqual(String(setOpts.EX), SHORT_TTL,
        'claim TTL is the short value (default 600s)');

      // Tombstone: a direct (non-multi) expire on the claim key with the
      // long TTL must have been called after the test completed.
      const tombstone = client.calls.find(c =>
        c[0] === 'expire' && c[1] === testKey && String(c[2]) === LONG_TTL
      );
      assert.ok(tombstone,
        'claim key promoted to long-TTL tombstone after completion');
    });
  });

  // ---------------------------------------------------------------------------
  describe('honours MOCHA_DISTRIBUTED_CLAIM_EXPIRATION_TIME override', function () {
    let client, lib;

    before(function () {
      client = makeMockClient();
      injectMockRedis(client);
      process.env.MOCHA_DISTRIBUTED              = 'redis://mock';
      process.env.MOCHA_DISTRIBUTED_EXECUTION_ID = 'pre-exec-override';
      process.env.MOCHA_DISTRIBUTED_RUNNER_ID    = 'runner-override';
      process.env.MOCHA_DISTRIBUTED_CLAIM_EXPIRATION_TIME = '120';
      delete process.env.MOCHA_DISTRIBUTED_EXPIRATION_TIME;
      lib = loadFreshLib();
    });

    after(function () {
      delete process.env.MOCHA_DISTRIBUTED_CLAIM_EXPIRATION_TIME;
      restoreRedis();
      clearLib();
    });

    it('SETs claim with the configured short TTL', async function () {
      const m = new Mocha({ reporter: 'min' });
      m.rootHooks(lib.mochaHooks);
      m.globalSetup([lib.mochaGlobalSetup]);
      m.globalTeardown([lib.mochaGlobalTeardown]);
      const suite = Suite.create(m.suite, 'override-suite');
      suite.addTest(new Test('passes', function () {}));
      await new Promise(resolve => m.run(resolve));

      const setCmd = findSetCmd(client.calls);
      assert.ok(setCmd);
      assert.strictEqual(String(setCmd[3].EX), '120',
        'claim TTL honours env override');
    });
  });

  // ---------------------------------------------------------------------------
  describe('skipped test (claim already owned)', function () {
    let client, lib;

    before(function () {
      // Pre-populate the claim owner so the runner SET+GET returns a foreign id
      client = makeMockClient({
        // any key in the execution will be owned by "other-runner"
        // (matched in mock by env var fallback OR explicit map; we just
        // ignore the runner id env and force a different owner via a Proxy)
      });
      // Wrap multi exec to always return a different runner id
      const realMulti = client.multi;
      client.multi = () => {
        const chain = realMulti();
        const realExec = chain.exec;
        chain.exec = async function () {
          const result = await realExec();
          // beforeEach pipeline: replace assignedRunnerId with foreign id
          if (Array.isArray(result) && result.length === 2 && result[0] === null) {
            return [null, 'other-runner'];
          }
          return result;
        };
        return chain;
      };
      injectMockRedis(client);

      process.env.MOCHA_DISTRIBUTED              = 'redis://mock';
      process.env.MOCHA_DISTRIBUTED_EXECUTION_ID = 'pre-exec-skip';
      process.env.MOCHA_DISTRIBUTED_RUNNER_ID    = 'runner-skip';
      delete process.env.MOCHA_DISTRIBUTED_CLAIM_EXPIRATION_TIME;
      delete process.env.MOCHA_DISTRIBUTED_EXPIRATION_TIME;
      lib = loadFreshLib();
    });

    after(function () { restoreRedis(); clearLib(); });

    it('does not write a tombstone when another runner owns the claim', async function () {
      const m = new Mocha({ reporter: 'min' });
      m.rootHooks(lib.mochaHooks);
      m.globalSetup([lib.mochaGlobalSetup]);
      m.globalTeardown([lib.mochaGlobalTeardown]);
      const suite = Suite.create(m.suite, 'skip-suite');
      let testRan = false;
      suite.addTest(new Test('skipped', function () { testRan = true; }));
      await new Promise(resolve => m.run(resolve));

      assert.strictEqual(testRan, false, 'test body did not execute (skipped)');

      const setCmd = findSetCmd(client.calls);
      assert.ok(setCmd, 'SET NX still attempted');
      const testKey = setCmd[1];

      const tombstone = client.calls.find(c =>
        c[0] === 'expire' && c[1] === testKey && String(c[2]) === LONG_TTL
      );
      assert.strictEqual(tombstone, undefined,
        'no tombstone written for a claim owned by another runner');

      // No rPush either (skipped tests don't write a result row).
      const resultWrite = client.calls.find(c =>
        c[0] === 'multi' && c[1].some(cmd => cmd[0] === 'rPush')
      );
      assert.strictEqual(resultWrite, undefined,
        'no result row written for a skipped test');
    });
  });

  // ---------------------------------------------------------------------------
  describe('keepalive refresh during a running test', function () {
    let client, lib;
    let realSetInterval, capturedInterval;

    before(function () {
      client = makeMockClient();
      injectMockRedis(client);

      realSetInterval = global.setInterval;
      capturedInterval = null;
      global.setInterval = function (fn, ms) {
        capturedInterval = { fn, ms };
        // Return a real (but inert) timer handle so clearInterval works.
        return realSetInterval(() => {}, 1 << 30);
      };

      process.env.MOCHA_DISTRIBUTED              = 'redis://mock';
      process.env.MOCHA_DISTRIBUTED_EXECUTION_ID = 'pre-exec-keepalive';
      process.env.MOCHA_DISTRIBUTED_RUNNER_ID    = 'runner-keepalive';
      delete process.env.MOCHA_DISTRIBUTED_CLAIM_EXPIRATION_TIME;
      delete process.env.MOCHA_DISTRIBUTED_EXPIRATION_TIME;
      lib = loadFreshLib();
    });

    after(function () {
      global.setInterval = realSetInterval;
      restoreRedis();
      clearLib();
    });

    it('refreshes the claim with the short TTL while the test runs', async function () {
      this.timeout(10000);

      const m = new Mocha({ reporter: 'min' });
      m.rootHooks(lib.mochaHooks);
      m.globalSetup([lib.mochaGlobalSetup]);
      m.globalTeardown([lib.mochaGlobalTeardown]);

      const suite = Suite.create(m.suite, 'keepalive-suite');
      suite.addTest(new Test('long-running', async function () {
        // beforeEach has registered the keepalive by now; fire it twice
        // to simulate the interval firing while the test runs.
        assert.ok(capturedInterval, 'keepalive setInterval was registered');
        capturedInterval.fn();
        capturedInterval.fn();
        // Let the awaited expire() promises resolve.
        await new Promise(r => setImmediate(r));
      }));

      // Snapshot calls made before mocha runs the test.
      const callsBefore = client.calls.length;
      await new Promise(resolve => m.run(resolve));

      const setCmd = findSetCmd(client.calls);
      const testKey = setCmd[1];

      // The interval interval value must be claim_ttl/3 in seconds = 200s
      // for the default 600s TTL. Allow >= 30s floor, default => 200000ms.
      assert.strictEqual(capturedInterval.ms, 200 * 1000,
        'keepalive interval is claim_ttl/3 seconds');

      // Two refresh calls with the short TTL must have been recorded.
      const refreshes = client.calls
        .slice(callsBefore)
        .filter(c => c[0] === 'expire' && c[1] === testKey
                     && String(c[2]) === SHORT_TTL);
      assert.ok(refreshes.length >= 2,
        `>=2 keepalive refreshes recorded (got ${refreshes.length})`);
    });
  });

  // ---------------------------------------------------------------------------
  describe('SIGTERM releases the in-flight claim', function () {
    let client, lib;
    let realExit;

    before(function () {
      client = makeMockClient();
      injectMockRedis(client);
      process.env.MOCHA_DISTRIBUTED              = 'redis://mock';
      process.env.MOCHA_DISTRIBUTED_EXECUTION_ID = 'pre-exec-sigterm';
      process.env.MOCHA_DISTRIBUTED_RUNNER_ID    = 'runner-sigterm';
      delete process.env.MOCHA_DISTRIBUTED_CLAIM_EXPIRATION_TIME;
      delete process.env.MOCHA_DISTRIBUTED_EXPIRATION_TIME;

      // The lib calls process.exit at the end of the SIGTERM handler;
      // stub it so we don't kill the outer mocha runner.
      realExit = process.exit;
      process.exit = function () { /* no-op for tests */ };

      lib = loadFreshLib();
    });

    after(function () {
      process.exit = realExit;
      restoreRedis();
      clearLib();
    });

    it('DELs the claim key when this runner owns it', async function () {
      this.timeout(10000);

      const m = new Mocha({ reporter: 'min' });
      m.rootHooks(lib.mochaHooks);
      m.globalSetup([lib.mochaGlobalSetup]);
      m.globalTeardown([lib.mochaGlobalTeardown]);

      const suite = Suite.create(m.suite, 'sigterm-suite');
      let testKeyAtSigterm = null;
      suite.addTest(new Test('preempted', async function () {
        // Grab the lib's SIGTERM listener (the most recently installed one)
        // and invoke it directly. The lib registers it as
        //   () => releaseClaimAndExit("SIGTERM")
        // which returns the promise, so we can await it.
        const listeners = process.listeners('SIGTERM');
        assert.ok(listeners.length > 0, 'SIGTERM listener installed');
        // Capture the claim key from the most recent SET in the mock.
        const setCmd = findSetCmd(client.calls);
        testKeyAtSigterm = setCmd && setCmd[1];
        await listeners[listeners.length - 1]();
      }));

      await new Promise(resolve => m.run(resolve));

      assert.ok(testKeyAtSigterm, 'claim key was set before SIGTERM');
      const del = client.calls.find(c =>
        c[0] === 'del' && c[1] === testKeyAtSigterm
      );
      assert.ok(del, 'SIGTERM handler DELd the in-flight claim key');
    });
  });
});
