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
  const kv = new Map();                       // shared in-memory kv store
  const calls = [];
  const client = {
    calls,
    kv,
    on:      () => {},
    connect: async () => {},
    quit:    async () => { calls.push(['quit']); },
    expire:  async (k, ttl) => { calls.push(['expire', k, ttl]); return 1; },
    get:     async (k) => {
      calls.push(['get', k]);
      if (kv.has(k)) return kv.get(k);
      if (claimOwners[k]) return claimOwners[k];
      return null;
    },
    set:     async (k, v /*, opts */) => {
      calls.push(['set', k, v]);
      kv.set(k, String(v));
      return 'OK';
    },
    incr:    async (k) => {
      calls.push(['incr', k]);
      const n = (parseInt(kv.get(k) || '0', 10) || 0) + 1;
      kv.set(k, String(n));
      return n;
    },
    decr:    async (k) => {
      calls.push(['decr', k]);
      const n = (parseInt(kv.get(k) || '0', 10) || 0) - 1;
      kv.set(k, String(n));
      return n;
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
        sAdd:   (...a) => { cmds.push(['sAdd',   ...a]); return chain; },
        exec: async () => {
          calls.push(['multi', cmds.slice()]);
          // beforeEach pipeline: SET NX + GET (+ sAdd/expire on universe).
          // Persist the SET into the kv store so later top-level GETs
          // (e.g. the SIGTERM ownership check) see the correct owner.
          if (cmds[0] && cmds[0][0] === 'set') {
            const testKey = cmds[0][1];
            const owner = claimOwners[testKey] ||
                          process.env.MOCHA_DISTRIBUTED_RUNNER_ID;
            if (!kv.has(testKey)) kv.set(testKey, owner);
            return [null, owner, ...cmds.slice(2).map(() => 1)];
          }
          // afterEach pipeline: rPush + expire + incr + expire (+ maybe sAdd/expire)
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
          // beforeEach pipeline: replace assignedRunnerId with foreign id.
          // Detect it by the shape: SET NX returns null and GET returns a
          // string runner id, whereas afterEach returns only integers.
          if (Array.isArray(result) && result[0] === null &&
              typeof result[1] === 'string') {
            result[1] = 'other-runner';
            return result;
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

  // ---------------------------------------------------------------------------
  describe('pre-walk key stability (dup-N + serial collapse)', function () {
    // The pre-walk assigns each test its canonical key (with :dup-N suffix
    // for duplicated titles, and a collapsed [serial-x] key for serial
    // tests) before any test runs. This test asserts that the SET NX calls
    // observed by redis match the deterministic keys produced by the walk.
    let client, lib;

    before(function () {
      client = makeMockClient();
      injectMockRedis(client);
      process.env.MOCHA_DISTRIBUTED              = 'redis://mock';
      process.env.MOCHA_DISTRIBUTED_EXECUTION_ID = 'pre-exec-keys';
      process.env.MOCHA_DISTRIBUTED_RUNNER_ID    = 'runner-keys';
      delete process.env.MOCHA_DISTRIBUTED_CLAIM_EXPIRATION_TIME;
      delete process.env.MOCHA_DISTRIBUTED_EXPIRATION_TIME;
      lib = loadFreshLib();
    });

    after(function () { restoreRedis(); clearLib(); });

    it('assigns :dup-N suffixes and collapses serial keys as expected', async function () {
      this.timeout(10000);

      const m = new Mocha({ reporter: 'min' });
      m.rootHooks(lib.mochaHooks);
      m.globalSetup([lib.mochaGlobalSetup]);
      m.globalTeardown([lib.mochaGlobalTeardown]);

      // Two duplicated titles + a serial group of three tests.
      const suite = Suite.create(m.suite, 'keys-suite');
      suite.addTest(new Test('dup-title', function () {}));
      suite.addTest(new Test('dup-title', function () {}));
      suite.addTest(new Test('dup-title', function () {}));
      suite.addTest(new Test('serial-1 [serial-group-a]', function () {}));
      suite.addTest(new Test('serial-2 [serial-group-a]', function () {}));
      suite.addTest(new Test('serial-3 [serial-group-a]', function () {}));

      await new Promise(resolve => m.run(resolve));

      // Collect the keys used in every SET NX call in order.
      const setKeys = client.calls
        .filter(c => c[0] === 'multi')
        .map(c => c[1].find(cmd => cmd[0] === 'set'))
        .filter(Boolean)
        .map(cmd => cmd[1]);

      const execId = 'pre-exec-keys';
      // Duplicated titles must get consecutive :dup-N suffixes on the same
      // base key. Order matches suite registration order (DFS in the walk).
      assert.deepStrictEqual(setKeys.slice(0, 3), [
        `${execId}:keys-suite:dup-title:dup-1`,
        `${execId}:keys-suite:dup-title:dup-2`,
        `${execId}:keys-suite:dup-title:dup-3`,
      ], 'duplicated titles get stable :dup-N suffixes from the pre-walk');

      // All three serial tests must collapse to the same claim key derived
      // from the [serial-group-a] substring — no dup suffix, no per-test
      // uniqueness. This is what makes the whole group run on one runner.
      const serialKey = `${execId}:[serial-group-a]`;
      assert.deepStrictEqual(setKeys.slice(3, 6),
        [serialKey, serialKey, serialKey],
        'serial-group tests share one collapsed claim key');
    });
  });

  // ---------------------------------------------------------------------------
  describe('test_universe + done_tests bookkeeping', function () {
    // The drain phase relies on two sets in redis:
    //   test_universe : every test key that has been attempted anywhere.
    //   done_tests    : every test key that has been fully accounted for.
    // Serial groups collapse to one claim key; they should be marked done
    // only when the last test in the group finishes, otherwise a drain-phase
    // peer could conclude the group is complete mid-run.
    let client, lib;

    before(function () {
      client = makeMockClient();
      injectMockRedis(client);
      process.env.MOCHA_DISTRIBUTED              = 'redis://mock';
      process.env.MOCHA_DISTRIBUTED_EXECUTION_ID = 'pre-exec-sets';
      process.env.MOCHA_DISTRIBUTED_RUNNER_ID    = 'runner-sets';
      delete process.env.MOCHA_DISTRIBUTED_CLAIM_EXPIRATION_TIME;
      delete process.env.MOCHA_DISTRIBUTED_EXPIRATION_TIME;
      lib = loadFreshLib();
    });

    after(function () { restoreRedis(); clearLib(); });

    it('SADDs every attempt to test_universe and marks done_tests correctly', async function () {
      this.timeout(10000);

      const m = new Mocha({ reporter: 'min' });
      m.rootHooks(lib.mochaHooks);
      m.globalSetup([lib.mochaGlobalSetup]);
      m.globalTeardown([lib.mochaGlobalTeardown]);

      const suite = Suite.create(m.suite, 'sets-suite');
      suite.addTest(new Test('plain', function () {}));
      suite.addTest(new Test('s1 [serial-g]', function () {}));
      suite.addTest(new Test('s2 [serial-g]', function () {}));
      suite.addTest(new Test('s3 [serial-g]', function () {}));

      await new Promise(resolve => m.run(resolve));

      const execId = 'pre-exec-sets';
      const universeKey = `${execId}:test_universe`;
      const doneKey     = `${execId}:done_tests`;

      // Collect all sAdd commands across every beforeEach/afterEach pipeline
      // observed -- this excludes the separate prepopulation pipeline that
      // globalSetup fires once, upfront (identifiable as a multi pipeline
      // whose first command is itself 'sAdd', carrying the whole key set as
      // a single array member rather than one key per attempt).
      const sAdds = [];
      for (const c of client.calls) {
        if (c[0] !== 'multi' || (c[1][0] && c[1][0][0] === 'sAdd')) continue;
        for (const cmd of c[1]) {
          if (cmd[0] === 'sAdd') sAdds.push({ key: cmd[1], member: cmd[2] });
        }
      }

      const universeAdds = sAdds.filter(x => x.key === universeKey).map(x => x.member);
      const doneAdds     = sAdds.filter(x => x.key === doneKey    ).map(x => x.member);

      // Every beforeEach contributes one universe member: 4 attempts (1 plain
      // + 3 serial tests, though serial tests share one claim key, they still
      // each hit beforeEach independently).
      assert.strictEqual(universeAdds.length, 4,
        'test_universe SADD fires once per beforeEach');

      const plainKey  = `${execId}:sets-suite:plain:dup-1`;
      const serialKey = `${execId}:[serial-g]`;

      assert.deepStrictEqual(universeAdds.sort(),
        [plainKey, serialKey, serialKey, serialKey].sort(),
        'universe members match the expected claim keys');

      // done_tests: one entry for the plain test, and one for the serial
      // group (only from the last-in-group afterEach). Not three.
      assert.deepStrictEqual(doneAdds.sort(),
        [plainKey, serialKey].sort(),
        'done_tests receives the plain key and one entry for the whole serial group');
    });

    it('prepopulates test_universe at globalSetup, before any beforeEach fires', async function () {
      this.timeout(10000);

      const m = new Mocha({ reporter: 'min' });
      m.rootHooks(lib.mochaHooks);
      m.globalSetup([lib.mochaGlobalSetup]);
      m.globalTeardown([lib.mochaGlobalTeardown]);

      const suite = Suite.create(m.suite, 'sets-suite');
      suite.addTest(new Test('plain', function () {}));
      suite.addTest(new Test('s1 [serial-g]', function () {}));
      suite.addTest(new Test('s2 [serial-g]', function () {}));
      suite.addTest(new Test('s3 [serial-g]', function () {}));

      await new Promise(resolve => m.run(resolve));

      const execId = 'pre-exec-sets';
      const universeKey = `${execId}:test_universe`;
      const plainKey  = `${execId}:sets-suite:plain:dup-1`;
      const serialKey = `${execId}:[serial-g]`;

      // The prepopulation SADD fires from globalSetup, before any test runs,
      // so its multi() pipeline must be the very first one observed --
      // distinguishable from a beforeEach claim pipeline (which always
      // starts with 'set') by starting with 'sAdd' instead.
      const firstMulti = client.calls.find(c => c[0] === 'multi');
      assert.ok(firstMulti, 'at least one multi pipeline ran');
      assert.strictEqual(firstMulti[1][0][0], 'sAdd',
        'the first multi pipeline is the prepopulation SADD, not a beforeEach claim');

      const [, sadKey, members] = firstMulti[1][0];
      assert.strictEqual(sadKey, universeKey, 'prepopulation SADD targets test_universe');
      assert.deepStrictEqual([...members].sort(), [plainKey, serialKey].sort(),
        'prepopulation SADD carries every distinct claim key from the local pre-walk');
    });
  });

  // ---------------------------------------------------------------------------
  describe('expected_total + runners_active bookkeeping', function () {
    // expected_total is a global "we're done when done_tests reaches this"
    // marker written via max-tracking (GET current, SET if local > remote).
    // runners_active is a live-runner counter, INCRed at globalSetup and
    // DECRed exactly once at teardown or SIGTERM.
    let client, lib;

    before(function () {
      client = makeMockClient();
      injectMockRedis(client);
      process.env.MOCHA_DISTRIBUTED              = 'redis://mock';
      process.env.MOCHA_DISTRIBUTED_EXECUTION_ID = 'pre-exec-total';
      process.env.MOCHA_DISTRIBUTED_RUNNER_ID    = 'runner-total';
      delete process.env.MOCHA_DISTRIBUTED_CLAIM_EXPIRATION_TIME;
      delete process.env.MOCHA_DISTRIBUTED_EXPIRATION_TIME;
      delete process.env.MOCHA_DISTRIBUTED_EXPECTED_TOTAL_OVERRIDE;
      lib = loadFreshLib();
    });

    after(function () { restoreRedis(); clearLib(); });

    it('publishes expected_total (distinct claim keys) and INCR/DECRs runners_active', async function () {
      const m = new Mocha({ reporter: 'min' });
      m.rootHooks(lib.mochaHooks);
      m.globalSetup([lib.mochaGlobalSetup]);
      m.globalTeardown([lib.mochaGlobalTeardown]);

      // 2 plain + 3 serial (share one key) = 3 distinct claim keys.
      const suite = Suite.create(m.suite, 'total-suite');
      suite.addTest(new Test('a', function () {}));
      suite.addTest(new Test('b', function () {}));
      suite.addTest(new Test('s1 [serial-x]', function () {}));
      suite.addTest(new Test('s2 [serial-x]', function () {}));
      suite.addTest(new Test('s3 [serial-x]', function () {}));

      await new Promise(resolve => m.run(resolve));

      const execId = 'pre-exec-total';
      // expected_total should have been written with value 3.
      assert.strictEqual(client.kv.get(`${execId}:expected_total`), '3',
        'expected_total = number of distinct claim keys');

      // runners_active: INCR once at setup, DECR once at teardown.
      const incrs = client.calls.filter(c => c[0] === 'incr'
                                          && c[1] === `${execId}:runners_active`);
      const decrs = client.calls.filter(c => c[0] === 'decr'
                                          && c[1] === `${execId}:runners_active`);
      assert.strictEqual(incrs.length, 1, 'runners_active INCRed once');
      assert.strictEqual(decrs.length, 1, 'runners_active DECRed once');
      // Net value should be 0 after the run.
      assert.strictEqual(client.kv.get(`${execId}:runners_active`), '0',
        'runners_active net change is zero');
    });
  });

  // ---------------------------------------------------------------------------
  describe('MOCHA_DISTRIBUTED_EXPECTED_TOTAL_OVERRIDE', function () {
    // Escape hatch for dynamically-generated tests where the pre-walk
    // under-counts. Documented as unsupported by default; the override
    // env var lets the user tell us the correct total explicitly.
    let client, lib;

    before(function () {
      client = makeMockClient();
      injectMockRedis(client);
      process.env.MOCHA_DISTRIBUTED              = 'redis://mock';
      process.env.MOCHA_DISTRIBUTED_EXECUTION_ID = 'pre-exec-override-total';
      process.env.MOCHA_DISTRIBUTED_RUNNER_ID    = 'runner-override-total';
      process.env.MOCHA_DISTRIBUTED_EXPECTED_TOTAL_OVERRIDE = '99';
      delete process.env.MOCHA_DISTRIBUTED_CLAIM_EXPIRATION_TIME;
      delete process.env.MOCHA_DISTRIBUTED_EXPIRATION_TIME;
      lib = loadFreshLib();
    });

    after(function () {
      delete process.env.MOCHA_DISTRIBUTED_EXPECTED_TOTAL_OVERRIDE;
      restoreRedis();
      clearLib();
    });

    it('uses the override value verbatim regardless of local test count', async function () {
      const m = new Mocha({ reporter: 'min' });
      m.rootHooks(lib.mochaHooks);
      m.globalSetup([lib.mochaGlobalSetup]);
      m.globalTeardown([lib.mochaGlobalTeardown]);
      const suite = Suite.create(m.suite, 'override-total-suite');
      suite.addTest(new Test('only', function () {}));
      await new Promise(resolve => m.run(resolve));

      const execId = 'pre-exec-override-total';
      assert.strictEqual(client.kv.get(`${execId}:expected_total`), '99',
        'override value wins over the walk-computed local count');
    });
  });
});
