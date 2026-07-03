// -----------------------------------------------------------------------------
// Copyright (c) 2018 Pau Sanchez
//
// MIT Licensed
// -----------------------------------------------------------------------------
const redis = require("redis");
const crypto = require("crypto");

const SERIAL_PREFIX = "[serial";

const GRANULARITY = {
  TEST: "test",
  SUITE: "suite",
};

// Initialize variables from environment
const g_redisAddress = process.env.MOCHA_DISTRIBUTED || "";
const g_testExecutionId = process.env.MOCHA_DISTRIBUTED_EXECUTION_ID || "";
const g_expirationTime =
  process.env.MOCHA_DISTRIBUTED_EXPIRATION_TIME || `${7 * 24 * 3600}`;
// Short TTL for in-flight claim keys; refreshed by a keepalive while a test
// runs, DEL'd on SIGTERM, and promoted to g_expirationTime as a tombstone
// once the test completes. See README "Claim key lifecycle".
const g_claimExpirationTime =
  process.env.MOCHA_DISTRIBUTED_CLAIM_EXPIRATION_TIME || `${10 * 60}`;

// Generate a unique random id for this runner (with almost 100% certainty
// to be different on any machine/environment).
const _randomRunnerBuf = Buffer.alloc(16);
const _randomRunnerId = crypto.randomFillSync(_randomRunnerBuf).toString("hex");
const g_runnerId = process.env.MOCHA_DISTRIBUTED_RUNNER_ID || _randomRunnerId;
let g_granularity =
  process.env.MOCHA_DISTRIBUTED_GRANULARITY || GRANULARITY.TEST;
const g_mochaVerbose = process.env.MOCHA_DISTRIBUTED_VERBOSE === "true";

if (g_granularity !== GRANULARITY.TEST) {
  g_granularity = GRANULARITY.SUITE;
}

let g_redis = null;

let g_capture = { stdout: null, stderr: null };

// Track the in-flight claim so we can keepalive-refresh it, tombstone it
// after completion, and DEL it on SIGTERM.
let g_currentClaimKey = null;
let g_claimRefreshInterval = null;

// Cache errors from intermediate retry attempts. Mocha only sets test.err via
// the reporter on the final EVENT_TEST_FAIL; for non-final retries it emits
// EVENT_TEST_RETRY instead and never stores the error on the test object.
const g_retryErrors = new Map();

// Pre-walk map: Test -> TestKeyInfo. Populated once at mochaGlobalSetup from
// mocha's root suite, before any test runs. Used by beforeEach/afterEach as
// the single source of truth for test keys. Rationale:
//   - Deterministic across runners (DFS in mocha's registration order, which
//     is identical when all runners load the same test files).
//   - Encodes serial-collapse and :dup-N suffixing once, so the initial run
//     and drain-phase re-runs never disagree on which key a test claims.
//   - Also carries serial-group membership so afterEach knows when to mark
//     the group as done in the shared done_tests set (last-in-group only).
//
// TestKeyInfo = { key, isSerial, serialGroupId, isLastInSerialGroup }
const g_testKeyInfo = new WeakMap();
// Fallback dup counter for tests that were not present at pre-walk time
// (e.g. tests dynamically added inside a before() hook — an unsupported
// pattern, but we degrade gracefully instead of throwing).
const g_duplicateTestKeyFullPathCount = new Map();
let g_lastTestKeyFullPath = null;

// -----------------------------------------------------------------------------
// getTestPath
//
// Returns an array with the test suites and test name from a test context
// as found in the hooks
//
// Example:
//
//    >>> getTestPath(ctxt)
//
// -----------------------------------------------------------------------------
function getTestPath(testContext) {
  const path = [testContext.title];

  while (!testContext.root && testContext.parent) {
    testContext = testContext.parent;

    if (testContext && !testContext.root) {
      path.push(testContext.title);
    }
  }

  return path.reverse();
}

// -----------------------------------------------------------------------------
// walkSuite
//
// DFS iterator over every Test in a Suite tree, yielding tests in mocha's
// registration order (which matches execution order for a plain run). Used
// by computeTestKeys and by drain-phase filtering.
// -----------------------------------------------------------------------------
function walkSuite(suite, visit) {
  if (!suite) return;
  for (const test of suite.tests || []) visit(test);
  for (const child of suite.suites || []) walkSuite(child, visit);
}

// -----------------------------------------------------------------------------
// buildTestKeyFromPath
//
// Given a test's title-path array (root -> leaf, excluding the root suite),
// apply serial collapse and return the base test key (no dup suffix).
// -----------------------------------------------------------------------------
function buildTestKeyFromPath(pathArr) {
  const joined = pathArr.join(":");
  const collapsed = getSerialGranularity(joined);
  return `${g_testExecutionId}:${collapsed}`;
}

// -----------------------------------------------------------------------------
// getTestPathFromTest
//
// Reconstructs the title path for a Test object by walking its .parent chain.
// Equivalent to getTestPath(ctxt) but works on a raw Test rather than the
// hook's `this.currentTest` context (they are the same object in practice,
// but making the intent explicit here).
// -----------------------------------------------------------------------------
function getTestPathFromTest(test) {
  const path = [test.title];
  let p = test.parent;
  while (p && !p.root) {
    path.push(p.title);
    p = p.parent;
  }
  return path.reverse();
}

// -----------------------------------------------------------------------------
// computeTestKeys
//
// Walks the root suite once and assigns every Test its canonical key info.
// Rules (must match the historical behavior of beforeEach):
//   - Serial tests (`[serial...]` anywhere in the joined path) collapse to a
//     shared key derived from the serial substring; no dup suffix.
//   - Non-serial tests get a `:dup-N` suffix based on how many times their
//     base key has been seen so far in the walk (N starts at 1).
//   - Walk order is DFS in registration order, which is deterministic across
//     runners loading the same test files. This is what makes independent
//     runners agree on a test's key without any coordination.
//
// Also records serial-group membership so afterEach can tell when the last
// test in a group finished (used later to mark the group done in redis).
// -----------------------------------------------------------------------------
function computeTestKeys(rootSuite) {
  const dupCount = new Map();               // base key -> count seen so far
  const serialGroupMembers = new Map();     // serial key -> [Test, ...]

  walkSuite(rootSuite, (test) => {
    const path = getTestPathFromTest(test);
    const joined = path.join(":");
    const isSerial = joined.indexOf(SERIAL_PREFIX) !== -1;
    let key = buildTestKeyFromPath(path);

    if (!isSerial) {
      const n = (dupCount.get(key) || 0) + 1;
      dupCount.set(key, n);
      key = `${key}:dup-${n}`;
    } else {
      if (!serialGroupMembers.has(key)) serialGroupMembers.set(key, []);
      serialGroupMembers.get(key).push(test);
    }

    g_testKeyInfo.set(test, {
      key,
      isSerial,
      serialGroupId: isSerial ? key : null,
      isLastInSerialGroup: false,
    });
  });

  // Mark the last test in each serial group. Used by afterEach to decide when
  // the whole group is done (see drain-phase design in
  // docs/preemption-resilience-plan.md).
  for (const members of serialGroupMembers.values()) {
    const info = g_testKeyInfo.get(members[members.length - 1]);
    if (info) info.isLastInSerialGroup = true;
  }
}

// -----------------------------------------------------------------------------
// getSerialGranularity
//
// Returns the full string or the "serial string" which is whatever finds that
// follows this regex "[serial.*]" on the string. Only first instance is
// returned.
//
// This will allow serializing tests with given serial name.
// -----------------------------------------------------------------------------
function getSerialGranularity(testKey) {
  // NOTE: a regular expression might be trickier to get right, since you can
  //       have multiple instances of [serialxxxx] on the same string
  let index = testKey.indexOf(SERIAL_PREFIX)
  if (index === -1)
    return testKey

  let index2 = testKey.indexOf(']', index)
  if (index2 === -1)
    return testKey

  return testKey.substring(index, index2+1)
}

// -----------------------------------------------------------------------------
// captureStream
// -----------------------------------------------------------------------------
function captureStream(stream) {
  var oldWrite = stream.write;
  var buf = [];

  stream.write = function (chunk, encoding, callback) {
    buf.push(chunk.toString()); // chunk is a String or Buffer
    oldWrite.apply(stream, arguments);
  };

  return {
    unhook() {
      stream.write = oldWrite;
    },
    captured() {
      return buf;
    },
  };
}

// -----------------------------------------------------------------------------
// Initialize redis once before the tests
// -----------------------------------------------------------------------------
exports.mochaGlobalSetup = async function () {
  // `this` is the Mocha Runner — store errors from non-final retry attempts
  // so afterEach can record them (Mocha never sets test.err for those).
  this.on('retry', (test, err) => {
    g_retryErrors.set(test.fullTitle(), err);
  });
  if (g_mochaVerbose) {
    const redisNoCredentials = g_redisAddress.replace(
      /\/\/[^@]*@/,
      "//***:***@"
    );
    console.log("---------------------------------------------------");
    console.log(" Mocha Distributed");
    console.log("   - Runner Id                :", g_runnerId);
    console.log("   - Redis Address            :", redisNoCredentials);
    console.log("   - Execution Id             :", g_testExecutionId);
    console.log("   - Data Expiration Time     :", g_expirationTime);
    console.log("   - Test Parallel Granularity:", g_granularity);
    console.log("---------------------------------------------------");
  }

  if (!g_redisAddress || !g_testExecutionId) {
    console.log(g_redisAddress, g_testExecutionId);
    console.error(
      "You need to set at least the following environment variables:\n" +
        "  - MOCHA_DISTRIBUTED\n" +
        "  - MOCHA_DISTRIBUTED_EXECUTION_ID\n"
    );
    process.exit(-1);
  }

  g_redis = redis.createClient({ url: g_redisAddress });
  g_redis.on("error", (err) => {
    console.log("Redis Client Error", err);
    console.log("Closing application!");
    process.exit(-1);
  });
  await g_redis.connect();

  // Build the deterministic test-key map from mocha's root suite. `this` is
  // the mocha Runner in the globalSetup context (verified with a quick probe;
  // see the docs plan). Doing this here — before any test runs — lets
  // beforeEach look up keys instead of re-deriving them, and prepares the
  // ground for the drain phase which needs to reference tests by key.
  if (this && this.suite) {
    computeTestKeys(this.suite);
  }
};

// -----------------------------------------------------------------------------
// Quit from redis
// -----------------------------------------------------------------------------
exports.mochaGlobalTeardown = async function () {
  if (g_redis) {
    await g_redis.quit();
  }
};

// -----------------------------------------------------------------------------
// Hook tests
//
// Please note that we run skip before each test if the ownership of it has
// already been defined by another runner.
// -----------------------------------------------------------------------------
exports.mochaHooks = {
  beforeEach: async function () {
    // Prefer the pre-walk key when present; fall back to the historical
    // inline derivation for tests that weren't in the suite at globalSetup
    // time (dynamically added tests — unsupported, but degrade gracefully).
    const preInfo = g_testKeyInfo.get(this.currentTest);

    let testKeyFullPath;
    let testKeySuite;
    let isSerial;

    if (preInfo) {
      testKeyFullPath = preInfo.key;
      isSerial = preInfo.isSerial;
      // Suite-granularity key still derives from the first path segment,
      // as it did historically. Compute it from the live path.
      const livePath = getTestPath(this.currentTest);
      testKeySuite = `${g_testExecutionId}:${getSerialGranularity(livePath[0])}`;
      // Keep the legacy retry-fallback in sync: mocha clones a Test for
      // each retry (Runnable.retriedTest), and the clone is not in
      // g_testKeyInfo. When the retry fires, preInfo is missing and the
      // fallback below reads g_lastTestKeyFullPath — it must reflect the
      // key we assigned to the original attempt of this same test.
      g_lastTestKeyFullPath = testKeyFullPath;
    } else {
      const testPath = getTestPath(this.currentTest);
      // Reuse the same base-key builder the pre-walk uses (buildTestKeyFromPath)
      // instead of re-deriving the join+serial-collapse expression here, so
      // the two paths can't silently diverge on how a key is built.
      testKeyFullPath = buildTestKeyFromPath(testPath);
      testKeySuite = `${g_testExecutionId}:${getSerialGranularity(testPath[0])}`;
      isSerial = testPath.join(":").indexOf(SERIAL_PREFIX) !== -1;

      if (!isSerial) {
        // Legacy dup-suffix path for tests missing from the pre-walk. Kept
        // as a compatibility shim; the pre-walk is the intended source.
        if ((this.currentTest._currentRetry || 0) === 0) {
          g_duplicateTestKeyFullPathCount.set(
            testKeyFullPath,
            (g_duplicateTestKeyFullPathCount.get(testKeyFullPath) || 0) + 1
          );
          testKeyFullPath += `:dup-${g_duplicateTestKeyFullPathCount.get(testKeyFullPath)}`;
          g_lastTestKeyFullPath = testKeyFullPath;
        } else {
          testKeyFullPath = g_lastTestKeyFullPath;
        }
      }
    }

    const testKey =
      g_granularity === GRANULARITY.TEST ? testKeyFullPath : testKeySuite;

    // Atomically set/get the runner id associated to this test. Only the first
    // runner to get there will set the value to its own runner id.
    const [_, assignedRunnerId] = await g_redis
      .multi()
      .set(testKey, g_runnerId, { EX: g_claimExpirationTime, NX: true })
      .get(testKey)
      .exec();

    if (assignedRunnerId !== g_runnerId) {
      this.currentTest.title += " (skipped by mocha_distributted)";
      this.skip();
    } else {
      g_currentClaimKey = testKey;
      // Refresh well inside mocha's per-test timeout so a slow test never
      // lets its claim TTL expire while we're still running it.
      const refreshSecs = Math.max(
        30,
        Math.floor(Number(g_claimExpirationTime) / 3)
      );
      g_claimRefreshInterval = setInterval(() => {
        g_redis.expire(testKey, g_claimExpirationTime).catch(() => {});
      }, refreshSecs * 1000);
      g_capture.stdout = captureStream(process.stdout);
      g_capture.stderr = captureStream(process.stderr);
    }
  },

  afterEach: async function () {
    const SKIPPED = "pending";
    const FAILED = "failed";
    const PASSED = "passed";

    let capturedStdout = "";
    let capturedStderr = "";
    if (g_capture.stdout) {
      const stdoutArray = g_capture.stdout.captured();
      capturedStdout = stdoutArray.join("");
      capturedStdout = capturedStdout.replace(
        /\s*\u001b\[3[12]m[^\n]*\n$/g,
        ""
      );
      g_capture.stdout.unhook();
      g_capture.stdout = null;
    }

    if (g_capture.stderr) {
      capturedStderr = g_capture.stderr.captured().join("");
      g_capture.stderr.unhook();
      g_capture.stderr = null;
    }

    // Save all data in redis in a way it can be retrieved and aggregated
    // easily for all test by an external reporter
    if (this.currentTest.state !== SKIPPED) {
      const retryAttempt = this.currentTest._currentRetry || 0;
      const retryTotal = this.currentTest._retries || 1;

      // adjust state value accounting for exceptions, timeouts & retries
      let stateFixed = PASSED;
      if (
        this.currentTest.state === FAILED ||
        this.currentTest.timedOut ||
        (typeof this.currentTest.state === "undefined" &&
          retryAttempt < retryTotal)
      ) {
        stateFixed = FAILED;
      }

      // Error objects cannot be properly serialized with stringify, thus
      // we need to use this hack to make it look like a normal object.
      // Hopefully this should work as well with other sort of objects
      const err = this.currentTest.err
        || g_retryErrors.get(this.currentTest.fullTitle())
        || null;
      g_retryErrors.delete(this.currentTest.fullTitle());
      const errObj = JSON.parse(
        JSON.stringify(err, Object.getOwnPropertyNames(err || {}))
      );

      const testResult = {
        id: getTestPath(this.currentTest),
        type: this.currentTest.type,
        title: this.currentTest.title,
        timedOut: this.currentTest.timedOut,
        duration: this.currentTest.duration,
        startTime: Date.now() - (this.currentTest.duration || 0),
        endTime: Date.now(),
        retryAttempt: retryAttempt,
        retryTotal: retryTotal,
        file: this.currentTest.file,
        state: stateFixed,
        failed: stateFixed === FAILED,
        speed: this.currentTest.speed,
        err: errObj,
        stdout: capturedStdout,
        stderr: capturedStderr,
      };

      // save results as single line on purpose
      const resultKey = `${g_testExecutionId}:test_result`;
      const countKey = `${g_testExecutionId}:${stateFixed}_count`;

      await g_redis
        .multi()
        .rPush(resultKey, JSON.stringify(testResult))
        .expire(resultKey, g_expirationTime)
        .incr(countKey)
        .expire(countKey, g_expirationTime)
        .exec();
    }

    // Stop the keepalive and promote the claim key to a tombstone matching
    // the result-list lifetime, so a replacement pod won't re-run a test
    // that already produced a result.
    if (g_claimRefreshInterval) {
      clearInterval(g_claimRefreshInterval);
      g_claimRefreshInterval = null;
    }
    if (g_currentClaimKey) {
      try {
        await g_redis.expire(g_currentClaimKey, g_expirationTime);
      } catch (_) {}
      g_currentClaimKey = null;
    }
  },
};

// -----------------------------------------------------------------------------
// Graceful shutdown
//
// On SIGTERM (e.g. GKE spot preemption), release the in-flight claim so a
// replacement pod can re-run the interrupted test. Without this the claim
// would sit locked until g_claimExpirationTime, blocking recovery.
// -----------------------------------------------------------------------------
async function releaseClaimAndExit(signal) {
  try {
    if (g_claimRefreshInterval) {
      clearInterval(g_claimRefreshInterval);
      g_claimRefreshInterval = null;
    }
    if (g_currentClaimKey && g_redis) {
      const owner = await g_redis.get(g_currentClaimKey);
      if (owner === g_runnerId) {
        await g_redis.del(g_currentClaimKey);
      }
    }
  } catch (_) {}
  try {
    if (g_redis) await g_redis.quit();
  } catch (_) {}
  // 128 + signal number; SIGTERM=15, SIGINT=2
  process.exit(signal === "SIGINT" ? 130 : 143);
}
process.on("SIGTERM", () => releaseClaimAndExit("SIGTERM"));
process.on("SIGINT", () => releaseClaimAndExit("SIGINT"));
