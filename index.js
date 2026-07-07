// -----------------------------------------------------------------------------
// Copyright (c) 2018 Pau Sanchez
//
// MIT Licensed
// -----------------------------------------------------------------------------
const redis = require("redis");
const crypto = require("crypto");
const Mocha = require("mocha");

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
// Both TTLs come in as strings (env vars) or already-numbers (defaults).
// Parse once here so every redis call below passes a number and no call
// site has to remember to parseInt() it — the seemingly-safe raw fallback
// silently mangled some redis client encoder paths under load (see the
// integration-run notes later in this file's history).
const g_expirationTimeSec = parseInt(g_expirationTime, 10) || (7 * 24 * 3600);
const g_claimExpirationTimeSec = parseInt(g_claimExpirationTime, 10) || (10 * 60);

// Central schema for the fixed-name redis keys used for cross-runner
// coordination state (as opposed to per-test claim keys, which are built
// dynamically by buildTestKeyFromPath). Grown incrementally as new shared
// keys are introduced, so every reader/writer of a given key agrees on its
// exact name instead of re-typing the template literal at each call site.
const redisKeys = {
  testUniverse: () => `${g_testExecutionId}:test_universe`,
  doneTests: () => `${g_testExecutionId}:done_tests`,
  expectedTotal: () => `${g_testExecutionId}:expected_total`,
  expectedTotalIndividual: () => `${g_testExecutionId}:expected_total_individual`,
  runnersActive: () => `${g_testExecutionId}:runners_active`,
  capHitMarker: (testKey) => `${g_testExecutionId}:cap_hit_marker:${testKey}`,
  rescueCount: (testKey) => `${g_testExecutionId}:rescue_count:${testKey}`,
  testResult: () => `${g_testExecutionId}:test_result`,
  failedCount: () => `${g_testExecutionId}:failed_count`,
};

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

// runners_active bookkeeping. Every runner INCRs at globalSetup and DECRs
// exactly once at teardown or on SIGTERM/SIGINT. The flag guards against
// double-decrement when both paths could fire.
let g_runnersActiveDecremented = false;

// Local test count derived from computeTestKeys. Used to publish
// expected_total in redis via max-tracking, and later as a fast local
// check during drain.
let g_localExpectedCount = 0;

// Raw count of individual Test objects walked, before [serial-*] collapsing.
// expected_total counts coordination units (a whole serial group is one
// unit), which undercounts the true number of tests whenever serial groups
// are used. Published separately so consumers that want an exact "how many
// tests" figure aren't stuck with the coordination-unit number.
let g_localIndividualTestCount = 0;

// Reference to the root Suite captured at globalSetup. The drain phase
// reuses it to build fresh Mocha.Runner instances that re-execute only
// the orphaned tests (marked non-pending; everything else stays pending).
let g_rootSuite = null;
// Snapshot of each test's original `pending` flag, captured once, so drain
// iterations can restore state between attempts without accidentally
// unhiding user-authored `it.skip(...)` tests.
let g_originalPending = new WeakMap();
// True while the drain loop is active. Read by beforeEach so it can apply
// drain-specific bookkeeping (rescue budget etc., added in a later step).
let g_drainPhase = false;
// How many tests this runner personally rescued during drain. Reported in
// the completion banner — useful for post-hoc analysis of which pods did
// the rescuing work.
let g_localRescueCount = 0;

// Reverse map: serial-group key -> [Test, ...]. Populated in computeTestKeys
// so the cap-hit handler can enumerate every test belonging to a failing
// serial group and write one synthetic result row per test (see docs plan
// Q12 — individual failure rows read more naturally to external reporters
// than a single group-level row).
const g_serialGroupTests = new Map();

// Rescue budget per test. When a test has been picked up by the drain
// phase this many times without producing a result, it's declared broken
// (crashes its runner) and a synthetic failure row is written on its behalf.
const g_maxRescuesPerTest = Math.max(
  1,
  parseInt(process.env.MOCHA_DISTRIBUTED_MAX_RESCUES_PER_TEST || "3", 10) || 3
);

// Drain configuration. All have documented defaults; see docs plan.
const g_drainEnabled =
  (process.env.MOCHA_DISTRIBUTED_DRAIN_ENABLED || "true") !== "false";
const g_drainTimeoutSec =
  parseInt(process.env.MOCHA_DISTRIBUTED_DRAIN_TIMEOUT || "1800", 10) || 1800;
const g_drainPollIntervalSec = Math.max(
  1,
  parseInt(process.env.MOCHA_DISTRIBUTED_DRAIN_POLL_INTERVAL || "5", 10) || 5
);

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
// Full set of distinct claim keys from this runner's local pre-walk.
// Prepopulated into test_universe at mochaGlobalSetup (see
// publishTestUniverse) so a test whose every attempt is preempted before
// its beforeEach ever fires is still discoverable as an orphan during
// drain -- SDIFF against done_tests only rescues keys that are IN
// test_universe, and until now the only writer was the per-attempt SADD
// in beforeEach.
let g_localTestKeys = new Set();
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
// publishTestUniverse
//
// Prepopulate test_universe with every claim key this runner's local
// pre-walk knows about, before any test executes. Additive to (never a
// replacement for) the per-attempt SADD in beforeEach -- that SADD is the
// only path for tests added dynamically inside a before() hook (an
// unsupported-but-degraded-gracefully pattern), so it must stay. SADD is
// idempotent, so re-adding an already-present key here is a no-op.
// -----------------------------------------------------------------------------
async function publishTestUniverse() {
  if (g_localTestKeys.size === 0) return;
  const key = redisKeys.testUniverse();
  await g_redis
    .multi()
    .sAdd(key, Array.from(g_localTestKeys))
    .expire(key, g_expirationTimeSec)
    .exec();
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

  g_localTestKeys.clear();
  g_localIndividualTestCount = 0;
  walkSuite(rootSuite, (test) => {
    g_localIndividualTestCount += 1;
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

    g_localTestKeys.add(key);
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

  // Expose serial-group membership for the cap-hit handler (see
  // handleRescueCap). Cleared before repopulating so repeated invocations
  // — not expected in production, but common in tests — don't leak stale
  // entries.
  g_serialGroupTests.clear();
  for (const [key, members] of serialGroupMembers) {
    g_serialGroupTests.set(key, members.slice());
  }

  // Count distinct claim keys: serial groups collapse to one key regardless
  // of member count, plain tests contribute one each (with :dup-N making
  // duplicated titles distinct). This is what expected_total is compared
  // against globally. g_localTestKeys (populated in the walk above) already
  // holds exactly this set, so no second walk is needed to derive the count.
  g_localExpectedCount = g_localTestKeys.size;
}

// -----------------------------------------------------------------------------
// publishExpectedTotal
//
// Max-tracking publication of the globally expected test count. Every runner
// computes its local count from computeTestKeys (which should agree across
// runners loading the same test files), reads the current published value,
// and writes its own if higher. A MOCHA_DISTRIBUTED_EXPECTED_TOTAL_OVERRIDE
// env var takes precedence — escape hatch for suites with dynamically added
// tests where the pre-walk under-counts.
//
// The GET / SET pair is not atomic; two runners racing may both write the
// same or a lower value. Since all runners produce the same local count in
// the well-configured case, this is a self-correcting no-op in practice.
// Homogeneous-invocation divergence (Q6 in the plan) is reported as a WARN
// on startup rather than enforced.
// -----------------------------------------------------------------------------
async function publishExpectedTotal() {
  const override = process.env.MOCHA_DISTRIBUTED_EXPECTED_TOTAL_OVERRIDE;
  const local = override != null && override !== ''
    ? (parseInt(override, 10) || 0)
    : g_localExpectedCount;

  if (local <= 0) return;

  const key = redisKeys.expectedTotal();
  const raw = await g_redis.get(key);
  const remote = parseInt(raw || '0', 10) || 0;

  if (local > remote) {
    await g_redis.set(key, String(local), { EX: g_expirationTimeSec });
  } else if (local < remote) {
    console.log(
      `[mocha-distributed] WARN: this runner sees ${local} tests but ` +
      `another runner published ${remote} — heterogeneous invocation ` +
      `detected, drain may not converge. Ensure all runners use the same ` +
      `test files and the same --grep / .only() configuration.`
    );
  }
}

// -----------------------------------------------------------------------------
// publishIndividualExpectedTotal
//
// Same max-tracking publication as publishExpectedTotal, but for the raw
// per-test walk count (g_localIndividualTestCount) rather than the
// coordination-unit count. Diagnostic only - nothing in the claim/drain
// logic reads this back, so a stale or missing value can't affect
// correctness, only the accuracy of anything displaying it.
// -----------------------------------------------------------------------------
async function publishIndividualExpectedTotal() {
  const local = g_localIndividualTestCount;
  if (local <= 0) return;

  const key = redisKeys.expectedTotalIndividual();
  const raw = await g_redis.get(key);
  const remote = parseInt(raw || '0', 10) || 0;

  if (local > remote) {
    await g_redis.set(key, String(local), { EX: g_expirationTimeSec });
  }
}

// -----------------------------------------------------------------------------
// incrementRunnersActive / decrementRunnersActive
//
// Track the number of live runners so drain-phase peers can distinguish
// "still work in flight elsewhere" from "nobody left to rescue." Decrement is
// idempotent: both the normal teardown path and the SIGTERM path may call it,
// but the counter must only move once per runner.
// -----------------------------------------------------------------------------
async function incrementRunnersActive() {
  const key = redisKeys.runnersActive();
  await g_redis.incr(key);
  await g_redis.expire(key, g_expirationTimeSec);
}

async function decrementRunnersActive() {
  if (g_runnersActiveDecremented) return;
  g_runnersActiveDecremented = true;
  try {
    const key = redisKeys.runnersActive();
    await g_redis.decr(key);
  } catch (_) { /* best-effort during shutdown */ }
}

// -----------------------------------------------------------------------------
// handleRescueCap
//
// Called from beforeEach when a drain-phase attempt would exceed the
// per-test rescue budget. If we win the CAS (single-writer INCR on the
// cap_hit_marker key), we write a synthetic failure row for each test in
// the affected group so external reporters see a normal terminal state,
// mark the key as done, and bump failed_count. The current attempt is
// then skipped so we don't run a broken test yet again.
//
// If the CAS is lost (another runner got here first), we just skip — the
// other runner is responsible for writing the synthetic row.
// -----------------------------------------------------------------------------
async function handleRescueCap(testKey, currentTest, attempts) {
  const markerKey = redisKeys.capHitMarker(testKey);
  let winner = false;
  try {
    const n = await g_redis.incr(markerKey);
    await g_redis.expire(markerKey, g_expirationTimeSec);
    winner = (n === 1);
  } catch (_) { /* if INCR fails, be conservative and don't write */ }

  if (!winner) return;

  const info = g_testKeyInfo.get(currentTest);
  const affected =
    info && info.isSerial && g_serialGroupTests.has(testKey)
      ? g_serialGroupTests.get(testKey)
      : [currentTest];

  const resultKey = redisKeys.testResult();
  const countKey  = redisKeys.failedCount();
  const doneKey   = redisKeys.doneTests();

  const now = Date.now();
  const errObj = {
    message: `Test orphaned after ${attempts} rescue attempts; ` +
             `likely crashes its runner. See MOCHA_DISTRIBUTED_MAX_RESCUES_PER_TEST.`,
  };

  try {
    let pipe = g_redis.multi();
    for (const t of affected) {
      const row = {
        id: getTestPathFromTest(t),
        type: t.type || 'test',
        title: t.title,
        timedOut: false,
        duration: 0,
        startTime: now,
        endTime: now,
        retryAttempt: attempts,
        retryTotal: attempts,
        file: t.file,
        state: 'failed',
        failed: true,
        err: errObj,
        stdout: '',
        stderr: '',
        syntheticOrphan: true,
      };
      pipe = pipe
        .rPush(resultKey, JSON.stringify(row))
        .incr(countKey);
    }
    pipe = pipe
      .expire(resultKey, g_expirationTimeSec)
      .expire(countKey,  g_expirationTimeSec)
      .sAdd(doneKey, testKey)
      .expire(doneKey, g_expirationTimeSec);
    await pipe.exec();
  } catch (_) { /* best-effort */ }

  console.log(
    `[mocha-distributed] drain: ERROR test "${currentTest.title}" ` +
    `exhausted rescue budget (${attempts}/${g_maxRescuesPerTest}), ` +
    `marking as failed`
  );
}

// -----------------------------------------------------------------------------
// sleep
//
// Await-friendly sleep used by the drain-phase poll loop.
// -----------------------------------------------------------------------------
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// -----------------------------------------------------------------------------
// computeOrphans
//
// Return the set of test keys that are unaccounted for: present in
// test_universe, absent from done_tests, and with no live claim key. Fewer
// round-trips than the naive per-key polling; two pipelined multis in the
// worst case:
//   1. SDIFF test_universe done_tests — candidate orphans.
//   2. EXISTS candidate_i (batched) — filter out live in-flight claims.
//
// Returns an Array<string> so callers can iterate deterministically for
// logging.
// -----------------------------------------------------------------------------
async function computeOrphans() {
  const universeKey = redisKeys.testUniverse();
  const doneKey     = redisKeys.doneTests();

  let candidates = [];
  try {
    // node-redis v4 exposes sDiff / sMembers; fall back gracefully if the
    // client stub in tests doesn't implement it.
    if (typeof g_redis.sDiff === "function") {
      candidates = await g_redis.sDiff([universeKey, doneKey]);
    } else {
      const [uni, done] = await Promise.all([
        g_redis.sMembers(universeKey),
        g_redis.sMembers(doneKey),
      ]);
      const doneSet = new Set(done || []);
      candidates = (uni || []).filter((k) => !doneSet.has(k));
    }
  } catch (_) {
    return [];
  }

  if (!candidates || candidates.length === 0) return [];

  // Pipelined EXISTS: filter out any candidate whose claim key is still
  // present in redis. A present claim means either an in-flight test
  // (short TTL, someone else is running it) or a completed tombstone
  // (long TTL) — both mean we shouldn't try to rescue it right now.
  try {
    const multi = g_redis.multi();
    for (const k of candidates) multi.exists(k);
    const results = await multi.exec();
    const orphans = [];
    for (let i = 0; i < candidates.length; i++) {
      if (!results[i]) orphans.push(candidates[i]);
    }
    return orphans;
  } catch (_) {
    // If EXISTS fails, be conservative and return no orphans rather than
    // trigger a re-run against stale state.
    return [];
  }
}

// -----------------------------------------------------------------------------
// getDoneCount
//
// Cheap early-exit check for the drain loop: SCARD done_tests. Compared
// against expected_total to know when the execution is globally complete.
// -----------------------------------------------------------------------------
async function getDoneCount() {
  const key = redisKeys.doneTests();
  try { return await g_redis.sCard(key); }
  catch (_) { return 0; }
}

async function getExpectedTotal() {
  const key = redisKeys.expectedTotal();
  try {
    const v = await g_redis.get(key);
    return parseInt(v || "0", 10) || 0;
  } catch (_) { return 0; }
}

async function getRunnersActive() {
  const key = redisKeys.runnersActive();
  try {
    const v = await g_redis.get(key);
    return parseInt(v || "0", 10) || 0;
  } catch (_) { return 0; }
}

// -----------------------------------------------------------------------------
// runDrainIteration
//
// Re-execute the given orphan keys by walking the shared root Suite,
// marking non-orphans as pending, and driving a fresh Mocha.Runner over
// the tree. Uses public mocha API (Suite + Runner) to avoid reaching into
// internals; the reason we keep the root Suite alive across iterations is
// that Mocha's Runner is single-use once it has emitted its terminal
// EVENT_RUN_END, but the Suite tree itself is safely reusable.
//
// The claim/skip machinery in beforeEach continues to work naturally:
// several drain-phase runners may race on the same orphan, but SET NX
// serialises them.
// -----------------------------------------------------------------------------
async function runDrainIteration(orphanKeys) {
  if (!g_rootSuite) return;
  const orphanSet = new Set(orphanKeys);

  // Restore the original pending state, then mark non-orphans as pending.
  // Tests originally authored as `it.skip(...)` stay skipped no matter what.
  walkSuite(g_rootSuite, (test) => {
    const info = g_testKeyInfo.get(test);
    const originallyPending = g_originalPending.get(test);
    if (originallyPending) {
      test.pending = true;
      return;
    }
    const shouldRun = info && orphanSet.has(info.key);
    test.pending = !shouldRun;
    if (shouldRun) {
      // Clear leftover state from Phase A so the fresh Runner treats
      // this as a first-time execution. Without this, mocha's internal
      // hook machinery can carry over the previous "skipped" verdict
      // and short-circuit before beforeEach fires.
      test.state    = undefined;
      test.timedOut = false;
      test.duration = undefined;
      test.speed    = undefined;
      test.err      = undefined;
      if (typeof test._currentRetry === 'number') test._currentRetry = 0;
    }
  });

  try {
    const RunnerCtor = Mocha.Runner;
    const runner = new RunnerCtor(g_rootSuite, { delay: false });
    // Per-rescue observability: emit compact log lines on the same events
    // mocha's own reporter would use. We log the test title (not the redis
    // key) so users can grep in kubectl-logs by the same title mocha printed
    // during Phase A.
    const EVENTS = RunnerCtor.constants || {};
    const onPass = (test) => {
      g_localRescueCount++;
      const dur = typeof test.duration === 'number' ? `${test.duration}ms` : 'unknown';
      console.log(`[mocha-distributed] drain: rescued "${test.title}" — passed in ${dur}`);
    };
    const onFail = (test) => {
      g_localRescueCount++;
      const dur = typeof test.duration === 'number' ? `${test.duration}ms` : 'unknown';
      console.log(`[mocha-distributed] drain: rescued "${test.title}" — failed in ${dur}`);
    };
    if (EVENTS.EVENT_TEST_PASS) runner.on(EVENTS.EVENT_TEST_PASS, onPass);
    if (EVENTS.EVENT_TEST_FAIL) runner.on(EVENTS.EVENT_TEST_FAIL, onFail);

    await new Promise((resolve) => runner.run(resolve));
  } catch (err) {
    console.log(`[mocha-distributed] drain: iteration errored: ${err && err.message}`);
  }
}

// -----------------------------------------------------------------------------
// drainPhaseBanner
//
// One-shot header printed when drain begins. Deliberately in a fixed shape
// so it's easy to grep in kubectl-logs output.
// -----------------------------------------------------------------------------
async function drainPhaseBanner() {
  const expected = await getExpectedTotal();
  const done = await getDoneCount();
  const waiting = Math.max(0, expected - done);
  console.log(`[mocha-distributed] Local test iteration complete. Entering drain phase.`);
  console.log(`[mocha-distributed]   Local tests attempted : ${g_localExpectedCount}`);
  console.log(`[mocha-distributed]   Global tests expected : ${expected}`);
  console.log(`[mocha-distributed]   Global tests done     : ${done}`);
  console.log(`[mocha-distributed]   Waiting for           : ${waiting} tests`);
  console.log(`[mocha-distributed]   Drain timeout         : ${g_drainTimeoutSec}s`);
}

// -----------------------------------------------------------------------------
// runDrainLoop
//
// The heart of Phase B. Every runner enters this after its local mocha
// iteration finishes. Loop invariants:
//   - Exit as soon as SCARD done_tests >= expected_total.
//   - Exit non-zero (via caller) if wall-clock reaches drain timeout.
//   - On every iteration: compute orphans, rescue any we find, otherwise
//     sleep (with jitter) and re-check.
//
// The loop does not attempt to be clever about which runner rescues which
// orphan — the claim/skip mechanism in beforeEach serialises races.
// -----------------------------------------------------------------------------
async function runDrainLoop() {
  g_drainPhase = true;
  const startedAt = Date.now();
  const timeoutMs = g_drainTimeoutSec * 1000;

  await drainPhaseBanner();

  // Periodic status printer: keeps kubectl-logs readers oriented while the
  // loop is waiting. 30s cadence is decoupled from poll interval so the
  // status line stays readable even with a fast poll.
  const statusIntervalMs = 30 * 1000;
  let lastStatusAt = Date.now();
  const printStatus = async () => {
    try {
      const [done, expected, active] = await Promise.all([
        getDoneCount(), getExpectedTotal(), getRunnersActive(),
      ]);
      const elapsedSec = Math.round((Date.now() - startedAt) / 1000);
      const waiting = Math.max(0, expected - done);
      console.log(
        `[mocha-distributed] drain: ${done}/${expected} done, ` +
        `${waiting} waiting, ${active} runners active, elapsed ${elapsedSec}s`
      );
    } catch (_) { /* status is best-effort */ }
  };

  while (true) {
    // Timeout check
    const elapsedMs = Date.now() - startedAt;
    if (elapsedMs >= timeoutMs) {
      const done = await getDoneCount();
      const expected = await getExpectedTotal();
      console.log(
        `[mocha-distributed] drain: TIMEOUT after ${g_drainTimeoutSec}s — ` +
        `${done}/${expected} tests done, ` +
        `${Math.max(0, expected - done)} still unaccounted for`
      );
      return { timedOut: true, done, expected };
    }

    // Cheap early-exit: done count reached expected total?
    const done = await getDoneCount();
    const expected = await getExpectedTotal();
    if (expected > 0 && done >= expected) {
      const elapsedSec = Math.round((Date.now() - startedAt) / 1000);
      console.log(
        `[mocha-distributed] drain: complete — ${done}/${expected} tests done, ` +
        `elapsed ${elapsedSec}s, this runner rescued ${g_localRescueCount} tests`
      );
      return { timedOut: false, done, expected };
    }

    // Emit a periodic status line if it's been long enough.
    if (Date.now() - lastStatusAt >= statusIntervalMs) {
      await printStatus();
      lastStatusAt = Date.now();
    }

    // Find orphans; run them if any; otherwise sleep and re-check.
    const orphans = await computeOrphans();
    if (orphans.length > 0) {
      console.log(
        `[mocha-distributed] drain: found ${orphans.length} orphan(s), attempting rescue`
      );
      await runDrainIteration(orphans);
      // No sleep on a productive iteration; loop back immediately.
    } else {
      // Jittered sleep: base ± 20% to avoid thundering-herd polling.
      const base = g_drainPollIntervalSec * 1000;
      const jitter = Math.floor(base * 0.2 * (Math.random() * 2 - 1));
      await sleep(base + jitter);
    }
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
    g_rootSuite = this.suite;
    computeTestKeys(this.suite);
    // Snapshot original pending state so drain iterations can restore
    // user-authored `it.skip(...)` between orphan-filter passes.
    walkSuite(g_rootSuite, (test) => {
      g_originalPending.set(test, !!test.pending);
    });
  }

  // Publish shared state used by the drain phase:
  //   - test_universe  : full local key set, published upfront so a test
  //                       whose every attempt is preempted before its
  //                       beforeEach fires is still discoverable as an orphan.
  //   - expected_total : max across runners of local test count (a whole
  //                       [serial-*] group counts as one unit here).
  //   - expected_total_individual : same max-tracking, but the raw
  //                       per-test walk count - diagnostic only, not read
  //                       by any coordination/drain logic.
  //   - runners_active : live runner counter, INCR here / DECR at teardown.
  // All four are best-effort -- failures shouldn't prevent tests from running.
  try { await publishTestUniverse();    } catch (_) {}
  try { await publishExpectedTotal();   } catch (_) {}
  try { await publishIndividualExpectedTotal(); } catch (_) {}
  try { await incrementRunnersActive(); } catch (_) {}
};

// -----------------------------------------------------------------------------
// Quit from redis
// -----------------------------------------------------------------------------
exports.mochaGlobalTeardown = async function () {
  if (g_redis) {
    // Drain phase: block redis quit until the execution is globally
    // accounted for, or until the drain timeout hits. See docs plan for
    // rationale. Users can disable this with MOCHA_DISTRIBUTED_DRAIN_ENABLED
    // if they need the old "exit as soon as local iteration finishes"
    // behaviour — they should not, on preemptible infra.
    let drainResult = null;
    if (g_drainEnabled) {
      try { drainResult = await runDrainLoop(); } catch (err) {
        console.log(`[mocha-distributed] drain: aborted due to error: ${err && err.message}`);
      }
    } else {
      console.log(
        `[mocha-distributed] WARN: MOCHA_DISTRIBUTED_DRAIN_ENABLED=false — ` +
        `preemption resilience degraded. Runners may exit while others are ` +
        `still executing tests, orphaning any tests preempted afterward.`
      );
    }

    // Best-effort DECR before we tear down the connection. Idempotent, so
    // if SIGTERM fired first this is a no-op.
    try { await decrementRunnersActive(); } catch (_) {}

    // Compute the global exit-code verdict before quitting the client.
    // Contract (see docs/preemption-resilience-plan.md):
    //   0  drain completed AND global failed_count is 0
    //   1  drain completed AND global failed_count > 0
    //   2  drain timed out
    // We honour the contract only when drain actually ran; with drain
    // disabled, mocha's own exit code is left alone (backwards
    // compatibility for the escape hatch).
    let verdict = null;
    if (g_drainEnabled && drainResult) {
      if (drainResult.timedOut) {
        verdict = 2;
      } else {
        let failed = 0;
        try {
          const s = await g_redis.get(redisKeys.failedCount());
          failed = parseInt(s || '0', 10) || 0;
        } catch (_) { /* on read error, fall through to mocha default */ }
        verdict = failed > 0 ? 1 : 0;
      }
    }

    await g_redis.quit();

    // Apply the verdict once the client is closed. process.exit here
    // short-circuits mocha's own exit code (which is failCount capped at
    // 255 — not what we want for a distributed run). We defer via a
    // process.on('exit') so any queued afterAll hooks the harness might
    // add still run.
    if (verdict !== null) {
      process.exitCode = verdict;
      process.on('exit', () => process.exit(verdict));
    }
  }
};

// -----------------------------------------------------------------------------
// Hook tests
//
// Please note that we run skip before each test if the ownership of it has
// already been defined by another runner.
// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
// Test-only entry points
//
// Exposed to let unit tests invoke internal helpers directly — in
// particular the drain-phase beforeEach path (with g_drainPhase forced on)
// and the cap-hit handler. Not part of the public API.
// -----------------------------------------------------------------------------
exports.__testing = {
  setDrainPhase(v) { g_drainPhase = !!v; },
  isDrainPhase()   { return g_drainPhase; },
  handleRescueCap,
  computeTestKeys,
  computeOrphans,
};

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

    // Drain-phase rescue budget: if this runner is in drain (not the
    // initial iteration), bump the per-test counter and check the cap
    // before attempting to claim. On cap exhaustion, write a synthetic
    // failure row via the CAS-guarded handler and skip.
    if (g_drainPhase) {
      try {
        const rcKey = redisKeys.rescueCount(testKey);
        const attempts = await g_redis.incr(rcKey);
        await g_redis.expire(rcKey, g_expirationTimeSec);
        if (attempts > g_maxRescuesPerTest) {
          await handleRescueCap(testKey, this.currentTest, attempts);
          this.currentTest.title += ' (rescue budget exhausted)';
          return this.skip();
        }
      } catch (_) { /* best-effort; fall through to normal claim */ }
    }

    // Atomically set/get the runner id associated to this test. Only the first
    // runner to get there will set the value to its own runner id.
    //
    // Piggyback the test_universe SADD onto the same pipeline: every attempt
    // on any runner (whether it wins the claim or not) contributes a member
    // to the shared set. The drain phase later diffs test_universe against
    // done_tests to find orphans, so we want the set to reflect "tests that
    // have been attempted somewhere", not just "tests this runner claimed".
    const universeKey = redisKeys.testUniverse();
    const [_, assignedRunnerId] = await g_redis
      .multi()
      .set(testKey, g_runnerId, { EX: g_claimExpirationTime, NX: true })
      .get(testKey)
      .sAdd(universeKey, testKey)
      .expire(universeKey, g_expirationTimeSec)
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
        g_redis.expire(testKey, g_claimExpirationTimeSec).catch(() => {});
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

      // Mark this test key as done in the shared set, so the drain phase
      // can tell it has been accounted for. Serial groups share one claim
      // key and are semantically "done" only when the last test in the
      // group finishes; marking done earlier would let a drain-phase peer
      // conclude the group is complete while later tests still have to run
      // if the owner gets preempted mid-group.
      const preInfoAE = g_testKeyInfo.get(this.currentTest);
      let shouldMarkDone;
      if (preInfoAE) {
        shouldMarkDone = !preInfoAE.isSerial || preInfoAE.isLastInSerialGroup;
      } else {
        // Fallback for tests missing from the pre-walk: only mark non-serial
        // tests. Under-marking is safer than over-marking here — a missing
        // done entry causes an unnecessary rescue attempt (harmless, gated
        // by SET NX), whereas a premature done entry could hide an orphan.
        const livePath = getTestPath(this.currentTest).join(":");
        shouldMarkDone = livePath.indexOf(SERIAL_PREFIX) === -1;
      }
      const doneKey = redisKeys.doneTests();

      let pipe = g_redis
        .multi()
        .rPush(resultKey, JSON.stringify(testResult))
        .expire(resultKey, g_expirationTimeSec)
        .incr(countKey)
        .expire(countKey, g_expirationTimeSec);
      if (shouldMarkDone && g_currentClaimKey) {
        pipe = pipe
          .sAdd(doneKey, g_currentClaimKey)
          .expire(doneKey, g_expirationTimeSec);
      }
      await pipe.exec();
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
        await g_redis.expire(g_currentClaimKey, g_expirationTimeSec);
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
    // Decrement runners_active so peers see us leaving the pool. Guarded
    // against double-decrement by decrementRunnersActive itself.
    if (g_redis) {
      try { await decrementRunnersActive(); } catch (_) {}
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
