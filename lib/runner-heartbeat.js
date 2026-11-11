// -----------------------------------------------------------------------------
// lib/runner-heartbeat.js
//
// Live-runner tracking that survives hard kills. Each runner keeps its id in
// a sorted set scored by the last time it checked in (ms since epoch), and
// removes itself on a graceful exit. A runner that is SIGKILLed (OOM, spot
// reclaim, container teardown racing a SIGTERM handler) simply stops checking
// in and ages out of the live window, unlike the runners_active counter,
// which it never gets to decrement.
//
// The same beat refreshes the short TTL of every claim the runner holds
// (`heldClaims()`), in the same pipeline as the check-in. A claim therefore
// lives exactly as long as its runner keeps beating, however long the test
// takes, and expires within one claim TTL of the runner dying. The refresh
// uses EXPIRE ... GT (Redis 7+) so it never shortens a completed claim.
//
// Redis, the clock and the timer API are injected so this unit is testable on
// its own.
// -----------------------------------------------------------------------------
'use strict';

function createRunnerHeartbeat({
  redis,
  key,
  runnerId,
  intervalMs,
  ttlSec,
  heldClaims = () => [],
  claimTtlSec,
  now = Date.now,
  // unref'd: a heartbeat must never be what keeps the process alive.
  setInterval: startTimer = (fn, ms) => setInterval(fn, ms).unref(),
  clearInterval: stopTimer = (id) => clearInterval(id),
}) {
  let timer = null;

  async function beat() {
    try {
      let pipe = redis.multi()
        .zAdd(key, { score: now(), value: runnerId })
        .expire(key, ttlSec);
      // GT: only ever extend. A beat racing the test's completion must not
      // cut the claim's long tombstone TTL back to the short claim TTL.
      for (const claim of heldClaims()) pipe = pipe.expire(claim, claimTtlSec, 'GT');
      await pipe.exec();
    } catch (_) { /* best-effort; the next tick retries */ }
  }

  return {
    // Check in now, then on every interval until stopped.
    async start() {
      await beat();
      if (!timer) timer = startTimer(beat, intervalMs);
    },

    // Stop checking in and leave the live set right away.
    async stop() {
      if (timer) {
        stopTimer(timer);
        timer = null;
      }
      try { await redis.zRem(key, runnerId); } catch (_) { /* ages out */ }
    },
  };
}

// Number of runners that checked in within the last `windowMs`.
async function countLiveRunners(redis, key, windowMs, now = Date.now) {
  return redis.zCount(key, now() - windowMs, '+inf');
}

module.exports = { createRunnerHeartbeat, countLiveRunners };
