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
  now = Date.now,
  // unref'd: a heartbeat must never be what keeps the process alive.
  setInterval: startTimer = (fn, ms) => setInterval(fn, ms).unref(),
  clearInterval: stopTimer = (id) => clearInterval(id),
}) {
  let timer = null;

  async function beat() {
    try {
      await redis.zAdd(key, { score: now(), value: runnerId });
      await redis.expire(key, ttlSec);
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
