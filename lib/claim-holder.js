// -----------------------------------------------------------------------------
// lib/claim-holder.js
//
// Keeps claim keys alive while this runner holds them: one keepalive timer
// per key, refreshing its short TTL until the key is released.
//
// A plain test is held only while it runs. A serial group is held from its
// first test until its last one, even while the runner executes unrelated
// tests in between (a group may span several suites and files), so a runner
// that dies mid-group lets the claim expire and the drain phase rescue it.
//
// Redis and the timer API are injected so this unit is testable on its own.
// -----------------------------------------------------------------------------
'use strict';

function createClaimHolder({
  refresh,
  intervalMs,
  setInterval: startTimer = (fn, ms) => setInterval(fn, ms),
  clearInterval: stopTimer = (id) => clearInterval(id),
}) {
  const timers = new Map(); // claim key -> timer id

  return {
    // Start refreshing `key`. Idempotent: holding a held key keeps its timer.
    hold(key) {
      if (timers.has(key)) return;
      timers.set(key, startTimer(() => refresh(key), intervalMs));
    },

    isHeld(key) {
      return timers.has(key);
    },

    heldKeys() {
      return [...timers.keys()];
    },

    // Stop refreshing `key`. Returns whether it was held.
    release(key) {
      if (!timers.has(key)) return false;
      stopTimer(timers.get(key));
      timers.delete(key);
      return true;
    },

    // Stop every timer; returns the keys that were held so the caller can
    // decide what to do with them (e.g. DEL them on shutdown).
    releaseAll() {
      const keys = [...timers.keys()];
      for (const key of keys) this.release(key);
      return keys;
    },
  };
}

module.exports = { createClaimHolder };
