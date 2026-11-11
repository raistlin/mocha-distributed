// -----------------------------------------------------------------------------
// lib/claim-holder.js
//
// The set of claim keys this runner holds. It keeps no timers of its own:
// the runner heartbeat (lib/runner-heartbeat.js) reads it on every beat and
// refreshes each claim's short TTL, so a claim lives exactly as long as its
// runner keeps beating, however long the test takes.
//
// A plain test is held only while it runs. A serial group is held from its
// first test until its last one, even while the runner executes unrelated
// tests in between (a group may span several suites and files), so a runner
// that dies mid-group lets the claim expire and the drain phase rescue it.
// -----------------------------------------------------------------------------
'use strict';

function createClaimHolder() {
  const keys = new Set();

  return {
    // Idempotent: holding a held key keeps it once.
    hold(key) {
      keys.add(key);
    },

    isHeld(key) {
      return keys.has(key);
    },

    heldKeys() {
      return [...keys];
    },

    // Stop holding `key`. Returns whether it was held.
    release(key) {
      return keys.delete(key);
    },

    // Forget every key; returns the keys that were held so the caller can
    // decide what to do with them (e.g. DEL them on shutdown).
    releaseAll() {
      const held = [...keys];
      keys.clear();
      return held;
    },
  };
}

module.exports = { createClaimHolder };
