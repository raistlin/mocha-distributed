// -----------------------------------------------------------------------------
// test/test-claim-holder.js
//
// Unit tests for lib/claim-holder.js: keeps one keepalive timer per held
// claim key, independent of mocha and redis (both are injected).
// -----------------------------------------------------------------------------
'use strict';

const assert = require('assert');
const { createClaimHolder } = require('../lib/claim-holder');

// Fake timer API: records every interval so tests can fire them by hand.
function makeFakeTimers() {
  let nextId = 1;
  const active = new Map(); // id -> { fn, ms }
  return {
    active,
    setInterval: (fn, ms) => { const id = nextId++; active.set(id, { fn, ms }); return id; },
    clearInterval: (id) => { active.delete(id); },
    fireAll: () => { for (const { fn } of active.values()) fn(); },
  };
}

function makeHolder(timers, refreshed) {
  return createClaimHolder({
    refresh: (key) => refreshed.push(key),
    intervalMs: 1000,
    setInterval: timers.setInterval,
    clearInterval: timers.clearInterval,
  });
}

describe('claim holder', function () {

  it('refreshes a held key on every interval tick', function () {
    const timers = makeFakeTimers();
    const refreshed = [];
    const holder = makeHolder(timers, refreshed);

    holder.hold('k1');
    timers.fireAll();
    timers.fireAll();

    assert.deepStrictEqual(refreshed, ['k1', 'k1']);
    assert.strictEqual([...timers.active.values()][0].ms, 1000);
  });

  it('holding the same key twice keeps a single timer', function () {
    const timers = makeFakeTimers();
    const holder = makeHolder(timers, []);

    holder.hold('k1');
    holder.hold('k1');

    assert.strictEqual(timers.active.size, 1);
    assert.strictEqual(holder.isHeld('k1'), true);
  });

  it('release stops the refresh and forgets the key', function () {
    const timers = makeFakeTimers();
    const refreshed = [];
    const holder = makeHolder(timers, refreshed);

    holder.hold('k1');
    assert.strictEqual(holder.release('k1'), true);
    timers.fireAll();

    assert.deepStrictEqual(refreshed, []);
    assert.strictEqual(holder.isHeld('k1'), false);
    assert.strictEqual(timers.active.size, 0);
  });

  it('release of a key that is not held is a no-op', function () {
    const holder = makeHolder(makeFakeTimers(), []);
    assert.strictEqual(holder.release('missing'), false);
  });

  it('releaseAll stops every timer and returns the keys it was holding', function () {
    const timers = makeFakeTimers();
    const holder = makeHolder(timers, []);

    holder.hold('k1');
    holder.hold('k2');

    assert.deepStrictEqual(holder.releaseAll().sort(), ['k1', 'k2']);
    assert.strictEqual(timers.active.size, 0);
    assert.deepStrictEqual(holder.heldKeys(), []);
  });
});
