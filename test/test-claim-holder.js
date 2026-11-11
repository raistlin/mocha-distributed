// -----------------------------------------------------------------------------
// test/test-claim-holder.js
//
// Unit tests for lib/claim-holder.js: the set of claim keys this runner
// holds. It keeps no timers of its own; the runner heartbeat reads it on
// every beat to refresh the claims (see test-runner-heartbeat.js).
// -----------------------------------------------------------------------------
'use strict';

const assert = require('assert');
const { createClaimHolder } = require('../lib/claim-holder');

describe('claim holder', function () {

  it('holds a key until it is released', function () {
    const holder = createClaimHolder();

    holder.hold('k1');

    assert.strictEqual(holder.isHeld('k1'), true);
    assert.deepStrictEqual(holder.heldKeys(), ['k1']);
  });

  it('holding the same key twice keeps it once', function () {
    const holder = createClaimHolder();

    holder.hold('k1');
    holder.hold('k1');

    assert.deepStrictEqual(holder.heldKeys(), ['k1']);
  });

  it('release forgets the key and says it was held', function () {
    const holder = createClaimHolder();

    holder.hold('k1');

    assert.strictEqual(holder.release('k1'), true);
    assert.strictEqual(holder.isHeld('k1'), false);
    assert.deepStrictEqual(holder.heldKeys(), []);
  });

  it('release of a key that is not held is a no-op', function () {
    const holder = createClaimHolder();
    assert.strictEqual(holder.release('missing'), false);
  });

  it('releaseAll forgets every key and returns the keys it was holding', function () {
    const holder = createClaimHolder();

    holder.hold('k1');
    holder.hold('k2');

    assert.deepStrictEqual(holder.releaseAll().sort(), ['k1', 'k2']);
    assert.deepStrictEqual(holder.heldKeys(), []);
  });

  it('heldKeys returns a copy the caller cannot use to change the set', function () {
    const holder = createClaimHolder();

    holder.hold('k1');
    holder.heldKeys().push('k2');

    assert.deepStrictEqual(holder.heldKeys(), ['k1']);
  });
});
