/** Live and fresh-image requests may relay; recordings/implicit starts do not gain that permission. */
import assert from "node:assert/strict";
import { PassThrough } from "node:stream";
import type { ServerResponse } from "node:http";
import test from "node:test";
import { GatewayState } from "../src/domain/gateway-state.js";
import { LiveStreamManager } from "../src/stream/live-stream-manager.js";

const camera = { serial: "lte", name: "Camera", model: "T8151", stationSerial: "lte", streamSupported: true, doorbellSupported: false };

test("multiple live consumers use one explicit start and stop after final departure", async context => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const state = new GatewayState(); state.registerCamera(camera);
  let starts = 0, stops = 0;
  const manager = new LiveStreamManager(state, {} as never, {
    async startStream() { assert.fail("live viewers need explicit intent"); },
    async startOnDemandStream() { starts++; },
    async stopStream() { stops++; },
  }, 5);
  try {
    const first = new PassThrough() as unknown as ServerResponse, second = new PassThrough() as unknown as ServerResponse;
    await manager.addClient("lte", first); await manager.addClient("lte", second);
    assert.equal(starts, 1);
    first.emit("close"); context.mock.timers.tick(10); assert.equal(stops, 0);
    second.emit("close"); context.mock.timers.tick(10);
    await Promise.resolve(); assert.equal(stops, 1);
  } finally { await manager.close(); state.close(); }
});

for (const [budget, explicit, expected] of [[45000, undefined, 45000], [undefined, undefined, 30000], [45000, 10, 10]] as const) {
  test(`snapshot waits for its selected budget (${budget}, explicit ${explicit}) and releases its lease`, async context => {
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const state = new GatewayState(); state.registerCamera(camera);
    let starts = 0;
    const manager = new LiveStreamManager(state, {} as never, {
      async startStream() { assert.fail("fresh capture needs explicit intent"); },
      async startOnDemandStream() { starts++; },
      liveSnapshotTimeoutMilliseconds: () => budget,
      async stopStream() {},
    }, 5);
    try {
      let settled = false;
      const capture = manager.captureSnapshot("lte", explicit);
      const result = capture.then(() => { settled = true; }, error => { settled = true; throw error; });
      const failed = assert.rejects(result, /Timed out/);
      await Promise.resolve(); context.mock.timers.tick(expected - 1); await Promise.resolve();
      assert.equal(settled, false); assert.equal(starts, 1);
      context.mock.timers.tick(1); await failed;
      assert.equal(state.listenerCount("event"), 0);
    } finally { await manager.close(); state.close(); }
  });
}

test("recording acquisition never requests the LTE relay trial", async () => {
  const state = new GatewayState(); state.registerCamera(camera);
  let regular = 0;
  const manager = new LiveStreamManager(state, {} as never, {
    async startStream() { regular++; throw new Error("synthetic recording start rejected"); },
    async startOnDemandStream() { assert.fail("recording must not request relay"); },
    async stopStream() {},
  }, 5);
  try {
    await assert.rejects(manager.recordClip("lte", 1), /synthetic recording start rejected/);
    assert.equal(regular, 1);
  } finally { await manager.close(); state.close(); }
});

test("an LTE fresh image arriving after the normal 30s window completes inside its 45s budget", async context => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const state = new GatewayState(); state.registerCamera(camera);
  const manager = new LiveStreamManager(state, {} as never, {
    async startStream() { assert.fail("capture must carry explicit intent"); },
    async startOnDemandStream() {},
    liveSnapshotTimeoutMilliseconds: () => 45000,
    async stopStream() {},
  }, 5);
  try {
    const pending = manager.captureSnapshot("lte");
    await Promise.resolve(); context.mock.timers.tick(35000);
    state.updateSnapshot("lte", { capturedAt: new Date().toISOString(), contentType: "image/jpeg", source: "live", revision: 1 });
    assert.equal((await pending).revision, 1);
  } finally { await manager.close(); state.close(); }
});
