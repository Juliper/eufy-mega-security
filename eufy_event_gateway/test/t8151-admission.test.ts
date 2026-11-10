/** T8151 admission and existing standalone paths, using synthetic inventory and pushes. */
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import type { CameraIdentity } from "../src/domain/types.js";
import { MegaClient } from "../src/mega/client.js";
import { MegaPushReceiver, parsePushEvent, type MegaPushEvent } from "../src/mega/push.js";
import { catalogueIntegrationStatus } from "../src/provider/camera-capability-core.js";
import { describeCameraCapabilities } from "../src/provider/device-capabilities-core.js";
import { EufyProvider, inventoryLogSummaries, isPpcsStreamSupported, parseMegaInventory, ppcsStreamRoute,
  safePushLogSummary } from "../src/provider/eufy-provider.js";
import type { ProviderEvents } from "../src/provider/provider.js";
import { supportsStartupSnapshot } from "../src/startup-snapshot-warmup.js";

// Values demonstrate the existing decoders; they are not hardware measurements.
const inventory = { devices: [{
  device_sn: "lte-camera", parent_sn: "lte-camera", device_model: "T8151", device_type: 110,
  device_channel: 0, category: "eufy_security", p2p_did: "synthetic-did", p2p_conn: "synthetic-connection",
  params: [
    { param_type: 1101, param_value: "82" },
    { param_type: 2111, param_value: "4" },
    { param_type: 1138, param_value: "24.5" },
  ],
}] };

test("T8151 is ready to test with only its reported battery reads", () => {
  const device = parseMegaInventory(inventory)[0]!;
  assert.equal(catalogueIntegrationStatus(device.model, device.deviceType), "ready_to_test");
  const manifest = describeCameraCapabilities(device, { doorbellSupported: false, streamSupported: false });
  assert.equal(manifest.acceptedAsCamera, true);
  assert.deepEqual(manifest.capabilities.map(({ id }) => id), [
    "motion", "person", "retainedImage", "batteryLevel", "batteryCharging", "batteryTemperature",
  ]);
  assert.equal(manifest.matrix.find(({ id }) => id === "camera.live_stream")?.offerable, false);
});

test("T8151 self-parent routing uses its own peer and still requires transport prerequisites", () => {
  const device = parseMegaInventory(inventory)[0]!;
  const devices = new Map([[device.serial, device]]);
  assert.deepEqual(ppcsStreamRoute(device, devices), { peer: device, homeBaseAttached: false });
  assert.equal(isPpcsStreamSupported(device, devices, new Set()), false);
  assert.equal(isPpcsStreamSupported(device, devices, new Set([device.serial])), true);
  const summary = inventoryLogSummaries([device], new Set([device.serial]))[0]!;
  assert.equal(summary.hasParent, true);
  assert.equal(summary.stationPresent, true);
  assert.equal(summary.streamRoute, "direct");
  assert.equal(summary.acceptedAsCamera, true);
  const missingParent = { ...device, parentSerial: "absent-peer" };
  assert.equal(ppcsStreamRoute(missingParent, devices), null);
});

test("T8151 publishes battery state and camera detections without a managed station", async context => {
  const network = context.mock.method(globalThis, "fetch", async () => { throw new Error("Network forbidden"); });
  context.mock.method(MegaClient.prototype, "connect", async () => ({ state: "authenticated" }));
  context.mock.method(MegaClient.prototype, "inventory", async () => inventory);
  // Keep this a push/battery test: no session may open a real PPCS socket.
  context.mock.method(MegaClient.prototype, "dskKeys", async () => ({}));
  let deliver!: (event: MegaPushEvent) => void;
  context.mock.method(MegaPushReceiver.prototype, "start", async function(this: MegaPushReceiver) {
    deliver = Reflect.get(this, "onEvent") as (event: MegaPushEvent) => void;
  });
  const cameras: CameraIdentity[] = [];
  const motions: unknown[][] = [];
  const people: unknown[][] = [];
  const events = new Proxy({
    camera: (camera: CameraIdentity) => cameras.push(camera),
    motion: (...args: unknown[]) => motions.push(args),
    person: (...args: unknown[]) => people.push(args),
  }, { get: (target, key) => Reflect.get(target, key) ?? (() => undefined) }) as unknown as ProviderEvents;
  const directory = await mkdtemp(join(tmpdir(), "eufy-t8151-"));
  const provider = new EufyProvider({ username: "fixture", password: "redacted", country: "DE",
    persistentDirectory: directory, maxStreamSeconds: 120 });
  try {
    await provider.start(events);
    assert.equal(cameras.length, 1);
    assert.equal(cameras[0]!.startupSnapshotSupported, false);
    assert.equal(supportsStartupSnapshot({ ...cameras[0]!, streamSupported: true }), false);
    assert.equal(provider.liveSnapshotTimeoutMilliseconds("lte-camera"), 45_000);
    assert.equal(provider.liveSnapshotTimeoutMilliseconds("unknown"), undefined);
    assert.deepEqual(cameras[0]!.battery, {
      supported: ["level", "charging", "temperature"], level: 82, charging: true,
      health: null, temperature: 24.5, lastChargingDays: null,
    });
    for (const [eventType, handling] of [[3101, "motion"], [3102, "person"], [3111, "person"]] as const) {
      const event = parsePushEvent({ device_sn: "lte-camera", event_type: eventType,
        msg_type: 18, notification_style: 1 });
      assert.ok(event);
      assert.equal(event.cameraSerial, "lte-camera");
      assert.equal(event.stationSerial, "lte-camera");
      const summary = safePushLogSummary(event, parseMegaInventory(inventory)[0]!, false, false);
      assert.match(summary, /camera_accepted=true station_present=false station_managed=false/);
      assert.ok(summary.includes(`handling=${handling}`));
      deliver(event);
    }
    assert.deepEqual(motions, [["lte-camera", true]]);
    assert.equal(people.length, 2);
    for (const args of people) assert.deepEqual(args.slice(0, 3), ["lte-camera", true, null]);
    assert.equal(network.mock.callCount(), 0);
  } finally { await provider.close(); await rm(directory, { recursive: true, force: true }); }
});
