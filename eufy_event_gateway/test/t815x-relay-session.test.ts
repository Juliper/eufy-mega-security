/** Exercise the real session's relay winner/cleanup using inert UDP sockets. */
import assert from "node:assert/strict";
import { Socket } from "node:dgram";
import { Resolver } from "node:dns/promises";
import test, { type TestContext } from "node:test";
import { FirstPartyPpcsSession, type PpcsCameraOptions } from "../src/stream/first-party-ppcs.js";

const cloud = { address: "198.51.100.10", port: 32100 };
const remote = { address: "203.0.113.20", port: 32101 };
const did = Buffer.alloc(20); did.write("TEST"); did.writeUInt32BE(12345678, 8); did.write("TEST", 12);
const options: PpcsCameraOptions = { stationSerial: "synthetic", p2pDid: "TEST-12345678-TEST",
  appConnection: encodeConnection(cloud.address), dskKey: "synthetic", channel: 0, cameraModel: "T8151",
  cameraDeviceType: 110, accountId: null, onDemandLteRelay: true, maxSeconds: 60 };

/** Invert the connection-string wire encoding for a documentation-only IPv4 address. */
function encodeConnection(host: string): string {
  const table = Buffer.from("4959433db5bf6da347534f6165e371e9677f02030badb3892b2f35c16b8b959711e5a70deff1050783fb9d3bc5c713171d1f2529d3df", "hex");
  const text = Buffer.from(host); let mask = 57, out = "";
  for (let i = 0; i < text.length; i++) {
    const byte = text[i]! ^ mask ^ table[i % table.length]!;
    out += String.fromCharCode(65 + (byte >> 4), 65 + (byte & 15)); mask ^= text[i]!;
  }
  return out;
}
function packet(opcode: number, body = Buffer.alloc(0)): Buffer {
  const p = Buffer.alloc(4 + body.length); p.set([0xf1, opcode]); p.writeUInt16BE(body.length, 2); body.copy(p, 4); return p;
}
function candidate(opcode: number, address = remote): Buffer {
  const body = Buffer.alloc(20); body.writeUInt16BE(2); body.writeUInt16LE(address.port, 2);
  body.set(address.address.split(".").map(Number).reverse(), 4); body.writeUInt32BE(123, 16);
  return packet(opcode, body);
}
async function fixture(context: TestContext, changes: Partial<PpcsCameraOptions> = {}) {
  context.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
  let now = 0;
  context.mock.method(performance, "now", () => now);
  const sockets: Socket[] = [], closed = new Set<Socket>();
  const sent: { bytes: Buffer; socket: Socket; host: string; port: number }[] = [];
  const logs: string[] = [];
  context.mock.method(process.stdout, "write", (value: unknown) => { logs.push(String(value)); return true; });
  context.mock.method(Socket.prototype, "bind", function(this: Socket, _port: number, callback: () => void) {
    sockets.push(this); queueMicrotask(callback); return this;
  });
  context.mock.method(Socket.prototype, "address", function(this: Socket) {
    return { address: "192.0.2.1", port: 40000 + sockets.indexOf(this), family: "IPv4" };
  });
  context.mock.method(Socket.prototype, "setBroadcast", function(this: Socket) { return this; });
  context.mock.method(Socket.prototype, "setRecvBufferSize", () => undefined);
  context.mock.method(Socket.prototype, "connect", function(this: Socket, _port: number, _host: string, callback: () => void) {
    queueMicrotask(callback); return this;
  });
  context.mock.method(Socket.prototype, "send", function(this: Socket, bytes: Buffer, port: number, host: string, callback?: () => void) {
    assert.equal(closed.has(this), false, "closed sockets must never transmit");
    sent.push({ bytes: Buffer.from(bytes), socket: this, host, port }); callback?.();
  });
  context.mock.method(Socket.prototype, "close", function(this: Socket) { closed.add(this); this.emit("close"); return this; });
  const session = new FirstPartyPpcsSession({ ...options, ...changes });
  const starting = session.start();
  // Observe rejection immediately, including a close that wins before start resolves.
  const result = starting.then(() => null, error => error as Error);
  for (let i = 0; i < 15; i++) await Promise.resolve();
  const receive = (p: Buffer, source = cloud, socket = sockets[0]!) => socket.emit("message", p, source);
  context.after(async () => { session.close(); await session.waitUntilClosed(); });
  const advance = (milliseconds: number) => { now += milliseconds; context.mock.timers.tick(milliseconds); };
  return { session, starting, result, sockets, closed, sent, logs, receive, advance };
}

test("accepted lookup drives the complete primary-socket relay negotiation on the existing tick", async context => {
  const f = await fixture(context);
  f.receive(packet(0x21, Buffer.from([0])));
  const before = f.sent.length; f.advance(3000);
  assert.ok(f.sent.slice(before).some(s => s.bytes[1] === 0x6a && s.socket === f.sockets[0]));
  const list = Buffer.alloc(20); list[0] = 1;
  list.writeUInt16BE(2, 4); list.writeUInt16LE(32100, 6);
  list.set(remote.address.split(".").map(Number).reverse(), 8);
  f.receive(packet(0x69, list));
  assert.equal(f.sent.at(-1)!.bytes[1], 0x70);
  f.receive(packet(0x71), { ...remote, port: 32100 });
  assert.equal(f.sent.at(-1)!.bytes[1], 0x72);
  const port = Buffer.alloc(6); port.writeUInt32BE(123); port.writeUInt16BE(remote.port, 4);
  f.receive(packet(0x73, port), { ...remote, port: 32100 });
  assert.equal(f.sent.at(-1)!.bytes[1], 0x80);
  f.receive(packet(0x84, did), remote);
  assert.equal(await f.result, null);
  assert.ok(f.logs.some(line => line.includes("stage=lookup_complete path=relay")));
});

test("LTE resolves configured cloud hosts before accepting rendezvous replies", async context => {
  const resolve = context.mock.method(Resolver.prototype, "resolve4", async () => [cloud.address]);
  const f = await fixture(context, { appConnection: encodeConnection("lookup.example.invalid") });
  assert.equal(resolve.mock.callCount(), 1);
  assert.ok(f.sent.some(s => s.host === cloud.address));
  f.receive(candidate(0x40), { ...cloud, address: "203.0.113.99" });
  f.receive(packet(0x42), remote); assert.equal(f.session.stats.camId, 0);
  f.receive(candidate(0x40)); f.receive(packet(0x42), remote);
  assert.equal(await f.result, null);
});

test("close while DNS is pending cancels lookup and a late resolution cannot send", async context => {
  let finish!: (values: string[]) => void;
  context.mock.method(Resolver.prototype, "resolve4", () => new Promise<string[]>(resolve => { finish = resolve; }));
  const cancel = context.mock.method(Resolver.prototype, "cancel", () => undefined);
  const f = await fixture(context, { appConnection: encodeConnection("lookup.example.invalid") });
  f.session.close();
  assert.match((await f.result)?.message ?? "", /cancelled/);
  assert.equal(cancel.mock.callCount(), 1);
  const before = f.sent.length; finish([cloud.address]);
  for (let i = 0; i < 10; i++) await Promise.resolve();
  f.advance(100000); assert.equal(f.sent.length, before);
  assert.ok(f.sockets.every(socket => f.closed.has(socket)));
});

test("LTE close before bind completion rejects start and a late callback performs no I/O", async context => {
  let bound!: () => Promise<void>;
  context.mock.method(Socket.prototype, "bind", function(this: Socket, _port: number, callback: () => Promise<void>) {
    bound = callback; return this;
  });
  context.mock.method(Socket.prototype, "close", function(this: Socket) { return this; });
  context.mock.method(Socket.prototype, "setBroadcast", () => { assert.fail("late bind must not use the closed socket"); });
  context.mock.method(Socket.prototype, "send", () => { assert.fail("cancelled lookup must not send"); });
  const session = new FirstPartyPpcsSession(options);
  const failed = assert.rejects(session.start(), /cancelled/);
  session.close(); await failed; await bound(); await session.waitUntilClosed();
});

test("LTE relay endpoint wins on a probe socket, starts media once, and drops late foreign packets", async context => {
  const f = await fixture(context); const probe = f.sockets[1]!;
  f.receive(packet(0x21, Buffer.from([0])), cloud, probe);
  f.receive(candidate(0x82), cloud, probe);
  const registrations = f.sent.filter(s => s.bytes[1] === 0x83);
  assert.equal(registrations.length, 4);
  assert.ok(registrations.every(s => s.socket === probe && s.host === remote.address));
  assert.equal(f.sent.some(s => s.bytes[1] === 0x41 && s.host === remote.address), false);
  f.receive(packet(0x84, Buffer.alloc(20)), remote, probe);
  assert.equal(f.session.stats.camId, 0);
  f.receive(packet(0x84, did), remote, probe);
  assert.equal(await f.result, null);
  assert.equal(f.session.stats.camId, 1); assert.equal(f.session.stats.mediaStartAttempts, 1);
  assert.equal(f.closed.has(probe), false);
  assert.ok(f.sockets.every(socket => socket === probe || f.closed.has(socket)));
  f.receive(packet(0x42), { ...remote, address: "203.0.113.99" }, probe);
  f.receive(packet(0x84, did), remote, f.sockets[0]!);
  assert.equal(f.session.stats.camId, 1);
  assert.ok(f.logs.some(line => line.includes("stage=lookup_complete path=relay")));
  assert.ok(f.logs.some(line => line.includes("stage=media_start_sent")));
  const media = f.sent.find(s => s.bytes[1] === 0xd0 && s.bytes.length >= 14 && s.bytes.readUInt16LE(12) === 1700)!;
  const ack = Buffer.from([0xf1, 0xd1, 0, 6, 0xd1, 0, 0, 1, 0, 0]);
  ack.writeUInt16BE(media.bytes.readUInt16BE(6), 8);
  f.receive(ack, remote, probe); f.receive(ack, remote, probe);
  assert.equal(f.logs.filter(line => line.includes("stage=media_start_acked")).length, 1);
  const nal = Buffer.from("000000016742001e898b600000000168ce3c8000000001658884000000010940", "hex");
  for (let sequence = 0; sequence < 2; sequence++) {
    const body = Buffer.alloc(4 + 16 + 22 + nal.length);
    body.set([0xd1, 1]); body.writeUInt16BE(sequence, 2); body.write("XZYH", 4);
    body.writeUInt16LE(1300, 8); body.writeUInt32LE(22 + nal.length, 10);
    body.writeUInt32LE(nal.length, 20); body[24] = 1; body[25] = 1; nal.copy(body, 42);
    f.receive(packet(0xd0, body), remote, probe);
  }
  assert.ok(f.session.stats.videoOutputFrames > 0);
  assert.equal(f.logs.filter(line => line.includes("stage=first_video_output")).length, 1);
  for (const value of ["synthetic", "12345678", cloud.address, remote.address]) assert.equal(f.logs.join("").includes(value), false);
  f.session.close(); await f.session.waitUntilClosed();
  const before = f.sent.length; context.mock.timers.tick(100000);
  assert.equal(f.sent.length, before);
});

test("direct LTE success cancels the competing relay and rejects an unprobed identity", async context => {
  const f = await fixture(context);
  f.receive(packet(0x42), remote); assert.equal(f.session.stats.camId, 0);
  f.receive(candidate(0x40), cloud);
  f.receive(packet(0x42), remote);
  assert.equal(await f.result, null); assert.equal(f.session.stats.camId, 1);
  assert.ok(f.logs.some(line => line.includes("stage=lookup_complete path=direct")));
  const before = f.sent.filter(s => [0x6a, 0x70, 0x72, 0x80, 0x83].includes(s.bytes[1]!)).length;
  f.receive(packet(0x21, Buffer.from([0]))); context.mock.timers.tick(5000);
  assert.equal(f.sent.filter(s => [0x6a, 0x70, 0x72, 0x80, 0x83].includes(s.bytes[1]!)).length, before);
});

test("offline explicit LTE start keeps the original 20s deadline and closes every probe", async context => {
  const f = await fixture(context);
  context.mock.timers.tick(20000);
  assert.match((await f.result)?.message ?? "", /lookup timed out/);
  assert.ok(f.sockets.every(socket => f.closed.has(socket)));
  const before = f.sent.length; context.mock.timers.tick(100000);
  assert.equal(f.sent.length, before);
});

for (const changes of [
  { onDemandLteRelay: false }, { cameraDeviceType: 111 }, { cameraModel: "T8151-X" },
  { cameraModel: "T8171", cameraDeviceType: 88 }, { homeBaseAttached: true }, { channel: 1 },
  { purpose: "control" as const }, { purpose: "passive" as const },
]) {
  test(`relay cannot affect another identity/purpose: ${JSON.stringify(changes)}`, async context => {
    const f = await fixture(context, changes);
    assert.equal(f.logs.some(line => line.includes("camera_lte_stage")), false);
    assert.ok(f.sent.some(s => s.host === "255.255.255.255"));
    f.session.close(); assert.match((await f.result)?.message ?? "", /cancelled/);
  });
}
