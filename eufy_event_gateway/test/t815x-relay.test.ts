/** Synthetic PPCS relay peers: finite negotiation, byte order and foreign/stale reply rejection. */
import assert from "node:assert/strict";
import test from "node:test";
import { T815xRelay, type RelayAddress, type LteLookupStage } from "../src/stream/t815x-relay.js";
import { isT815xLteCamera } from "../src/provider/t815x-profile.js";

const server = { host: "198.51.100.10", port: 32100 };
const relay = { host: "203.0.113.20", port: 32100 };
const allocated = { ...relay, port: 32101 };
const did = Buffer.alloc(20, 7);
const other = { host: "203.0.113.99", port: 32100 };
const main = {}, probe = {};

function packet(opcode: number, body = Buffer.alloc(0)): Buffer {
  const header = Buffer.from([0xf1, opcode, 0, 0]); header.writeUInt16BE(body.length, 2);
  return Buffer.concat([header, body]);
}
function address(value: RelayAddress): Buffer {
  const bytes = Buffer.alloc(16); bytes.writeUInt16BE(2); bytes.writeUInt16LE(value.port, 2);
  value.host.split(".").reverse().forEach((octet, i) => { bytes[4 + i] = Number(octet); });
  return bytes;
}
function list(values = [relay]): Buffer {
  return packet(0x69, Buffer.concat([Buffer.from([values.length, 0, 0, 0]), ...values.map(address)]));
}
function fixture() {
  let now = 0;
  const sent: { opcode: number; body: Buffer; target: RelayAddress; lane: object }[] = [];
  const stages: LteLookupStage[] = [];
  const instance = new T815xRelay(did, "synthetic-dsk", main,
    (opcode, body, target, lane) => sent.push({ opcode, body: Buffer.from(body), target, lane }),
    stage => stages.push(stage), () => now);
  instance.setLookupServers([server]);
  const advance = (milliseconds: number) => { now += milliseconds; instance.tick(); };
  const receive = (p: Buffer, from = server, lane = main) => instance.receive(p, from, lane);
  const accepted = () => { receive(packet(0x21, Buffer.from([0])), server, probe); advance(3000); };
  return { instance, sent, stages, advance, receive, accepted };
}

test("relay admission is restricted to the four exact LTE models and numeric type", () => {
  for (const model of ["T8150", "T8151", "T8152", "T8153"]) {
    assert.equal(isT815xLteCamera({ model, deviceType: 110 }), true);
    assert.equal(isT815xLteCamera({ model, deviceType: 111 }), false);
  }
  for (const model of ["T8151-X", "T8154", "T8171", "unknown"]) assert.equal(isT815xLteCamera({ model, deviceType: 110 }), false);
});

test("only a valid accepted rendezvous request starts the delayed, bounded fallback", () => {
  const f = fixture();
  f.receive(packet(0x21, Buffer.from([0])), other);
  f.receive(packet(0x21, Buffer.from([1])));
  const malformed = packet(0x21, Buffer.from([0])); malformed[3] = 2; f.receive(malformed);
  f.advance(10000); assert.equal(f.sent.length, 0);
  f.receive(packet(0x21, Buffer.from([0])));
  f.advance(2999); assert.equal(f.sent.length, 0);
  f.advance(1); assert.equal(f.sent.length, 1);
  assert.equal(f.sent[0]!.opcode, 0x6a);
  assert.equal(f.sent[0]!.lane, main);
  assert.deepEqual(f.sent[0]!.body, Buffer.concat([did, Buffer.from("synthetic-dsk"), Buffer.alloc(4)]));
  f.advance(1000); f.advance(1000); f.advance(1000);
  f.receive(list()); f.advance(100000);
  assert.equal(f.sent.length, 3);
  assert.equal(f.stages.at(-1), "exhausted");
});

test("relay list, hello, port allocation and request validate stage, sender and lane", () => {
  const f = fixture(); f.accepted();
  f.receive(list(), other); f.receive(list(), server, probe);
  f.receive(list().subarray(0, 10));
  assert.deepEqual(f.sent.map(p => p.opcode), [0x6a]);
  f.receive(list());
  assert.equal(f.sent.at(-1)!.opcode, 0x70);
  f.receive(packet(0x71), other); f.receive(packet(0x71), relay, probe);
  assert.equal(f.sent.length, 2);
  f.receive(packet(0x71), relay);
  assert.equal(f.sent.at(-1)!.opcode, 0x72);
  const allocation = Buffer.alloc(6); allocation.writeUInt32BE(0x01020304); allocation.writeUInt16BE(allocated.port, 4);
  f.receive(packet(0x73, allocation), other);
  f.receive(packet(0x73, Buffer.alloc(1)), relay);
  assert.equal(f.sent.length, 3);
  f.receive(packet(0x73, allocation), relay);
  const request = f.sent.at(-1)!;
  assert.equal(request.opcode, 0x80);
  assert.deepEqual(request.body.subarray(0, 20), did);
  assert.deepEqual(request.body.subarray(20, 36), address(allocated));
  assert.deepEqual(request.body.subarray(36), Buffer.from([1, 2, 3, 4]));
  assert.equal(f.receive(packet(0x84, Buffer.alloc(20)), allocated), false);
  assert.equal(f.receive(packet(0x84, did), other), false);
  assert.equal(f.receive(packet(0x84, did), allocated, probe), false);
  assert.equal(f.receive(packet(0x84, did), allocated), true);
  f.instance.close(); const count = f.sent.length;
  f.receive(list()); f.receive(packet(0x84, did), allocated); f.advance(10000);
  assert.equal(f.sent.length, count);
});

test("RLY_TO registers four client packets on its socket instead of probing it as a direct camera", () => {
  const f = fixture();
  const advertisement = packet(0x82, Buffer.concat([address(allocated), Buffer.from([1, 2, 3, 4])]));
  f.receive(advertisement); assert.equal(f.sent.length, 0);
  f.receive(packet(0x21, Buffer.from([0])));
  f.receive(advertisement, other, probe); assert.equal(f.sent.length, 0);
  f.receive(advertisement, server, probe);
  assert.equal(f.sent.length, 4);
  for (const sent of f.sent) {
    assert.equal(sent.opcode, 0x83); assert.equal(sent.lane, probe);
    assert.deepEqual(sent.target, allocated);
    assert.deepEqual(sent.body, Buffer.concat([Buffer.from([1, 2, 3, 4]), did, Buffer.alloc(4)]));
  }
  f.receive(advertisement, server, probe); assert.equal(f.sent.length, 4);
  assert.equal(f.receive(packet(0x84, did), allocated, main), false);
  assert.equal(f.receive(packet(0x84, did), allocated, probe), true);
});

test("bounds advertised relay endpoints and rejects unusable, truncated or oversized lists", () => {
  const f = fixture(); f.accepted();
  f.receive(list([{ host: "0.0.0.0", port: 1 }]));
  f.receive(list([{ host: "127.0.0.1", port: 1 }]));
  f.receive(list([{ host: "224.0.0.1", port: 1 }]));
  f.receive(list([{ ...relay, port: 0 }]));
  assert.equal(f.sent.length, 1);
  f.receive(list(Array.from({ length: 5 }, (_, i) => ({ ...relay, port: 32000 + i }))));
  assert.equal(f.sent.filter(p => p.opcode === 0x70).length, 4);
});

for (const phase of ["waiting", "list", "hello", "port", "request"] as const) {
  test(`cancellation at ${phase} prevents all subsequent relay traffic`, () => {
    const f = fixture();
    if (phase !== "waiting") f.accepted();
    if (["hello", "port", "request"].includes(phase)) f.receive(list());
    if (["port", "request"].includes(phase)) f.receive(packet(0x71), relay);
    if (phase === "request") { const body = Buffer.alloc(6); body.writeUInt16BE(32101, 4); f.receive(packet(0x73, body), relay); }
    f.instance.close(); const before = f.sent.length;
    f.advance(100000); f.receive(list()); f.receive(packet(0x21, Buffer.from([0])));
    assert.equal(f.sent.length, before);
  });
}
