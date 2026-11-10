/**
 * Bounded PPCS relay negotiation for the standalone T815x media trial.
 * The session lends one socket, its tick and monotonic clock. This object owns
 * no timers or sockets. Packet layouts describe the wire protocol; no reference
 * implementation code is embedded here. All destinations originate in pinned
 * rendezvous replies and all observations are closed stage names/counts.
 */
import { isIP } from "node:net";

export interface RelayAddress { readonly host: string; readonly port: number }
type Stage = "waiting" | "list" | "hello" | "port" | "request" | "exhausted" | "closed";
export type LteLookupStage = Stage | "lookup_accepted" | "lookup_rejected" | "relay_advertised" | "relay_ready";
const MAX_DESTINATIONS = 4;
const MAX_SENDS = 3;

function same(a: RelayAddress, b: RelayAddress): boolean { return a.host === b.host && a.port === b.port; }
function usable(a: RelayAddress): boolean {
  const first = Number(a.host.split(".")[0]);
  return isIP(a.host) === 4 && first > 0 && first < 224 && first !== 127
    && Number.isInteger(a.port) && a.port > 0 && a.port <= 65535;
}

/** PPCS sockaddr: 16 bytes, family BE, UDP port LE, IPv4 octets reversed. */
function addressBytes(address: RelayAddress): Buffer {
  const bytes = Buffer.alloc(16);
  bytes.writeUInt16BE(2); bytes.writeUInt16LE(address.port, 2);
  bytes.set(address.host.split(".").map(Number).reverse(), 4);
  return bytes;
}
function readAddress(bytes: Buffer, offset: number): RelayAddress {
  return { host: [...bytes.subarray(offset + 4, offset + 8)].reverse().join("."), port: bytes.readUInt16LE(offset + 2) };
}

/** One explicit session's relay state; closed/exhausted states never restart. */
export class T815xRelay<Lane> {
  #stage: Stage = "waiting";
  #acceptedAt: number | null = null;
  #rejectionLogged = false;
  #sent = 0;
  #lastSend = -Infinity;
  #relays: RelayAddress[] = [];
  #via: RelayAddress | null = null;
  #control: RelayAddress | null = null;
  #mark = 0;
  #registrations: { address: RelayAddress; lane: Lane }[] = [];
  #lookupServers: RelayAddress[] = [];
  readonly stats = { accepted: false, sent: 0, advertised: 0, selected: false };

  constructor(
    private readonly did: Buffer,
    private readonly dsk: string,
    private readonly lane: Lane,
    private readonly send: (opcode: number, payload: Buffer, address: RelayAddress, lane: Lane) => void,
    private readonly report: (stage: LteLookupStage) => void,
    private readonly now: () => number = () => performance.now(),
  ) {}

  /** Only resolved current-inventory rendezvous endpoints may authorize relay destinations. */
  setLookupServers(servers: readonly RelayAddress[]): void {
    if (this.#stage !== "closed") this.#lookupServers = servers.filter(usable).slice(0, 8);
  }
  fromLookupServer(source: RelayAddress): boolean { return this.#lookupServers.some(server => same(server, source)); }

  /** Borrow the existing 1s lookup tick; three sends per stage, no reconnect loop. */
  tick(): void {
    if (this.#stage === "waiting" && this.#acceptedAt !== null && this.now() - this.#acceptedAt >= 3000) this.#advance("list");
    if (["waiting", "closed", "exhausted"].includes(this.#stage) || this.now() - this.#lastSend < 1000) return;
    if (this.#sent >= MAX_SENDS) { this.#advance("exhausted"); return; }
    this.#sent++; this.#lastSend = this.now();
    if (this.#stage === "list") {
      const body = Buffer.concat([this.did, Buffer.from(this.dsk), Buffer.alloc(4)]);
      for (const server of this.#lookupServers) this.#transmit(0x6a, body, server, this.lane);
    } else if (this.#stage === "hello") {
      for (const relay of this.#relays) this.#transmit(0x70, Buffer.alloc(0), relay, this.lane);
    } else if (this.#stage === "port" && this.#control) this.#transmit(0x72, Buffer.alloc(0), this.#control, this.lane);
    else if (this.#stage === "request" && this.#via) {
      const mark = Buffer.alloc(4); mark.writeUInt32BE(this.#mark);
      const body = Buffer.concat([this.did, addressBytes(this.#via), mark]);
      for (const server of this.#lookupServers) this.#transmit(0x80, body, server, this.lane);
    }
  }

  /**
   * Handle relay opcodes exclusively; true means a validated RLY_RDY may win.
   * Frame length is checked before every field. Replies must belong to the
   * current stage, advertised endpoint and socket, and RLY_RDY must name our DID.
   */
  receive(packet: Buffer, source: RelayAddress, lane: Lane): boolean {
    if (this.#stage === "closed" || packet.length < 4 || packet[0] !== 0xf1
      || packet.readUInt16BE(2) !== packet.length - 4) return false;
    const opcode = packet[1];
    if (opcode === 0x21 && this.fromLookupServer(source) && packet.length >= 5) {
      if (packet[4] !== 0 && !this.#rejectionLogged) {
        this.#rejectionLogged = true; this.report("lookup_rejected");
      } else if (packet[4] === 0 && this.#acceptedAt === null) {
        this.#acceptedAt = this.now(); this.stats.accepted = true; this.report("lookup_accepted");
      }
    } else if (opcode === 0x69 && lane === this.lane && this.#stage === "list" && this.fromLookupServer(source)) {
      if (packet.length < 8) return false;
      const count = packet[4]!;
      if (!count || count > 32 || packet.length !== 8 + count * 16) return false;
      const addresses = Array.from({ length: count }, (_, i) => readAddress(packet, 8 + i * 16));
      this.#relays = addresses.filter(usable).filter((a, i, valid) => valid.findIndex(b => same(a, b)) === i).slice(0, MAX_DESTINATIONS);
      if (!this.#relays.length) return false;
      this.#advance("hello"); this.tick();
    } else if (opcode === 0x71 && lane === this.lane && this.#stage === "hello" && packet.length === 4
      && this.#relays.some(address => same(address, source))) {
      this.#control = source; this.#advance("port"); this.tick();
    } else if (opcode === 0x73 && lane === this.lane && this.#stage === "port" && this.#control
      && same(source, this.#control) && packet.length === 10) {
      const via = { host: source.host, port: packet.readUInt16BE(8) };
      if (!usable(via)) return false;
      this.#via = via; this.#mark = packet.readUInt32BE(4); this.#advance("request"); this.tick();
    } else if (opcode === 0x82 && this.#acceptedAt !== null && this.fromLookupServer(source)
      && this.#stage !== "exhausted" && packet.length === 24 && this.#registrations.length < MAX_DESTINATIONS) {
      // RLY_TO supplies sockaddr followed by a BE handle. It is not a direct CAM_CHECK target.
      const address = readAddress(packet, 4);
      if (!usable(address) || this.#registrations.some(entry => entry.lane === lane && same(entry.address, address))) return false;
      this.#register(address, lane, packet.readUInt32BE(20));
    } else if (opcode === 0x84 && packet.length === 24 && packet.subarray(4).equals(this.did)) {
      // A request can yield RLY_RDY directly, or a server may advertise RLY_TO first.
      const requested = lane === this.lane && this.#stage === "request" && this.#via && same(source, this.#via);
      const registered = this.#registrations.some(entry => entry.lane === lane && same(entry.address, source));
      if (requested || registered) { this.stats.selected = true; this.report("relay_ready"); return true; }
    }
    return false;
  }

  /** Retire negotiation on direct/relay selection or cancellation; counters remain readable. */
  close(): void {
    this.#stage = "closed"; this.#lookupServers = []; this.#relays = []; this.#registrations = [];
  }

  #register(address: RelayAddress, lane: Lane, handle: number): void {
    this.#registrations.push({ address, lane }); this.stats.advertised++;
    this.report("relay_advertised");
    // Four client-role packets are the bounded PPCS relay endpoint registration.
    const body = Buffer.alloc(28); body.writeUInt32BE(handle); this.did.copy(body, 4);
    for (let copy = 0; copy < 4; copy++) this.#transmit(0x83, body, address, lane);
  }
  #advance(stage: Stage): void {
    this.#stage = stage; this.#sent = 0; this.#lastSend = -Infinity; this.report(stage);
  }
  #transmit(opcode: number, body: Buffer, address: RelayAddress, lane: Lane): void {
    this.stats.sent++; this.send(opcode, body, address, lane);
  }
}
