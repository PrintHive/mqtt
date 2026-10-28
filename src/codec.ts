import { Buffer } from "node:buffer"
import * as Data from "effect/Data"
import type { Connect, Packet, QoS, Subscription, SubackCode } from "./packet.ts"

export class MalformedPacket extends Data.TaggedError("MalformedPacket")<{ readonly reason: string }> {}

const malformed = (reason: string) => new MalformedPacket({ reason })

/** The largest remaining length a 4-byte variable byte integer can hold. */
export const MAX_REMAINING_LENGTH = 268_435_455
const MAX_STRING_BYTES = 65_535

const varintSize = (n: number): number => (n < 128 ? 1 : n < 16_384 ? 2 : n < 2_097_152 ? 3 : 4)

const stringSize = (s: string): number => {
  const n = Buffer.byteLength(s)

  if (n > MAX_STRING_BYTES) throw malformed(`string of ${n} bytes exceeds the 65535-byte limit`)

  return 2 + n
}

/** Allocates a packet and writes its fixed header. The body starts at `buf.length - remaining`. */
const allocate = (first: number, remaining: number): Buffer => {
  if (remaining > MAX_REMAINING_LENGTH) throw malformed(`packet of ${remaining} bytes exceeds the MQTT limit`)
  const buf = Buffer.allocUnsafe(1 + varintSize(remaining) + remaining)
  buf[0] = first
  let offset = 1
  let n = remaining

  do {
    const digit = n % 128
    n = Math.floor(n / 128)
    buf[offset++] = n > 0 ? digit | 128 : digit
  } while (n > 0)

  return buf
}

/** Writes a length-prefixed UTF-8 string whose byte length the caller already measured with `stringSize`. */
const writeString = (buf: Buffer, offset: number, s: string, size: number): number => {
  buf.writeUInt16BE(size - 2, offset)
  buf.write(s, offset + 2, size - 2, "utf8")

  return offset + size
}

const writeBytes = (buf: Buffer, offset: number, bytes: Uint8Array): number => {
  buf.writeUInt16BE(bytes.length, offset)
  buf.set(bytes, offset + 2)

  return offset + 2 + bytes.length
}

const ack = (first: number, packetId: number): Buffer => Buffer.from([first, 2, packetId >> 8, packetId & 255])

const PINGREQ = Buffer.from([0xc0, 0])
const PINGRESP = Buffer.from([0xd0, 0])
const DISCONNECT = Buffer.from([0xe0, 0])
const PROTOCOL = Buffer.from([0, 4, 0x4d, 0x51, 0x54, 0x54, 4])

const encodeConnect = (p: Connect): Buffer => {
  const clientId = stringSize(p.clientId)
  const willTopic = p.will ? stringSize(p.will.topic) : 0
  const username = p.username === undefined ? 0 : stringSize(p.username)

  const remaining =
    PROTOCOL.length + 3 + clientId + willTopic + (p.will ? 2 + p.will.payload.length : 0) + username +
    (p.password ? 2 + p.password.length : 0)

  const buf = allocate(0x10, remaining)
  const offset = buf.length - remaining
  PROTOCOL.copy(buf, offset)
  let o = offset + PROTOCOL.length
  buf[o++] =
    (p.username === undefined ? 0 : 0x80) |
    (p.password ? 0x40 : 0) |
    (p.will ? (p.will.retain ? 0x20 : 0) | (p.will.qos << 3) | 0x04 : 0) |
    (p.cleanSession ? 0x02 : 0)
  buf.writeUInt16BE(p.keepAlive, o)
  o = writeString(buf, o + 2, p.clientId, clientId)

  if (p.will) {
    o = writeString(buf, o, p.will.topic, willTopic)
    o = writeBytes(buf, o, p.will.payload)
  }

  if (p.username !== undefined) o = writeString(buf, o, p.username, username)

  if (p.password) writeBytes(buf, o, p.password)

  return buf
}

const encodeSubscriptions = (packetId: number, subscriptions: ReadonlyArray<Subscription>): Buffer => {
  const sizes = subscriptions.map((s) => stringSize(s.filter))
  const remaining = sizes.reduce((sum, n) => sum + n + 1, 2)
  const buf = allocate(0x82, remaining)
  const offset = buf.length - remaining
  buf.writeUInt16BE(packetId, offset)
  let o = offset + 2
  subscriptions.forEach((s, i) => {
    o = writeString(buf, o, s.filter, sizes[i] ?? 0)
    buf[o++] = s.qos
  })

  return buf
}

const encodeUnsubscribe = (packetId: number, filters: ReadonlyArray<string>): Buffer => {
  const sizes = filters.map(stringSize)
  const remaining = sizes.reduce((sum, n) => sum + n, 2)
  const buf = allocate(0xa2, remaining)
  const offset = buf.length - remaining
  buf.writeUInt16BE(packetId, offset)
  let o = offset + 2
  filters.forEach((f, i) => {
    o = writeString(buf, o, f, sizes[i] ?? 0)
  })

  return buf
}

/** Encodes a packet into one buffer. Throws `MalformedPacket` when a field exceeds a protocol limit. */
export const encode = (p: Packet): Buffer => {
  switch (p._tag) {
    case "Publish": {
      const topic = stringSize(p.topic)
      const id = p.qos === 0 ? 0 : 2
      const first = 0x30 | (p.qos === 0 ? 0 : (p.dup ? 8 : 0) | (p.qos << 1)) | (p.retain ? 1 : 0)
      const remaining = topic + id + p.payload.length
      const buf = allocate(first, remaining)
      const offset = buf.length - remaining
      const o = writeString(buf, offset, p.topic, topic)

      if (p.qos !== 0) buf.writeUInt16BE(p.packetId, o)
      buf.set(p.payload, o + id)

      return buf
    }

    case "Puback":
      return ack(0x40, p.packetId)
    case "Pubrec":
      return ack(0x50, p.packetId)
    case "Pubrel":
      return ack(0x62, p.packetId)
    case "Pubcomp":
      return ack(0x70, p.packetId)
    case "Unsuback":
      return ack(0xb0, p.packetId)
    case "Connect":
      return encodeConnect(p)
    case "Connack":
      return Buffer.from([0x20, 2, p.sessionPresent ? 1 : 0, p.returnCode])
    case "Subscribe":
      return encodeSubscriptions(p.packetId, p.subscriptions)
    case "Suback": {
      const remaining = 2 + p.codes.length
      const buf = allocate(0x90, remaining)
      const offset = buf.length - remaining
      buf.writeUInt16BE(p.packetId, offset)
      buf.set(p.codes, offset + 2)

      return buf
    }

    case "Unsubscribe":
      return encodeUnsubscribe(p.packetId, p.filters)
    case "Pingreq":
      return PINGREQ
    case "Pingresp":
      return PINGRESP
    case "Disconnect":
      return DISCONNECT
  }
}

/** A bounds-checked cursor over one packet body. */
class Reader {
  readonly buf: Buffer
  readonly end: number
  offset: number

  constructor(buf: Buffer, start: number, end: number) {
    this.buf = buf
    this.offset = start
    this.end = end
  }
  u8(): number {
    if (this.offset >= this.end) throw malformed("packet ends early")

    return this.buf[this.offset++] ?? 0
  }
  u16(): number {
    if (this.offset + 2 > this.end) throw malformed("packet ends early")
    const n = this.buf.readUInt16BE(this.offset)
    this.offset += 2

    return n
  }
  packetId(): number {
    const id = this.u16()

    if (id === 0) throw malformed("packet identifier 0 is reserved")

    return id
  }
  bytes(): Buffer {
    const n = this.u16()

    if (this.offset + n > this.end) throw malformed("field runs past the packet")
    const out = this.buf.subarray(this.offset, this.offset + n)
    this.offset += n

    return out
  }
  string(): string {
    const n = this.u16()

    if (this.offset + n > this.end) throw malformed("string runs past the packet")
    const s = this.buf.toString("utf8", this.offset, this.offset + n)
    this.offset += n

    return s
  }
  rest(): Buffer {
    const out = this.buf.subarray(this.offset, this.end)
    this.offset = this.end

    return out
  }
  qos(): QoS {
    const q = this.u8()

    if (q > 2) throw malformed(`QoS ${q} is not 0, 1, or 2`)

    return q === 0 ? 0 : q === 1 ? 1 : 2
  }
  more(): boolean {
    return this.offset < this.end
  }
}

const decodePublish = (flags: number, r: Reader): Packet => {
  const qosBits = (flags >> 1) & 3

  if (qosBits === 3) throw malformed("PUBLISH with QoS 3")
  const topic = r.string()
  const retain = (flags & 1) === 1

  if (qosBits === 0) return { _tag: "Publish", qos: 0, topic, payload: r.rest(), retain }
  const packetId = r.packetId()

  return { _tag: "Publish", qos: qosBits === 1 ? 1 : 2, topic, payload: r.rest(), retain, dup: (flags & 8) === 8, packetId }
}

const decodeConnect = (r: Reader): Connect => {
  const protocol = r.string()
  const level = r.u8()

  if (protocol !== "MQTT" || level !== 4) throw malformed(`unsupported protocol ${protocol} level ${level}`)
  const flags = r.u8()
  const keepAlive = r.u16()
  const clientId = r.string()
  const willQos = (flags >> 3) & 3

  if (willQos === 3) throw malformed("will with QoS 3")

  const will =
    flags & 0x04
      ? { topic: r.string(), payload: r.bytes(), qos: willQos === 0 ? 0 : willQos === 1 ? 1 : 2, retain: (flags & 0x20) !== 0 } as const
      : undefined

  const username = flags & 0x80 ? r.string() : undefined
  const password = flags & 0x40 ? r.bytes() : undefined

  return { _tag: "Connect", clientId, cleanSession: (flags & 0x02) !== 0, keepAlive, username, password, will }
}

const subackCode = (code: number): SubackCode => {
  if (code === 0 || code === 1 || code === 2 || code === 0x80) return code
  throw malformed(`SUBACK return code ${code}`)
}

/** Required fixed-header flags per packet type, for every type except PUBLISH. */
const FLAGS = [-1, 0, 0, -1, 0, 0, 2, 0, 2, 0, 2, 0, 0, 0, 0]

const decodeBody = (first: number, r: Reader): Packet => {
  const type = first >> 4
  const flags = first & 15

  if (type === 3) return decodePublish(flags, r)

  if (FLAGS[type] !== flags) throw malformed(`packet type ${type} with flags ${flags}`)

  switch (type) {
    case 1:
      return decodeConnect(r)
    case 2: {
      const ackFlags = r.u8()

      return { _tag: "Connack", sessionPresent: (ackFlags & 1) === 1, returnCode: r.u8() }
    }

    case 4:
      return { _tag: "Puback", packetId: r.packetId() }
    case 5:
      return { _tag: "Pubrec", packetId: r.packetId() }
    case 6:
      return { _tag: "Pubrel", packetId: r.packetId() }
    case 7:
      return { _tag: "Pubcomp", packetId: r.packetId() }
    case 8: {
      const packetId = r.packetId()
      const subscriptions: Array<Subscription> = []

      while (r.more()) subscriptions.push({ filter: r.string(), qos: r.qos() })

      if (subscriptions.length === 0) throw malformed("SUBSCRIBE with no filters")

      return { _tag: "Subscribe", packetId, subscriptions }
    }

    case 9: {
      const packetId = r.packetId()
      const codes: Array<SubackCode> = []

      while (r.more()) codes.push(subackCode(r.u8()))

      return { _tag: "Suback", packetId, codes }
    }

    case 10: {
      const packetId = r.packetId()
      const filters: Array<string> = []

      while (r.more()) filters.push(r.string())

      if (filters.length === 0) throw malformed("UNSUBSCRIBE with no filters")

      return { _tag: "Unsubscribe", packetId, filters }
    }

    case 11:
      return { _tag: "Unsuback", packetId: r.packetId() }
    case 12:
      return { _tag: "Pingreq" }
    case 13:
      return { _tag: "Pingresp" }
    case 14:
      return { _tag: "Disconnect" }
    default:
      throw malformed(`packet type ${type} is reserved`)
  }
}

/**
 * An incremental decoder. Feed it socket chunks in order; it calls `onPacket` once per complete packet.
 *
 * Payloads are views into the chunk they arrived in, not copies.
 */
export class Decoder {
  private chunks: Array<Buffer> = []
  private buffered = 0
  /** Bytes the first buffered packet needs, or 0 while its fixed header is still incomplete. */
  private needed = 0

  private readonly onPacket: (packet: Packet) => void

  constructor(onPacket: (packet: Packet) => void) {
    this.onPacket = onPacket
  }

  push(chunk: Buffer): void {
    if (this.chunks.length > 0) {
      this.chunks.push(chunk)
      this.buffered += chunk.length

      if (this.buffered < this.needed) return
      chunk = Buffer.concat(this.chunks, this.buffered)
      this.chunks = []
      this.buffered = 0
    }

    let offset = 0

    while (offset < chunk.length) {
      const size = packetSize(chunk, offset)

      if (size === 0 || offset + size > chunk.length) {
        this.needed = size
        break
      }

      const headerEnd = offset + size - remainingLength(chunk, offset)
      const r = new Reader(chunk, headerEnd, offset + size)
      const packet = decodeBody(chunk[offset] ?? 0, r)

      if (r.more()) throw malformed("packet has trailing bytes")
      offset += size
      this.onPacket(packet)
    }

    if (offset < chunk.length) {
      this.chunks.push(chunk.subarray(offset))
      this.buffered = chunk.length - offset
    }
  }
}

/** Remaining length of the packet at `offset`. Only called once `packetSize` has read a complete header. */
const remainingLength = (buf: Buffer, offset: number): number => {
  let n = 0
  let scale = 1

  for (let i = offset + 1; ; i++) {
    const b = buf[i] ?? 0
    n += (b & 127) * scale

    if (b < 128) return n
    scale *= 128
  }
}

/** Total size of the packet at `offset`, or 0 when its fixed header has not fully arrived. */
const packetSize = (buf: Buffer, offset: number): number => {
  let n = 0
  let scale = 1

  for (let i = 1; i <= 4; i++) {
    const at = offset + i

    if (at >= buf.length) return 0
    const b = buf[at] ?? 0
    n += (b & 127) * scale

    if (b < 128) return 1 + i + n
    scale *= 128
  }

  throw malformed("remaining length longer than 4 bytes")
}
