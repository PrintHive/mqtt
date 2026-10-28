import { Buffer } from "node:buffer"
import { describe, expect, it } from "vitest"
import { Decoder, encode, MalformedPacket } from "../src/codec.ts"
import * as P from "../src/packet.ts"
import type { Packet } from "../src/packet.ts"

const bytes = (hex: string) => Buffer.from(hex.replace(/\s+/g, ""), "hex")

const text = (s: string) => Buffer.from(s)

const decodeAll = (chunks: ReadonlyArray<Buffer>): Array<Packet> => {
  const out: Array<Packet> = []
  const decoder = new Decoder((p) => out.push(p))

  for (const chunk of chunks) decoder.push(chunk)

  return out
}

const payloadOf = (p: Packet | undefined) => (p !== undefined && "payload" in p ? p.payload : undefined)

const samples: ReadonlyArray<Packet> = [
  P.connect({
    clientId: "printer-7",
    cleanSession: false,
    keepAlive: 20,
    username: "bblp",
    password: text("secret"),
    will: { topic: "status/printer-7", payload: text("offline"), qos: 1, retain: true }
  }),
  P.connect({ clientId: "", cleanSession: true, keepAlive: 0, username: undefined, password: undefined, will: undefined }),
  P.connack(true, 0),
  P.connack(false, 5),
  P.publishAtMostOnce("device/01S/report", text('{"print":{}}'), false),
  P.publishAcknowledged("a/b", text("x"), 1, 65_535, true, true),
  P.publishAcknowledged("ünïcode/🔥", Buffer.alloc(0), 2, 1, false, false),
  P.puback(7),
  P.pubrec(8),
  P.pubrel(9),
  P.pubcomp(10),
  P.subscribe(11, [{ filter: "a/+", qos: 0 }, { filter: "b/#", qos: 2 }]),
  P.suback(11, [0, 2, 0x80]),
  P.unsubscribe(12, ["a/+", "b/#"]),
  P.unsuback(12),
  P.pingreq,
  P.pingresp,
  P.disconnect
]


describe("encode", () => {
  it("writes a CONNECT with credentials byte for byte", () => {
    const packet = P.connect({ clientId: "c1", cleanSession: true, keepAlive: 60, username: "u", password: text("p"), will: undefined })

    expect(encode(packet)).toEqual(bytes("10 14 0004 4d515454 04 c2 003c 0002 6331 0001 75 0001 70"))
  })

  it("writes a retained QoS 1 PUBLISH byte for byte", () => {
    expect(encode(P.publishAcknowledged("a/b", text("hi"), 1, 10, true, false))).toEqual(bytes("33 09 0003 612f62 000a 6869"))
  })

  it("encodes a remaining length of 321 as the two bytes C1 02 from the spec", () => {
    expect(encode(P.publishAtMostOnce("t", new Uint8Array(318), false)).subarray(0, 3)).toEqual(bytes("30 c1 02"))
  })

  it("refuses a topic longer than 65535 bytes", () => {
    expect(() => encode(P.publishAtMostOnce("x".repeat(65_536), new Uint8Array(0), false))).toThrow(MalformedPacket)
  })
})

describe("Decoder", () => {
  const stream = Buffer.concat(samples.map(encode))

  it("round-trips every packet type", () => {
    expect(decodeAll([stream])).toEqual(samples)
  })

  it("decodes the same packets at every split point", () => {
    for (let at = 0; at <= stream.length; at++) {
      expect(decodeAll([stream.subarray(0, at), stream.subarray(at)])).toEqual(samples)
    }
  })

  it("decodes the same packets fed one byte at a time", () => {
    const chunks = Array.from(stream, (b) => Buffer.from([b]))

    expect(decodeAll(chunks)).toEqual(samples)
  })

  it("reassembles a 1 MB payload from 1 KB chunks", () => {
    const payload = Buffer.alloc(1_048_576, 7)
    const wire = encode(P.publishAtMostOnce("big", payload, false))
    const chunks = Array.from({ length: Math.ceil(wire.length / 1024) }, (_, i) => wire.subarray(i * 1024, (i + 1) * 1024))
    const decoded = payloadOf(decodeAll(chunks)[0])

    expect(decoded && Buffer.compare(decoded, payload)).toBe(0)
  })

  it("returns payloads as views of the received chunk, not copies", () => {
    const wire = encode(P.publishAtMostOnce("t", text("abc"), false))

    expect(payloadOf(decodeAll([wire])[0])?.buffer).toBe(wire.buffer)
  })

  it.each([
    ["a PUBLISH with QoS 3", "36 03 0001 74"],
    ["a remaining length longer than 4 bytes", "30 ff ff ff ff 01"],
    ["the reserved packet type 15", "f0 00"],
    ["packet identifier 0", "40 02 0000"],
    ["a SUBSCRIBE without the required flags", "80 06 0001 0001 61 00"],
    ["a SUBSCRIBE with no filters", "82 02 0001"],
    ["a PUBACK with trailing bytes", "40 03 0001 00"],
    ["a SUBACK with an unknown return code", "90 03 0001 03"]
  ])("rejects %s", (_name, hex) => {
    expect(() => decodeAll([bytes(hex)])).toThrow(MalformedPacket)
  })
})
