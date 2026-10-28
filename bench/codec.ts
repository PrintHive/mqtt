import { Buffer } from "node:buffer"
import mqttPacket from "mqtt-packet"
import { Decoder, encode } from "../src/codec.ts"
import * as P from "../src/packet.ts"
import { payload, TOPIC } from "./fixture.ts"

const N = 200_000
const body = payload(1024)
const ours = encode(P.publishAtMostOnce(TOPIC, body, false))
const stream = Buffer.concat(Array.from({ length: N }, () => ours))
const chunks = Array.from({ length: Math.ceil(stream.length / 65_536) }, (_, i) => stream.subarray(i * 65_536, (i + 1) * 65_536))

const theirs = mqttPacket.generate({ cmd: "publish", topic: TOPIC, payload: body, qos: 0, dup: false, retain: false })

if (Buffer.compare(ours, theirs) !== 0) throw new Error("the two encoders disagree on the wire bytes")

const time = (f: () => number) => {
  const t = performance.now()
  const count = f()

  return { ms: performance.now() - t, count }
}

const runs = {
  "decode ours": () => {
    let count = 0
    let bytes = 0
    const d = new Decoder((p) => {
      count++
      bytes += "payload" in p ? p.payload.length : 0
    })

    for (const c of chunks) d.push(c)

    if (bytes !== N * 1024) throw new Error(`decoded ${bytes} payload bytes`)

    return count
  },
  "decode mqtt-packet": () => {
    let count = 0
    let bytes = 0
    const parser = mqttPacket.parser({ protocolVersion: 4 })

    parser.on("packet", (p: { payload: Buffer }) => {
      count++
      bytes += p.payload.length
    })

    for (const c of chunks) parser.parse(c)

    if (bytes !== N * 1024) throw new Error(`decoded ${bytes} payload bytes`)

    return count
  },
  "encode ours": () => {
    let bytes = 0

    for (let i = 0; i < N; i++) bytes += encode(P.publishAtMostOnce(TOPIC, body, false)).length

    return bytes / ours.length
  },
  "encode mqtt-packet": () => {
    let bytes = 0

    for (let i = 0; i < N; i++) bytes += mqttPacket.generate({ cmd: "publish", topic: TOPIC, payload: body, qos: 0, dup: false, retain: false }).length

    return bytes / ours.length
  }
}

const results: Record<string, Array<number>> = {}

for (let round = 0; round < 7; round++) {
  for (const [name, f] of Object.entries(runs)) {
    const { ms, count } = time(f)

    if (count !== N) throw new Error(`${name} handled ${count} of ${N} packets`)

    if (round > 1) (results[name] ??= []).push(N / ms / 1000)
  }
}

for (const [name, rates] of Object.entries(results)) {
  rates.sort((a, b) => a - b)
  console.log(`${name.padEnd(20)} median ${rates[2]?.toFixed(2)} M packets/s  range ${rates[0]?.toFixed(2)} to ${rates.at(-1)?.toFixed(2)} (5 runs, 1 KB payload)`)
}
