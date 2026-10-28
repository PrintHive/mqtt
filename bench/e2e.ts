/**
 * One end-to-end run: a subscriber and a publisher on the same library, through a broker, in this process.
 * Prints one JSON line: messages per second from the first publish until the subscriber has every message.
 *
 * Usage: e2e.ts <ours|mqttjs> <qos 0|1> <payload bytes> <messages> <broker port>
 */
import mqtt from "mqtt"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import { connect } from "../src/index.ts"
import { payload, TOPIC } from "./fixture.ts"

const [lib = "ours", qosArg = "0", sizeArg = "1024", countArg = "100000", portArg = "18830"] = process.argv.slice(2)
const qos = qosArg === "1" ? 1 : 0
const size = Number(sizeArg)
const count = Number(countArg)
const port = Number(portArg)
const body = payload(size)
/** Outstanding QoS 1 publishes allowed at once, for both libraries. */
const WINDOW = 1000

const report = (ms: number, received: number, bytes: number) =>
  console.log(JSON.stringify({ lib, qos, size, count, received, bytesOk: bytes === received * size, ms: Math.round(ms), rate: Math.round((received / ms) * 1000) }))

const ours = Effect.gen(function* () {
  const sub = yield* connect({ host: "127.0.0.1", port })
  const pub = yield* connect({ host: "127.0.0.1", port })
  const messages = yield* sub.subscribe(TOPIC, { qos })
  let received = 0
  let bytes = 0
  const start = performance.now()
  const publishing = Effect.forEach(Array.from({ length: count }), () => pub.publish(TOPIC, body, { qos }), { concurrency: qos === 0 ? 1 : WINDOW, discard: true })

  yield* Effect.forkChild(publishing, { startImmediately: true })
  yield* messages.pipe(
    Stream.take(count),
    Stream.runForEach((m) =>
      Effect.sync(() => {
        received++
        bytes += m.payload.length
      })
    )
  )
  report(performance.now() - start, received, bytes)
}).pipe(Effect.scoped)

const theirs = () =>
  new Promise<void>((resolve) => {
    const sub = mqtt.connect({ host: "127.0.0.1", port, reconnectPeriod: 0 })
    const pub = mqtt.connect({ host: "127.0.0.1", port, reconnectPeriod: 0 })
    let received = 0
    let bytes = 0
    let start = 0

    sub.on("message", (_topic, m) => {
      received++
      bytes += m.length

      if (received === count) {
        report(performance.now() - start, received, bytes)
        sub.end()
        pub.end()
        resolve()
      }
    })

    const go = () => {
      start = performance.now()

      if (qos === 0) {
        for (let i = 0; i < count; i++) pub.publish(TOPIC, body, { qos: 0 })

        return
      }

      let sent = 0
      let outstanding = 0
      const pump = () => {
        while (outstanding < WINDOW && sent < count) {
          sent++
          outstanding++
          pub.publish(TOPIC, body, { qos: 1 }, () => {
            outstanding--
            pump()
          })
        }
      }

      pump()
    }

    sub.on("connect", () => sub.subscribe(TOPIC, { qos }, () => (pub.connected ? go() : pub.once("connect", go))))
  })

if (lib === "ours") await Effect.runPromise(ours)
else await theirs()

process.exit(0)
