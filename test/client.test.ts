import { Buffer } from "node:buffer"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Schedule from "effect/Schedule"
import * as Stream from "effect/Stream"
import { describe, expect, it } from "vitest"
import { encode } from "../src/codec.ts"
import { ConnectionError, ConnectionRefused, connect, ConnectionStatus, InvalidTopic, SubscribeRejected } from "../src/index.ts"
import type { Message } from "../src/index.ts"
import * as P from "../src/packet.ts"
import { aedes, closedPort, scripted, selfSigned } from "./brokers.ts"

const run = <A, E>(effect: Effect.Effect<A, E, import("effect/Scope").Scope>) => Effect.runPromise(Effect.scoped(effect))

const host = "127.0.0.1"

const summary = (m: Message) => [m.topic, Buffer.from(m.payload).toString(), m.qos, m.retain]

const reconnected = (statusChanges: Stream.Stream<ConnectionStatus>) =>
  statusChanges.pipe(
    Stream.dropWhile((s) => !ConnectionStatus.$is("Reconnecting")(s)),
    Stream.filter(ConnectionStatus.$is("Connected")),
    Stream.take(1),
    Stream.runDrain
  )

describe("connect", () => {
  it("delivers messages published after subscribe at QoS 0, 1, and 2", () =>
    run(
      Effect.gen(function* () {
        const { port } = yield* aedes()
        const client = yield* connect({ host, port })
        const messages = yield* client.subscribe("printers/+/report", { qos: 2 })

        yield* client.publish("printers/a/report", "zero")
        yield* client.publish("printers/b/report", "one", { qos: 1 })
        yield* client.publish("printers/c/report", "two", { qos: 2 })

        const got = yield* messages.pipe(Stream.take(3), Stream.runCollect)

        expect(got.map(summary)).toEqual([
          ["printers/a/report", "zero", 0, false],
          ["printers/b/report", "one", 1, false],
          ["printers/c/report", "two", 2, false]
        ])
      })
    ))

  it("delivers a retained message to a later subscriber", () =>
    run(
      Effect.gen(function* () {
        const { port } = yield* aedes()
        const client = yield* connect({ host, port })

        yield* client.publish("config/fan", "on", { qos: 1, retain: true })

        const messages = yield* client.subscribe("config/#", { qos: 1 })
        const got = yield* messages.pipe(Stream.take(1), Stream.runCollect)

        expect(got.map(summary)).toEqual([["config/fan", "on", 1, true]])
      })
    ))

  it("routes each message only to the subscriptions whose filter matches", () =>
    run(
      Effect.gen(function* () {
        const { port } = yield* aedes()
        const client = yield* connect({ host, port })
        const reports = yield* client.subscribe("device/+/report")
        const requests = yield* client.subscribe("device/+/request")

        yield* client.publish("device/A/request", "pushall")
        yield* client.publish("device/A/report", "r1")
        yield* client.publish("device/B/report", "r2")

        expect((yield* reports.pipe(Stream.take(2), Stream.runCollect)).map(summary)).toEqual([
          ["device/A/report", "r1", 0, false],
          ["device/B/report", "r2", 0, false]
        ])
        expect((yield* requests.pipe(Stream.take(1), Stream.runCollect)).map(summary)).toEqual([["device/A/request", "pushall", 0, false]])
      })
    ))

  it("fails with the broker's refusal code on bad credentials", () =>
    run(
      Effect.gen(function* () {
        const { port } = yield* aedes({
          authenticate: (_client, _username, _password, done) => done(Object.assign(new Error("denied"), { returnCode: 4 }), false)
        })

        const error = yield* Effect.flip(connect({ host, port, username: "bblp", password: "wrong" }))

        expect(error).toBeInstanceOf(ConnectionRefused)
        expect(error instanceof ConnectionRefused && error.returnCode).toBe(4)
      })
    ))

  it("connects over TLS to an IP address, as Bambu Lab printers are reached", () =>
    run(
      Effect.gen(function* () {
        const port = yield* scripted(() => {}, selfSigned())
        const client = yield* connect({ host, port, tls: { rejectUnauthorized: false } })

        expect((yield* client.status)._tag).toBe("Connected")
      })
    ))

  it("fails fast with a ConnectionError when nothing listens on the port", () =>
    run(
      Effect.gen(function* () {
        const port = yield* closedPort
        const error = yield* Effect.flip(connect({ host, port }))

        expect(error).toBeInstanceOf(ConnectionError)
      })
    ))

  it("rejects a topic with wildcards before sending anything", () =>
    run(
      Effect.gen(function* () {
        const { port } = yield* aedes()
        const client = yield* connect({ host, port })

        expect(yield* Effect.flip(client.publish("a/+", "x"))).toBeInstanceOf(InvalidTopic)
        expect(yield* Effect.flip(client.subscribe("a/#/b"))).toBeInstanceOf(InvalidTopic)
      })
    ))

  it("reconnects after a dropped connection and keeps the subscription", () =>
    run(
      Effect.gen(function* () {
        const { port, drop } = yield* aedes()
        const client = yield* connect({ host, port, reconnect: Schedule.spaced("20 millis") })
        const messages = yield* client.subscribe("t")
        const back = yield* Effect.forkChild(reconnected(client.statusChanges), { startImmediately: true })

        drop(client.clientId)
        yield* Fiber.join(back)
        yield* client.publish("t", "after")

        expect((yield* messages.pipe(Stream.take(1), Stream.runCollect)).map(summary)).toEqual([["t", "after", 0, false]])
      })
    ))

  it("finishes a QoS 1 publish that was cut off by a dropped connection", () =>
    run(
      Effect.gen(function* () {
        const { port, drop } = yield* aedes()
        const observer = yield* connect({ host, port })
        const messages = yield* observer.subscribe("jobs", { qos: 1 })
        const client = yield* connect({ host, port, reconnect: Schedule.spaced("20 millis") })

        drop(client.clientId)
        yield* client.publish("jobs", "print", { qos: 1 })

        const [first] = yield* messages.pipe(Stream.take(1), Stream.runCollect)

        expect(first && summary(first)).toEqual(["jobs", "print", 1, false])
      })
    ))

  it("sends DISCONNECT on scope close, so the broker does not publish the will", () =>
    run(
      Effect.gen(function* () {
        const { port, drop } = yield* aedes()
        const observer = yield* connect({ host, port })
        const status = yield* observer.subscribe("status/+")
        const will = (name: string) => ({ topic: `status/${name}`, payload: Buffer.from(`${name} offline`), qos: 0 as const, retain: false })

        yield* Effect.scoped(connect({ host, port, will: will("graceful") }))

        const crashed = yield* connect({ host, port, will: will("crashed"), reconnect: false })

        drop(crashed.clientId)

        const [first] = yield* status.pipe(Stream.take(1), Stream.runCollect)

        expect(first && summary(first)).toEqual(["status/crashed", "crashed offline", 0, false])
      })
    ))

  it("unsubscribes when the last subscription on a filter closes", () =>
    run(
      Effect.gen(function* () {
        const { broker, port } = yield* aedes()
        const unsubscribed = yield* Deferred.make<ReadonlyArray<string>>()

        broker.on("unsubscribe", (topics) => Deferred.doneUnsafe(unsubscribed, Effect.succeed(topics)))

        const client = yield* connect({ host, port })

        yield* client.subscribe("a/b")
        yield* Effect.scoped(client.subscribe("x/y"))

        expect(yield* Deferred.await(unsubscribed)).toEqual(["x/y"])
      })
    ))

  it("keeps a confirmed subscription when a second one on the same filter is cancelled before its SUBACK", () =>
    run(
      Effect.gen(function* () {
        const seen: Array<string> = []

        const port = yield* scripted((packet, socket) => {
          seen.push(packet._tag)

          // Confirm only the first SUBSCRIBE, so the second stays pending.
          if (seen.length === 1 && "packetId" in packet) socket.write(encode(P.suback(packet.packetId, [0])))
        })

        const client = yield* connect({ host, port })

        yield* client.subscribe("t")

        const pending = yield* Effect.forkChild(Effect.scoped(client.subscribe("t")), { startImmediately: true })

        yield* Effect.sleep("50 millis")
        yield* Fiber.interrupt(pending)
        yield* Effect.sleep("50 millis")
        expect(seen).toEqual(["Subscribe", "Subscribe"])
      })
    ))

  it("keeps the highest QoS on a filter when a lower-QoS subscription joins it", () =>
    run(
      Effect.gen(function* () {
        const { port } = yield* aedes()
        const client = yield* connect({ host, port })
        const high = yield* client.subscribe("t", { qos: 2 })

        yield* client.subscribe("t", { qos: 0 })
        yield* client.publish("t", "x", { qos: 2 })

        expect((yield* high.pipe(Stream.take(1), Stream.runCollect)).map(summary)).toEqual([["t", "x", 2, false]])
      })
    ))

  it("lets a filter be retried at a lower QoS after the broker rejects a higher one", () =>
    run(
      Effect.gen(function* () {
        const requestedQos: Array<number> = []

        const port = yield* scripted((packet, socket) => {
          if (!("subscriptions" in packet)) return

          const qos = packet.subscriptions[0]?.qos ?? 0

          requestedQos.push(qos)
          socket.write(encode(P.suback(packet.packetId, [qos === 2 ? 0x80 : qos])))
        })

        const client = yield* connect({ host, port })

        expect(yield* Effect.flip(client.subscribe("t", { qos: 2 }))).toBeInstanceOf(SubscribeRejected)
        yield* client.subscribe("t", { qos: 0 })
        expect(requestedQos).toEqual([2, 0])
      })
    ))

  it("waits for the reconnect schedule before the first reconnect", () =>
    run(
      Effect.gen(function* () {
        const { port, drop } = yield* aedes()
        const client = yield* connect({ host, port, reconnect: Schedule.spaced("300 millis") })
        const back = yield* Effect.forkChild(reconnected(client.statusChanges), { startImmediately: true })
        const droppedAt = Date.now()

        drop(client.clientId)
        yield* Fiber.join(back)
        expect(Date.now() - droppedAt).toBeGreaterThanOrEqual(250)
      })
    ))

  it("never lets a full keep-alive period pass without sending a packet", () =>
    run(
      Effect.gen(function* () {
        const { broker, port } = yield* aedes()
        const sent: Array<number> = []

        broker.on("ping", () => sent.push(Date.now()))
        sent.push(Date.now())

        const client = yield* connect({ host, port, keepAlive: "1 second" })

        yield* Effect.sleep("600 millis")
        yield* client.publish("t", "x")
        sent.push(Date.now())
        yield* Effect.sleep("2600 millis")

        const gaps = sent.slice(1).map((t, i) => t - (sent[i] ?? t))

        expect(sent.length).toBeGreaterThanOrEqual(4)
        expect(Math.max(...gaps)).toBeLessThanOrEqual(1100)
      })
    ), 6000)

  it("drops a connection whose broker stops answering PINGREQ", () =>
    run(
      Effect.gen(function* () {
        const port = yield* scripted(() => {})
        const client = yield* connect({ host, port, keepAlive: "1 second", reconnect: false })
        const closed = yield* client.statusChanges.pipe(Stream.filter(ConnectionStatus.$is("Closed")), Stream.take(1), Stream.runCollect)

        expect(closed.map((s) => s.error.message)).toEqual(["no PINGRESP within the keep-alive period"])
      })
    ), 5000)

  it("closes on a malformed packet from the broker", () =>
    run(
      Effect.gen(function* () {
        const port = yield* scripted((_packet, socket) => socket.write(Buffer.from([0xf0, 0x00])))
        const client = yield* connect({ host, port, reconnect: false })

        yield* client.publish("t", "x")

        const closed = yield* client.statusChanges.pipe(Stream.filter(ConnectionStatus.$is("Closed")), Stream.take(1), Stream.runCollect)

        expect(closed.map((s) => s.error._tag)).toEqual(["MalformedPacket"])
      })
    ))

  it("acknowledges QoS 1 with PUBACK, and delivers a resent QoS 2 message once through PUBREC and PUBCOMP", () =>
    run(
      Effect.gen(function* () {
        const seen: Array<string> = []

        const port = yield* scripted((packet, socket) => {
          seen.push(packet._tag)

          if (seen.length > 1 || !("packetId" in packet)) return

          socket.write(
            Buffer.concat([
              encode(P.suback(packet.packetId, [2])),
              encode(P.publishAcknowledged("t", Buffer.from("one"), 1, 8, false, false)),
              encode(P.publishAcknowledged("t", Buffer.from("once"), 2, 9, false, false)),
              encode(P.publishAcknowledged("t", Buffer.from("once"), 2, 9, false, true)),
              encode(P.pubrel(9))
            ])
          )
        })

        const client = yield* connect({ host, port })
        const messages = yield* client.subscribe("t", { qos: 2 })
        const got: Array<Message> = []

        yield* Effect.forkChild(Stream.runForEach(messages, (m) => Effect.sync(() => got.push(m))), { startImmediately: true })
        yield* Effect.sleep("100 millis")
        expect(got.map(summary)).toEqual([
          ["t", "one", 1, false],
          ["t", "once", 2, false]
        ])
        expect(seen).toEqual(["Subscribe", "Puback", "Pubrec", "Pubrec", "Pubcomp"])
      })
    ))
})
