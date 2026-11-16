import { Buffer } from "node:buffer"
import * as net from "node:net"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Predicate from "effect/Predicate"
import * as Stream from "effect/Stream"
import { describe, expect, it } from "vitest"
import { encode } from "../src/codec.ts"
import { connect, ConnectionRefused, ConnectionStatus, MalformedPacket, SubscribeRejected } from "../src/index.ts"
import * as P from "../src/packet.ts"
import type { Packet } from "../src/packet.ts"
import { serve, ServeError, type ServeOptions, type ServerSession } from "../src/server.ts"
import { selfSigned } from "./brokers.ts"

const run = <A, E>(effect: Effect.Effect<A, E, import("effect/Scope").Scope>) => Effect.runPromise(Effect.scoped(effect))

const host = "127.0.0.1"

const accept: ServeOptions<never>["authenticate"] = () => Effect.succeed(0)

/** The next session the server accepts. */
const nextSession = (sessions: Stream.Stream<ServerSession>) =>
  Effect.flatMap(
    Stream.runHead(sessions),
    Option.match({ onNone: () => Effect.die("the server closed with no session"), onSome: (session) => Effect.succeed(session) })
  )

/** A raw TCP client for faults the real client never produces. Resolves once connected. */
const rawClient = (port: number, options?: { readonly allowHalfOpen?: boolean }) =>
  Effect.acquireRelease(
    Effect.callback<net.Socket>((resume) => {
      const socket = net.connect({ host, port, allowHalfOpen: options?.allowHalfOpen ?? false })

      // Node reports end of stream and close only on a socket that reads, so read and discard.
      socket.resume()
      socket.once("connect", () => resume(Effect.succeed(socket)))
    }),
    (socket) => Effect.sync(() => socket.destroy())
  )

const connectPacket = (keepAlive: number) =>
  encode(P.connect({ clientId: "raw", cleanSession: true, keepAlive, username: undefined, password: undefined, will: undefined }))

const socketClosed = (socket: net.Socket) =>
  Effect.callback<void>((resume) => {
    if (socket.destroyed) return resume(Effect.void)

    socket.once("close", () => resume(Effect.void))
  })

/** The status that closed the client, once it closes. */
const closedWith = (statusChanges: Stream.Stream<ConnectionStatus>) =>
  statusChanges.pipe(Stream.filter(ConnectionStatus.$is("Closed")), Stream.take(1), Stream.runCollect)

describe("serve", () => {
  it("authenticates, grants a subscription, and delivers a publish to the client", () =>
    run(
      Effect.gen(function* () {
        const seen: Array<{ readonly clientId: string; readonly username: string | undefined; readonly password: string }> = []

        const server = yield* serve({
          authenticate: (request) =>
            Effect.sync(() => {
              seen.push({ clientId: request.clientId, username: request.username, password: Buffer.from(request.password ?? []).toString() })

              return 0
            })
        })

        const client = yield* connect({ host, port: server.address.port, clientId: "printer-test", username: "bblp", password: "12345678" })
        const session = yield* nextSession(server.sessions)
        const reports = yield* client.subscribe("device/X/report", { qos: 1 })

        expect(seen).toEqual([{ clientId: "printer-test", username: "bblp", password: "12345678" }])
        expect([session.clientId, session.username]).toEqual(["printer-test", "bblp"])
        expect(yield* session.subscriptions).toEqual([{ filter: "device/X/report", qos: 1 }])
        expect(yield* session.publish("device/Y/report", "nobody")).toBe(false)
        expect(yield* session.publish("device/X/report", '{"print":{}}', { qos: 1 })).toBe(true)

        const [message] = yield* reports.pipe(Stream.take(1), Stream.runCollect)

        expect([message?.topic, Buffer.from(message?.payload ?? []).toString(), message?.qos]).toEqual(["device/X/report", '{"print":{}}', 1])
      })
    ))

  it("sends the refusal code authenticate chooses", () =>
    run(
      Effect.gen(function* () {
        const server = yield* serve({ authenticate: () => Effect.succeed(5) })
        const error = yield* Effect.flip(connect({ host, port: server.address.port }))

        expect(error).toBeInstanceOf(ConnectionRefused)
        expect(error).toMatchObject({ returnCode: 5 })
      })
    ))

  it("puts every client packet on inbound, and acknowledges QoS 1 and 2 publishes itself", () =>
    run(
      Effect.gen(function* () {
        const server = yield* serve({ authenticate: accept })
        const client = yield* connect({ host, port: server.address.port })
        const session = yield* nextSession(server.sessions)

        // Each publish completes only once the server has answered its handshake.
        yield* client.publish("device/X/request", "one", { qos: 1 })
        yield* client.publish("device/X/request", "two", { qos: 2 })

        const tags = yield* session.inbound.pipe(
          Stream.filter((packet) => !Predicate.isTagged(packet, "Pingreq")),
          Stream.take(3),
          Stream.map((packet: Packet) => (Predicate.isTagged(packet, "Publish") ? `Publish:${Buffer.from(packet.payload).toString()}` : packet._tag)),
          Stream.runCollect
        )

        expect(tags).toEqual(["Publish:one", "Publish:two", "Pubrel"])
      })
    ))

  it("holds SUBACK until onSubscribe completes, and keeps answering PINGREQ meanwhile", () =>
    run(
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>()
        const asked = yield* Deferred.make<void>()

        const server = yield* serve({
          authenticate: accept,
          onSubscribe: (_session, request) =>
            Effect.andThen(Deferred.succeed(asked, undefined), Effect.as(Deferred.await(gate), request.subscriptions.map((s) => s.qos)))
        })

        const client = yield* connect({ host, port: server.address.port, keepAlive: "1 second", reconnect: false })
        const subscribing = yield* Effect.forkChild(client.subscribe("device/X/report"), { startImmediately: true })

        yield* Deferred.await(asked)
        // Two keep-alive periods pass with SUBACK held; the client stays connected only if PINGRESP still flows.
        yield* Effect.sleep("2500 millis")
        expect(subscribing.pollUnsafe()).toBeUndefined()
        expect((yield* client.status)._tag).toBe("Connected")

        yield* Deferred.succeed(gate, undefined)
        yield* Fiber.join(subscribing)
      })
    ), 10_000)

  it("refuses a filter with 0x80 and destroys the socket when onSubscribe fails", () =>
    run(
      Effect.gen(function* () {
        const refusing = yield* serve({ authenticate: accept, onSubscribe: () => Effect.succeed([0x80]) })
        const first = yield* connect({ host, port: refusing.address.port })

        expect(yield* Effect.flip(first.subscribe("device/X/report"))).toBeInstanceOf(SubscribeRejected)

        const failing = yield* serve({ authenticate: accept, onSubscribe: () => Effect.fail("fault") })
        const second = yield* connect({ host, port: failing.address.port, reconnect: false })
        const session = yield* nextSession(failing.sessions)

        yield* Effect.forkChild(Effect.ignore(second.subscribe("device/X/report")), { startImmediately: true })
        yield* session.closed
      })
    ))

  it("drops every write while muted, so the client's keep-alive gives up", () =>
    run(
      Effect.gen(function* () {
        const server = yield* serve({ authenticate: accept })
        const client = yield* connect({ host, port: server.address.port, keepAlive: "1 second", reconnect: false })
        const session = yield* nextSession(server.sessions)

        yield* session.mute(true)

        const closed = yield* closedWith(client.statusChanges)

        expect(closed.map((s) => s.error.message)).toEqual(["no PINGRESP within the keep-alive period"])
      })
    ), 10_000)

  it("closes a session by destroying it, and completes closed", () =>
    run(
      Effect.gen(function* () {
        const server = yield* serve({ authenticate: accept })
        const client = yield* connect({ host, port: server.address.port, reconnect: false })
        const session = yield* nextSession(server.sessions)

        yield* session.close("destroy")
        yield* session.closed

        yield* closedWith(client.statusChanges)
        expect(yield* Effect.flip(Stream.runDrain(session.inbound))).toBeDefined()
      })
    ))

  it("half-closes: the client reads end of stream while the server still reads what it sends", () =>
    run(
      Effect.gen(function* () {
        const server = yield* serve({ authenticate: accept })
        const socket = yield* rawClient(server.address.port, { allowHalfOpen: true })
        const ended = yield* Deferred.make<void>()

        socket.on("end", () => Deferred.doneUnsafe(ended, Effect.void))
        socket.write(connectPacket(0))

        const session = yield* nextSession(server.sessions)

        yield* session.close("half-close")
        yield* Deferred.await(ended)
        socket.write(encode(P.publishAtMostOnce("device/X/request", Buffer.from("after"), false)))

        const [packet] = yield* session.inbound.pipe(Stream.take(1), Stream.runCollect)

        expect(packet?._tag === "Publish" ? Buffer.from(packet.payload).toString() : packet?._tag).toBe("after")
      })
    ))

  it("ends a session with FIN and closes it without waiting for the client's FIN", () =>
    run(
      Effect.gen(function* () {
        const server = yield* serve({ authenticate: accept })
        // A half-open-capable client never sends FIN on its own, so only the server can finish the close.
        const socket = yield* rawClient(server.address.port, { allowHalfOpen: true })

        socket.write(connectPacket(0))

        const session = yield* nextSession(server.sessions)

        yield* session.close("end")
        yield* session.closed
      })
    ))

  it("pauses reading while a session's inbound is full, then delivers every packet in order", () =>
    run(
      Effect.gen(function* () {
        const COUNT = 2000
        const server = yield* serve({ authenticate: accept, bufferSize: 16 })
        const client = yield* connect({ host, port: server.address.port })
        const session = yield* nextSession(server.sessions)

        const payload = (i: number) => {
          const bytes = Buffer.alloc(16_384)

          bytes.writeUInt32BE(i)

          return bytes
        }

        // Backpressure reaches the client: its publishes wait while the server is paused, so they run alongside the read.
        const publishing = yield* Effect.forkChild(
          Effect.forEach(Array.from({ length: COUNT }, (_, i) => i), (i) => client.publish("device/X/request", payload(i)), { discard: true }),
          { startImmediately: true }
        )

        // 32 MB is more than the kernel buffers on both ends of a loopback socket, so a server that kept reading
        // would let every publish finish at once; a paused one leaves the client still sending.
        yield* Effect.sleep("500 millis")
        expect(publishing.pollUnsafe()).toBeUndefined()

        const indexes = yield* session.inbound.pipe(
          Stream.filter((packet) => Predicate.isTagged(packet, "Publish")),
          Stream.take(COUNT),
          Stream.map((packet) => Buffer.from(packet.payload).readUInt32BE(0)),
          Stream.runCollect
        )

        expect(indexes).toEqual(Array.from({ length: COUNT }, (_, i) => i))
        yield* Fiber.join(publishing)
      })
    ), 30_000)

  it("drops a client silent for one and a half keep-alive periods, unless told to ignore keep-alive", () =>
    run(
      Effect.gen(function* () {
        const enforcing = yield* serve({ authenticate: accept })
        const silent = yield* rawClient(enforcing.address.port)

        silent.write(connectPacket(1))
        yield* socketClosed(silent)

        const ignoring = yield* serve({ authenticate: accept, keepAlive: "ignore" })
        const halfOpen = yield* rawClient(ignoring.address.port)

        halfOpen.write(connectPacket(1))
        yield* Effect.sleep("2500 millis")
        expect(halfOpen.destroyed).toBe(false)
      })
    ), 10_000)

  it("closes a connection that never sends CONNECT", () =>
    run(
      Effect.gen(function* () {
        const server = yield* serve({ authenticate: accept, connectTimeout: "200 millis" })
        const socket = yield* rawClient(server.address.port)

        yield* socketClosed(socket)
      })
    ))

  it("serves TLS with a caller-supplied certificate", () =>
    run(
      Effect.gen(function* () {
        const server = yield* serve({ authenticate: accept, tls: selfSigned() })
        const client = yield* connect({ host, port: server.address.port, tls: { rejectUnauthorized: false } })

        expect((yield* client.status)._tag).toBe("Connected")
        expect((yield* nextSession(server.sessions)).clientId).toBe(client.clientId)
      })
    ))

  it("buffers sessions until they are read, so none is lost to a late reader", () =>
    run(
      Effect.gen(function* () {
        const server = yield* serve({ authenticate: accept })
        const a = yield* connect({ host, port: server.address.port, clientId: "a" })
        const b = yield* connect({ host, port: server.address.port, clientId: "b" })

        // Both connected before anyone read sessions; SUBACK still flows for an unread session.
        yield* a.subscribe("x")
        yield* b.subscribe("y")

        const ids = yield* server.sessions.pipe(Stream.take(2), Stream.map((s) => s.clientId), Stream.runCollect)

        expect(ids).toEqual(["a", "b"])
      })
    ))

  it("writes raw bytes, so a client sees a malformed packet", () =>
    run(
      Effect.gen(function* () {
        const server = yield* serve({ authenticate: accept })
        const client = yield* connect({ host, port: server.address.port, reconnect: false })
        const session = yield* nextSession(server.sessions)

        // Packet type 0 is reserved (section 2.2.1).
        yield* session.writeRaw(Uint8Array.of(0x00, 0x00))

        const closed = yield* closedWith(client.statusChanges)

        expect(closed[0]?.error).toBeInstanceOf(MalformedPacket)
      })
    ))

  it("closes every connection and ends sessions when the server closes", () =>
    run(
      Effect.gen(function* () {
        const server = yield* serve({ authenticate: accept })
        const client = yield* connect({ host, port: server.address.port, reconnect: false })
        const session = yield* nextSession(server.sessions)

        yield* server.close()
        yield* session.closed
        yield* closedWith(client.statusChanges)
        expect(yield* Stream.runCollect(server.sessions)).toEqual([])
        yield* server.close()
      })
    ))

  it("closes promptly while a TLS handshake is still pending", () =>
    run(
      Effect.gen(function* () {
        // A raw TCP client on a TLS port never finishes the handshake, so it never becomes a session.
        const forced = yield* serve({ authenticate: accept, tls: selfSigned() })

        // The pause lets the server accept the connection, so the handshake is pending when close starts.
        yield* rawClient(forced.address.port)
        yield* Effect.sleep("100 millis")
        yield* forced.close({ force: true }).pipe(Effect.timeout("2 seconds"))

        const graceful = yield* serve({ authenticate: accept, tls: selfSigned(), connectTimeout: "300 millis" })

        yield* rawClient(graceful.address.port)
        yield* Effect.sleep("100 millis")
        yield* graceful.close().pipe(Effect.timeout("2 seconds"))
      })
    ))

  it("interrupts authenticate when its socket closes, and never emits that session", () =>
    run(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>()
        const interrupted = yield* Deferred.make<void>()

        const server = yield* serve({
          authenticate: () =>
            Effect.andThen(Deferred.succeed(started, undefined), Effect.never).pipe(
              Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined))
            )
        })

        const socket = yield* rawClient(server.address.port)

        socket.write(connectPacket(0))
        yield* Deferred.await(started)
        socket.destroy()
        yield* Deferred.await(interrupted)

        const session = yield* Stream.runHead(server.sessions).pipe(Effect.timeout("300 millis"), Effect.option)

        expect(Option.isNone(session)).toBe(true)
      })
    ))

  it("does not let a late SUBSCRIBE decision undo a later UNSUBSCRIBE", () =>
    run(
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>()
        const asked = yield* Deferred.make<void>()

        const server = yield* serve({
          authenticate: accept,
          onSubscribe: (_session, request) =>
            Effect.andThen(Deferred.succeed(asked, undefined), Effect.as(Deferred.await(gate), request.subscriptions.map((s) => s.qos)))
        })

        const socket = yield* rawClient(server.address.port)
        const subacks = yield* Deferred.make<void>()

        socket.on("data", (chunk: Buffer) => {
          // SUBACK is packet type 9 (0x90); it may share a chunk with CONNACK or UNSUBACK.
          if (chunk.includes(0x90)) Deferred.doneUnsafe(subacks, Effect.void)
        })
        socket.write(connectPacket(0))

        const session = yield* nextSession(server.sessions)

        socket.write(encode(P.subscribe(1, [{ filter: "device/X/report", qos: 0 }])))
        yield* Deferred.await(asked)
        socket.write(encode(P.unsubscribe(2, ["device/X/report"])))
        yield* session.inbound.pipe(
          Stream.filter((packet) => Predicate.isTagged(packet, "Unsubscribe")),
          Stream.take(1),
          Stream.runDrain
        )
        yield* Deferred.succeed(gate, undefined)
        yield* Deferred.await(subacks)

        expect(yield* session.subscriptions).toEqual([])
      })
    ))

  it("rejects a bufferSize below 1 and a port already in use", () =>
    run(
      Effect.gen(function* () {
        expect(yield* Effect.flip(serve({ authenticate: accept, bufferSize: 0 }))).toBeInstanceOf(ServeError)

        const first = yield* serve({ authenticate: accept })

        expect(yield* Effect.flip(serve({ authenticate: accept, port: first.address.port }))).toBeInstanceOf(ServeError)
      })
    ))
})
