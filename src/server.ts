import { Buffer } from "node:buffer"
import * as net from "node:net"
import * as tls from "node:tls"
import * as Arr from "effect/Array"
import * as Cause from "effect/Cause"
import * as Clock from "effect/Clock"
import * as Data from "effect/Data"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Predicate from "effect/Predicate"
import * as Queue from "effect/Queue"
import * as Schedule from "effect/Schedule"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { Decoder, encode, MalformedPacket } from "./codec.ts"
import { ConnectionError } from "./errors.ts"
import * as P from "./packet.ts"
import type { Connect, Packet, QoS, Subscribe, SubackCode, Subscription } from "./packet.ts"
import { isValidTopic, TopicTree } from "./topic.ts"

/** The listener could not start, for example because the port is taken. */
export class ServeError extends Data.TaggedError("ServeError")<{ readonly message: string }> {}

/** CONNACK return codes, section 3.2.2.3. 0 accepts; 1 to 5 refuse. */
export type ConnackCode = 0 | 1 | 2 | 3 | 4 | 5

export interface Peer {
  readonly address: string
  readonly port: number
}

export interface ServeOptions<R> {
  /** Defaults to 127.0.0.1. */
  readonly host?: string | undefined
  /** Defaults to 0, which picks a free port; read it from `Server.address`. */
  readonly port?: number | undefined
  /** A caller-supplied key and certificate. Absent means plain TCP. */
  readonly tls?: tls.TlsOptions | undefined
  /** How long a connection may take to send CONNECT. Defaults to 10 seconds. */
  readonly connectTimeout?: Duration.Input | undefined
  /**
   * `"enforce"`, the default, drops a client that sends nothing for one and a half keep-alive periods
   * (section 3.1.2.10). `"ignore"` never does, which models a server that never notices a half-open socket.
   */
  readonly keepAlive?: "enforce" | "ignore" | undefined
  /**
   * Packets a session holds unread, 1024 by default. Past that, the server stops reading that socket until the
   * consumer drains to half, so TCP pushes back on the client. While paused it answers nothing, PINGREQ included.
   */
  readonly bufferSize?: number | undefined
  /**
   * Chooses the CONNACK code. Anything but 0 refuses the connection after sending CONNACK. A failure or defect
   * here destroys the socket without a CONNACK.
   */
  readonly authenticate: (connect: Connect, peer: Peer) => Effect.Effect<ConnackCode, unknown, R>
  /**
   * Chooses the SUBACK codes, one per requested filter. By default every filter is granted at the QoS it asked
   * for. The SUBACK waits for this Effect, so a test can stall it by sleeping, drop it with `Effect.never`, or
   * refuse with 0x80. A failure or defect destroys the socket. Other packets keep flowing while it runs.
   */
  readonly onSubscribe?: ((session: ServerSession, request: Subscribe) => Effect.Effect<ReadonlyArray<SubackCode>, unknown, R>) | undefined
}

export interface ServerPublishOptions {
  /** Defaults to 0. It is capped at the QoS the subscription was granted. */
  readonly qos?: QoS | undefined
  readonly retain?: boolean | undefined
}

export type CloseMode = "end" | "half-close" | "destroy"

/** One client connection, from an accepted CONNECT until its socket closes. */
export interface ServerSession {
  readonly id: string
  readonly clientId: string
  readonly username: string | undefined
  readonly peer: Peer
  /**
   * Every packet the client sends after CONNECT, in order, including the ones the server answers itself. It ends
   * when the socket closes after a DISCONNECT and fails otherwise. Read it once.
   */
  readonly inbound: Stream.Stream<Packet, ConnectionError | MalformedPacket>
  /** The filters currently granted, with their granted QoS. */
  readonly subscriptions: Effect.Effect<ReadonlyArray<Subscription>>
  /**
   * Publishes to the client if one of its granted filters matches, and returns false if none does. It completes
   * once the PUBLISH is written; the client's acknowledgements arrive on `inbound`.
   */
  readonly publish: (topic: string, payload: string | Uint8Array, options?: ServerPublishOptions) => Effect.Effect<boolean, ConnectionError>
  /** Writes any packet, bypassing subscriptions; for faults such as a SUBACK with the wrong packet id. */
  readonly write: (packet: Packet) => Effect.Effect<void, ConnectionError>
  /** Writes bytes as they are; for malformed-packet faults. */
  readonly writeRaw: (bytes: Uint8Array) => Effect.Effect<void, ConnectionError>
  /**
   * While muted, every write is dropped without buffering, the server's own replies included, PINGRESP among
   * them: a printer that has gone silent but keeps its socket open.
   */
  readonly mute: (enabled: boolean) => Effect.Effect<void>
  /**
   * `"end"` sends FIN and closes the socket once queued writes have flushed. `"half-close"` sends FIN but keeps
   * reading, so the client sees end of stream while its writes still arrive. `"destroy"` drops the socket at once. It returns once the
   * close starts; wait on `closed` for the socket to go.
   */
  readonly close: (mode: CloseMode) => Effect.Effect<void>
  /** Completes once the socket has closed, whoever closed it. */
  readonly closed: Effect.Effect<void>
}

export interface Server {
  readonly address: { readonly host: string; readonly port: number }
  /**
   * Each session once its CONNECT is accepted, in order. Sessions are buffered from the moment the server starts,
   * so none is lost to a late reader. Read it once. It ends when the server closes.
   */
  readonly sessions: Stream.Stream<ServerSession>
  /**
   * Stops accepting and closes every connection: with `force`, at once; otherwise with FIN, destroying any socket
   * still open after `connectTimeout`. Completes once every socket is gone. Closing again does nothing.
   */
  readonly close: (options?: { readonly force?: boolean | undefined }) => Effect.Effect<void>
}

const DEFAULT_BUFFER_SIZE = 1024
const MAX_PACKET_ID = 65_535

type Inbound = Queue.Queue<Packet, ConnectionError | MalformedPacket | Cause.Done>

/** The socket side of one connection. Packets queue as they decode; the queue ends or fails when the socket goes. */
class Wire {
  readonly socket: net.Socket
  readonly packets: Inbound
  readonly gone = Deferred.makeUnsafe<void>()
  muted = false
  private error: ConnectionError | MalformedPacket | undefined = undefined
  private disconnected = false

  constructor(socket: net.Socket, packets: Inbound) {
    this.socket = socket
    this.packets = packets

    const decoder = new Decoder((packet) => {
      if (packet._tag === "Disconnect") this.disconnected = true

      Queue.offerUnsafe(this.packets, packet)
    })

    socket.on("data", (chunk: Buffer) => {
      try {
        decoder.push(chunk)
      } catch (cause) {
        this.fail(cause instanceof MalformedPacket ? cause : new ConnectionError({ message: String(cause) }))
      }
    })
    // allowHalfOpen keeps our side open after the client's FIN so "half-close" can be modelled. A client that ends
    // its side is answered with ours, as a server without allowHalfOpen would.
    socket.on("end", () => {
      if (socket.writable) socket.end()
    })
    socket.on("error", (e) => {
      this.error ??= new ConnectionError({ message: e.message })
    })
    socket.on("close", () => {
      if (this.disconnected && this.error === undefined) Queue.endUnsafe(this.packets)
      else Queue.failCauseUnsafe(this.packets, Cause.fail(this.error ?? new ConnectionError({ message: "the connection closed" })))

      Deferred.doneUnsafe(this.gone, Effect.void)
    })
  }

  fail(error: ConnectionError | MalformedPacket): void {
    this.error ??= error
    this.socket.destroy()
  }

  /** Writes unless muted. Fails once the socket can no longer take bytes. */
  send(bytes: Uint8Array): Effect.Effect<void, ConnectionError> {
    return Effect.suspend(() => {
      if (!this.socket.writable) return Effect.fail(new ConnectionError({ message: "the connection closed" }))

      if (!this.muted) this.socket.write(bytes)

      return Effect.void
    })
  }

  close(mode: CloseMode): void {
    if (mode === "destroy") this.socket.destroy()
    else if (mode === "half-close") this.socket.end()
    // Sends FIN, then closes once queued writes have flushed, whether or not the client answers with its own.
    else this.socket.destroySoon()
  }
}

const peerOf = (socket: net.Socket): Peer => ({ address: socket.remoteAddress ?? "", port: socket.remotePort ?? 0 })

const bindListener = (listener: net.Server, host: string, port: number) =>
  Effect.callback<void, ServeError>((resume) => {
    const onError = (e: Error) => resume(Effect.fail(new ServeError({ message: e.message })))

    listener.once("error", onError)
    listener.listen(port, host, () => {
      listener.off("error", onError)
      resume(Effect.void)
    })
  })

/**
 * Runs an MQTT 3.1.1 server for tests and emulators. It answers PINGREQ, UNSUBSCRIBE and the QoS 1 and 2 handshakes
 * itself, unless a session is muted, and routes SUBSCRIBE through `onSubscribe`. Everything else is up to the caller,
 * through each session's `inbound` stream and its write methods. The server closes with the scope.
 */
export const serve = <R>(options: ServeOptions<R>): Effect.Effect<Server, ServeError, Scope.Scope | R> =>
  Effect.gen(function* () {
    const host = options.host ?? "127.0.0.1"
    const capacity = options.bufferSize ?? DEFAULT_BUFFER_SIZE
    const connectTimeout = options.connectTimeout ?? "10 seconds"
    const enforceKeepAlive = (options.keepAlive ?? "enforce") === "enforce"

    if (!Number.isInteger(capacity) || capacity < 1) {
      return yield* Effect.fail(new ServeError({ message: `bufferSize must be a whole number of at least 1, got ${capacity}` }))
    }

    const scope = yield* Effect.scope
    const accepted = yield* Queue.unbounded<net.Socket>()
    const sessions = yield* Queue.unbounded<ServerSession, Cause.Done>()
    const wires = new Set<Wire>()
    let nextSession = 0
    let closing: Effect.Effect<void> | undefined = undefined

    const onSocket = (socket: net.Socket) => {
      socket.setNoDelay(true)
      Queue.offerUnsafe(accepted, socket)
    }

    const listener = options.tls
      ? tls.createServer({ ...options.tls, allowHalfOpen: true }, onSocket)
      : net.createServer({ allowHalfOpen: true }, onSocket)

    // A TLS handshake failure surfaces here, not on any socket we hold.
    listener.on("tlsClientError", (_error, socket) => socket.destroy())

    yield* bindListener(listener, host, options.port ?? 0)

    const bound = listener.address()
    const address = bound === null || Predicate.isString(bound) ? { host, port: options.port ?? 0 } : { host: bound.address, port: bound.port }

    const handle = (socket: net.Socket) =>
      Effect.gen(function* () {
        const packets: Inbound = yield* Queue.unbounded<Packet, ConnectionError | MalformedPacket | Cause.Done>()
        const wire = new Wire(socket, packets)

        // A refused or finished connection stays tracked until its socket is gone, so close waits for it.
        wires.add(wire)
        socket.once("close", () => wires.delete(wire))

        const first = yield* Queue.take(packets).pipe(
          Effect.timeoutOrElse({ duration: connectTimeout, orElse: () => Effect.fail(new ConnectionError({ message: "timed out waiting for CONNECT" })) }),
          Effect.option
        )

        if (first._tag === "None" || first.value._tag !== "Connect") return wire.fail(new ConnectionError({ message: "the first packet was not CONNECT" }))

        const connect = first.value
        const peer = peerOf(socket)
        const code = yield* Effect.exit(options.authenticate(connect, peer))

        if (!Exit.isSuccess(code)) return wire.fail(new ConnectionError({ message: "authenticate failed" }))

        yield* Effect.ignore(wire.send(encode(P.connack(false, code.value))))

        if (code.value !== 0) return wire.close("end")

        yield* session(wire, connect, peer)
      }).pipe(Effect.scoped)

    const session = (wire: Wire, connect: Connect, peer: Peer) =>
      Effect.gen(function* () {
        const clock = yield* Clock.Clock
        // Work forked for this connection, such as a held-back SUBACK, ends with it.
        const connectionScope = yield* Effect.scope
        const granted = new TopicTree<Subscription>()
        const filters = new Map<string, Subscription>()
        const inbound = yield* Queue.unbounded<Packet, ConnectionError | MalformedPacket | Cause.Done>()
        let held = 0
        let reading = true
        let lastReceived = clock.currentTimeMillisUnsafe()
        let packetId = 0

        const buffered = () => Queue.sizeUnsafe(inbound) + held

        const setReading = (next: boolean) => {
          if (next === reading) return

          reading = next

          if (next) wire.socket.resume()
          else wire.socket.pause()
        }

        const nextPacketId = () => {
          packetId = packetId === MAX_PACKET_ID ? 1 : packetId + 1

          return packetId
        }

        const send = (packet: Packet) => wire.send(encode(packet))

        const subscriptions = Effect.sync(() => [...filters.values()])

        const grant = (request: Subscribe, codes: ReadonlyArray<SubackCode>) =>
          Effect.suspend(() => {
            request.subscriptions.forEach((subscription, i) => {
              const code = codes[i]

              if (code === undefined || code === 0x80) return

              const previous = filters.get(subscription.filter)

              if (previous !== undefined) granted.remove(previous.filter, previous)

              const next = { filter: subscription.filter, qos: code }

              filters.set(subscription.filter, next)
              granted.add(next.filter, next)
            })

            return send(P.suback(request.packetId, codes))
          })

        const self: ServerSession = {
          id: String(++nextSession),
          clientId: connect.clientId,
          username: connect.username,
          peer,
          inbound: Stream.fromPull(
            Effect.succeed(
              Effect.suspend(() => {
                // A pull means the consumer finished its previous chunk, so only now does that chunk stop counting.
                held = 0

                if (!reading && buffered() <= capacity / 2) setReading(true)

                return Effect.flatMap(Queue.takeBetween(inbound, 1, capacity), (taken) => {
                  held = taken.length

                  return Arr.isReadonlyArrayNonEmpty(taken) ? Effect.succeed(taken) : Effect.die("takeBetween returned no packets")
                })
              })
            )
          ),
          subscriptions,
          publish: (topic, payload, publishOptions) =>
            Effect.suspend(() => {
              if (!isValidTopic(topic)) return Effect.fail(new ConnectionError({ message: `cannot publish to ${topic}` }))

              let ceiling: QoS | undefined = undefined

              granted.match(topic, (subscription) => {
                if (ceiling === undefined || subscription.qos > ceiling) ceiling = subscription.qos
              })

              if (ceiling === undefined) return Effect.succeed(false)

              const bytes = payload instanceof Uint8Array ? payload : Buffer.from(payload)
              const retain = publishOptions?.retain ?? false
              const qos = Math.min(publishOptions?.qos ?? 0, ceiling)

              const packet =
                qos === 0 ? P.publishAtMostOnce(topic, bytes, retain) : P.publishAcknowledged(topic, bytes, qos === 1 ? 1 : 2, nextPacketId(), retain, false)

              return Effect.as(send(packet), true)
            }),
          write: send,
          writeRaw: (bytes) => wire.send(bytes),
          mute: (enabled) =>
            Effect.sync(() => {
              wire.muted = enabled
            }),
          close: (mode) => Effect.sync(() => wire.close(mode)),
          closed: Deferred.await(wire.gone)
        }

        const answer = (packet: Packet): Effect.Effect<void, ConnectionError, R> => {
          switch (packet._tag) {
            case "Pingreq":
              return send(P.pingresp)
            case "Unsubscribe":
              for (const filter of packet.filters) {
                const previous = filters.get(filter)

                if (previous === undefined) continue

                filters.delete(filter)
                granted.remove(filter, previous)
              }

              return send(P.unsuback(packet.packetId))
            case "Publish":
              if (packet.qos === 1) return send(P.puback(packet.packetId))

              if (packet.qos === 2) return send(P.pubrec(packet.packetId))

              return Effect.void
            case "Pubrel":
              return send(P.pubcomp(packet.packetId))
            case "Pubrec":
              return send(P.pubrel(packet.packetId))
            case "Subscribe": {
              const decide =
                options.onSubscribe === undefined
                  ? Effect.succeed(packet.subscriptions.map((subscription): SubackCode => subscription.qos))
                  : options.onSubscribe(self, packet)

              // The SUBACK may be held back on purpose, so later packets must not wait for it.
              return Effect.asVoid(
                Effect.forkIn(
                  Effect.matchCauseEffect(decide, {
                    onFailure: () => Effect.sync(() => wire.fail(new ConnectionError({ message: "onSubscribe failed" }))),
                    onSuccess: (codes) => Effect.ignore(grant(packet, codes))
                  }),
                  connectionScope
                )
              )
            }

            case "Disconnect":
              return Effect.sync(() => wire.close("end"))
            case "Connect":
              // Section 3.1.0: a second CONNECT is a protocol violation.
              return Effect.sync(() => wire.fail(new ConnectionError({ message: "a second CONNECT" })))
            default:
              return Effect.void
          }
        }

        if (enforceKeepAlive && connect.keepAlive > 0) {
          const limitMs = connect.keepAlive * 1500

          const check = Effect.sync(() => {
            if (clock.currentTimeMillisUnsafe() - lastReceived >= limitMs) {
              wire.fail(new ConnectionError({ message: "no packet within one and a half keep-alive periods" }))
            }
          })

          yield* Effect.forkScoped(Effect.repeat(check, Schedule.spaced(limitMs / 6)))
        }

        yield* Queue.offer(sessions, self)

        // Relay decoded packets into the session's inbound queue, answering the ones the protocol requires.
        yield* Effect.gen(function* () {
          while (true) {
            const packet = yield* Queue.take(wire.packets)

            lastReceived = clock.currentTimeMillisUnsafe()
            Queue.offerUnsafe(inbound, packet)

            if (reading && buffered() >= capacity) setReading(false)

            yield* Effect.ignore(answer(packet))
          }
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.sync(() => {
              Queue.failCauseUnsafe(inbound, cause)
            })
          )
        )
      })

    // One fiber per connection, all in the server's scope, so nothing outlives the server.
    yield* Effect.forkIn(
      Effect.forever(Effect.flatMap(Queue.take(accepted), (socket) => Effect.forkIn(handle(socket), scope))),
      scope
    )

    const close = (closeOptions?: { readonly force?: boolean | undefined }) =>
      Effect.suspend(() => {
        if (closing !== undefined) return closing

        const force = closeOptions?.force ?? false

        const stopped = Effect.callback<void>((resume) => {
          listener.close(() => resume(Effect.void))
        })

        const live = [...wires]

        for (const wire of live) wire.close(force ? "destroy" : "end")

        const gone = Effect.forEach(live, (wire) => Deferred.await(wire.gone), { discard: true })

        closing = Effect.andThen(
          Effect.all(
            [
              stopped,
              gone.pipe(
                Effect.timeoutOrElse({
                  duration: connectTimeout,
                  orElse: () => Effect.andThen(Effect.sync(() => live.forEach((wire) => wire.close("destroy"))), gone)
                })
              )
            ],
            { discard: true, concurrency: "unbounded" }
          ),
          Queue.end(sessions)
        ).pipe(Effect.asVoid, Effect.cached, Effect.runSync)

        return closing
      })

    yield* Effect.addFinalizer(() => close({ force: true }))

    return { address, sessions: Stream.fromQueue(sessions), close } satisfies Server
  })
