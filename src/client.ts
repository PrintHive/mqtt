import { Buffer } from "node:buffer"
import { randomBytes } from "node:crypto"
import * as net from "node:net"
import * as tls from "node:tls"
import * as Cause from "effect/Cause"
import * as Data from "effect/Data"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Latch from "effect/Latch"
import * as Match from "effect/Match"
import * as Queue from "effect/Queue"
import * as Schedule from "effect/Schedule"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import { Decoder, encode, MalformedPacket } from "./codec.ts"
import { ClientClosed, ConnectionError, ConnectionRefused, InvalidTopic, ProtocolError, refused, SubscribeRejected } from "./errors.ts"
import type { MqttError } from "./errors.ts"
import * as P from "./packet.ts"
import type { Publish, QoS, Will } from "./packet.ts"
import { Session } from "./session.ts"
import type { Link } from "./session.ts"
import { isValidFilter, isValidTopic, TopicTree } from "./topic.ts"

export interface ConnectOptions {
  readonly host: string
  /** Defaults to 8883 with TLS and 1883 without. */
  readonly port?: number | undefined
  /** `true` uses TLS with Node's defaults. An object passes through to `tls.connect`, e.g. `{ rejectUnauthorized: false }`. */
  readonly tls?: boolean | tls.ConnectionOptions | undefined
  /** Defaults to `printhive-` and 12 random hex digits. */
  readonly clientId?: string | undefined
  readonly username?: string | undefined
  readonly password?: string | Uint8Array | undefined
  /** Defaults to true. With false, the broker keeps subscriptions and queued QoS 1 and 2 messages across reconnects. */
  readonly cleanSession?: boolean | undefined
  /** Defaults to 60 seconds. Rounded down to whole seconds; 0 turns keep-alive off. */
  readonly keepAlive?: Duration.Input | undefined
  /** How long to wait for the socket and CONNACK. Defaults to 10 seconds. */
  readonly connectTimeout?: Duration.Input | undefined
  readonly will?: Will | undefined
  /**
   * When to retry after an established connection drops. Defaults to exponential backoff from 500 ms, capped at
   * 30 s, forever. `false` closes the client on the first drop. The first connection is never retried, so
   * `connect` fails fast when the broker is unreachable.
   */
  readonly reconnect?: Schedule.Schedule<unknown, MqttError> | false | undefined
}

export interface PublishOptions {
  /** Defaults to 0. QoS 1 and 2 publishes complete when the broker acknowledges them. */
  readonly qos?: QoS | undefined
  readonly retain?: boolean | undefined
}

export interface SubscribeOptions {
  /** The highest QoS to receive at. Defaults to 0. */
  readonly qos?: QoS | undefined
}

/** A received message. `payload` is a view into the socket buffer it arrived in. */
export type Message = Publish

export type ConnectionStatus = Data.TaggedEnum<{
  Connecting: {}
  Connected: { readonly sessionPresent: boolean }
  Reconnecting: { readonly attempt: number; readonly error: MqttError }
  Closed: { readonly error: MqttError }
}>

export const ConnectionStatus = Data.taggedEnum<ConnectionStatus>()

export interface MqttClient {
  readonly clientId: string
  readonly publish: (topic: string, payload: string | Uint8Array, options?: PublishOptions) => Effect.Effect<void, MqttError>
  /**
   * Subscribes and waits for the broker's SUBACK, so a message published afterwards is received. The subscription
   * lasts until the scope closes; UNSUBSCRIBE goes out when the last subscription on a filter ends. Each call sends
   * its own SUBSCRIBE, so it also receives the filter's retained messages.
   */
  readonly subscribe: (filter: string, options?: SubscribeOptions) => Effect.Effect<Stream.Stream<Message, MqttError>, MqttError, Scope.Scope>
  readonly status: Effect.Effect<ConnectionStatus>
  readonly statusChanges: Stream.Stream<ConnectionStatus>
}

interface Subscriber {
  readonly filter: string
  readonly queue: Queue.Queue<Message, MqttError | Cause.Done>
}

interface Config {
  readonly host: string
  readonly port: number
  readonly tls: tls.ConnectionOptions | undefined
  readonly connect: P.Connect
  readonly keepAliveMs: number
  readonly connectTimeout: Duration.Input
  readonly reconnect: Schedule.Schedule<unknown, MqttError> | false
}

const DEFAULT_RECONNECT = Schedule.min([Schedule.exponential("500 millis"), Schedule.spaced("30 seconds")])

const tlsOptions = (option: ConnectOptions["tls"]): tls.ConnectionOptions | undefined =>
  Match.value(option).pipe(
    Match.when(true, () => ({})),
    Match.when(false, () => undefined),
    Match.orElse((o) => o)
  )

const configure = (options: ConnectOptions): Config => {
  const secure = tlsOptions(options.tls)
  const keepAlive = Math.floor(Duration.toSeconds(Duration.fromInputUnsafe(options.keepAlive ?? "60 seconds")))

  return {
    host: options.host,
    port: options.port ?? (secure ? 8883 : 1883),
    tls: secure,
    connect: P.connect({
      clientId: options.clientId ?? `printhive-${randomBytes(6).toString("hex")}`,
      cleanSession: options.cleanSession ?? true,
      keepAlive,
      username: options.username,
      password: options.password === undefined ? undefined : Buffer.from(options.password),
      will: options.will
    }),
    keepAliveMs: keepAlive * 1000,
    connectTimeout: options.connectTimeout ?? "10 seconds",
    reconnect: options.reconnect ?? DEFAULT_RECONNECT
  }
}

/** One TCP or TLS connection, from CONNECT until the socket closes. */
class Connection implements Link {
  readonly connack = Deferred.makeUnsafe<P.Connack, MqttError>()
  /** Completes with the reason once the socket is gone. */
  readonly lost = Deferred.makeUnsafe<MqttError>()
  private readonly socket: net.Socket
  private readonly onDrain: () => void
  private error: MqttError | undefined = undefined
  private corked = false
  private lastWrite = Date.now()
  private pingOutstanding = false
  private timer: ReturnType<typeof setInterval> | undefined = undefined

  constructor(socket: net.Socket, session: Session, onDrain: () => void) {
    this.socket = socket
    this.onDrain = onDrain

    const decoder = new Decoder((packet) => {
      if (Deferred.isDoneUnsafe(this.connack)) session.receive(packet)
      else if (P.isConnack(packet)) Deferred.doneUnsafe(this.connack, Effect.succeed(packet))
      else this.fail(new ProtocolError({ message: "the broker sent a packet before CONNACK" }))
    })

    socket.on("data", (chunk: Buffer) => {
      try {
        decoder.push(chunk)
      } catch (cause) {
        this.fail(cause instanceof MalformedPacket ? cause : new ProtocolError({ message: String(cause) }))
      }
    })
    socket.on("drain", () => this.onDrain())
    socket.on("error", (e) => {
      this.error ??= new ConnectionError({ message: e.message })
    })
    socket.on("close", () => {
      clearInterval(this.timer)

      const error = this.error ?? new ConnectionError({ message: "the connection closed" })

      Deferred.doneUnsafe(this.connack, Effect.fail(error))
      Deferred.doneUnsafe(this.lost, Effect.succeed(error))
    })
  }

  write(buf: Buffer): boolean {
    this.lastWrite = Date.now()

    // Corking until the microtask queue drains turns a burst of small packets into one syscall.
    if (!this.corked) {
      this.corked = true
      this.socket.cork()
      queueMicrotask(() => {
        this.corked = false
        this.socket.uncork()
      })
    }

    return this.socket.write(buf)
  }

  pong(): void {
    this.pingOutstanding = false
  }

  fail(error: MqttError): void {
    this.error ??= error
    this.socket.destroy()
  }

  /**
   * Sends PINGREQ after a quiet keep-alive period, and drops the connection when the previous PINGREQ got no
   * PINGRESP within one period (section 3.1.2.10).
   */
  startKeepAlive(periodMs: number): void {
    if (periodMs === 0) return

    this.timer = setInterval(() => {
      if (this.pingOutstanding) return this.fail(new ConnectionError({ message: "no PINGRESP within the keep-alive period" }))

      if (Date.now() - this.lastWrite >= periodMs / 2) {
        this.pingOutstanding = true
        this.write(encode(P.pingreq))
      }
    }, periodMs)
    this.timer.unref()
  }

  close(): void {
    clearInterval(this.timer)

    if (this.socket.destroyed) return

    this.socket.end(encode(P.disconnect))
  }
}

const openSocket = (config: Config) =>
  Effect.callback<net.Socket, ConnectionError>((resume, signal) => {
    const socket = config.tls
      ? tls.connect({ ...config.tls, host: config.host, port: config.port, servername: config.tls.servername ?? config.host })
      : net.connect({ host: config.host, port: config.port })

    const onError = (e: Error) => {
      socket.destroy()
      resume(Effect.fail(new ConnectionError({ message: e.message })))
    }

    socket.setNoDelay(true)
    socket.once("error", onError)
    socket.once(config.tls ? "secureConnect" : "connect", () => {
      socket.off("error", onError)
      resume(Effect.succeed(socket))
    })
    signal.addEventListener("abort", () => socket.destroy(), { once: true })
  })

/** Opens a socket and completes the CONNECT handshake. The connection closes with the enclosing scope. */
const establish = (config: Config, session: Session, onDrain: () => void) =>
  Effect.gen(function* () {
    const socket = yield* openSocket(config)
    const connection = new Connection(socket, session, onDrain)

    yield* Effect.addFinalizer(() => Effect.sync(() => connection.close()))
    connection.write(encode(config.connect))

    const connack = yield* Deferred.await(connection.connack)

    if (connack.returnCode !== 0) return yield* Effect.fail(refused(connack.returnCode))

    connection.startKeepAlive(config.keepAliveMs)

    return { connection, sessionPresent: connack.sessionPresent }
  }).pipe(
    Effect.timeoutOrElse({
      duration: config.connectTimeout,
      orElse: () => Effect.fail(new ConnectionError({ message: "timed out connecting" }))
    })
  )

/** Errors worth reconnecting after. Refusals other than "server unavailable" need a config change. */
const transient = (error: MqttError): boolean =>
  error instanceof ConnectionError || (error instanceof ConnectionRefused && error.returnCode === 3)

/**
 * Connects to a broker. The client lives until the scope closes, reconnecting per `options.reconnect` when an
 * established connection drops.
 */
export const connect = (options: ConnectOptions): Effect.Effect<MqttClient, MqttError, Scope.Scope> =>
  Effect.gen(function* () {
    const config = configure(options)
    const clientScope = yield* Effect.scope
    const subscribers = new TopicTree<Subscriber>()
    const live = new Set<Subscriber>()
    /** Filters confirmed by SUBACK, with the QoS granted, for resubscribing when the broker loses the session. */
    const confirmed = new Map<string, { qos: QoS; streams: number }>()
    const session = new Session((message) => subscribers.match(message.topic, (s) => Queue.offerUnsafe(s.queue, message)))
    const writable = yield* Latch.make(false)
    const status = yield* SubscriptionRef.make<ConnectionStatus>(ConnectionStatus.Connecting())
    const ready = yield* Deferred.make<void, MqttError>()
    let closed: ClientClosed | undefined = undefined

    const failRejected = (filters: ReadonlyArray<string>, codes: ReadonlyArray<P.SubackCode>) =>
      Effect.sync(() => {
        filters.forEach((filter, i) => {
          if (codes[i] !== 0x80) return

          confirmed.delete(filter)

          for (const s of live) if (s.filter === filter) Queue.failCauseUnsafe(s.queue, Cause.fail(new SubscribeRejected({ filter })))
        })
      })

    const run = Effect.gen(function* () {
      const { connection, sessionPresent } = yield* establish(config, session, () => writable.openUnsafe())

      session.attach(connection, sessionPresent)

      if (!sessionPresent && confirmed.size > 0) {
        const filters = [...confirmed.keys()]
        const resubscribe = session.subscribe(filters.map((filter) => ({ filter, qos: confirmed.get(filter)?.qos ?? 0 })))

        // Starting immediately writes the SUBSCRIBE now, ahead of any publish waiting for the connection.
        yield* Effect.forkScoped(Effect.flatMap(resubscribe, (codes) => failRejected(filters, codes)), { startImmediately: true })
      }

      writable.openUnsafe()
      yield* SubscriptionRef.set(status, ConnectionStatus.Connected({ sessionPresent }))
      yield* Deferred.succeed(ready, undefined)

      const error = yield* Deferred.await(connection.lost)

      writable.closeUnsafe()
      session.detach()

      return error
    }).pipe(Effect.scoped)

    const supervise = Effect.gen(function* () {
      let lost = yield* run

      while (config.reconnect !== false) {
        let attempt = 0

        yield* SubscriptionRef.set(status, ConnectionStatus.Reconnecting({ attempt, error: lost }))
        lost = yield* run.pipe(
          Effect.tapError((error) => SubscriptionRef.set(status, ConnectionStatus.Reconnecting({ attempt: ++attempt, error }))),
          Effect.retry({ schedule: config.reconnect, while: transient })
        )
      }

      return yield* Effect.fail(lost)
    })

    const shutdown = (error: MqttError) =>
      Effect.gen(function* () {
        closed = error instanceof ClientClosed ? error : new ClientClosed({ message: `the client closed after ${error._tag}: ${error.message}` })
        session.close(closed)
        writable.openUnsafe()

        for (const s of live) Queue.failCauseUnsafe(s.queue, Cause.fail(error))

        yield* SubscriptionRef.set(status, ConnectionStatus.Closed({ error }))
        yield* Deferred.fail(ready, error)
      })

    yield* supervise.pipe(
      Effect.catch(shutdown),
      Effect.ensuring(Effect.suspend(() => (closed ? Effect.void : shutdown(new ClientClosed({ message: "the client scope closed" }))))),
      Effect.forkScoped
    )
    yield* Deferred.await(ready)

    const whenWritable = <A>(f: () => Effect.Effect<A, MqttError>): Effect.Effect<A, MqttError> =>
      Effect.suspend(() => {
        if (closed) return Effect.fail(closed)

        if (writable.isOpen()) return f()

        return Effect.flatMap(writable.await, () => whenWritable(f))
      })

    const publish = (topic: string, payload: string | Uint8Array, publishOptions?: PublishOptions) => {
      if (!isValidTopic(topic)) return Effect.fail(new InvalidTopic({ topic }))

      const bytes = payload instanceof Uint8Array ? payload : Buffer.from(payload)
      const retain = publishOptions?.retain ?? false
      const qos = publishOptions?.qos ?? 0

      if (qos !== 0) return Effect.suspend(() => (closed ? Effect.fail(closed) : session.publishAcknowledged(topic, bytes, qos, retain)))

      return whenWritable(() => {
        if (!session.publishAtMostOnce(topic, bytes, retain)) writable.closeUnsafe()

        return Effect.void
      })
    }

    const release = (subscriber: Subscriber) =>
      Effect.suspend(() => {
        subscribers.remove(subscriber.filter, subscriber)
        live.delete(subscriber)

        const entry = confirmed.get(subscriber.filter)

        if (entry === undefined || --entry.streams > 0 || closed) return Effect.void

        confirmed.delete(subscriber.filter)

        return Effect.forkIn(Effect.ignore(session.unsubscribe([subscriber.filter])), clientScope)
      })

    const subscribe = (filter: string, subscribeOptions?: SubscribeOptions) =>
        Effect.gen(function* () {
          if (!isValidFilter(filter)) return yield* Effect.fail(new InvalidTopic({ topic: filter }))

          if (closed) return yield* Effect.fail(closed)

          const qos = subscribeOptions?.qos ?? 0
          const queue = yield* Queue.unbounded<Message, MqttError | Cause.Done>()
          const subscriber: Subscriber = { filter, queue }

          // Register before SUBSCRIBE goes out, so retained messages that race the SUBACK are kept.
          subscribers.add(filter, subscriber)
          live.add(subscriber)
          yield* Effect.addFinalizer(() => release(subscriber))

          const [code] = yield* session.subscribe([{ filter, qos }])

          if (code === 0x80 || code === undefined) return yield* Effect.fail(new SubscribeRejected({ filter }))

          const entry = confirmed.get(filter)

          confirmed.set(filter, { qos: entry === undefined || code > entry.qos ? code : entry.qos, streams: (entry?.streams ?? 0) + 1 })

          return Stream.fromQueue(queue)
        })

    return {
      clientId: config.connect.clientId,
      publish,
      subscribe,
      status: SubscriptionRef.get(status),
      statusChanges: SubscriptionRef.changes(status)
    }
  })
