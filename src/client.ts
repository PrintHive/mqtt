import { Buffer } from "node:buffer"
import { randomBytes } from "node:crypto"
import * as net from "node:net"
import * as tls from "node:tls"
import * as Cause from "effect/Cause"
import * as Data from "effect/Data"
import * as Deferred from "effect/Deferred"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Latch from "effect/Latch"
import * as Match from "effect/Match"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schedule from "effect/Schedule"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import * as SubscriptionRef from "effect/SubscriptionRef"
import { Decoder, encode, MalformedPacket } from "./codec.ts"
import { ClientClosed, ConnectionError, ConnectionRefused, InvalidOption, InvalidTopic, ProtocolError, refused, SubscribeRejected } from "./errors.ts"
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

export interface CloseOptions {
  /** Destroys the socket without sending DISCONNECT. Defaults to false. */
  readonly force?: boolean | undefined
}

export interface PublishOptions {
  /** Defaults to 0. QoS 1 and 2 publishes complete when the broker acknowledges them. */
  readonly qos?: QoS | undefined
  readonly retain?: boolean | undefined
}

export interface SubscribeOptions {
  /** The highest QoS to receive at. Defaults to 0. */
  readonly qos?: QoS | undefined
  /**
   * Messages this subscription holds before the client stops reading from the broker. Defaults to 1024.
   *
   * While any subscription is full, the client reads nothing, so TCP pushes back on the broker; reading resumes once
   * every full subscription has drained to half. Messages already read when a buffer fills are kept, so a buffer can
   * exceed this by one socket read's worth.
   */
  readonly bufferSize?: number | undefined
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
  /**
   * Closes the client before its scope does, and completes once the socket is gone. It stops reconnecting, then sends
   * DISCONNECT and waits up to `connectTimeout` for the broker to hang up. With `force`, it destroys the socket at
   * once without DISCONNECT, so the broker publishes the will. Calling it again does nothing.
   */
  readonly close: (options?: CloseOptions) => Effect.Effect<void>
}

interface Subscriber {
  readonly filter: string
  readonly qos: QoS
  readonly capacity: number
  readonly queue: Queue.Queue<Message, MqttError | Cause.Done>
}

const DEFAULT_BUFFER_SIZE = 1024

interface Config {
  readonly host: string
  readonly port: number
  readonly tls: tls.ConnectionOptions | undefined
  readonly connect: P.Connect
  readonly keepAliveMs: number
  readonly connectTimeout: Duration.Input
  readonly reconnect: Schedule.Schedule<unknown, MqttError> | false
}

const crashed = (cause: Cause.Cause<MqttError>) => new ClientClosed({ message: `the client crashed: ${Cause.pretty(cause)}` })

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
  /** When the unanswered PINGREQ went out, or undefined when none is outstanding. */
  private pingSentAt: number | undefined = undefined
  private reading = true
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
    this.pingSentAt = undefined
  }

  /** Stops or resumes reading from the socket, which is how the client pushes back on the broker. */
  setReading(reading: boolean): void {
    if (reading === this.reading) return

    this.reading = reading
    // A PINGRESP may be sitting unread in the socket, so the wait for it starts over on resume.
    this.pingSentAt = undefined

    if (reading) this.socket.resume()
    else this.socket.pause()
  }

  fail(error: MqttError): void {
    this.error ??= error
    this.socket.destroy()
  }

  /**
   * Keeps the gap between outgoing packets under one keep-alive period (section 3.1.2.10) and drops the connection
   * when a PINGREQ gets no PINGRESP within one period. Checking every quarter period and pinging after three quarters
   * of quiet sends the PINGREQ before the full period passes.
   */
  startKeepAlive(periodMs: number): void {
    if (periodMs === 0) return

    this.timer = setInterval(() => {
      const now = Date.now()

      // While paused, keep pinging so the broker keeps the connection, but a PINGRESP cannot be read yet.
      if (!this.reading) {
        if (now - this.lastWrite >= (periodMs * 3) / 4) this.write(encode(P.pingreq))

        return
      }

      if (this.pingSentAt !== undefined) {
        if (now - this.pingSentAt >= periodMs) this.fail(new ConnectionError({ message: "no PINGRESP within the keep-alive period" }))

        return
      }

      if (now - this.lastWrite >= (periodMs * 3) / 4) {
        this.pingSentAt = now
        this.write(encode(P.pingreq))
      }
    }, periodMs / 4)
    this.timer.unref()
  }

  /**
   * Closes the socket and completes once it is gone. A graceful close sends DISCONNECT and waits up to `graceMs` for
   * the broker to hang up before destroying the socket. A forced close, or one before CONNACK, destroys it at once.
   */
  close(force: boolean, graceMs: Duration.Input): Effect.Effect<void> {
    return Effect.suspend(() => {
      clearInterval(this.timer)

      const gone = Effect.asVoid(Deferred.await(this.lost))

      if (this.socket.destroyed) return gone

      if (force || !Deferred.isDoneUnsafe(this.connack)) {
        this.socket.destroy()

        return gone
      }

      this.socket.end(encode(P.disconnect))

      return gone.pipe(
        Effect.timeoutOrElse({
          duration: graceMs,
          orElse: () => Effect.andThen(Effect.sync(() => this.socket.destroy()), gone)
        })
      )
    })
  }
}

/** SNI must be a host name; RFC 6066 forbids an IP address, and `tls.connect` throws on one. */
const serverName = (config: Config, secure: tls.ConnectionOptions) =>
  secure.servername ?? (net.isIP(config.host) === 0 ? config.host : undefined)

const createSocket = (config: Config): net.Socket =>
  config.tls
    ? tls.connect({ ...config.tls, host: config.host, port: config.port, servername: serverName(config, config.tls) })
    : net.connect({ host: config.host, port: config.port })

const openSocket = (config: Config) =>
  Effect.callback<net.Socket, ConnectionError>((resume, signal) => {
    let socket: net.Socket

    try {
      socket = createSocket(config)
    } catch (cause) {
      return resume(Effect.fail(new ConnectionError({ message: cause instanceof Error ? cause.message : String(cause) })))
    }

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
const establish = (config: Config, session: Session, onDrain: () => void, forced: () => boolean) =>
  Effect.gen(function* () {
    const socket = yield* openSocket(config)
    const connection = new Connection(socket, session, onDrain)

    yield* Effect.addFinalizer(() => connection.close(forced(), config.connectTimeout))
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
    /** Live subscribers by filter. The broker holds one subscription per filter, at the highest QoS any of them asked for. */
    const filters = new Map<string, Set<Subscriber>>()

    const requested = (filter: string): QoS => {
      let qos: QoS = 0

      for (const s of filters.get(filter) ?? []) if (s.qos > qos) qos = s.qos

      return qos
    }

    /** Subscribers at capacity. While any are, the client reads nothing from the broker. */
    const full = new Set<Subscriber>()
    let current: Connection | undefined = undefined

    const deliver = (s: Subscriber, message: Message) => {
      Queue.offerUnsafe(s.queue, message)

      if (full.has(s) || Queue.sizeUnsafe(s.queue) < s.capacity) return

      full.add(s)
      current?.setReading(false)
    }

    /** Called after a subscriber's stream takes messages, and when it leaves. */
    const drained = (s: Subscriber) => {
      if (!full.has(s) || Queue.sizeUnsafe(s.queue) > s.capacity / 2) return

      full.delete(s)

      if (full.size === 0) current?.setReading(true)
    }

    const session = new Session((message) => subscribers.match(message.topic, (s) => deliver(s, message)))
    const writable = yield* Latch.make(false)
    const status = yield* SubscriptionRef.make<ConnectionStatus>(ConnectionStatus.Connecting())
    const ready = yield* Deferred.make<void, MqttError>()
    let closed: ClientClosed | undefined = undefined
    let force = false

    const failRejected = (rejected: ReadonlyArray<string>, codes: ReadonlyArray<P.SubackCode>) =>
      Effect.sync(() => {
        rejected.forEach((filter, i) => {
          if (codes[i] !== 0x80) return

          for (const s of filters.get(filter) ?? []) Queue.failCauseUnsafe(s.queue, Cause.fail(new SubscribeRejected({ filter })))
        })
      })

    const run = Effect.gen(function* () {
      const { connection, sessionPresent } = yield* establish(config, session, () => writable.openUnsafe(), () => force)

      current = connection
      connection.setReading(full.size === 0)
      session.attach(connection, sessionPresent)

      if (!sessionPresent && filters.size > 0) {
        const active = [...filters.keys()]
        const resubscribe = session.subscribe(active.map((filter) => ({ filter, qos: requested(filter) })))

        // Starting immediately writes the SUBSCRIBE now, ahead of any publish waiting for the connection.
        yield* Effect.forkScoped(Effect.flatMap(resubscribe, (codes) => failRejected(active, codes)), { startImmediately: true })
      }

      writable.openUnsafe()
      yield* SubscriptionRef.set(status, ConnectionStatus.Connected({ sessionPresent }))
      yield* Deferred.succeed(ready, undefined)

      const error = yield* Deferred.await(connection.lost)

      current = undefined
      writable.closeUnsafe()
      session.detach()

      return error
    }).pipe(Effect.scoped)

    const supervise = Effect.gen(function* () {
      let lost = yield* run

      while (config.reconnect !== false) {
        const dropped = lost
        let attempt = 0
        let waited = false

        // The drop counts as the first failure, so the schedule's first delay comes before the first reconnect.
        const attemptAfterDelay = Effect.suspend(() => {
          if (waited) return run

          waited = true

          return Effect.fail(dropped)
        })

        lost = yield* attemptAfterDelay.pipe(
          Effect.tapError((error) => SubscriptionRef.set(status, ConnectionStatus.Reconnecting({ attempt: ++attempt, error }))),
          Effect.retry({ schedule: config.reconnect, while: (error) => error === dropped || transient(error) })
        )
      }

      return yield* Effect.fail(lost)
    })

    const shutdown = (error: MqttError) =>
      Effect.gen(function* () {
        closed = error instanceof ClientClosed ? error : new ClientClosed({ message: `the client closed after ${error._tag}: ${error.message}` })
        session.close(closed)
        writable.openUnsafe()

        for (const set of filters.values()) for (const s of set) Queue.failCauseUnsafe(s.queue, Cause.fail(error))

        yield* SubscriptionRef.set(status, ConnectionStatus.Closed({ error }))
        yield* Deferred.fail(ready, error)
      })

    const supervisor = yield* supervise.pipe(
      // A defect still closes the client with its cause, so `connect` and the streams report it instead of hanging.
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause) ? Effect.failCause(cause) : shutdown(Cause.findErrorOption(cause).pipe(Option.getOrElse(() => crashed(cause))))
      ),
      Effect.ensuring(Effect.suspend(() => (closed ? Effect.void : shutdown(new ClientClosed({ message: "the client closed" }))))),
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

    /** Unregisters a subscriber. Safe to call again for the same subscriber. */
    const release = (subscriber: Subscriber) =>
      Effect.suspend(() => {
        subscribers.remove(subscriber.filter, subscriber)

        if (full.delete(subscriber) && full.size === 0) current?.setReading(true)

        const set = filters.get(subscriber.filter)

        set?.delete(subscriber)

        if (set === undefined || set.size > 0 || closed) return Effect.void

        filters.delete(subscriber.filter)

        return Effect.forkIn(Effect.ignore(session.unsubscribe([subscriber.filter])), clientScope)
      })

    const subscribe = (filter: string, subscribeOptions?: SubscribeOptions) =>
        Effect.gen(function* () {
          if (!isValidFilter(filter)) return yield* Effect.fail(new InvalidTopic({ topic: filter }))

          if (closed) return yield* Effect.fail(closed)

          const qos = subscribeOptions?.qos ?? 0
          const capacity = subscribeOptions?.bufferSize ?? DEFAULT_BUFFER_SIZE

          if (!Number.isInteger(capacity) || capacity < 1) {
            return yield* Effect.fail(new InvalidOption({ option: "bufferSize", message: `bufferSize must be a whole number of at least 1, got ${capacity}` }))
          }

          // Unbounded underneath: the bound is enforced by pausing the socket, so a message already read is never dropped.
          const queue = yield* Queue.unbounded<Message, MqttError | Cause.Done>()
          const subscriber: Subscriber = { filter, qos, capacity, queue }
          const set = filters.get(filter) ?? new Set()

          // Register before SUBSCRIBE goes out, so retained messages that race the SUBACK are kept.
          filters.set(filter, set.add(subscriber))
          subscribers.add(filter, subscriber)
          yield* Effect.addFinalizer(() => release(subscriber))

          // SUBSCRIBE replaces the broker's subscription on an identical filter, QoS included (section 3.8.4).
          // A failed subscriber leaves now rather than at scope close, so a retry at a lower QoS is not held to this one's.
          const [code] = yield* Effect.onError(session.subscribe([{ filter, qos: requested(filter) }]), () => release(subscriber))

          if (code === 0x80 || code === undefined) {
            yield* release(subscriber)

            return yield* Effect.fail(new SubscribeRejected({ filter }))
          }

          return Stream.fromQueue(queue).pipe(
            Stream.mapArray((messages) => {
              drained(subscriber)

              return messages
            })
          )
        })

    return {
      clientId: config.connect.clientId,
      publish,
      subscribe,
      status: SubscriptionRef.get(status),
      statusChanges: SubscriptionRef.changes(status),
      close: (closeOptions?: CloseOptions) =>
        Effect.suspend(() => {
          force ||= closeOptions?.force ?? false

          return Fiber.interrupt(supervisor)
        })
    }
  })
