import type { Buffer } from "node:buffer"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import { encode } from "./codec.ts"
import { ClientClosed, ProtocolError } from "./errors.ts"
import type { MqttError } from "./errors.ts"
import * as P from "./packet.ts"
import type { Packet, Publish, PublishAcknowledged, QoS, SubackCode, Subscription } from "./packet.ts"

/** The open connection a session writes to. */
export interface Link {
  /** Returns false when the socket buffer is full and the caller should wait for drain. */
  write(buf: Buffer): boolean
  pong(): void
  fail(error: MqttError): void
}

/**
 * Where an outgoing QoS 1 or 2 message is in its handshake (section 4.3).
 * `Unacked` waits for PUBACK (QoS 1) or PUBREC (QoS 2). `Released` has sent PUBREL and waits for PUBCOMP.
 */
type Stage = "Unacked" | "Released"

interface InFlight {
  readonly packet: PublishAcknowledged
  stage: Stage
  readonly done: Deferred.Deferred<void, MqttError>
}

type Request =
  | { readonly kind: "subscribe"; readonly packet: P.Subscribe; readonly done: Deferred.Deferred<ReadonlyArray<SubackCode>, MqttError> }
  | { readonly kind: "unsubscribe"; readonly packet: P.Unsubscribe; readonly done: Deferred.Deferred<void, MqttError> }

/**
 * MQTT session state (section 4.1): packet identifiers, unfinished QoS 1 and 2 handshakes in both directions, and
 * unanswered SUBSCRIBE and UNSUBSCRIBE requests.
 *
 * The state outlives a connection. When the next connection attaches, every unfinished exchange is sent again.
 */
export class Session {
  private readonly deliver: (message: Publish) => void
  private readonly inFlight = new Map<number, InFlight>()
  private readonly requests = new Map<number, Request>()
  /** Incoming QoS 2 messages delivered and waiting for PUBREL, so a resent PUBLISH is not delivered twice. */
  private readonly received = new Set<number>()
  private nextId = 1
  private link: Link | undefined = undefined

  constructor(deliver: (message: Publish) => void) {
    this.deliver = deliver
  }

  get attached(): boolean {
    return this.link !== undefined
  }

  /** Sends a QoS 0 message on the current link. Returns false when the socket wants a drain first. */
  publishAtMostOnce(topic: string, payload: Uint8Array, retain: boolean): boolean {
    return this.link?.write(encode(P.publishAtMostOnce(topic, payload, retain))) ?? false
  }

  /** Starts a QoS 1 or 2 handshake. It completes on PUBACK or PUBCOMP, across reconnects if needed. */
  publishAcknowledged(topic: string, payload: Uint8Array, qos: 1 | 2, retain: boolean): Effect.Effect<void, MqttError> {
    return this.withPacketId((packetId) => {
      const done = Deferred.makeUnsafe<void, MqttError>()
      const packet = P.publishAcknowledged(topic, payload, qos, packetId, retain, false)

      this.inFlight.set(packetId, { packet, stage: "Unacked", done })
      this.link?.write(encode(packet))

      return Deferred.await(done)
    })
  }

  subscribe(subscriptions: ReadonlyArray<Subscription>): Effect.Effect<ReadonlyArray<SubackCode>, MqttError> {
    return this.withPacketId((packetId) => {
      const done = Deferred.makeUnsafe<ReadonlyArray<SubackCode>, MqttError>()
      const packet = P.subscribe(packetId, subscriptions)

      this.requests.set(packet.packetId, { kind: "subscribe", packet, done })
      this.link?.write(encode(packet))

      return Deferred.await(done)
    })
  }

  unsubscribe(filters: ReadonlyArray<string>): Effect.Effect<void, MqttError> {
    return this.withPacketId((packetId) => {
      const done = Deferred.makeUnsafe<void, MqttError>()
      const packet = P.unsubscribe(packetId, filters)

      this.requests.set(packet.packetId, { kind: "unsubscribe", packet, done })
      this.link?.write(encode(packet))

      return Deferred.await(done)
    })
  }

  /**
   * Attaches a new connection. With `sessionPresent` false the broker forgot the session, so incoming QoS 2 state is
   * dropped; outgoing handshakes are still resent, since the caller is still waiting on them (section 4.4).
   */
  attach(link: Link, sessionPresent: boolean): void {
    this.link = link

    if (!sessionPresent) this.received.clear()

    for (const [id, f] of this.inFlight) {
      link.write(encode(f.stage === "Released" ? P.pubrel(id) : { ...f.packet, dup: true }))
    }

    for (const r of this.requests.values()) link.write(encode(r.packet))
  }

  detach(): void {
    this.link = undefined
  }

  /** Fails every waiting caller. The session is unusable afterwards. */
  close(error: ClientClosed): void {
    this.link = undefined

    for (const f of this.inFlight.values()) Deferred.doneUnsafe(f.done, Effect.fail(error))

    for (const r of this.requests.values()) {
      if (r.kind === "subscribe") Deferred.doneUnsafe(r.done, Effect.fail(error))
      else Deferred.doneUnsafe(r.done, Effect.fail(error))
    }

    this.inFlight.clear()
    this.requests.clear()
  }

  /** Handles one packet from the broker after CONNACK. */
  receive(packet: Packet): void {
    const link = this.link

    if (link === undefined) return

    switch (packet._tag) {
      case "Publish":
        return this.incoming(link, packet)
      case "Pubrel":
        this.received.delete(packet.packetId)
        link.write(encode(P.pubcomp(packet.packetId)))

        return
      case "Puback":
        return this.finish(packet.packetId, 1)
      case "Pubrec": {
        const f = this.inFlight.get(packet.packetId)

        if (f !== undefined && f.packet.qos === 2) f.stage = "Released"

        // Answer even an unknown PUBREC, so a broker resending after our restart can finish the exchange.
        link.write(encode(P.pubrel(packet.packetId)))

        return
      }

      case "Pubcomp":
        return this.finish(packet.packetId, 2)
      case "Suback": {
        const r = this.requests.get(packet.packetId)

        if (r?.kind !== "subscribe") return

        this.requests.delete(packet.packetId)
        Deferred.doneUnsafe(r.done, Effect.succeed(packet.codes))

        return
      }

      case "Unsuback": {
        const r = this.requests.get(packet.packetId)

        if (r?.kind !== "unsubscribe") return

        this.requests.delete(packet.packetId)
        Deferred.doneUnsafe(r.done, Effect.void)

        return
      }

      case "Pingresp":
        return link.pong()
      case "Connect":
      case "Connack":
      case "Subscribe":
      case "Unsubscribe":
      case "Pingreq":
      case "Disconnect":
        return link.fail(new ProtocolError({ message: `the broker sent ${packet._tag.toUpperCase()}` }))
    }
  }

  private incoming(link: Link, packet: Publish): void {
    if (packet.qos === 0) return this.deliver(packet)

    if (packet.qos === 1) {
      this.deliver(packet)
      link.write(encode(P.puback(packet.packetId)))

      return
    }

    if (!this.received.has(packet.packetId)) {
      this.received.add(packet.packetId)
      this.deliver(packet)
    }

    link.write(encode(P.pubrec(packet.packetId)))
  }

  private finish(packetId: number, qos: QoS): void {
    const f = this.inFlight.get(packetId)

    if (f === undefined || f.packet.qos !== qos) return

    this.inFlight.delete(packetId)
    Deferred.doneUnsafe(f.done, Effect.void)
  }

  /** Runs `f` with the next free packet identifier. Identifiers are free once their exchange completes (section 2.3.1). */
  private withPacketId<A>(f: (packetId: number) => Effect.Effect<A, MqttError>): Effect.Effect<A, MqttError> {
    return Effect.suspend(() => {
      const packetId = this.allocate()

      return packetId === undefined ? Effect.fail(new ProtocolError({ message: "all 65535 packet identifiers are in use" })) : f(packetId)
    })
  }

  private allocate(): number | undefined {
    for (let tries = 0; tries < 65_535; tries++) {
      const id = this.nextId

      this.nextId = id === 65_535 ? 1 : id + 1

      if (!this.inFlight.has(id) && !this.requests.has(id)) return id
    }

    return undefined
  }
}
