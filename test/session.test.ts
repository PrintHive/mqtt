import { Buffer } from "node:buffer"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Predicate from "effect/Predicate"
import { describe, expect, it } from "vitest"
import { Decoder } from "../src/codec.ts"
import { DeliveryUnknown } from "../src/errors.ts"
import * as P from "../src/packet.ts"
import type { Packet } from "../src/packet.ts"
import { Session } from "../src/session.ts"
import type { Link } from "../src/session.ts"

/** A link that records what the session writes to it. */
const recording = (accepting: boolean) => {
  const written: Array<Packet> = []
  const decoder = new Decoder((p) => written.push(p))
  const link: Link = { accepting, write: (buf) => (decoder.push(buf), true), pong: () => {}, fail: () => {} }

  return { link, written }
}

/** A written packet's tag, and for a PUBLISH whether DUP was set. */
const withDup = (p: Packet) => (Predicate.isTagged(p, "Publish") ? [p._tag, "dup" in p && p.dup] : [p._tag])

describe("Session", () => {
  it.each([1, 2] as const)("fails a written QoS %i publish with DeliveryUnknown when the next connection has no session", (qos) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const session = new Session(() => {})
        const first = recording(true)

        session.attach(first.link, false)

        const publish = yield* Effect.forkChild(session.publishAcknowledged("jobs", Buffer.from("print"), qos, false), { startImmediately: true })

        session.detach()

        const next = recording(true)

        session.attach(next.link, false)

        const error = yield* Effect.flip(Fiber.join(publish))

        expect(error).toBeInstanceOf(DeliveryUnknown)
        expect([first.written.map((p) => p._tag), next.written]).toEqual([["Publish"], []])
      })
    ))

  it("fails a QoS 2 publish that got PUBREC with DeliveryUnknown, instead of sending PUBREL, when the next connection has no session", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const session = new Session(() => {})
        const first = recording(true)

        session.attach(first.link, false)

        const publish = yield* Effect.forkChild(session.publishAcknowledged("jobs", Buffer.from("print"), 2, false), { startImmediately: true })

        session.receive(P.pubrec(1))
        session.detach()

        const next = recording(true)

        session.attach(next.link, false)

        expect(yield* Effect.flip(Fiber.join(publish))).toBeInstanceOf(DeliveryUnknown)
        expect(next.written).toEqual([])
      })
    ))

  it("resends a written publish with DUP set when the broker kept the session", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const session = new Session(() => {})
        const first = recording(true)

        session.attach(first.link, false)
        yield* Effect.forkChild(session.publishAcknowledged("jobs", Buffer.from("print"), 2, false), { startImmediately: true })
        session.detach()

        const next = recording(true)

        session.attach(next.link, true)
        expect(next.written.map(withDup)).toEqual([["Publish", true]])
      })
    ))

  it("sends a publish made while disconnected for the first time on a connection with no session", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const session = new Session(() => {})

        yield* Effect.forkChild(session.publishAcknowledged("jobs", Buffer.from("print"), 2, false), { startImmediately: true })

        const next = recording(true)

        session.attach(next.link, false)
        expect(next.written.map(withDup)).toEqual([["Publish", false]])
      })
    ))

  it("resends an unanswered SUBSCRIBE on a connection with no session", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const session = new Session(() => {})
        const first = recording(true)

        session.attach(first.link, false)
        yield* Effect.forkChild(session.subscribe([{ filter: "jobs", qos: 1 }]), { startImmediately: true })
        session.detach()

        const next = recording(true)

        session.attach(next.link, false)
        expect(next.written.map((p) => p._tag)).toEqual(["Subscribe"])
      })
    ))

  it("never resends an interrupted QoS 1 publish that went to an already-destroyed socket", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const session = new Session(() => {})
        const dead = recording(false)

        session.attach(dead.link, false)

        const publish = yield* Effect.forkChild(session.publishAcknowledged("jobs", Buffer.from("print"), 1, false), { startImmediately: true })

        yield* Fiber.interrupt(publish)
        session.detach()

        const next = recording(true)

        session.attach(next.link, true)
        expect([dead.written, next.written]).toEqual([[], []])
      })
    ))

  it("does not count a replay onto an already-destroyed link as written", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const session = new Session(() => {})
        const publish = yield* Effect.forkChild(session.publishAcknowledged("jobs", Buffer.from("print"), 1, false), { startImmediately: true })
        const dead = recording(false)

        session.attach(dead.link, false)
        session.detach()
        yield* Fiber.interrupt(publish)

        const next = recording(true)

        session.attach(next.link, true)
        expect([dead.written, next.written]).toEqual([[], []])
      })
    ))
})
