import { Buffer } from "node:buffer"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import { describe, expect, it } from "vitest"
import { Decoder } from "../src/codec.ts"
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

describe("Session", () => {
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
