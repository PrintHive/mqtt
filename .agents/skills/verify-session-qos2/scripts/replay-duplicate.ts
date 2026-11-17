// Replays the SessionForgotten bug path against the real client, with serve() as a broker that keeps no sessions
// (it answers every CONNECT with sessionPresent = 0, as a broker does for cleanSession = true).
//   1. you publish at QoS 2          2. the broker gets PUBLISH
//   3. the connection drops          4. the client reconnects [the broker forgot the session]
//   5. before 0.6.0 the client resent the PUBLISH, which a broker without the session delivers again; now the publish
//      fails with DeliveryUnknown and nothing is resent
// Prints what happened, and exits 1 when the PUBLISH reached the broker twice or the publish did not fail.
// Usage: bun .agents/skills/verify-session-qos2/scripts/replay-duplicate.ts
import * as Effect from "effect/Effect"
import * as Schedule from "effect/Schedule"
import * as Stream from "effect/Stream"
import * as Exit from "effect/Exit"
import { connect } from "../../../../src/index.ts"
import { serve } from "../../../../src/server.ts"

const program = Effect.gen(function* () {
  const server = yield* serve({ host: "127.0.0.1", port: 0, authenticate: () => Effect.succeed(0) })
  let connection = 0
  let publishes = 0

  yield* server.sessions.pipe(
    Stream.runForEach((session) =>
      Effect.gen(function* () {
        const n = ++connection

        // serve() answers PUBREC and PUBCOMP itself. The first connection stays silent, so it drops before PUBREC.
        if (n === 1) yield* session.mute(true)

        yield* session.inbound.pipe(
          Stream.runForEach((packet) =>
            Effect.gen(function* () {
              if (packet._tag !== "Publish") return

              publishes++
              console.log(`connection ${n}: PUBLISH dup=${packet.dup} qos=${packet.qos}`)

              // Step 3: the connection drops after the broker has the PUBLISH, before PUBREC reaches the client.
              if (n === 1) yield* session.close("destroy")
            })
          ),
          Effect.ignore
        )
      }).pipe(Effect.forkScoped)
    ),
    Effect.forkScoped
  )

  const client = yield* connect({
    host: "127.0.0.1",
    port: server.address.port,
    reconnect: Schedule.spaced("50 millis")
  })

  const result = yield* Effect.exit(client.publish("printers/p1s/command", "start", { qos: 2 }))
  const failure = Exit.isFailure(result) ? Exit.findErrorOption(result) : undefined
  const unknown = failure !== undefined && failure._tag === "Some" && failure.value._tag === "DeliveryUnknown"

  // Give a resend, if any, time to arrive.
  yield* Effect.sleep("200 millis")
  console.log(`publish ${unknown ? "failed with DeliveryUnknown" : Exit.isSuccess(result) ? "succeeded" : "failed otherwise"}; PUBLISH reached a broker ${publishes} times`)

  return publishes === 1 && unknown
})

const fixed = await Effect.runPromise(Effect.scoped(program))

process.exit(fixed ? 0 : 1)
