// Replays the SessionForgotten bug path against the real client, with serve() as a broker that keeps no sessions
// (it answers every CONNECT with sessionPresent = 0, as a broker does for cleanSession = true).
//   1. you publish at QoS 2          2. the broker gets PUBLISH
//   3. the connection drops          4. the client reconnects [the broker forgot the session]
//   5. the broker gets PUBLISH again: a broker without the session must treat it as a new message
// Prints how many PUBLISH packets for the one message reached a fresh broker session, and exits 1 when that is 2.
// Usage: bun .agents/skills/verify-session-qos2/scripts/replay-duplicate.ts
import * as Effect from "effect/Effect"
import * as Schedule from "effect/Schedule"
import * as Stream from "effect/Stream"
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

  yield* client.publish("printers/p1s/command", "start", { qos: 2 })
  console.log(`publish succeeded; PUBLISH reached a fresh broker session ${publishes} times`)

  return publishes
})

const publishes = await Effect.runPromise(Effect.scoped(program))

process.exit(publishes > 1 ? 1 : 0)
