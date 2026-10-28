/**
 * Prints live status reports from a Bambu Lab printer in LAN mode.
 *
 *   PRINTER_HOST=192.0.2.10 PRINTER_SERIAL=... PRINTER_ACCESS_CODE=... bun examples/bambu-report.ts
 */
import { Buffer } from "node:buffer"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import { connect } from "../src/index.ts"

const env = (name: string) => {
  const value = process.env[name]

  if (value === undefined) throw new Error(`set ${name}`)

  return value
}

const serial = env("PRINTER_SERIAL")

const program = Effect.gen(function* () {
  const printer = yield* connect({
    host: env("PRINTER_HOST"),
    // Printers present a certificate signed by Bambu Lab's own CA.
    tls: { rejectUnauthorized: false },
    username: "bblp",
    password: env("PRINTER_ACCESS_CODE"),
    keepAlive: "20 seconds"
  })

  const reports = yield* printer.subscribe(`device/${serial}/report`)

  // Ask for a full report. The subscription is already confirmed, so the answer cannot be missed.
  yield* printer.publish(`device/${serial}/request`, JSON.stringify({ pushing: { sequence_id: "0", command: "pushall" } }))

  yield* reports.pipe(
    Stream.take(5),
    Stream.runForEach((m) => Effect.sync(() => console.log(Buffer.from(m.payload).toString().slice(0, 200))))
  )
})

Effect.runPromise(Effect.scoped(program)).catch((error: Error) => {
  console.error(error)
  process.exitCode = 1
})
