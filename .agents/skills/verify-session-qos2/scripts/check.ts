// Runs TLC on formal/session. `check` checks both configs; `mutants` breaks the model one code change at a time and
// requires TLC to name the rule each break should violate.
// Needs TLA2TOOLS (path to tla2tools.jar 1.7.4) and JAVA (Java 11 or later) in the environment.
// Usage: bun .agents/skills/verify-session-qos2/scripts/check.ts check|mutants
import { spawnSync } from "node:child_process"
import { copyFileSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

const dir = new URL("../../../../formal/session/", import.meta.url).pathname
const model = readFileSync(join(dir, "QoS2Session.tla"), "utf8")
const jar = process.env["TLA2TOOLS"]
const java = process.env["JAVA"] ?? "java"

if (jar === undefined) {
  console.error("set TLA2TOOLS to the path of tla2tools.jar 1.7.4, and JAVA to a Java 11+ binary")
  process.exit(2)
}

/** Runs TLC and returns the first broken rule, "deadlock", or undefined when every rule holds. */
const tlc = (text: string, config: string): string | undefined => {
  const work = mkdtempSync(join(tmpdir(), "verify-session-qos2-"))

  try {
    writeFileSync(join(work, "QoS2Session.tla"), text)
    copyFileSync(join(dir, config), join(work, config))
    const run = spawnSync(java, ["-XX:+UseParallelGC", "-cp", jar, "tlc2.TLC", "-workers", "1", "-metadir", join(work, "states"), "-config", config, "QoS2Session.tla"], { cwd: work, encoding: "utf8" })
    const out = run.stdout + run.stderr

    if (/Model checking completed. No error has been found/.test(out)) return undefined

    const broken = /Invariant (\w+) is violated/.exec(out)?.[1] ?? (/Deadlock reached/.test(out) ? "deadlock" : undefined)

    if (broken === undefined) throw new Error(`TLC did not finish:\n${out}`)

    return broken
  } finally {
    rmSync(work, { recursive: true, force: true })
  }
}

// Each mutant is a change to src/session.ts written as the same change to the model, the rule it must break, and the
// config that shows it (SessionKept unless named).
const mutants: ReadonlyArray<readonly [string, string, string, string, string?]> = [
  [
    "incoming delivers a resent PUBLISH again (session.ts 247: no received check)",
    "    /\\ IF received\n          THEN UNCHANGED <<received, delivered>>",
    "    /\\ IF FALSE\n          THEN UNCHANGED <<received, delivered>>",
    "BrokersMessageAtMostOnce"
  ],
  [
    "attach clears received even with sessionPresent (session.ts 133)",
    "           /\\ received' = (received /\\ present)",
    "           /\\ received' = FALSE",
    "BrokersMessageAtMostOnce"
  ],
  [
    "attach resends PUBLISH for a Released entry instead of PUBREL (session.ts 148)",
    "                              [] out = \"Released\" -> << \"PUBREL\" >>",
    "                              [] out = \"Released\" -> << \"PUBLISH\" >>",
    "YourMessageAtMostOnce"
  ],
  [
    "attach does not resend unfinished exchanges (session.ts 147-150)",
    "           /\\ toBroker' = clientResend",
    "           /\\ toBroker' = << >>",
    "deadlock"
  ],
  [
    "an interrupt removes a written entry too (session.ts 84)",
    "    /\\ out = \"Unacked\" /\\ ~written\n    /\\ out' = \"dropped\"",
    "    /\\ out = \"Unacked\"\n    /\\ out' = \"dropped\"",
    "GivenUpMeansNeverSent"
  ],
  [
    "publish resolves at once while offline",
    "    /\\ out' = \"Unacked\"\n    /\\ written' = up",
    "    /\\ out' = IF up THEN \"Unacked\" ELSE \"done\"\n    /\\ written' = up",
    "DoneMeansDelivered"
  ],
  [
    "incoming answers PUBREC without delivering (session.ts deliver call removed)",
    "               /\\ delivered' = delivered + 1",
    "               /\\ delivered' = delivered",
    "BrokerDoneMeansReceived"
  ],
  [
    "attach resends a written publish to a broker that lost the session (session.ts 135-142 removed, the behaviour before 0.6.0)",
    "            lostOut == ~present /\\ written /\\ out \\in {\"Unacked\", \"Released\"}",
    "            lostOut == FALSE",
    "YourMessageAtMostOnce",
    "SessionForgotten.cfg"
  ],
  [
    "attach fails an unwritten publish too on a fresh session (session.ts 136)",
    "            lostOut == ~present /\\ written /\\ out \\in {\"Unacked\", \"Released\"}",
    "            lostOut == ~present /\\ out \\in {\"Unacked\", \"Released\"}",
    "UnknownOnlyWhenWritten",
    "SessionForgotten.cfg"
  ]
]

const mode = process.argv[2]

if (mode === "check") {
  const kept = tlc(model, "SessionKept.cfg")
  const forgotten = tlc(model, "SessionForgotten.cfg")

  console.log(`SessionKept: ${kept ?? "every rule holds"}`)
  console.log(`SessionForgotten: ${forgotten ?? "every rule holds"}`)
  process.exit(kept === undefined && forgotten === undefined ? 0 : 1)
}

if (mode === "mutants") {
  const baseline = tlc(model, "SessionKept.cfg") ?? tlc(model, "SessionForgotten.cfg")

  if (baseline !== undefined) {
    console.log(`REFUSED: the model breaks ${baseline} before any mutant`)
    process.exit(1)
  }

  let missed = 0

  for (const [name, from, to, rule, config = "SessionKept.cfg"] of mutants) {
    const at = model.indexOf(from)

    if (at === -1 || model.indexOf(from, at + 1) !== -1) {
      console.log(`STALE   ${name}: its anchor no longer occurs exactly once`)
      missed++
      continue
    }

    const broken = tlc(model.replace(from, to), config)
    const caught = broken === rule

    if (!caught) missed++
    console.log(`${caught ? "CAUGHT" : "MISSED"}  ${name}: ${broken ?? "nothing broke"}${caught ? "" : `, expected ${rule}`}`)
  }

  process.exit(missed === 0 ? 0 : 1)
}

console.error("usage: check.ts check|mutants")
process.exit(2)
