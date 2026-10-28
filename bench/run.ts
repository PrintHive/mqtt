/**
 * Compares @printhive/mqtt with mqtt.js end to end. Alternates the two libraries run by run, so both see the same
 * machine noise, and fails if any run loses a message.
 *
 * Needs a broker on MQTT_BENCH_PORT (default 18830), e.g.
 *   docker run -d --name mqtt-bench -p 18830:1883 -v $PWD/bench/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro eclipse-mosquitto:2
 */
import { spawnSync } from "node:child_process"

const port = process.env["MQTT_BENCH_PORT"] ?? "18830"
const ROUNDS = 5

const scenarios = [
  { qos: "0", size: "64", count: "200000" },
  { qos: "0", size: "1024", count: "100000" },
  { qos: "1", size: "1024", count: "50000" }
]

const runtimes = [
  ["bun", "bench/e2e.ts"],
  ["node", "bench/e2e.ts"]
]

interface Run {
  readonly received: number
  readonly bytesOk: boolean
  readonly rate: number
}

const once = (runtime: string, script: string, lib: string, s: (typeof scenarios)[number]): Run => {
  const out = spawnSync(runtime, [script, lib, s.qos, s.size, s.count, port], { encoding: "utf8", timeout: 300_000 })
  const line = out.stdout.trim().split("\n").at(-1) ?? ""

  if (out.status !== 0 || !line.startsWith("{")) throw new Error(`${runtime} ${lib} failed: ${out.stderr}`)

  // SAFETY: e2e.ts prints exactly this JSON object as its last line.
  const run = JSON.parse(line) as Run

  if (run.received !== Number(s.count) || !run.bytesOk) throw new Error(`${runtime} ${lib} lost messages: ${line}`)

  return run
}

const stats = (xs: Array<number>) => {
  const sorted = [...xs].sort((a, b) => a - b)

  return { median: sorted[Math.floor(sorted.length / 2)] ?? 0, min: sorted[0] ?? 0, max: sorted.at(-1) ?? 0 }
}

const k = (n: number) => `${(n / 1000).toFixed(0)}k`

console.log("| Runtime | QoS | Payload | @printhive/mqtt msg/s | mqtt.js msg/s | Ratio |")

console.log("| --- | --- | --- | --- | --- | --- |")

for (const [runtime = "bun", script = ""] of runtimes) {
  for (const s of scenarios) {
    const ours: Array<number> = []
    const theirs: Array<number> = []

    for (let round = 0; round < ROUNDS; round++) {
      const order = round % 2 === 0 ? ["ours", "mqttjs"] : ["mqttjs", "ours"]

      for (const lib of order) (lib === "ours" ? ours : theirs).push(once(runtime, script, lib, s).rate)
    }

    const a = stats(ours)
    const b = stats(theirs)

    console.log(
      `| ${runtime} | ${s.qos} | ${s.size} B | ${k(a.median)} (${k(a.min)} to ${k(a.max)}) | ${k(b.median)} (${k(b.min)} to ${k(b.max)}) | ${(a.median / b.median).toFixed(2)}x |`
    )
  }
}
