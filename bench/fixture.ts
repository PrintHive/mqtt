import { Buffer } from "node:buffer"

export const TOPIC = "device/01S00A000000000/report"

/** A payload of `size` bytes shaped like a Bambu Lab printer report. */
export const payload = (size: number): Buffer => {
  const base = JSON.stringify({ print: { command: "push_status", sequence_id: "20000", nozzle_temper: 219.8, bed_temper: 55, mc_percent: 42 } })

  return Buffer.from(base.padEnd(size, " ").slice(0, size))
}
