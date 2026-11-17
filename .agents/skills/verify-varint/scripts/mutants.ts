// Breaks the generated Dafny one way at a time, in a temporary copy, and requires Dafny to reject every break.
// The committed files are never touched. Usage: bun .agents/skills/verify-varint/scripts/mutants.ts
import { spawnSync } from "node:child_process"
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

const proof = readFileSync(new URL("../../../../src/varint.verify.dfy", import.meta.url), "utf8")

const verify = (text: string): boolean => {
  const dir = mkdtempSync(join(tmpdir(), "verify-varint-"))

  try {
    writeFileSync(join(dir, "m.dfy"), text)
    const run = spawnSync("dafny", ["verify", "--standard-libraries", "--verification-time-limit=60", join(dir, "m.dfy")], { encoding: "utf8" })

    if (run.error !== undefined) throw run.error

    return run.status === 0
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
}

const mutants: ReadonlyArray<readonly [string, string, string]> = [
  ["packetSize takes 128 as a last byte", "    if (b < 128) {\n      if i > 1", "    if (b <= 128) {\n      if i > 1"],
  ["packetSize leaves the first byte out of the size", "      return ((1 + i) + n);", "      return (i + n);"],
  ["packetSize reads a fifth length byte", "  while (i <= 4)\n    invariant (1 <= i)\n    invariant (i <= 5)\n    invariant (((offset + i) - 1)", "  while (i <= 5)\n    invariant (1 <= i)\n    invariant (i <= 6)\n    invariant (((offset + i) - 1)"],
  ["packetSize scales by 127", "    scale := (scale * 128);\n    i := (i + 1);\n  }\n  return -1;", "    scale := (scale * 127);\n    i := (i + 1);\n  }\n  return -1;"],
  ["remainingLength scales by 127", "    scale := (scale * 128);\n    i := (i + 1);\n  }\n  return n;", "    scale := (scale * 127);\n    i := (i + 1);\n  }\n  return n;"],
  ["encodeVarint sets the wrong continuation bit", "(digit + 128) else digit)", "(digit + 127) else digit)"],
  ["encodeVarint stops one digit early", "(if (rest > 0) then", "(if (rest > 1) then"],
  ["varintSize moves the 1-byte boundary", "  if (n < 128) then\n    1", "  if (n <= 128) then\n    1"],
  ["fieldLength ignores a 4-byte field", "                if (bytes[(offset + 4)] < 128) then\n                  4", "                if (bytes[(offset + 4)] < 128) then\n                  0"],
  ["RoundTrip asserts false (vacuity probe)", "  assert remaining == n;\n", "  assert remaining == n;\n  assert false;\n"]
]

if (!verify(proof)) {
  console.log("REFUSED: the committed proof does not verify, so no mutant can be credited")
  process.exit(1)
}

let missed = 0

for (const [name, from, to] of mutants) {
  const at = proof.indexOf(from)

  if (at === -1 || proof.indexOf(from, at + 1) !== -1) {
    console.log(`STALE   ${name}: its anchor no longer occurs exactly once; update this script`)
    missed++
    continue
  }

  const caught = !verify(proof.replace(from, to))

  if (!caught) missed++
  console.log(`${caught ? "CAUGHT" : "MISSED"}  ${name}`)
}

process.exit(missed === 0 ? 0 : 1)
