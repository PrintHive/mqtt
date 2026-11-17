---
name: verify-varint
description: Check the proved MQTT Remaining Length code when changing src/varint.verify.ts, its proof src/varint.verify.dfy, or how src/codec.ts sizes and frames packets.
---

# Verify varint

## Property and harm

The fixed header of every MQTT packet carries its Remaining Length as a variable byte integer of 1 to 4 bytes
(MQTT 3.1.1 section 2.2.3). The decoder uses it to find where each packet ends in a TCP stream, so a wrong answer
desynchronizes the stream: every later packet is decoded from the wrong offset, or a hostile peer's header makes
the decoder read past a packet. The encoder uses it to write every packet the client and server send.

`src/varint.verify.dfy` proves, for every input, not a sample:

- `packetSize` returns -1 exactly when the 4 bytes after `offset` all have the continuation bit, 0 exactly when the
  field has not fully arrived, and otherwise `1 + fieldLength + value`, where `value` is the 7-bit digits of the
  field, least significant first.
- `remainingLength` returns that same value once the field is complete.
- `encodeVarint(n)` writes `varintSize(n)` bytes, the continuation bit on all but the last, whose digits are `n`.
- `RoundTrip`: a header written by `encodeVarint`, followed by any bytes at all, decodes to `1 + size + n`, and
  `remainingLength` reads back `n`.
- `varintSize` matches the 128, 16384 and 2097152 boundaries.

Production calls the proved `packetSize`, `remainingLength` and `varintSize` directly from `src/codec.ts`.
The packet writer (`allocate` in `src/codec.ts`) keeps its own loop for speed; the codec test
"writes each remaining length as the proved encoder does, at every size boundary" pins it to `encodeVarint`.

## Tool and why

[LemmaScript](https://github.com/midspiral/LemmaScript) 0.6.10 (exact devDependency) compiles the annotated
TypeScript to Dafny; Dafny 4.11.0 proves it. The proof runs on the shipping TypeScript, not a hand copy. Load the
`lemmascript` skill before changing either file: it holds the gen/check/regen loop and the additions-only rule.

## Run it

Dafny 4.11.0 must be on PATH. CI downloads the release zip and checks its SHA-256 (the `proofs` job in
`.github/workflows/ci.yml`). On macOS, unzip `dafny-4.11.0-arm64-macos-13.zip` from the Dafny release; Homebrew's
formula is deprecated and builds .NET 8 from source.

```sh
bunx lsc check --backend=dafny                     # every file in LemmaScript-files.txt, as CI does
bun .agents/skills/verify-varint/scripts/mutants.ts  # about 90 s
```

After changing `src/varint.verify.ts`, run `bunx lsc regen --backend=dafny src/varint.verify.ts` before any `check`.
`regen` merges the new generated Dafny into the proof; `check` would overwrite the merge base it needs.
Commit the `.dfy.gen` too: CI fails when it is stale.

`mutants.ts` first verifies the committed proof and refuses (`REFUSED`) when it is red. It then breaks the
generated Dafny one way at a time, in a temporary copy, and requires Dafny to reject each break. The last mutant
adds `assert false` to `RoundTrip`, which catches a proof that holds only because its assumptions contradict.
An anchor that no longer occurs exactly once prints `STALE`; update the script with the code.

| Mutant | Result |
| --- | --- |
| packetSize takes 128 as a last byte | caught |
| packetSize leaves the first byte out of the size | caught |
| packetSize reads a fifth length byte | caught |
| packetSize scales by 127 | caught |
| remainingLength scales by 127 | caught |
| encodeVarint sets the wrong continuation bit | caught |
| encodeVarint stops one digit early | caught |
| varintSize moves the 1-byte boundary | caught |
| fieldLength ignores a 4-byte field | caught |
| RoundTrip asserts false (vacuity probe) | caught |

## Bounds and assumptions

- Bytes are integers from 0 to 255, which a Buffer guarantees; the contracts require it.
- `number` is a mathematical integer to Dafny. Every value here stays below 2^29, far inside a double's exact range,
  so no rounding the model ignores can occur.
- `b & 127` lowers to `b % 128`; the two agree on 0 to 255.
- `packetSize` requires `offset < bytes.length`; the decoder loop only calls it then.

## Not covered

The packet writer's own loop (covered by the boundary test, not the proof), body decoding, UTF-8 strings,
packet identifiers, the Decoder's chunk buffering, sessions, and the network. MQTT 3.1.1 does not require the
shortest encoding, so the decoder accepts a longer one such as `80 00` for 0, as the proof states.
