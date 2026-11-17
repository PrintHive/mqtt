//@ backend dafny
/**
 * The Remaining Length field of an MQTT fixed header (MQTT 3.1.1 section 2.2.3): a variable byte integer of 1 to 4
 * bytes, 7 bits per byte, least significant group first, with the high bit set on every byte but the last.
 *
 * These functions are proved with LemmaScript (`lsc check --backend=dafny`); `varint.verify.dfy` holds the proofs.
 */

/** The bytes of a packet: a Buffer at runtime, a sequence of integers to the prover. */
//@ declare-type Bytes = number[]
export type Bytes = ArrayLike<number>

/** The largest remaining length a 4-byte variable byte integer can hold. */
export const MAX_REMAINING_LENGTH = 268_435_455

/** 128 to the power `k`. Spec only. */
//@ verify
export function pow128(k: number): number {
  //@ requires 0 <= k
  //@ ensures \result >= 1
  //@ decreases k
  if (k === 0) return 1

  return 128 * pow128(k - 1)
}

/** The value of `k` remaining-length bytes starting at `at`, least significant group first. Spec only. */
//@ verify
export function digitsValue(bytes: Bytes, at: number, k: number): number {
  //@ requires 0 <= at && 0 <= k && at + k <= bytes.length
  //@ requires forall(j: nat, j < bytes.length ==> 0 <= bytes[j])
  //@ ensures \result >= 0
  //@ decreases k
  if (k === 0) return 0

  return digitsValue(bytes, at, k - 1) + (bytes[at + k - 1]! % 128) * pow128(k - 1)
}

/** Bytes needed to encode `n` as a variable byte integer. */
//@ verify
export function varintSize(n: number): number {
  //@ requires 0 <= n && n <= MAX_REMAINING_LENGTH
  //@ ensures 1 <= \result && \result <= 4
  //@ ensures \result === 1 ==> n < 128
  //@ ensures \result === 2 ==> 128 <= n && n < 16384
  //@ ensures \result === 3 ==> 16384 <= n && n < 2097152
  //@ ensures \result === 4 ==> 2097152 <= n
  return n < 128 ? 1 : n < 16_384 ? 2 : n < 2_097_152 ? 3 : 4
}

/** The variable byte integer encoding of `n`: 7 bits per byte, least significant first, high bit on all but the last. */
//@ verify
export function encodeVarint(n: number): number[] {
  //@ requires 0 <= n && n <= MAX_REMAINING_LENGTH
  //@ ensures \result.length === varintSize(n)
  //@ ensures forall(j: nat, j < \result.length ==> 0 <= \result[j] && \result[j] <= 255)
  //@ ensures forall(j: nat, j + 1 < \result.length ==> \result[j] >= 128)
  //@ ensures \result[\result.length - 1] < 128
  //@ ensures digitsValue(\result, 0, \result.length) === n
  const out: number[] = []
  let rest = n

  while (true) {
    //@ invariant 0 <= rest
    //@ decreases rest
    const digit = rest % 128
    rest = Math.floor(rest / 128)
    out.push(rest > 0 ? digit + 128 : digit)

    if (rest === 0) return out
  }
}

/** Whether the `k` bytes after `offset` all have the continuation bit set. Spec only. */
//@ verify
export function continues(bytes: Bytes, offset: number, k: number): boolean {
  //@ requires 0 <= offset && 0 <= k && offset + k < bytes.length
  //@ decreases k
  if (k === 0) return true

  return bytes[offset + k]! >= 128 && continues(bytes, offset, k - 1)
}

/**
 * How many bytes the remaining-length field after `offset` has: the first of the next 4 bytes without the
 * continuation bit. 0 when there is none among them, either because they have not arrived or because all 4 have it.
 * Spec only.
 */
//@ verify
export function fieldLength(bytes: Bytes, offset: number): number {
  //@ requires 0 <= offset && offset < bytes.length
  return offset + 1 >= bytes.length ? 0
    : bytes[offset + 1]! < 128 ? 1
    : offset + 2 >= bytes.length ? 0
    : bytes[offset + 2]! < 128 ? 2
    : offset + 3 >= bytes.length ? 0
    : bytes[offset + 3]! < 128 ? 3
    : offset + 4 >= bytes.length ? 0
    : bytes[offset + 4]! < 128 ? 4
    : 0
}

/**
 * Total size of the packet at `offset`: 1 for the first byte, then the remaining-length bytes, then the remaining
 * length. 0 when the remaining-length field has not fully arrived, and -1 when it runs past 4 bytes.
 */
//@ verify
export function packetSize(bytes: Bytes, offset: number): number {
  //@ requires 0 <= offset && offset < bytes.length
  //@ requires forall(j: nat, j < bytes.length ==> 0 <= bytes[j] && bytes[j] <= 255)
  //@ ensures \result >= -1
  //@ ensures \result === -1 <==> offset + 4 < bytes.length && continues(bytes, offset, 4)
  //@ ensures \result === 0 <==> offset + 4 >= bytes.length && continues(bytes, offset, bytes.length - 1 - offset)
  //@ ensures \result > 0 ==> \result === 1 + fieldLength(bytes, offset) + digitsValue(bytes, offset + 1, fieldLength(bytes, offset))
  let n = 0
  let scale = 1

  for (let i = 1; i <= 4; i++) {
    //@ invariant 1 <= i && i <= 5
    //@ invariant offset + i - 1 < bytes.length
    //@ invariant continues(bytes, offset, i - 1)
    //@ invariant n === digitsValue(bytes, offset + 1, i - 1)
    //@ invariant scale === pow128(i - 1)
    const at = offset + i

    if (at >= bytes.length) return 0
    const b = bytes[at]!
    n += (b & 127) * scale

    if (b < 128) return 1 + i + n
    scale *= 128
  }

  return -1
}

/** Remaining length of the packet at `offset`. Only called once `packetSize` has read a complete header. */
//@ verify
export function remainingLength(bytes: Bytes, offset: number): number {
  //@ requires 0 <= offset && offset < bytes.length
  //@ requires forall(j: nat, j < bytes.length ==> 0 <= bytes[j] && bytes[j] <= 255)
  //@ requires fieldLength(bytes, offset) > 0
  //@ ensures \result === digitsValue(bytes, offset + 1, fieldLength(bytes, offset))
  let n = 0
  let scale = 1

  for (let i = 1; i <= 4; i++) {
    //@ invariant 1 <= i && i <= fieldLength(bytes, offset)
    //@ invariant continues(bytes, offset, i - 1)
    //@ invariant n === digitsValue(bytes, offset + 1, i - 1)
    //@ invariant scale === pow128(i - 1)
    const b = bytes[offset + i]!
    n += (b & 127) * scale

    if (b < 128) return n
    scale *= 128
  }

  return n
}
