# Benchmark results

Measured on 2026-10-28 on an Apple M3 Pro (12 cores) with Bun 1.4.0, Node.js 26.5.0, mqtt.js 5.16.1, mqtt-packet 9.0.2, and Mosquitto 2.1.2 in Docker. The machine was busy (load average 15 to 25 from unrelated work), so every comparison alternates the two libraries run by run and reports the median and range of 5 runs.

## End to end

`bun bench/run.ts`: one publisher and one subscriber on the same library, through Mosquitto, in one process. The rate is messages per second from the first publish until the subscriber has every message. Every run checks that all messages and payload bytes arrived. QoS 1 keeps 1000 publishes outstanding for both libraries.

| Runtime | QoS | Payload | @printhive/mqtt msg/s | mqtt.js msg/s | Ratio |
| --- | --- | --- | --- | --- | --- |
| bun | 0 | 64 B | 298k (258k to 328k) | 203k (196k to 213k) | 1.47x |
| bun | 0 | 1024 B | 141k (127k to 154k) | 115k (111k to 122k) | 1.23x |
| bun | 1 | 1024 B | 28k (24k to 37k) | 25k (15k to 29k) | 1.12x |
| node | 0 | 64 B | 295k (268k to 311k) | 170k (163k to 179k) | 1.73x |
| node | 0 | 1024 B | 139k (131k to 150k) | 83k (72k to 105k) | 1.68x |
| node | 1 | 1024 B | 30k (24k to 40k) | 23k (20k to 28k) | 1.33x |

- QoS 0 is faster on both runtimes; the ranges do not overlap.
- QoS 1 shows no measurable difference; the ranges overlap. Each acknowledgement crosses Docker's port forwarding, so round trips bound it rather than either client.
- During a QoS 0, 1 KB run of @printhive/mqtt, Mosquitto used one full core (101%, and it is single-threaded) while the client process used 61 to 67%. The broker is the limit, so the QoS 0 ratios understate the client-side difference. The codec numbers below isolate it.

## Codec

`bun bench/codec.ts` and `node bench/codec.ts`: decoding 200,000 PUBLISH packets with a 1 KB payload from 64 KB chunks, and encoding them, against mqtt-packet, the codec inside mqtt.js. The two encoders produce identical bytes, and the decoders must return every payload byte.

| Runtime | Operation | @printhive/mqtt packets/s | mqtt-packet packets/s |
| --- | --- | --- | --- |
| bun | decode | 3.41M (2.22M to 4.28M) | 0.87M (0.34M to 1.18M) |
| bun | encode | 3.83M (2.90M to 4.36M) | 2.64M (2.00M to 3.02M) |
| node | decode | 3.24M (1.67M to 5.23M) | 0.73M (0.34M to 1.10M) |
| node | encode | 5.65M (4.61M to 6.19M) | 1.79M (1.59M to 2.05M) |

Decoding returns payloads as views of the socket chunk instead of copies. Encoding first measured 1.91M packets/s on Bun, the same as mqtt-packet. Bun's `Buffer.allocUnsafe` has no small-buffer pool, so every packet was a fresh allocation; slicing packets from a 64 KB slab doubled it.
