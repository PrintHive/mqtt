# @printhive/mqtt

A fast MQTT 3.1.1 client for [Effect](https://effect.website), on Bun and Node.js.

- QoS 0, 1, and 2 in both directions, retained messages, wills, and TLS.
- Reconnects with an Effect `Schedule`, resends unfinished QoS 1 and 2 messages, and resubscribes when the broker lost the session.
- Subscriptions are scoped streams that stay open until their scope closes.
- No runtime dependencies besides the `effect` peer dependency. It runs on `node:net` and `node:tls`, which Bun also implements.
- About 4 times faster than mqtt.js at decoding packets, and 1.2 to 1.7 times its end-to-end QoS 0 throughput. See [Performance](#performance).

## Install

```sh
bun add @printhive/mqtt effect
# or
npm install @printhive/mqtt effect
```

It needs Node.js 22 or later, or Bun.

## Publish and subscribe

```ts
import { Buffer } from "node:buffer"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import { connect } from "@printhive/mqtt"

const program = Effect.gen(function* () {
  const client = yield* connect({ host: "broker.emqx.io", tls: true })
  const messages = yield* client.subscribe("printers/+/status", { qos: 1 })

  yield* client.publish("printers/p1s/status", "printing", { qos: 1 })

  yield* messages.pipe(
    Stream.take(1),
    Stream.runForEach((m) => Effect.log(m.topic, Buffer.from(m.payload).toString()))
  )
})

Effect.runPromise(Effect.scoped(program))
```

The client stays connected until its scope closes. Closing the scope sends DISCONNECT, so the broker does not publish the client's will.

`subscribe` waits for the broker's SUBACK before it returns. A message published after it returns is received, which matters for request and response protocols. The subscription lasts until the scope that created it closes.

## Talk to a Bambu Lab printer

Bambu Lab printers in LAN mode run an MQTT broker on port 8883. The user name is `bblp` and the password is the printer's access code. [`examples/bambu-report.ts`](examples/bambu-report.ts) subscribes to a printer's reports and asks it for a full status report:

```sh
PRINTER_HOST=192.0.2.10 PRINTER_SERIAL=... PRINTER_ACCESS_CODE=... bun examples/bambu-report.ts
```

## Connect options

| Option | Default | Meaning |
| --- | --- | --- |
| `host` | required | Broker host name or IP address. |
| `port` | 8883 with TLS, 1883 without | Broker port. |
| `tls` | off | `true` uses TLS with the runtime's CA store. An object passes through to `tls.connect`, for example `{ ca }` or `{ rejectUnauthorized: false }`. |
| `clientId` | `printhive-` and 12 random hex digits | MQTT client identifier. |
| `username`, `password` | none | Credentials. The password can be a string or bytes. |
| `cleanSession` | `true` | With `false`, the broker keeps subscriptions and queued QoS 1 and 2 messages across reconnects. |
| `keepAlive` | 60 seconds | The client sends PINGREQ after a quiet half period and drops a connection with no PINGRESP within one period. 0 turns it off. |
| `connectTimeout` | 10 seconds | Time allowed for the socket and CONNACK. |
| `will` | none | `{ topic, payload, qos, retain }`, published by the broker if the client disappears without DISCONNECT. |
| `reconnect` | exponential from 500 ms, capped at 30 s, forever | An Effect `Schedule` for retrying after an established connection drops. `false` closes the client on the first drop. |

The first connection is not retried, so `connect` fails fast when the broker is unreachable or refuses the credentials. Reconnecting stops early on a refusal that retrying cannot fix, such as bad credentials.

## Publish

`client.publish(topic, payload, { qos, retain })` takes a string or a `Uint8Array`.

- QoS 0 completes once the packet is written. While the client is reconnecting, or the socket's buffer is full, it waits.
- QoS 1 completes on PUBACK, and QoS 2 on PUBCOMP. A message whose connection drops is sent again with DUP set after the reconnect.

## Connection status

`client.status` reads the current `ConnectionStatus`, and `client.statusChanges` is a stream of it. The status is one of:
- `Connecting`
- `Connected`, with `sessionPresent`
- `Reconnecting`, with `attempt` and the `error` that caused it
- `Closed`, with the final `error`

Once the client is `Closed`, every subscription stream fails with the error that closed it.

## Errors

Every error is a tagged error, so `Effect.catchTag` can handle each one.

| Error | Cause |
| --- | --- |
| `ConnectionError` | The socket failed, timed out, or closed, or keep-alive expired. |
| `ConnectionRefused` | The broker refused CONNECT. `returnCode` holds its reason (section 3.2.2.3). |
| `SubscribeRejected` | The broker answered SUBSCRIBE with 0x80 for the filter. |
| `InvalidTopic` | A topic has wildcards, or a filter misplaces `#` or `+`. |
| `MalformedPacket` | The broker sent bytes that are not valid MQTT 3.1.1. |
| `ProtocolError` | The broker sent a packet a client must not receive. |
| `ClientClosed` | The client's scope closed, or reconnecting gave up. |

## Performance

The decoder reads packets straight out of the socket chunks it is given. Each payload is a view into the chunk, not a copy. Small outgoing packets are sliced from a shared slab, and bursts of writes are corked into one syscall.

[`bench/RESULTS.md`](bench/RESULTS.md) has the method, every range, and the limits. Summary, median of 5 alternating runs on an Apple M3 Pro:

| | Bun | Node.js |
| --- | --- | --- |
| Decode, vs mqtt-packet | 3.9x | 4.4x |
| Encode, vs mqtt-packet | 1.5x | 3.2x |
| End to end QoS 0, 64 B, vs mqtt.js | 1.47x | 1.73x |
| End to end QoS 0, 1 KB, vs mqtt.js | 1.23x | 1.68x |
| End to end QoS 1, 1 KB, vs mqtt.js | no measurable difference | no measurable difference |

The end-to-end QoS 0 runs are limited by Mosquitto, which used a full core while this client used about two thirds of one.

Effect's tagged constructors and `Match` measured 4 to 6 times slower than plain objects with a `switch`. So `src/packet.ts`, `src/codec.ts`, and `src/session.ts` keep plain tagged objects, and the lint configuration exempts only those files from three Effect rules.

## Limits

- MQTT 3.1.1 only. MQTT 5 is planned.
- Each subscription buffers messages without a limit. A consumer that falls far behind uses memory instead of slowing the broker.
- With overlapping subscriptions, a broker may send a message once per matching filter. Each copy reaches every local stream whose filter matches.

## Develop

```sh
bun install
bun run check       # oxlint with the anti-slop rules, tsc, and the tests on Bun
bun run test:node   # the same tests on Node.js through Vitest
bun run build
```

The tests run against a real broker, [aedes](https://github.com/moscajs/aedes), and against scripted brokers built on the codec. They never mock modules.

Lint runs [oxlint](https://oxc.rs) with the [anti-slop](https://github.com/dmmulroy/anti-slop) rules, including its Effect rules. They are vendored in `tools/oxlint/anti-slop/`, and `UPSTREAM.md` there records the source commit.

To run the benchmarks, start Mosquitto, then run them:

```sh
docker run -d --name mqtt-bench -p 18830:1883 -v $PWD/bench/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro eclipse-mosquitto:2
bun bench/run.ts
bun bench/codec.ts && node bench/codec.ts
```

CI runs the checks on Bun, and the tests on Node.js 22, 24, and 26, all on Blacksmith runners. To release, set the version and push its tag. The release workflow publishes to npm with provenance.

```sh
npm version patch
git push --follow-tags
```

## License

MIT
