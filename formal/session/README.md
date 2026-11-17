# QoS 2 across dropped connections

`QoS2Session.tla` models the QoS 2 handshakes in `src/session.ts`, one message each way, while the connection drops
up to `MaxDrops` times and loses whatever is in flight. TLC checks every order of steps within that bound.
The `verify-session-qos2` skill (`.agents/skills/verify-session-qos2/SKILL.md`) says how to run it and keep it in step
with the code. Read every result with the `/statechart` skill.

## Rules

| Rule | Sentence | Harm when it breaks |
| --- | --- | --- |
| `YourMessageAtMostOnce` | Your QoS 2 message reaches the broker's subscribers at most once. | A command such as "start print" runs twice. |
| `DoneMeansDelivered` | When publish succeeds, the broker has delivered your message. | The caller moves on from a message that never arrived. |
| `GivenUpMeansNeverSent` | A publish interrupted before it was written is never delivered or sent. | A retry after an interrupt sends the message twice. |
| `UnknownOnlyWhenWritten` | A publish fails with `DeliveryUnknown` only if it was written. | A message that never left the client is reported as maybe delivered, and is never sent. |
| `BrokersMessageAtMostOnce` | The broker's QoS 2 message reaches your subscription at most once. | Your consumer handles one event twice. |
| `BrokerDoneMeansReceived` | When the broker finishes sending, your subscription got the message. | A message is acknowledged but never delivered. |
| `NoDeadlock` | Every unfinished exchange can still finish. | A publish waits forever after a reconnect. |

## Configs

- `SessionKept.cfg`: every reconnect finds the session (`cleanSession: false` with a broker that kept it).
  Every rule holds: 300 states. Chart: `SessionKept.mmd`.
- `SessionForgotten.cfg`: a reconnect may report `sessionPresent = 0`, which is every reconnect with the default
  `cleanSession: true`. Every rule holds: 400 states. Chart: `SessionForgotten.mmd`.

## Fixed finding: a QoS 2 publish could arrive twice with cleanSession true

Before 0.6.0 this path broke `YourMessageAtMostOnce`:

1. You publish at QoS 2.
2. The broker gets PUBLISH and delivers it.
3. The connection drops before PUBREC reaches the client.
4. The client reconnects. The broker answers `sessionPresent = 0`: it kept no record of the message.
5. `Session.attach` resent the PUBLISH with DUP set, and the broker, with no session to match it against, delivered
   it again.

MQTT 3.1.1 section 3.1.2.4 says a client with CleanSession 1 discards its session state, and section 4.4 resends only
with CleanSession 0. Now `attach` (`src/session.ts` 129-153) follows that: with `sessionPresent = 0`, a publish that
was written fails with `DeliveryUnknown` instead of going out again, and one never written is sent for the first time.
`UnknownOnlyWhenWritten` checks the second half. `.agents/skills/verify-session-qos2/scripts/replay-duplicate.ts`
replays the path on the real client, with `serve()` as a broker that keeps no sessions, and checks the broker sees
the PUBLISH once and the publish fails with `DeliveryUnknown`.

## Bounds and assumptions

- One QoS 2 message in each direction, so packet identifiers, their reuse, and two exchanges interleaving are not
  modeled. QoS 1 and QoS 0 are not modeled.
- `MaxDrops = 2`. A drop loses both channels; each channel is otherwise a FIFO, as TCP is.
- The broker follows section 4.3.3 method B: it delivers on PUBLISH and holds the id until PUBREL. A broker that
  forgets the session forgets both directions; its own unfinished message is then lost.
- One action is one synchronous step of `Session`, which runs on one fiber. The link that fails between CONNACK and
  `attach` (`src/session.ts` 131) is not modeled.
- SUBSCRIBE and UNSUBSCRIBE requests, keep-alive, backpressure and `close` are not modeled.
