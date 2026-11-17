import * as Data from "effect/Data"
import type { MalformedPacket } from "./codec.ts"

/** The socket could not be opened, timed out, or closed. */
export class ConnectionError extends Data.TaggedError("ConnectionError")<{ readonly message: string }> {}

/** The broker answered CONNECT with a refusal (section 3.2.2.3). */
export class ConnectionRefused extends Data.TaggedError("ConnectionRefused")<{
  readonly returnCode: number
  readonly message: string
}> {}

/** The broker sent a packet a client must not receive, or one that does not fit the session. */
export class ProtocolError extends Data.TaggedError("ProtocolError")<{ readonly message: string }> {}

/** The broker refused a subscription with return code 0x80. */
export class SubscribeRejected extends Data.TaggedError("SubscribeRejected")<{ readonly filter: string }> {}

/**
 * A backpressure subscription stayed full for longer than `maxPaused`, holding up every other subscription, so the
 * client dropped it and resumed reading.
 */
export class SubscriptionStalled extends Data.TaggedError("SubscriptionStalled")<{ readonly filter: string; readonly message: string }> {}

/**
 * A QoS 1 or 2 publish was written to a connection that dropped before its handshake finished, and the broker came
 * back without the session (`sessionPresent` 0, as on every reconnect with `cleanSession: true`). The broker may or
 * may not have delivered the message. It is not sent again: a resend to a fresh session can deliver it twice
 * (section 3.1.2.4). Retry only if a second delivery is safe.
 */
export class DeliveryUnknown extends Data.TaggedError("DeliveryUnknown")<{ readonly topic: string; readonly message: string }> {}

/** A topic or filter breaks the rules in section 4.7. */
export class InvalidTopic extends Data.TaggedError("InvalidTopic")<{ readonly topic: string }> {}

/** An option is outside its allowed range. */
export class InvalidOption extends Data.TaggedError("InvalidOption")<{ readonly option: string; readonly message: string }> {}

/** The client's scope closed, or reconnecting gave up. */
export class ClientClosed extends Data.TaggedError("ClientClosed")<{ readonly message: string }> {}

export type MqttError =
  | ConnectionError
  | ConnectionRefused
  | ProtocolError
  | SubscribeRejected
  | SubscriptionStalled
  | DeliveryUnknown
  | InvalidTopic
  | InvalidOption
  | ClientClosed
  | MalformedPacket

const REFUSALS = [
  "",
  "unacceptable protocol version",
  "identifier rejected",
  "server unavailable",
  "bad user name or password",
  "not authorized"
]

export const refused = (returnCode: number) =>
  new ConnectionRefused({ returnCode, message: REFUSALS[returnCode] ?? `return code ${returnCode}` })
