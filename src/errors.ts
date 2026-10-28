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

/** A topic or filter breaks the rules in section 4.7. */
export class InvalidTopic extends Data.TaggedError("InvalidTopic")<{ readonly topic: string }> {}

/** The client's scope closed, or reconnecting gave up. */
export class ClientClosed extends Data.TaggedError("ClientClosed")<{ readonly message: string }> {}

export type MqttError =
  | ConnectionError
  | ConnectionRefused
  | ProtocolError
  | SubscribeRejected
  | InvalidTopic
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
