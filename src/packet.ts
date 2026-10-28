/** MQTT 3.1.1 control packets, as defined in OASIS MQTT Version 3.1.1, section 3. */

export type QoS = 0 | 1 | 2

export interface Will {
  readonly topic: string
  readonly payload: Uint8Array
  readonly qos: QoS
  readonly retain: boolean
}

export interface Connect {
  readonly _tag: "Connect"
  readonly clientId: string
  readonly cleanSession: boolean
  /** Seconds. 0 turns keep-alive off. */
  readonly keepAlive: number
  readonly username: string | undefined
  readonly password: Uint8Array | undefined
  readonly will: Will | undefined
}

export interface Connack {
  readonly _tag: "Connack"
  readonly sessionPresent: boolean
  /** 0 accepts the connection. 1 to 5 are the refusal codes in section 3.2.2.3. */
  readonly returnCode: number
}

/** A QoS 0 message has no packet identifier and is never a duplicate. */
export interface PublishAtMostOnce {
  readonly _tag: "Publish"
  readonly qos: 0
  readonly topic: string
  readonly payload: Uint8Array
  readonly retain: boolean
}

export interface PublishAcknowledged {
  readonly _tag: "Publish"
  readonly qos: 1 | 2
  readonly topic: string
  readonly payload: Uint8Array
  readonly retain: boolean
  readonly dup: boolean
  readonly packetId: number
}

export type Publish = PublishAtMostOnce | PublishAcknowledged

export interface Ack<Tag extends string> {
  readonly _tag: Tag
  readonly packetId: number
}

export type Puback = Ack<"Puback">

export type Pubrec = Ack<"Pubrec">

export type Pubrel = Ack<"Pubrel">

export type Pubcomp = Ack<"Pubcomp">

export type Unsuback = Ack<"Unsuback">

export interface Subscription {
  readonly filter: string
  readonly qos: QoS
}

export interface Subscribe {
  readonly _tag: "Subscribe"
  readonly packetId: number
  readonly subscriptions: ReadonlyArray<Subscription>
}

/** 0x80 is the broker refusing that filter. */
export type SubackCode = QoS | 0x80

export interface Suback {
  readonly _tag: "Suback"
  readonly packetId: number
  readonly codes: ReadonlyArray<SubackCode>
}

export interface Unsubscribe {
  readonly _tag: "Unsubscribe"
  readonly packetId: number
  readonly filters: ReadonlyArray<string>
}

export interface Pingreq {
  readonly _tag: "Pingreq"
}

export interface Pingresp {
  readonly _tag: "Pingresp"
}

export interface Disconnect {
  readonly _tag: "Disconnect"
}

export type Packet =
  | Connect
  | Connack
  | Publish
  | Puback
  | Pubrec
  | Pubrel
  | Pubcomp
  | Subscribe
  | Suback
  | Unsubscribe
  | Unsuback
  | Pingreq
  | Pingresp
  | Disconnect

export const connect = (fields: Omit<Connect, "_tag">): Connect => ({ _tag: "Connect", ...fields })

export const connack = (sessionPresent: boolean, returnCode: number): Connack => ({ _tag: "Connack", sessionPresent, returnCode })

export const publishAtMostOnce = (topic: string, payload: Uint8Array, retain: boolean): PublishAtMostOnce => ({
  _tag: "Publish",
  qos: 0,
  topic,
  payload,
  retain
})

export const publishAcknowledged = (
  topic: string,
  payload: Uint8Array,
  qos: 1 | 2,
  packetId: number,
  retain: boolean,
  dup: boolean
): PublishAcknowledged => ({ _tag: "Publish", qos, topic, payload, retain, dup, packetId })

export const puback = (packetId: number): Puback => ({ _tag: "Puback", packetId })

export const pubrec = (packetId: number): Pubrec => ({ _tag: "Pubrec", packetId })

export const pubrel = (packetId: number): Pubrel => ({ _tag: "Pubrel", packetId })

export const pubcomp = (packetId: number): Pubcomp => ({ _tag: "Pubcomp", packetId })

export const subscribe = (packetId: number, subscriptions: ReadonlyArray<Subscription>): Subscribe => ({
  _tag: "Subscribe",
  packetId,
  subscriptions
})

export const suback = (packetId: number, codes: ReadonlyArray<SubackCode>): Suback => ({ _tag: "Suback", packetId, codes })

export const unsubscribe = (packetId: number, filters: ReadonlyArray<string>): Unsubscribe => ({
  _tag: "Unsubscribe",
  packetId,
  filters
})

export const unsuback = (packetId: number): Unsuback => ({ _tag: "Unsuback", packetId })

export const pingreq: Pingreq = { _tag: "Pingreq" }

export const pingresp: Pingresp = { _tag: "Pingresp" }

export const disconnect: Disconnect = { _tag: "Disconnect" }
