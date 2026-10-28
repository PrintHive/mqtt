import type { Buffer } from "node:buffer"
import { readFileSync } from "node:fs"
import * as net from "node:net"
import * as tls from "node:tls"
import { Aedes } from "aedes"
import type { AedesOptions, Client } from "aedes"
import * as Effect from "effect/Effect"
import { Decoder, encode } from "../src/codec.ts"
import * as P from "../src/packet.ts"
import type { Packet } from "../src/packet.ts"

/** Listens on a random local port. Closing destroys open connections first, since `server.close` waits for them. */
const listen = (server: net.Server) => {
  const sockets = new Set<net.Socket>()

  server.on("connection", (socket) => {
    sockets.add(socket)
    socket.on("close", () => sockets.delete(socket))
  })

  return Effect.acquireRelease(
    Effect.callback<number>((resume) => {
      server.listen(0, "127.0.0.1", () => {
        // SAFETY: a TCP server listening on a host and port reports an AddressInfo, never a pipe name.
        resume(Effect.succeed((server.address() as net.AddressInfo).port))
      })
    }),
    () =>
      Effect.callback<void>((resume) => {
        sockets.forEach((s) => s.destroy())
        server.close(() => resume(Effect.void))
      })
  )
}

/** A real broker (aedes) on a random local port, closed with the scope. */
export const aedes = (options?: AedesOptions) =>
  Effect.gen(function* () {
    const broker = yield* Effect.acquireRelease(
      Effect.promise(() => Aedes.createBroker(options)),
      (b) => Effect.callback<void>((resume) => b.close(() => resume(Effect.void)))
    )

    const clients = new Map<string, Client>()

    broker.on("client", (c) => clients.set(c.id, c))

    const server = net.createServer((socket) => broker.handle(socket))
    const port = yield* listen(server)

    return {
      broker,
      port,
      /** Cuts a client's TCP connection without a DISCONNECT, as a network failure would. */
      drop: (clientId: string) => clients.get(clientId)?.conn.destroy()
    }
  })

export interface ScriptOptions {
  readonly tls?: tls.TlsOptions | undefined
  /** Keeps the broker's half of the connection open after the client's FIN, as some peers do. */
  readonly allowHalfOpen?: boolean | undefined
  /** Defaults to true. With false, CONNECT goes to `onPacket` and nothing answers it. */
  readonly connack?: boolean | undefined
}

/**
 * A scripted broker that answers the first packet (CONNECT) with CONNACK and hands every later packet to `onPacket`.
 * `sockets` holds every connection it accepted.
 */
export const scripted = (onPacket: (packet: Packet, socket: net.Socket) => void, options?: ScriptOptions) =>
  Effect.gen(function* () {
    const sockets: Array<net.Socket> = []

    const handle = (socket: net.Socket) => {
      let connected = options?.connack === false

      sockets.push(socket)

      const decoder = new Decoder((packet) => {
        if (connected) return onPacket(packet, socket)

        connected = true
        socket.write(encode(P.connack(false, 0)))
      })

      socket.on("data", (chunk: Buffer) => {
        // Other local processes probe random ports (an HTTP client sent `GET /` here); hang up on anything not MQTT.
        try {
          decoder.push(chunk)
        } catch {
          socket.destroy()
        }
      })
      socket.on("error", () => socket.destroy())
    }

    const allowHalfOpen = options?.allowHalfOpen ?? false
    const server = options?.tls ? tls.createServer({ ...options.tls, allowHalfOpen }, handle) : net.createServer({ allowHalfOpen }, handle)
    const port = yield* listen(server)

    return { port, sockets }
  })

/** A local port with nothing listening on it. */
export const closedPort = Effect.scoped(listen(net.createServer()))

/** A self-signed certificate for 127.0.0.1 that exists only for these tests. It protects nothing. */
export const selfSigned = (): tls.TlsOptions => ({
  key: readFileSync(new URL("fixtures/test-only-key.pem", import.meta.url)),
  cert: readFileSync(new URL("fixtures/test-only-cert.pem", import.meta.url))
})
