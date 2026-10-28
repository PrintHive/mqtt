import type { Buffer } from "node:buffer"
import * as net from "node:net"
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

/** A scripted broker that answers the first packet (CONNECT) with CONNACK and hands every later packet to `onPacket`. */
export const scripted = (onPacket: (packet: Packet, socket: net.Socket) => void) =>
  Effect.gen(function* () {
    const server = net.createServer((socket) => {
      let connected = false

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
    })

    return yield* listen(server)
  })

/** A local port with nothing listening on it. */
export const closedPort = Effect.scoped(listen(net.createServer()))
