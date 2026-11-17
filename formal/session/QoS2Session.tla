---------------------------- MODULE QoS2Session ----------------------------
(***************************************************************************)
(* QoS 2 handshakes of src/session.ts across dropped connections.         *)
(*                                                                         *)
(* One QoS 2 message goes each way: "your message" from the client's      *)
(* publishAcknowledged to the broker, and "the broker's message" from the *)
(* broker to the client's deliver callback. The connection can drop at    *)
(* any moment, up to MaxDrops times, losing whatever is in flight. On     *)
(* each reconnect the broker reports whether it kept the session          *)
(* (sessionPresent); with BrokerMayForget it can say no.                  *)
(*                                                                         *)
(* Each channel is a FIFO, as TCP is. One action is one synchronous step  *)
(* of Session, which runs on one fiber. Packet identifiers are not        *)
(* modeled: each direction has one message.                               *)
(***************************************************************************)
EXTENDS Naturals, Sequences

CONSTANTS
    MaxDrops,         \* how many times the connection may drop
    BrokerMayForget   \* whether a reconnect can come back with sessionPresent = 0

VARIABLES
    \* Client: src/session.ts
    out,        \* your message's InFlight entry: "none" (not yet published), "Unacked", "Released",
                \* "done" (PUBCOMP resolved the caller), "dropped" (interrupted before written) or
                \* "unknown" (failed with DeliveryUnknown: written, then the broker lost the session)
    written,    \* InFlight.written
    received,   \* Session.received holds the broker's message id
    delivered,  \* times the deliver callback got the broker's message
    \* Broker, per MQTT 3.1.1 section 4.3.3 (method B: deliver on PUBLISH, hold the id until PUBREL)
    held,       \* the broker holds your message's id, waiting for PUBREL
    onward,     \* times the broker delivered your message to its subscribers
    sending,    \* the broker's message: "none", "Unacked", "Released", "done", or "lost" when the
                \* broker forgot the session before finishing it
    \* Network
    up,         \* a connection is attached
    drops,      \* drops so far
    toBroker,   \* packets in flight from client to broker
    toClient    \* packets in flight from broker to client

vars == <<out, written, received, delivered, held, onward, sending, up, drops, toBroker, toClient>>

Init ==
    /\ out = "none" /\ written = FALSE
    /\ received = FALSE /\ delivered = 0
    /\ held = FALSE /\ onward = 0
    /\ sending = "none"
    /\ up = TRUE /\ drops = 0
    /\ toBroker = << >> /\ toClient = << >>

Send(p) == toBroker' = Append(toBroker, p)
Reply(p) == toClient' = Append(toClient, p)

(* -------------------------- client: session.ts -------------------------- *)

\* publishAcknowledged, session.ts 70-88: the PUBLISH goes out only on an accepting link.
YouPublish ==
    /\ out = "none"
    /\ out' = "Unacked"
    /\ written' = up
    /\ IF up THEN Send("PUBLISH") ELSE UNCHANGED toBroker
    /\ UNCHANGED <<received, delivered, held, onward, sending, up, drops, toClient>>

\* The onInterrupt at session.ts 82-86: an unwritten entry is removed; a written one must finish.
YouGiveUp ==
    /\ out = "Unacked" /\ ~written
    /\ out' = "dropped"
    /\ UNCHANGED <<written, received, delivered, held, onward, sending, up, drops, toBroker, toClient>>

\* receive, Pubrec, session.ts 190-199: mark Released if waiting, and answer PUBREL in any case.
ClientGetsPubrec ==
    /\ toClient /= << >> /\ Head(toClient) = "PUBREC"
    /\ toClient' = Tail(toClient)
    /\ out' = IF out = "Unacked" THEN "Released" ELSE out
    /\ Send("PUBREL")
    /\ UNCHANGED <<written, received, delivered, held, onward, sending, up, drops>>

\* receive, Pubcomp -> finish, session.ts 201-202 and 255-262: finish does not check the stage.
ClientGetsPubcomp ==
    /\ toClient /= << >> /\ Head(toClient) = "PUBCOMP"
    /\ toClient' = Tail(toClient)
    /\ out' = IF out \in {"Unacked", "Released"} THEN "done" ELSE out
    /\ UNCHANGED <<written, received, delivered, held, onward, sending, up, drops, toBroker>>

\* incoming, session.ts 247-252: deliver unless the id is held, then PUBREC.
ClientGetsPublish ==
    /\ toClient /= << >> /\ Head(toClient) = "PUBLISH"
    /\ toClient' = Tail(toClient)
    /\ IF received
          THEN UNCHANGED <<received, delivered>>
          ELSE /\ received' = TRUE
               /\ delivered' = delivered + 1
    /\ Send("PUBREC")
    /\ UNCHANGED <<out, written, held, onward, sending, up, drops>>

\* receive, Pubrel, session.ts 183-187.
ClientGetsPubrel ==
    /\ toClient /= << >> /\ Head(toClient) = "PUBREL"
    /\ toClient' = Tail(toClient)
    /\ received' = FALSE
    /\ Send("PUBCOMP")
    /\ UNCHANGED <<out, written, delivered, held, onward, sending, up, drops>>

(* ------------------------------- broker -------------------------------- *)

BrokerGetsPublish ==
    /\ toBroker /= << >> /\ Head(toBroker) = "PUBLISH"
    /\ toBroker' = Tail(toBroker)
    /\ IF held
          THEN UNCHANGED <<held, onward>>
          ELSE /\ held' = TRUE
               /\ onward' = onward + 1
    /\ Reply("PUBREC")
    /\ UNCHANGED <<out, written, received, delivered, sending, up, drops>>

BrokerGetsPubrel ==
    /\ toBroker /= << >> /\ Head(toBroker) = "PUBREL"
    /\ toBroker' = Tail(toBroker)
    /\ held' = FALSE
    /\ Reply("PUBCOMP")
    /\ UNCHANGED <<out, written, received, delivered, onward, sending, up, drops>>

BrokerSends ==
    /\ up /\ sending = "none"
    /\ sending' = "Unacked"
    /\ Reply("PUBLISH")
    /\ UNCHANGED <<out, written, received, delivered, held, onward, up, drops, toBroker>>

BrokerGetsPubrec ==
    /\ toBroker /= << >> /\ Head(toBroker) = "PUBREC"
    /\ toBroker' = Tail(toBroker)
    /\ sending' = IF sending = "Unacked" THEN "Released" ELSE sending
    /\ Reply("PUBREL")
    /\ UNCHANGED <<out, written, received, delivered, held, onward, up, drops>>

BrokerGetsPubcomp ==
    /\ toBroker /= << >> /\ Head(toBroker) = "PUBCOMP"
    /\ toBroker' = Tail(toBroker)
    /\ sending' = IF sending = "Released" THEN "done" ELSE sending
    /\ UNCHANGED <<out, written, received, delivered, held, onward, up, drops, toClient>>

(* ------------------------------- network ------------------------------- *)

\* Connection.lost, then client.ts 488-490 detaches the session. Unread packets are gone.
ConnectionDrops ==
    /\ up /\ drops < MaxDrops
    /\ up' = FALSE /\ drops' = drops + 1
    /\ toBroker' = << >> /\ toClient' = << >>
    /\ UNCHANGED <<out, written, received, delivered, held, onward, sending>>

\* client.ts 468-472, then attach, session.ts 129-153. With sessionPresent 0 a written entry
\* fails with DeliveryUnknown and is not resent; an unwritten one goes out for the first time.
\* Otherwise every unfinished exchange is resent. The broker resends its own per section 4.4.
\* A broker that forgets the session forgets both directions.
Reconnects ==
    /\ ~up
    /\ \E present \in IF BrokerMayForget THEN BOOLEAN ELSE {TRUE}:
        LET brokerSending == IF present \/ sending \notin {"Unacked", "Released"} THEN sending ELSE "lost"
            lostOut == ~present /\ written /\ out \in {"Unacked", "Released"}
            clientResend == CASE lostOut          -> << >>
                              [] out = "Released" -> << "PUBREL" >>
                              [] out = "Unacked"  -> << "PUBLISH" >>
                              [] OTHER            -> << >>
            brokerResend == CASE brokerSending = "Released" -> << "PUBREL" >>
                              [] brokerSending = "Unacked"  -> << "PUBLISH" >>
                              [] OTHER                      -> << >>
        IN /\ up' = TRUE
           /\ out' = IF lostOut THEN "unknown" ELSE out
           /\ received' = (received /\ present)
           /\ held' = (held /\ present)
           /\ sending' = brokerSending
           /\ written' = IF lostOut THEN written ELSE (written \/ out \in {"Unacked", "Released"})
           /\ toBroker' = clientResend
           /\ toClient' = brokerResend
    /\ UNCHANGED <<delivered, onward, drops>>

\* Nothing left to happen: both exchanges ended and nothing is in flight.
Done ==
    /\ out \in {"done", "dropped", "unknown"} /\ sending \in {"done", "lost"}
    /\ toBroker = << >> /\ toClient = << >>
    /\ UNCHANGED vars

Next ==
    \/ YouPublish \/ YouGiveUp
    \/ ClientGetsPubrec \/ ClientGetsPubcomp \/ ClientGetsPublish \/ ClientGetsPubrel
    \/ BrokerGetsPublish \/ BrokerGetsPubrel \/ BrokerSends \/ BrokerGetsPubrec \/ BrokerGetsPubcomp
    \/ ConnectionDrops \/ Reconnects
    \/ Done

Spec == Init /\ [][Next]_vars

(* -------------------------------- rules -------------------------------- *)

\* Your QoS 2 message reaches the broker's subscribers at most once.
YourMessageAtMostOnce == onward <= 1

\* When publish succeeds, the broker has delivered your message.
DoneMeansDelivered == out = "done" => onward >= 1

\* A publish interrupted before it was written is never delivered and never sent.
GivenUpMeansNeverSent == out = "dropped" => onward = 0 /\ \A i \in 1..Len(toBroker): toBroker[i] /= "PUBLISH"

\* DeliveryUnknown only for a message that may have reached the broker.
UnknownOnlyWhenWritten == out = "unknown" => written

\* The broker's QoS 2 message reaches your subscription at most once.
BrokersMessageAtMostOnce == delivered <= 1

\* When the broker finishes sending, your subscription got the message.
BrokerDoneMeansReceived == sending = "done" => delivered >= 1

TypeOK ==
    /\ out \in {"none", "Unacked", "Released", "done", "dropped", "unknown"}
    /\ sending \in {"none", "Unacked", "Released", "done", "lost"}
    /\ drops \in 0..MaxDrops
=============================================================================
