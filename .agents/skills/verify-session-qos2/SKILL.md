---
name: verify-session-qos2
description: Model-check QoS 2 delivery across dropped connections with TLA+ (formal/session): at most once each way, publish success means delivered, an interrupted unsent publish is never sent, DeliveryUnknown only for a written publish, nothing waits forever. Use when a change touches Session in src/session.ts (attach, detach, receive, incoming, finish, publishAcknowledged, close) or the reconnect loop in src/client.ts that attaches and detaches it.
---

# Verify session QoS 2

`formal/session/QoS2Session.tla` models the QoS 2 handshakes of `src/session.ts` while the connection drops and the
broker may or may not keep the session. Read [formal/session/README.md](../../../formal/session/README.md) for the
seven rules, their harm, the bounds, and the fixed finding. Read every TLC result with the `/statechart` skill: it
gives each broken rule as its shortest path in plain words and draws the chart.

## Coverage

The model describes this code at the sync commit.

Sync commit: `37c839a1340f83ae90c7f0181728b5d57c0a11e0`

| File | Functions and lines at the sync commit | Model action |
| --- | --- | --- |
| `src/session.ts` | `publishAcknowledged` 70-88 | `YouPublish`, `YouGiveUp` |
| `src/session.ts` | `attach` 129-153 | `Reconnects` (client side) |
| `src/session.ts` | `receive` Pubrel 183-187, Pubrec 190-199, Pubcomp 201-202 | `ClientGetsPubrel`, `ClientGetsPubrec`, `ClientGetsPubcomp` |
| `src/session.ts` | `incoming` 237-253 | `ClientGetsPublish` |
| `src/session.ts` | `finish` 255-262 | `ClientGetsPubcomp` |
| `src/client.ts` | `run` 486-490 (detach on `connection.lost`), 468-472 (attach after CONNACK) | `ConnectionDrops`, `Reconnects` |

The broker actions follow MQTT 3.1.1 section 4.3.3, not code in this repo.

## Check a change

1. See what covered code changed since the sync commit, including uncommitted edits:

   ```sh
   git diff --stat 37c839a1340f83ae90c7f0181728b5d57c0a11e0 -- src/session.ts src/client.ts
   ```

   If nothing changed, go to step 3.

2. Read each changed function against its model action in the table. Read the code, not the commit message. A
   change is covered when the model still does what the code does in one synchronous step: what is written to the
   link, what `inFlight`, `written` and `received` hold after, and when a caller's Deferred completes. Change the
   model where the code changed on purpose, and update the line numbers in this table and in the `.tla` comments.

3. Run the check. It needs `tla2tools.jar` 1.7.4 (SHA-256
   `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`, from the tlaplus v1.7.4 release) and Java 11
   or later:

   ```sh
   export TLA2TOOLS=/path/to/tla2tools.jar JAVA=/path/to/java
   bun .agents/skills/verify-session-qos2/scripts/check.ts check     # about 2 s
   bun .agents/skills/verify-session-qos2/scripts/check.ts mutants   # about 6 s
   ```

   `check` passes when both `SessionKept.cfg` and `SessionForgotten.cfg` hold every rule. `mutants` first refuses
   (`REFUSED`) if the model is already broken, then applies nine code changes to the model, each written as the
   matching change to `src/session.ts`, and requires TLC to name the rule each one breaks. Every rule, `NoDeadlock`
   included, has a mutant; one puts back the resend that caused the fixed finding.

4. For a picture or a new break, run `/statechart` on `formal/session/QoS2Session.tla` with the config, the view
   `formal/session/QoS2Session.view.mjs`, and a fresh output folder. When it changes the chart, copy `chart.mmd` over
   the config's `.mmd` file.

5. Only after confirming the model describes the code at a new commit, move the sync commit forward.

## Tell a bug from a modeling error

A new broken rule is a bug in the code until shown otherwise. Replay its path against the real client, one step per
action, as `scripts/replay-duplicate.ts` does for the fixed finding with `serve()` as the broker
(`bun .agents/skills/verify-session-qos2/scripts/replay-duplicate.ts` exits 1 if the duplicate comes back). If the
path cannot happen in the real code, the model is wrong: fix the model, never the rule.

## Fixed finding

Before 0.6.0, with `sessionPresent = 0`, `attach` resent an unfinished QoS 2 PUBLISH, and the broker delivered it a
second time. Now a written publish fails with `DeliveryUnknown` instead. See the README. The tests that pin it are
"fails a written QoS %i publish with DeliveryUnknown" in `test/session.test.ts` and `test/client.test.ts`.

## Not covered

Packet identifiers and their reuse, two exchanges at once, QoS 0 and 1, SUBSCRIBE and UNSUBSCRIBE, keep-alive,
backpressure, `close`, the server in `src/server.ts`, and anything past two drops.
