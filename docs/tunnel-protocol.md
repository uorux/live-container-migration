# lcm tunnel protocol (v1)

The link between `lcm-gateway` (stable pod, holds the players' TCP connections) and `lcm-relay`
(inside the migrating Minecraft container). See [design.md](design.md) §2 for why it exists.
Code: `internal/wire` (encoding), `internal/tunnel` (session engine), `internal/gateway`,
`internal/relay`.

## Model

- The relay **dials** the gateway. One TCP connection (the *tunnel*) carries many *streams*.
  Each stream is one player connection. The relay bridges it to a loopback TCP connection to
  the server and writes a PROXY v2 header first.
- A *session* outlives tunnel connections. Its state (streams, send buffers, offsets) lives in
  memory on both sides. On the relay, that memory goes into the CRIU image. The relay picks a
  random 128-bit session id when it starts.
- Each stream direction is a byte sequence numbered from offset 0. The sender keeps every byte
  until the receiver acknowledges it. After any reconnect, each side sends again from the
  offset the peer says it has received. Duplicates are dropped by offset, so the byte stream
  to the player and to the server is exact.
- One gateway serves one session per *backend* (one Minecraft server). The relay names its
  backend in HELLO. Today there is a single backend called `default`. The gateway code keeps
  a backend → session map, so more backends can be added later.

## Framing

Every frame has a 9-byte header followed by a payload. All integers are big-endian.

```
type u8 | stream u32 | length u32 | payload[length]       (length <= 1 MiB)
```

Stream 0 carries connection-level frames. Stream ids are allocated only by the gateway, in
increasing order, and are never reused within a gateway process.

| type | name | dir | payload |
|---|---|---|---|
| 1 | CHALLENGE | G→R | `"LCMT"` · version u16 · nonce[32] |
| 2 | HELLO | R→G | version u16 · session[16] · backend (u8 len + bytes) · window u32 · max_seen u32 · entries · mac[32] |
| 3 | HELLO_ACK | G→R | status u8 · window u32 · message (u16 len + bytes) · entries · mac[32] |
| 4 | OPEN | G→R | src addr · dst addr (each: family u8 {0,4,6} · ip[4/16] · port u16) |
| 5 | DATA | both | offset u64 · bytes (≤ 16 KiB per frame) |
| 6 | ACK | both | offset u64 · flags u8 (bit0 FIN: the peer's CLOSE has been received and applied) |
| 7 | CLOSE | both | fin_offset u64 (the sender writes no bytes at or after this offset) |
| 8 | RESET | both | code u8 · message (u16 len) |
| 9/10 | PING/PONG | both | u64 echo value |
| 11 | GOAWAY | both | flags u8 (bit0 FINAL) · message (u16 len) |

`entries` is a u32 count followed by that many `{id u32, rx_offset u64, flags u8 (bit0 FIN received)}`.

## Handshake and authentication

1. The gateway accepts the connection and sends CHALLENGE with a fresh random nonce.
2. The relay sends HELLO. Its `mac` is `HMAC-SHA256(token, "lcm-hello-v1" ‖ nonce ‖ HELLO body)`.
   The token is a shared secret, set with `-token`, `-token-file`, `LCM_TOKEN` or
   `LCM_TOKEN_FILE`. The token never crosses the wire, and a captured HELLO cannot be replayed
   because the nonce changes on every connection.
3. The gateway checks the MAC, version and backend, reconciles state (see below), and answers
   with HELLO_ACK. Its `mac` is `HMAC(token, "lcm-hello-ack-v1" ‖ nonce ‖ session ‖ body)`, so
   the relay authenticates the gateway too. Status values: 0 = RESUMED, 1 = NEW, 2 = REJECTED
   (the reason is in `message`; the gateway then closes the connection).
4. Neither side sends anything else until the handshake has finished. The whole handshake has
   a 10 s deadline.

`window` in HELLO and HELLO_ACK is the most unacknowledged bytes per stream the *peer* may send
to the advertiser.

## Reconciliation on (re)connect

HELLO carries the relay's view. For every stream it still has: `rx_offset` (bytes received from
the gateway, including bytes queued but not yet written to the server) and FIN-received. It also
carries `max_seen`, the highest stream id whose OPEN the relay has processed.

On the gateway, under one lock:

- **Different session id, or the gateway has never seen one** → status NEW. Every stream that
  ever had an OPEN sent is reset: those players are disconnected, because the server they
  talked to is gone. Streams never opened (players who arrived while no relay was attached) are
  kept and opened on the new session. The new id is adopted.
- **Same session** → RESUMED. For each gateway stream:
  - id > `max_seen`, or no OPEN ever sent → the relay never saw it. Send OPEN again and start
    from offset 0. The gateway still holds those bytes, because the relay cannot have
    acknowledged anything on a stream it never saw.
  - id ≤ `max_seen` and listed by the relay → set `tx_next = relay.rx_offset`. This must lie
    between the last ACK received and the end of the buffer; otherwise the stream is reset.
    Mark FIN as delivered if the relay has it. List the stream in HELLO_ACK with the gateway's
    `rx_offset`.
  - id ≤ `max_seen` and *not* listed → the relay closed it while the two sides were apart.
    Deliver anything queued to the player, then close the player connection.
- Any older tunnel connection still attached to the session is closed and replaced.

On the relay, applying HELLO_ACK:

- NEW → drop all streams, closing the loopback connections, and reset `max_seen` to 0. This
  happens when the gateway restarted and lost the session.
- RESUMED → each listed stream sets `tx_next = gateway.rx_offset` (checked the same way). Any
  stream not listed is closed: the player is gone. Any listed stream the relay no longer has
  gets a RESET.
- Both sides then send a fresh ACK for every stream. The ACK sent before the drop may have been
  lost.

Nothing changes a session's offsets while no tunnel is attached, because frames from a detached
connection are ignored under the session lock. So the views exchanged in the handshake stay
consistent.

## Data, acknowledgement, flow control

- **Sender state per direction:** `tx_base` (first unacknowledged byte), `tx_next` (next byte to
  send on this connection) and `tx_end` (end of the bytes read from the local socket).
  DATA is sent while `tx_next - tx_base < peer window`.
- **The local reader stops reading** once `tx_end - tx_base` reaches its own `-window`
  (default 1 MiB). That is the per-stream buffer cap. During an outage, the player's TCP
  connection is no longer read, and TCP backpressure pushes back on the player. Nothing is
  dropped, and the gateway never times out a player.
- **The receiver ACKs the offset it has *delivered*,** meaning written to its local socket. So
  its queue of undelivered bytes never exceeds the window. A peer that overruns the window gets
  RESET.
- **ACKs are batched.** One is sent once 64 KiB (or window/4) has been delivered since the last
  ACK, or 5 ms after the first unacknowledged delivery, and immediately when a FIN is applied.
- **The writer is pull-based.** Each batch, in this order: queued control frames, all pending
  OPENs in id order, ACKs, then DATA round-robin across streams (16 KiB chunks, up to 256 KiB
  per batch), then CLOSE/RESET after a stream's last byte. Resuming only rewinds `tx_next`.
  The same loop then sends the gap again.

## Closing

- **Half-close:** local EOF → CLOSE{fin_offset}, sent after all the data. The receiver
  delivers everything, calls `shutdown(SHUT_WR)` on its local socket, and ACKs with the FIN
  flag. A stream is freed once its own FIN has been ACKed and the peer's FIN has been applied
  and ACKed.
- **Local socket failure** (RST, write error): the bytes already read from that socket are
  still sent, then RESET (so a kick message sent just before the server's close still
  arrives). The receiver delivers its queue and then closes.
- **Graceful local closes** use `shutdown(SHUT_WR)`, then drain input until EOF (at most 10 s),
  then `close()`. A plain `close()` with unread input makes Linux send RST and throw away bytes
  still in the send buffer. Explicit aborts use `SO_LINGER=0`.
- **Upstream dial failure** on the relay → RESET (code 1) → the player is disconnected.

## Readiness (relay) and Minecraft mode (gateway)

Neither changes the wire protocol.

- On a fresh start the relay dials the gateway only after the server is ready (`-wait-ready`, on
  by default: TCP connect to `-upstream`; with `-minecraft-ready`: a status ping answered). Players
  who arrive earlier stay queued in the gateway as never-opened streams and are opened when the
  session attaches (NEW keeps them, see Reconciliation). A restored relay has already passed the
  check and reconnects at once.
- `lcm-gateway -minecraft` inspects the first packets of each player connection before calling
  `AddStream`, replays them byte-exact, answers status locally while no tunnel is attached, and
  keeps login streams that have not yet received a server byte alive with no-op packets. See
  [on-demand.md](on-demand.md) §6.

## Liveness, GOAWAY, quiesce

- **Heartbeat:** both sides send PING every 2 s (`-ping-interval`). A connection that has
  received no frame at all for 10 s (`-dead-timeout`) is closed, and the relay reconnects. The
  relay backs off 100 ms → 2 s with jitter, but reconnects at once after a connection that was
  up.
- **GOAWAY** (R→G) is sent after any pending frames. The relay then half-closes, and the
  gateway closes the connection. A non-final GOAWAY (quiesce) means "hold the players, I'll be
  back". A **FINAL** GOAWAY (the server process exited) makes the gateway deliver anything
  queued and close every opened stream of the session. Players who have not been opened yet
  stay queued for the next session.
- **Quiesce, via the control socket** (`/run/lcm/relay.sock`, one command per line):
  - `quiesce` → GOAWAY and close the tunnel, stop reconnecting, wait for in-flight loopback
    dials, then check `/proc/self/fd` against `/proc/self/net/{tcp,tcp6,udp,udp6}` until no
    non-loopback socket remains. Reply `ok`, then close the control connection. The agent
    should wait for EOF before dumping, because EOF means the relay's end of the unix socket is
    gone. Errors are reported as `error <msg>`.
  - `resume` → `ok`, and a reconnect starts at once. Poll `status` to see `"connected": true`.
  - `status` → one line of JSON (session, state, connected, streams, counters, child_pid, ready).
  - If no `resume` arrives within `-quiesce-timeout` (120 s), the relay resumes by itself.
    Under CRIU's time namespace, the frozen period does not count toward this.

## CRIU constraints honoured by the relay

- It never listens on TCP. Dialers call `SetMultipathTCP(false)`, and `//go:debug
  multipathtcp=0` is set as well. Verified with strace: sockets are `IPPROTO_IP`, never MPTCP.
- The child is started with `syscall.ForkExec` (plain `clone(CLONE_VFORK|CLONE_VM)`, no
  `CLONE_PIDFD`) and reaped with `wait4(-1, WNOHANG)` on SIGCHLD, plus a 1 s poll as a safety
  net. strace shows no `pidfd_*`, `io_uring` or `inotify` calls.
- It runs as PID 1. It forwards TERM, INT, HUP, QUIT, USR1 and USR2 to the child, reaps
  orphans, and exits with the child's status (128+signal if the child was killed).
