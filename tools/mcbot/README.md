# mcbot

Offline-mode [mineflayer](https://github.com/PrismarineJS/mineflayer) bots that keep "playing"
(random short walks and jumps near spawn, a chat line with a sequence number every 10 s) and log
what a connected player experiences, as JSON lines on stdout. It is the ground truth for
"players saw a short freeze but stayed connected" during a migration.

mineflayer 4.39.0 handles Paper 1.21.10. Its tested-version list says 1.21.9 and 1.21.11, but
1.21.10 uses the same protocol as 1.21.9 (773), and minecraft-data has a 1.21.10 entry.

## Run on node1

```sh
cp -r /lcm/tools/mcbot /root/mcbot && cd /root/mcbot     # /lcm is read-only
nix shell nixpkgs#nodejs_22 -c npm ci
nix shell nixpkgs#nodejs_22 -c node mcbot.js \
  --host <pod-or-LB-IP> --port 25565 --count 5 --prefix bot --duration 60 | tee run.jsonl
tail -1 run.jsonl          # the summary line
```

Bots join `--stagger-ms` (default 4200) apart, because Paper's `bukkit.yml`
`settings.connection-throttle` (4000 ms) kicks repeated logins from one IP. Set it to 0 on the
server, or lower `--stagger-ms`, to join faster. `--duration` counts from the `all_connected` line,
so start the migration after that line appears.

Exit code: 0 pass, 1 any disconnect, 2 other failure (a bot never connected, chat lost, duplicated
or out of order, a gap over `--fail-gap-ms`, or Ctrl-C).

Other options: `--gap-ms 500` (gap threshold), `--probe-ms 200` (RTT probe interval, 0 = off),
`--chat-ms 10000`, `--stats-ms 10000`, `--read-timeout-ms 30000`, `--fail-gap-ms 0` (off),
`--no-move`, `--no-reconnect`, `--version 1.21.10`.

## Events (`event` field; every line also has `t` (ISO time) and `ms` (since start))

| event | meaning |
|---|---|
| `connected` / `reconnected` | spawned; `connect_ms` with `tcp_ms`/`config_ms`/`login_ms` steps; `downtime_ms` on reconnect |
| `gap` | no bytes received for more than `--gap-ms`: `start`, `gap_ms`, and `client_stall_ms` (how much of the gap the bot's own event loop was stalled) |
| `stats` | every 10 s per bot: ping_request/pong RTT p50/p99/max, unanswered probes, server-measured ping (tab list), keepalive count |
| `keepalive_late` | gap between server keepalives over 2 s (Paper sends one about every second) |
| `chat_echo` | own chat line came back: `seq`, `latency_ms`, `in_order` |
| `chat_duplicate`, `chat_others_gap`, `chat_others_out_of_order` | ordering problems in own or other bots' chat |
| `server_teleport` | server corrected or teleported the bot's position (rubber-banding) |
| `position_jump` | client position moved implausibly far between 500 ms checks |
| `kicked`, `disconnect`, `read_timeout`, `error` | connection loss, with the kick reason as plain text |
| `reconnect_attempt` | the bot is reconnecting after a disconnect (this should never happen in a good run) |
| `loop_stall` | the Node event loop stalled for more than 100 ms (the bot itself was not running) |
| `bot_summary`, `summary` | per-bot totals, then one overall line with `pass` and `failures` |

## Notes

- The bot sends a `ping_request` every 200 ms, and the server answers each with a `pong_response`.
  This keeps server-to-client traffic steady, so a `gap` means a real stall, and it gives a true
  RTT. Probes sent during a freeze are answered together afterwards, which raises `rtt_avg_ms`
  and `rtt_max_ms` by about the freeze length.
- Keepalive RTT is measured by the server, not the client. The client answers at once, and the
  server's figure appears as `server_ping_ms`.
- The bot drops the connection, as the vanilla client does, after 30 s with no data
  (`--read-timeout-ms`). mineflayer's own keepalive timeout is turned off.
- After a disconnect the bot reconnects by default, so the rest of the run is still measured. Use
  `--no-reconnect` to stop there. Either way the run fails.
- All bots run in one Node process. Check that `client_max_stall_ms` in the summary stays small
  before blaming the server for a gap.
