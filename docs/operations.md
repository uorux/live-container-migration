# Operating live migration

How to see what the system is doing, what the alerts mean, and how to debug a migration.
Design background: [design.md](design.md). Rollout: [production.md](production.md).

## The pieces and where they run

| Piece | Runs as | Talks to | Logs |
| --- | --- | --- | --- |
| `lcm-agent` | systemd service on every k3s node (`services.lcm`) | other agents on node IPs :7790, runc/CRIU locally | journald `-u lcm-agent`; shipped to VictoriaLogs as `unit:lcm-agent` when `logShipping` is set |
| `lcm-runc` | executed by containerd's shim for every `runtimeClassName: lcm` container | real runc, CRIU | `/var/lib/lcm/lcm-runc.log` (lifecycle calls, restores, refusals); shipped as `unit:lcm-runc` |
| CRIU | child of runc | the kernel | per migration under `/var/lib/lcm/logs/<migration-id>/` (dumps, source node) and `/var/lib/lcm/logs/<ns_pod_ctr>/restore.log` (destination) |
| `lcm-gateway` | Deployment in the workload's namespace | players; relays | pod logs (already collected into VictoriaLogs) |
| `lcm-relay` | PID 1 of the workload container | gateway; the server on loopback | pod logs of the workload |
| `lcm` CLI | wherever you run it (a node: it reads `/run/lcm/token`) | Kubernetes API, agents | stderr |

## Quick look

```sh
lcm status            # per node: agent up, version, in-flight work, staged restores, fences, staging RAM
lcm history -limit 20    # recent migrations across nodes: outcome, blackout, total time, error
curl -s http://<gateway-admin>:8080/status | jq   # tunnel up/down, players, last outage
```

## Metrics (Prometheus text format)

`lcm-agent` (`http://<node-ip>:7790/metrics`, scraped by the `lcm-agent` VMNodeScrape):

| Metric | Meaning |
| --- | --- |
| `lcm_migrations_total{outcome}` | finished migrations reported by the CLI: success / rolled_back / aborted / failed |
| `lcm_migration_blackout_seconds` (histogram) | how long the workload was stopped; what clients feel |
| `lcm_migration_last_timestamp_seconds{outcome}` | when the last migration of each outcome happened |
| `lcm_checkpoints_total{result}` | source side: success / aborted (source kept running) / failed_after_stop |
| `lcm_checkpoint_phase_seconds{phase}` | predump_dump, predump_ship (live), final_dump, final_ship, frozen (blackout before restore) |
| `lcm_checkpoint_bytes_total{kind}` | predump / final / rootfs bytes |
| `lcm_checkpoint_predump_rounds` | pre-copy rounds per migration (many rounds = the workload dirties memory fast) |
| `lcm_restores_total{result}`, `lcm_restore_seconds{part}` | destination side, from lcm-runc |
| `lcm_staging_bytes{dir}` | checkpoint images in RAM; must return to 0 after each migration |
| `lcm_staged_restores`, `lcm_fences`, `lcm_agent_inflight_checkpoints` | node state |
| `lcm_agent_build_info{version}` | all nodes should agree (a restore needs the same relay binary everywhere) |
| `lcm_logship_lines{state}` | log lines shipped to / dropped for VictoriaLogs |

`lcm-gateway` (`/metrics` on the admin port, ServiceMonitor): tunnel state, players, the
per-outage duration histogram (the client-side view of each migration), bytes, held logins. See
the metric HELP text for the exact list.

## Alerts

| Alert | Severity | What it means / what to do |
| --- | --- | --- |
| `LcmMigrationFailed` | critical | A migration failed after the source stopped and was not rolled back: the workload may be down. `lcm history` shows the error and where the images are; `lcm recover` restores them. |
| `LcmGatewayBackendDown` | critical | The gateway has had no relay for minutes: the server is not running or can't reach the gateway. Players are held (new joins see "starting"). Check the workload pod. |
| `LcmAgentDown` | warning | An agent isn't scraped: migrations to/from that node will fail (they abort safely before the source stops). `systemctl status lcm-agent` on the node. |
| `LcmMigrationRolledBack` | warning | A restore failed on the destination; the workload was restored on the source node (players saw a ~10-15 s freeze). Read the destination's `restore.log`. |
| `LcmRestoreFailures` | warning | lcm-runc restore attempts failed (each failed attempt also shows up in `lcm-runc.log`). |
| `LcmBlackoutSlow` | warning | A migration's blackout exceeded 12 s (clients give up at 30 s). Look at `lcm_checkpoint_phase_seconds` to see which phase grew. |
| `LcmStagingNotCleaned` | warning | Checkpoint images have sat in RAM for 30 min with nothing in flight: a failed migration left them. If `lcm history` shows no failed migration needing them, delete `/var/lib/lcm/{ckpt,restore}/*` on that node. |
| `LcmAgentVersionSkew` | warning | Nodes run different lcm versions; finish the rollout before migrating. |

## Failure behaviour (what happens when things break mid-migration)

| What breaks | When | Outcome |
| --- | --- | --- |
| anything (CRIU error, network, destination agent) | before the final dump | migration aborts; the source never stopped; the relay resumes; players see nothing |
| the source `lcm-agent` restarts | before the final dump | its runc/CRIU keep running (`KillMode=process`); the restarted agent aborts the migration and lifts the fence; the CLI learns this by polling |
| the source `lcm-agent` restarts | during/after the final dump | runc finishes the dump; the restarted agent ships it and marks the destination ready; the parked destination pod restores (tested in the lab with players connected: no disconnects) |
| shipping fails | after the final dump | retried 4x; then the CLI reports where the images are (final dump on the source, pre-dumps on the destination) |
| the restore fails on the destination | after shipping | the destination pod is fenced, the images are forwarded back, the workload is restored on the source node (~10-15 s freeze) |
| the CLI dies / its connection drops | any time | nothing stops; re-run `lcm history`/`lcm status` to see the outcome; leftover destination pods carry the `lcm.rooty.dev/migration-id` annotation |

## Performance knobs

- Transfers are zstd-compressed (JVM heap pages compress 6-8x); on links slower than ~1 GB/s
  this is the single biggest factor for pre-copy convergence and the final ship.
- `--predump-max` / `--predump-target`: more rounds help only while each round shrinks.
- `--transfer-network CIDR`: ship over agent addresses in this network instead of the node
  InternalIPs (e.g. a LAN faster than the tailnet). The LAN must be allowed to reach port 7790.
- `--lazy-restore` (experimental): CRIU lazy pages; the process resumes before its memory is
  loaded. Works in the lab, but wasn't faster there than eager restores.

## Debugging a migration

1. `lcm history` — outcome and error message; the error always says which node holds what.
2. Source node: `journalctl -u lcm-agent` (phases with timings), `/var/lib/lcm/logs/<id>/work*-dump.log` (CRIU).
3. Destination node: `/var/lib/lcm/lcm-runc.log` (did containerd call create? was it refused by a
   fence? did the restore run?), `/var/lib/lcm/logs/<ns_pod_ctr>/restore.log` (CRIU).
   `tools/criu-gaps.py restore.log` shows where a slow restore spent its time.
4. Workload pod logs: the relay logs quiesce/resume and tunnel reconnects.

Common CRIU errors:

| Message | Cause |
| --- | --- |
| `File ... has bad size N (expect M)` on restore | the file changed after the dump: something else wrote it (another copy of the workload, or the source kept running). CRIU refusing is the safety net working. |
| `Can't create link remap for .../.nfsXXXX` / `Stale file handle` on dump | a deleted-but-open file on a network filesystem. Keep temp files on the container rootfs (`-Djava.io.tmpdir=/tmp -Djna.tmpdir=/tmp`). |
| `Can't bind inet socket back: Cannot assign requested address` | an outbound connection's old pod IP couldn't be aliased; check `lcm-runc.log` for the alias step. |
| `iptables-restore ... not found` | CRIU network lock without tools: the module configures `network-lock nftables`; check `/etc/criu/runc.conf`. |
| `Dumping FAILED` with `UDPLITE: -2` | CRIU older than 4.2 on a kernel >= 7.1. |

## State on a node (`/var/lib/lcm`)

| Path | What | Lifetime |
| --- | --- | --- |
| `ckpt/<migration>/` (tmpfs) | source images during a migration | deleted on success/abort; kept after a post-stop failure |
| `restore/<ns_pod_ctr>/` (tmpfs) | destination staging | deleted after success |
| `fence/<pod-uid>` | pods that must never start here again | GC after 14 days |
| `consumed/<ns_pod_ctr>` | restore tombstones (a recreated sandbox cold-starts instead of waiting) | small, kept |
| `logs/` | CRIU logs per migration | kept; prune by hand |
| `history.jsonl`, `events/restores.jsonl` | migration reports, restore events | kept |
| `bin/` | stable symlinks (lcm-runc, ip, ss, nsenter) | managed by the module |
