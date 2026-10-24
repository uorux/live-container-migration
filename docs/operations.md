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

## Node maintenance (reboot or upgrade a node without kicking players)

```sh
kubectl cordon carrack
lcm drain carrack        # live-migrates every runtimeClassName: lcm pod off it, one at a time,
                         # each to the ready node with the most available memory
kubectl drain carrack --ignore-daemonsets --delete-emptydir-data   # the rest, as usual
# ... reboot / nixos-rebuild ...
kubectl uncordon carrack
```

`lcm migrate <pod> --to auto` picks a target the same way for a single pod.

## Upgrading lcm (version skew)

Deploying a new lcm restarts `lcm-agent` only. In-flight migrations survive it (runc and CRIU
keep running; the restarted agent finishes or aborts them), and running workloads are not touched.
Upgrade every node in one go and migrate afterwards; `LcmAgentVersionSkew` fires while versions
differ.

While versions differ, migrations between agents that both negotiate (anything from 2026-10-22
on: `/v1/info` lists `"encodings"`) work as usual: a sender only compresses for receivers that
decode zstd, and the source's check that the final dump fits the destination's staging runs if
either side is new. `lcm migrate` refuses a migration involving an older agent (the version first
deployed, `version=dev` in `lcm status`), because with one of those a migration that doesn't
converge can fill the destination's staging after the workload stopped (seen in the lab). Override
with `--allow-version-skew` for small workloads; restores across versions work, and a rollback
forwards uncompressed to an old source.

## Ingresses, routing and on-demand servers

- **Several ingresses:** run an lcm-gateway next to each ingress (e.g. behind HAProxy:
  `server gw 127.0.0.1:25566 send-proxy-v2`, gateway `-accept-proxy 127.0.0.1/32` so players keep
  their addresses) and give each server's relay all of them: `lcm-relay -gateway a:7000,b:7000`.
  Migrations quiesce and resume every tunnel; each gateway holds its own players (lab: 3 bots on
  each of two gateways, 1.24 s blackout, 0 disconnects, both tunnels resumed).
- **Many servers behind one gateway:** `-minecraft -backend lobby -backends atm9,atm10`; players
  reach `atm10.<domain>`. Each relay sets `-backend <name>`.
- **Server menu instead of a lobby:** add `-menu` (plus `-menu-title`, `-label atm9=All the Mods
  9`). The main address then shows a picker (1.21.6+ clients) that wakes the chosen server and
  transfers the player to it; pack hostnames keep working directly. See
  [on-demand.md](on-demand.md) "Server menu". Metrics: `lcm_gateway_menu_total{result}` and
  `lcm_gateway_menu_transfers_total{backend}`.
- **On demand:** `spec.onDemand` on the LivePod plus `-wake-url`/`-wake-token-file` on the
  gateways; see [on-demand.md](on-demand.md) "Wake and idle stop". Give the pod a
  `terminationGracePeriodSeconds` longer than the world save.
  The wake token needs no distribution: with `livePods.enable` (and the default
  `deriveTokenFromK3s`) every node derives it from the k3s token into `/run/lcm/wake-token`, and
  `services.lcm.livePods.wakeTokenSecrets = [ "ns/name" ]` makes the leading controller keep it
  in those Secrets (key `token`) for gateways in the cluster. Gateways outside the cluster get
  a copy of `/run/lcm/wake-token` once.
- **Who may wake a server:** without `-whitelist`, any login attempt wakes the server it names,
  so a stranger could wake every pack and fill the cluster's memory. `-whitelist
  'backend=file[+file],*=file'` (env `LCM_WHITELIST`; vanilla `whitelist.json`/`ops.json` files,
  re-read within seconds of a change) makes the gateway refuse a login to an *asleep* backend
  from anyone not listed, with the vanilla "not white-listed" message, before anything wakes. The
  menu does the same with a notice. Running servers are untouched: they check for themselves.
  Login names aren't authenticated, so this stops scanners and bots, not someone who knows a
  listed name. Refusals count as `lcm_gateway_player_connections_total{result="not_whitelisted"}`.
  An unreadable list allows nobody, and an empty one is logged at startup.

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
| `lcm_agent_build_info{version}` | all nodes should agree outside a rollout (see "Upgrading lcm") |
| `lcm_logship_lines{state}` | log lines shipped to / dropped for VictoriaLogs |
| `lcm_livepod_running{namespace,livepod,node,phase}` | per LivePod, from the leading controller: 1 if its pod is Running |
| `lcm_livepod_controller_leader` | 1 on the agent leading the LivePod controller |

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
| `LcmLivePodNotRunning` | critical | A LivePod has had no Running pod for 10 min (paused ones excepted). `kubectl get livepod` shows its pod and phase. A pod stuck on a dead node blocks a replacement by design; force-delete it once the node is really gone. A fenced source after a failed migration needs `lcm recover`. |
| `LcmLivePodControllerAbsent` | warning | No agent leads the LivePod controller (Lease `kube-system/lcm-livepod-controller`): LivePods won't be recreated. Check lcm-agent logs. |

## Failure behaviour (what happens when things break mid-migration)

| What breaks | When | Outcome |
| --- | --- | --- |
| anything (CRIU error, network, destination agent) | before the final dump | migration aborts; the source never stopped; the relay resumes; players see nothing |
| the source `lcm-agent` restarts | before the final dump | its runc/CRIU keep running (`KillMode=process`); the restarted agent aborts the migration and lifts the fence; the CLI learns this by polling |
| the source `lcm-agent` restarts | during/after the final dump | runc finishes the dump; the restarted agent ships it and marks the destination ready; the parked destination pod restores (tested in the lab with players connected: no disconnects) |
| the destination's staging can't hold the final dump (pre-copy didn't converge) | right before the final dump | aborted; the source keeps running (the source asks the destination's free staging first) |
| shipping fails | after the final dump | retried for 90 s (e.g. a destination agent restart); then the CLI reports where the images are (final dump on the source, pre-dumps on the destination) for `lcm recover` |
| the restore fails on the destination | after shipping | the destination pod is fenced, the images are forwarded back, the workload is restored on the source node (~10-15 s freeze) |
| the CLI dies / its connection drops | any time | nothing stops; the agents reclaim RAM staging of migrations that completed; with a LivePod, the controller deletes the fenced source pod 2 min after its successor is Ready (otherwise delete it by hand); leftover destination pods carry the `lcm.rooty.dev/migration-id` annotation |

## How a migration hides its latency

- Pre-copy: memory is copied while the workload runs; only the last dirty pages are dumped
  while it is frozen. Transfers are zstd-compressed.
- The destination pod is created and its images pulled before anything is dumped.
- While pre-copy runs, the destination agent opens (and closes) the files the workload holds
  open on shared volumes, so NFS delegation / CephFS cap recalls don't happen inside CRIU's
  restore (each first open from a new node used to cost ~100 ms on NFS).
- Just before the final dump the source marks the staging `FINAL`: the parked destination pod
  leaves its wait init container and kubelet starts the container, whose `lcm-runc create`
  waits for `READY`. Kubelet's ~1-2 s of start-up overlaps the dump and transfer.

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
| `ckpt/<migration>/` (tmpfs) | source images during a migration | deleted on success/abort; if the CLI died, the agent deletes it 10 min after shipping; kept after a post-stop failure |
| `restore/<ns_pod_ctr>/` (tmpfs) | destination staging | deleted after success; if the CLI died, the agent drops the images 1 min after a successful restore |
| `fence/<pod-uid>` | pods that must never start here again | GC after 14 days |
| `consumed/<ns_pod_ctr>` | restore tombstones (a recreated sandbox cold-starts instead of waiting) | small, kept |
| `logs/` | CRIU logs per migration | kept; prune by hand |
| `history.jsonl`, `events/restores.jsonl` | migration reports, restore events | kept |
| `bin/` | stable symlinks (lcm-runc, ip, ss, nsenter) | managed by the module |
