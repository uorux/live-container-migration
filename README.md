# live-container-migration (lcm)

Live migration of running pods between Kubernetes nodes, using CRIU. Built for moving a
Minecraft server (vanilla, Paper, Fabric, NeoForge) between k3s nodes while players stay
connected, but nothing in it is Minecraft-specific: any Linux process tree with TCP connections
can move.

What clients see: a freeze, not a disconnect. In the lab (Fabric/NeoForge, 1.5 GiB heap, 3
bots, 50 MB/s link between nodes) the workload is stopped for 0.7-1.0 s per migration.

## How it works

```
 players ──► lcm-gateway (stable L4 endpoint, never moves)
                 │  resumable tunnel
                 ▼
          ┌─ pod on node A ─────────┐            ┌─ pod on node B ─────────┐
          │ lcm-relay (PID 1)       │  CRIU      │ lcm-relay (restored)    │
          │  └─ server on loopback  │ ────────►  │  └─ server (restored)   │
          └─────────────────────────┘  images    └─────────────────────────┘
          lcm-agent (node A) ───── pre-copy / final dump ────► lcm-agent (node B)
```

1. `lcm migrate` creates the destination pod (same spec, `runtimeClassName: lcm`) on the target
   node, parked in a wait init container. Kubernetes sets up its sandbox, IP, volumes and images
   before anything is frozen.
2. The source node's `lcm-agent` copies memory while the workload runs (CRIU pre-dumps,
   zstd-compressed, iterated until the dirty set is small). Meanwhile the destination agent
   opens the workload's files on shared volumes, so NFS/CephFS lock and lease recalls happen
   before the freeze.
3. The relay quiesces the tunnel; a final dump stops the process; the remaining pages, the
   container's writable layer and its addresses are shipped.
4. On the destination, the container runtime wrapper `lcm-runc` (a containerd runtime handler)
   restores the process tree into the pod's container instead of starting a fresh one. Kubelet's
   container start-up overlaps the final dump.
5. The relay reconnects the tunnel and the gateway replays anything buffered. The source pod is
   fenced (it can never start again) and deleted.

If the restore fails, the workload is restored on the source node from the same images
(rollback). The agents survive restarts mid-migration, and a dead CLI leaves nothing running
twice. See [docs/design.md](docs/design.md) for details and the failure table in
[docs/operations.md](docs/operations.md).

## Why part of it runs on the hosts

Restoring a process needs CRIU with `CAP_SYS_ADMIN`, the host's runc and containerd state, and
the ability to put the restored tree into the destination container's namespaces and cgroup.
Kubernetes has a checkpoint API but no restore API, so restore happens in the container runtime
itself (`lcm-runc`). `lcm-agent` and `lcm-runc` therefore run on each node (a NixOS module is
included). Everything else (gateway, workloads, the LivePod controller's objects) is ordinary
Kubernetes.

## Components

| | Where | What |
| --- | --- | --- |
| `lcm-agent` | systemd on every node | checkpoints, ships and stages images; fences; metrics on `:7790/metrics`; optional LivePod controller |
| `lcm-runc` | containerd runtime handler `lcm` | restores staged checkpoints on `create`; refuses fenced pods; otherwise plain runc |
| `lcm-relay` | PID 1 of a migratable container | resumable tunnel to the gateway; the server listens on loopback |
| `lcm-gateway` | Deployment | the players' stable endpoint; holds connections across migrations |
| `lcm` | CLI | `migrate`, `drain`, `status`, `history`, `recover` |
| LivePod | CRD + controller | keeps one migratable pod alive from a template (GitOps-friendly StatefulSet-of-one) |

## Requirements

- Linux nodes with identical CPU features (CRIU restores into the same instruction set);
  kernels >= 7.1 need a CRIU with the UDP-Lite removal fix (CRIU 4.3 is packaged here).
- containerd with runc (k3s works); cgroup v2.
- Pod volumes must be available on the target node: RWX (CephFS, NFS) or node-independent.
- The workload must not use hostPath volumes or devices CRIU can't restore.

## Quick start (NixOS + k3s)

```nix
# flake input: lcm.url = "github:uorux/live-container-migration";
imports = [ inputs.lcm.nixosModules.default ];
services.lcm.enable = true;   # agent, lcm-runc runtime handler, CRIU, tmpfs staging
# services.lcm.livePods.enable = true;   # optional LivePod controller
```

```sh
kubectl apply -f deploy/runtimeclass.yaml          # RuntimeClass "lcm"
kubectl apply -f deploy/homelab/lcm-test/gateway.yaml   # adapt: gateway + tunnel token Secret
# run the server with runtimeClassName: lcm and lcm-relay as PID 1:
#   deploy/examples/minecraft-livepod.yaml
lcm migrate livepod/survival -n games --to carrack   # or --to auto
lcm drain munificent      # node maintenance: move every lcm pod off a node
lcm history
```

Useful flags: `--predump-max`, `--predump-target`, `--transfer-network CIDR` (ship over a faster
network, TLS-encrypted: `services.lcm.transportTLS`), `--skip-memory-check`, `--lazy-restore` (experimental), `--json`.

## Observability

Prometheus metrics from every agent and gateway, alert rules and a Grafana dashboard
(`deploy/homelab/monitoring/`), logs shipped to VictoriaLogs, and `lcm status` / `lcm history`
on the command line. Runbook: [docs/operations.md](docs/operations.md).

## Docs

- [docs/design.md](docs/design.md): design and the reasoning behind it
- [docs/operations.md](docs/operations.md): metrics, alerts, failure behaviour, debugging
- [docs/production.md](docs/production.md): the homelab rollout and results
- [docs/tunnel-protocol.md](docs/tunnel-protocol.md): relay/gateway protocol
- [docs/on-demand.md](docs/on-demand.md): starting servers on demand behind a lobby (design)
- `lab/`: a 3-VM NixOS + k3s lab, end-to-end scripts and Minecraft bots (`tools/mcbot`)
