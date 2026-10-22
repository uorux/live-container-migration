# Live container migration on k3s + Cilium — design

Goal: move a running Paper (Minecraft Java) server pod from one k3s node to another while
players stay connected. They may see a lag spike (target: < 5 s, hard ceiling ~20 s, the client
read timeout), but no disconnect and no lost world state.

Research behind this: [docs/research/](research/).

## Cluster facts that shape the design (checked 2026-10-22)

- k3s v1.36.5, embedded containerd 2.3.4, runc; NixOS 26.11, kernel 7.2.11; 3 nodes
  (arquitens, carrack, munificent), all control-plane, 14 vCPU / 45 GiB each, node IPs on Tailscale.
- Cilium 1.20.2: `ipam: cluster-pool` (10.42.0.0/16, /24 per node), `routing-mode: tunnel` (VXLAN),
  kube-proxy replacement, LB-IPAM + L2 announcements, source-IP verification on.
- Storage: Rook-Ceph RBD (`ceph-block`, RWO) and CephFS (`ceph-filesystem`, RWX).
- nixpkgs CRIU (4.1.1) cannot dump anything on kernels >= 7.1 (UDP-Lite removal, criu#3002);
  we ship CRIU 4.3 via [nix/overlay.nix](../nix/overlay.nix).

## The three hard problems and the chosen answers

### 1. Getting a CRIU image restored into a pod on another node

Kubernetes can checkpoint (kubelet `POST /checkpoint/...`, beta, on by default) but has no restore
API. containerd's "restore via CreateContainer with a checkpoint image" exists in 2.3.x but is off
by default, needs an image build + pull inside the blackout, and is gone in containerd 2.4.

**Answer: a RuntimeClass whose handler is a thin runc wrapper (`lcm-runc`).**
containerd's runc shim already knows how to restore (it runs `runc restore` when a task is created
with a checkpoint); we just get there from the runtime side instead of the CRI side:

- The destination pod is created ahead of time with `runtimeClassName: lcm` and an init container
  that blocks until the checkpoint has landed on the destination node. The pod sandbox (netns,
  Cilium endpoint, IP), image pull and volume mounts all happen *before* the freeze.
- When kubelet then creates the main container, `lcm-runc create` sees a pending restore for this
  `<namespace>/<pod>/<container>` and runs `runc restore --detach` into the bundle containerd
  prepared (new netns, new cgroup, same image + rootfs diff), writing the pid file containerd
  expects. `lcm-runc start` for a restored container is a no-op. Everything else is passed to the
  real runc unchanged, so non-migrating pods using the class behave normally.
- Source side: the node agent drives `runc checkpoint` directly (not the kubelet API) so it can do
  iterative `--pre-dump` with memory tracking and stream pages, then a short final dump.

### 2. Keeping players connected when the pod IP changes

Cilium cannot move a pod IP between nodes (cilium#38576), and CRIU's TCP repair needs the original
local IP. Rather than fight the CNI, players never hold a TCP connection to the migrating pod:

```
player ──TCP──▶ LB IP (Cilium LB-IPAM/L2) ──▶ lcm-gateway (stable pod, not migrated)
                                                  │  resumable session (own framing, seq/ack)
                                                  ▼
                              ┌──────── migrating container ────────┐
                              │ lcm-relay ──loopback TCP──▶ Paper   │
                              └─────────────────────────────────────┘
```

- `lcm-relay` runs inside the Paper container (it is the entrypoint and execs/supervises Java), so
  it is in the same CRIU dump. Its connections to Paper are loopback (`127.0.0.1` exists in every
  netns), which CRIU restores with `--tcp-established`.
- `lcm-relay` dials out to `lcm-gateway`; each player connection is a stream in a session with
  byte sequence numbers and acks, and both sides keep unacked bytes buffered.
- Just before the final dump the agent tells the relay to quiesce: it closes its upstream links
  (so the dump has no non-loopback TCP), and the gateway holds players' bytes. After restore the
  relay reconnects, the two sides exchange last-received offsets, and resend the gap. The byte
  stream to Paper is exact, so encryption/compression are unaffected.
- The real client address reaches Paper via PROXY protocol v2 (`proxies.proxy-protocol: true`).
- The gateway is the one non-migratable piece; losing its node disconnects players, which is no
  worse than today's single pod. It is small and stateless apart from in-flight buffers.

### 3. Doing it fast enough

Blackout = final dump (dirty pages since last pre-dump) + transfer + restore + session resume.

- World data lives on CephFS (RWX), mounted on both nodes, so nothing about the world is copied
  in the blackout; CRIU reopens files by path and checks sizes, which match because the source is
  frozen and the filesystem is shared. (RBD would need detach/attach inside the blackout.)
- Memory goes ahead of time: repeated `runc checkpoint --pre-dump` with soft-dirty tracking,
  streamed to the destination agent, until the dirty set stops shrinking; then the final dump only
  carries the last delta. Lazy pages (userfaultfd) is a later optimisation.
- CRIU restores into a fresh time namespace, so the JVM's `System.nanoTime()` only advances by the
  blackout length: Paper's watchdog (60 s), keepalive (30 s) and tick loop are unaffected.
- Source fencing: once the final dump starts, the source container must never come back (kubelet
  would restart it under `restartPolicy: Always` and two servers would write one world). The
  wrapper refuses to create containers for a pod marked migrated-away, and the controller deletes
  the source pod right after a successful restore.

## Components

| Component | Where | What |
| --- | --- | --- |
| `lcm-runc` | node binary, containerd runtime handler `lcm` | runc passthrough + restore-on-create |
| `lcm-agent` | privileged DaemonSet (hostPID, /run/containerd, /var/lib/lcm) | pre-dump/dump via runc, page transfer, restore staging |
| `lcm-controller` | Deployment (initially a CLI) | orchestrates one migration end to end |
| `lcm-gateway` | Deployment + LoadBalancer Service | holds player TCP, resumable sessions to relays |
| `lcm-relay` | inside the workload container | loopback to Paper, reconnecting upstream |

Node prerequisites (NixOS module): CRIU 4.3 on k3s's PATH, the `lcm` runtime handler in the
containerd config drop-in, `/etc/criu/runc.conf`, sock_diag modules.

## Open questions to settle in the lab

- Does containerd/kubelet accept a container whose "create" was really a restore (state, pid,
  exit monitoring, logs)? containerd#12108 reports kubelet killing restored containers.
- CPU feature compatibility across the three nodes (same model?) vs `cpu-cap`.
- Real client silence limit (20 s vs 30 s) and Windows TCP behaviour is moot with the gateway, but
  the gateway must not time out its side either.
- Pre-dump convergence for a busy Paper heap of 4–8 GiB; transfer over Tailscale (WireGuard) speed.
