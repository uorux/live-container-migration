# Production rollout plan

Status 2026-10-22: proven in the lab (3 NixOS VMs mirroring prod: k3s 1.36.5, containerd 2.3.4,
Cilium 1.20.2 VXLAN/cluster-pool/KPR, kernel 7.3). Not yet on the real nodes.

## What changes where

| Where | Change | Who applies it |
| --- | --- | --- |
| nixos-dots (arquitens, carrack, munificent) | flake input on this repo; `imports = [ lcm.nixosModules.default ]`; `services.lcm = { enable = true; tokenFile = <secret>; firewallInterfaces = [ "tailscale0" ]; }` | proposed via `uorux/nixos-docs-agent-fork`, Jacob merges, `tpl.nixos-dots-rebuild` per node |
| uorux/homelab (Flux) | `RuntimeClass lcm` (deploy/runtimeclass.yaml); a namespace for the game server(s) with: gateway Deployment + LoadBalancer Service (Cilium LB-IPAM/L2) + tunnel ClusterIP + token Secret; the server pod (runtimeClassName lcm, relay injected, CephFS RWX PVC) | Hermes (homelab agent) opens the PR, Jacob merges |
| this repo | published somewhere nixos-dots can fetch it (GitHub `uorux/live-container-migration` or similar) | needs a repo-create grant |

Applying the NixOS module restarts k3s once per node (containerd only reads its drop-ins at
start). Running pods are unaffected (their shims keep running), but do one node at a time.

## Node facts to confirm first (read-only)

- CPU model/flags identical on all three nodes (CRIU refuses to restore onto a CPU missing
  features the dump used; otherwise pin `cpu-cap` or a JVM `-XX:UseAVX` level).
- Kernel options: CHECKPOINT_RESTORE, MEM_SOFT_DIRTY (pre-copy), INET_DIAG_DESTROY (resetting
  outbound sockets after the IP change), TIME_NS.
- k3s containerd imports `config-v3.toml.d/*.toml` (the module drops the runtime handler there).
- Free RAM: a migration stages roughly the workload's RSS in tmpfs on the destination (and the
  first pre-dump on the source, freed as soon as it is shipped).
- Tailscale node-to-node throughput: the first pre-copy round ships the whole heap while the
  server keeps running; only the final delta is inside the blackout.

## Workload requirements (documented contract)

- `runtimeClassName: lcm`; a bare pod (no ReplicaSet/StatefulSet owner yet; a LivePod controller
  is future work).
- Persistent data on RWX storage (`ceph-filesystem`), mounted at the same path. RBD (RWO) would
  need a detach/attach inside the blackout; it isn't supported (and not yet rejected by the CLI).
- No hostPath volumes.
- The relay is the container entrypoint: `/lcm/lcm-relay run -gateway <svc>:7000 -token-file ...
  [-proxy-protocol=false] -- <original entrypoint>`; an init container fetches the binary from the
  node agent (`http://$HOST_IP:7790/v1/bin/lcm-relay`). Paper: enable `proxies.proxy-protocol` to
  keep client IPs; vanilla/Fabric/Forge: `-proxy-protocol=false`.
- Java: `-Djna.tmpdir=/tmp -Djava.io.tmpdir=/tmp` so deleted-but-open temp files are on the
  container rootfs, not CephFS.
- Memory limit with headroom over heap + JVM overhead.

## Staged rollout

1. Inventory (above). Fix anything missing in the module.
2. nixos-dots: enable the module on one node, rebuild, verify `lcm-agent` health, `crictl info`
   shows the `lcm` runtime, `criu check` passes. Then the other two.
3. homelab: RuntimeClass + gateway in a `lcm-test` namespace.
4. Smoke test with the tiny `tcp-mixed` workload between each node pair (blackout should be
   < 1 s; outbound reset/reconnect; loopback continuity).
5. Test server: a throwaway Paper/Fabric world on CephFS, bots through the gateway's LB IP,
   ping-pong 10 hops, measure blackout on real hardware and Tailscale.
6. Jacob joins with a real client and tries it.
7. The real server.

## Operating

- Migrate: `lcm migrate <pod> -n <ns> --to <node> --token-file /run/secrets/lcm-token`
  (from any node; it needs the kubeconfig and the agents' port on the InternalIPs).
- A migration that fails before the final dump leaves the source running (players don't notice).
- If the destination restore fails, the controller fences it, forwards the images back and
  restores on the source node (blackout ~10-15 s). If even that fails it says where the images
  are: `lcm recover <source-pod> --to <node> --images-on <node> --key <ns_pod_ctr>`.
- Logs: `journalctl -u lcm-agent`, `/var/lib/lcm/lcm-runc.log`, CRIU logs under
  `/var/lib/lcm/logs/`.
- CRIU's file-size checks refuse to restore a snapshot over files that changed after the dump;
  that is a safety net, not an error to work around.
