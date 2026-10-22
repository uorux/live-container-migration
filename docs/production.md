# Production rollout plan

Status 2026-10-22: proven in the lab (3 NixOS VMs mirroring prod: k3s 1.36.5, containerd 2.3.4,
Cilium 1.20.2 VXLAN/cluster-pool/KPR, kernel 7.3). Not yet on the real nodes.

## Status (2026-10-22, overnight)

Live on all three nodes (nixos-dots #14 + the drop-in fix; homelab #370): agents, `lcm`
RuntimeClass, gateway at 10.0.0.201 / NodePort 31956, Paper test server in `lcm-test`.

Production results:
- smoke workload: 6/6 hops across all node pairs, 0.9-4.0 s blackouts, loopback continuity
  intact, outbound connections reset + reconnected;
- Paper (3 GiB heap) with Jacob connected: he stayed connected through a 23 s freeze. Without
  players: 8.8-9.0 s. First move onto a node: 45 s (15 s image pull + 25 s cold CephFS reads).

Fixes for all of that are in this repo and waiting on the nixos-dots branch
`uorux:feat/lcm-speed-and-restart-safety` (a flake.lock bump only; deploying restarts lcm-agent,
not k3s; it evaluates and builds at nixos-dots' nixpkgs pin):

- zstd-compressed transfers (6-8x on heap pages): the tailnet link was the bottleneck;
- CRIU `file-validation filesize`: no reads of every open jar on CephFS (the 25 s cold restore);
- image pre-pull on the destination before anything is dumped;
- kubelet's container start overlaps the end of pre-copy (FINAL marker; ~1 s);
- open files on shared volumes are opened on the destination during pre-copy (NFS delegation /
  CephFS cap recalls; up to ~1.3 s per restore on NFS, 0.02-0.6 s seen on the CephFS restores);
- restart safety, staging GC, a memory pre-flight check, and the LivePod controller (off).

Lab, link throttled to ~50 MB/s, 3 bots: Fabric 0.68-0.77 s blackouts (worst client gap
0.79 s), NeoForge 0.71-0.96 s, a TCP smoke pod 0.56-0.76 s; 0 disconnects. Expect more on
production for big heaps: restores there copy memory at ~1.6 GB/s (the lab VMs do ~4.9 GB/s),
so a 3.6 GB Paper heap restores in ~2.5 s. That's the VM layer (guest memory backing), not lcm;
JVM transparent huge pages didn't help and made pre-dumps larger.

## Running the real server

Use a LivePod (deploy/livepod-crd.yaml, example in deploy/examples/minecraft-livepod.yaml) so
GitOps manages the definition while migrations move the pod; enable the controller with
`services.lcm.livePods.enable = true` (it runs in lcm-agent, leader-elected). Migrate with
`lcm migrate livepod/<name> -n <ns> --to <node>`. The controller recreates the pod only when none
exists at all, so a node failure is recovered (cold start) without ever running two copies.

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
