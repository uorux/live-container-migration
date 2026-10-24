# Cilium native routing over the tailnet

Status (2026-10-24): **live in production since 19:58Z.** Pod to pod across nodes measured 130 MB/s
(1 and 4 streams; 112 MB/s reversed), against 40 MB/s over VXLAN. The first attempt failed; see
"Production incident" below before ever changing the routing mode again.

## Why

Cross-node pod traffic is Cilium VXLAN inside the tailnet. Tailscale moves host TCP at about
135 MB/s between the node VMs, because TCP reaches tailscale0 as large segments. VXLAN reaches it
as one small UDP datagram per packet, and pod traffic gets about 40 MB/s. Measured on production,
arquitens <-> munificent, iperf3:

| path | 1 stream | 4 streams |
|---|---|---|
| host -> other node's tailnet IP | 133-139 MB/s | 133 MB/s |
| pod -> pod on another node (VXLAN) | 40 MB/s | 42 MB/s |
| pod -> other node's tailnet IP (no VXLAN, what native routing does) | 130 MB/s | 145 MB/s |

Ceph runs pod to pod, so it gets the 40 (`rados bench`: 4 MiB writes 15-26 MB/s). With native
routing, each node's pod /24 becomes a Tailscale subnet route and pod packets go into tailscale0
unencapsulated. Everything stays encrypted by Tailscale.

## Lab rehearsal

The lab imitates the production underlay with a kernel WireGuard device named `tailscale0`
(MTU 1280) whose own UDP carries Tailscale's bypass mark 0x80000. It has Tailscale's ip rules
5210-5270 and a table 52 that routes the peers' node IPs, so VXLAN rides it as in production. nft
counters drop and count anything that leaks onto the plain NICs, which is how the production
mark collision lost its packets. Cilium 1.20.2 with the production settings (KPR, cluster-pool
/24s, legacy host routing, iptables masquerading, devices = LAN NIC + tailscale0).

Results:
- **The mark collision reproduces exactly.** A pod forced onto identity 9992 (0x2708, the identity
  that broke cnpg) lost 100% of cross-node traffic under VXLAN, and the leak counter saw its VXLAN
  packets (mark 0x27080400) leave on the LAN NIC. Adding `cilium-tailscale-mark-rules` fixed it.
- **Native routing doesn't remove the collision.** Forwarded pod packets carry Cilium's identity
  mark (nibble 0xf, e.g. 0x33050f00) into their route lookup. The 9992 pod lost 100% without the
  rules and 0% with them. The existing rule already covers 0xf, so keep it.
  - `enableIdentityMark=false` does remove the identity bits, and the 9992 pod worked without the
    rules. But Cilium uses that mark to keep a pod's identity on traffic that hairpins through the
    host stack (hostPort or other DNAT back to a local pod), so it's not the better fix.
- **Accepting the routes first is harmless.** With the other nodes' pod /24s in table 52 while
  Cilium was still on VXLAN, pod traffic stayed on VXLAN and host -> remote-pod traffic took
  tailscale0 directly. Nothing broke.
- **The switch costs about 5.5 s.** A helm upgrade, then all three agents deleted at once: cross-node
  pod pings stopped for 5.5 s and 5.6 s in two runs. A long-lived TCP stream stalled for about 6 s
  and continued on the same connection.
- **Rolling back cost nothing measurable** (native -> VXLAN: no ping gap over 0.5 s).
- **Services work in native mode.** ClusterIP from a pod on another node 12/12. NodePort from a
  node 12/12. NodePort from a LAN client with backends on every node 15/15 SYN-ACKs.
  - Test with raw SYNs: Cilium's socket LB rewrites `connect()` in every network namespace on a
    node, so an ordinary client on a node never reaches the NodePort path.
- Lab throughput went from 118 to 142 MB/s. The lab tunnel is kernel WireGuard, which doesn't
  penalise VXLAN the way tailscaled does, so the production numbers above are the ones to expect.

## Production incident (2026-10-24 19:33-19:47Z)

Following the runbook below as first written, with a Cilium config change and then all three
agents restarted together, took the control plane down for about 13 minutes:
- **What happened:** each agent rebuilt and reloaded the BPF programs of every endpoint on its node
  at once, about 107 per node, while the node ran its full load. The kernel BPF verifier
  (`bpf_prog_load` -> `bpf_check`) pinned CPUs: soft lockups of 23 s and RCU stalls on carrack,
  load average up to 177.
- **Effect:** tailscaled was starved, so the node dropped off the tailnet. etcd and the API run
  over the tailnet, so etcd lost peers and nodes went NotReady.
- **arquitens:** starved long enough that the softdog watchdog hard-reset it.
- **The rollback** (the same mode change back, agents restarted again) caused the same thing on
  carrack.
- **Why the lab didn't show it:** about 10 endpoints per node, no softdog, and etcd not running
  over the tunnel.

**What worked:** change the config only (running agents ignore ConfigMap changes), then reboot the
nodes. After a boot the agent comes up before the workloads, endpoints are built as pods start,
and there were no soft lockups at all on any node. Never change routing mode, tunnel mode or
anything else that forces a full endpoint regeneration with a live agent restart on these nodes.

## Production runbook (superseded by the incident above: use change-then-bounce)

1. **Tailnet policy (admin console):** auto-approve the pod CIDRs for the nodes and allow them
   between nodes:
   ```json
   "autoApprovers": { "routes": { "10.42.0.0/16": ["tag:k8s"] } }
   ```
   plus an ACL rule permitting tag:k8s -> 10.42.0.0/16:*. The nodes are tag:k8s.
2. **nixos-dots `feat/k3s-native-routing-prep`:** sets `modules.system.k3s.podCIDR` per node
   (arquitens 10.42.0.0/24, munificent 10.42.2.0/24, carrack 10.42.3.0/24, from
   `cilium-dbg node list`). That runs `tailscale set --advertise-routes=<podCIDR>
   --snat-subnet-routes=false --accept-routes` live, with no restart. No pod traffic changes yet.
   - Check: `ip route show table 52 | grep 10.42` shows the other two /24s on each node.
   - Check: `tailscale status --json` shows each node's PrimaryRoutes.
   - Check: pods and Ceph are healthy.
3. **Before the switch:** `ceph osd set noout`, so an OSD that misses heartbeats during the ~6 s
   doesn't start a rebalance.
4. **Switch (needs cluster-admin; Cilium isn't managed by Flux):**
   ```sh
   helm upgrade cilium cilium --repo https://helm.cilium.io --version 1.20.2 -n kube-system \
     --reuse-values --set routingMode=native --set ipv4NativeRoutingCIDR=10.42.0.0/16 \
     --set autoDirectNodeRoutes=false --set MTU=1280
   kubectl -n kube-system delete pod -l k8s-app=cilium --wait=false   # all three at once
   kubectl -n kube-system wait --for=condition=Ready pod -l k8s-app=cilium --timeout=300s
   ```
   Restarting all three agents at once matters: a node in native mode and a node in VXLAN mode
   can't reach each other's pods, so a rolling restart prolongs the gap.
5. **Verify, then `ceph osd unset noout`:**
   - `cilium-dbg status` shows `Routing: Network: Native`.
   - Cross-node pod iperf3 runs at about 130 MB/s.
   - `rados bench` improves; `ceph -s` is healthy.
   - Traefik's LAN address answers.
6. **Rollback:** the same helm command with `--set routingMode=tunnel --set tunnelProtocol=vxlan`,
   then the simultaneous agent restart. The Tailscale routes can stay.

Production specifics the lab doesn't cover:
- tailscaled's own ACL filtering of subnet traffic, which step 1 handles.
- Tailscale's netfilter chains. With `--snat-subnet-routes=false` they add no masquerade.
- Existing pods keep their old route MTU (1230) until they restart. That's smaller, so it's safe.
