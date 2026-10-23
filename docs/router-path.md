# Router path: one VIP, handed between nodes

The CenturyLink router can only port-forward to a fixed LAN address and can't run software. So
LAN-side players reach a VIP that lives on a k3s node, and `lcm-gateway -vip` moves that VIP,
with every player connection on it, to another node when the active gateway's node is drained
or the gateway is upgraded. Players see a pause of about 0.1 s; nobody is disconnected.

## How it works

- **One gateway per node** (DaemonSet, `hostNetwork`, `NET_ADMIN` + `SYS_ADMIN`). A Lease
  (`-vip-lease`) picks the active one.
- **The VIP never touches the node's own network.** Each gateway creates a private network
  namespace holding one ipvlan (L2 mode) on the LAN NIC, created directly inside the namespace
  (`internal/vip`). The ipvlan shares the NIC's MAC, so a crosvm guest's macvtap delivers its
  frames. Nothing Cilium watches changes. Adding an address to a node NIC is what took carrack
  offline on 2026-10-22: Cilium reloaded its datapath, and on carrack the reload never
  re-attached its host programs. The lab reproduced the reload, but not the failure.
- **Players** connect to the VIP; the gateway's player listener lives in that namespace. Its
  tunnel listener (relays) stays in the node's namespace.
- **Relays** reach any gateway through a Service over all of them. The active one serves them;
  standbys forward relay connections to the lease holder; a standby that doesn't know the holder
  yet refuses (a relay must never start a fresh session on a standby).
- **Handoff** (SIGTERM, i.e. node drain or rolling update, or `POST /vip/handoff` on the admin
  port):
  1. The active gateway takes its VIP link down. ipvlan drops frames for it silently (no RST),
     so players' packets are retransmitted later.
  2. It freezes every relay session (`tunnel.Session.Freeze`: pumps stopped with exact byte
     accounting) and takes each player socket out of the kernel with `TCP_REPAIR`
     (`internal/tcprepair`); closing a socket in repair mode sends nothing.
  3. It sends all of it to a ready standby (HMAC of the tunnel token).
  4. The standby restores the sockets in its own VIP namespace while its VIP is still down. A
     dummy `hold0` interface carries a last-resort route, so `connect()` in repair mode has a
     route and nothing leaks out. Then it brings the VIP up, sends gratuitous ARPs, and probes
     each restored connection so the flow resumes at once.
  5. The standby takes the lease. The old gateway hands the lease over too, and won't treat it
     as its own for two lease periods.
  6. Relays reconnect and resume their sessions by offset. The old gateway forwards them to the
     new one for 5 s, because the Service drops a terminating pod at once.
  7. If no standby takes the handoff, the old gateway restores everything itself.
- **What isn't handed off:** status pings, players in the menu, and logins on hold while a
  server starts. These are short-lived, and those players reconnect.
- **Failures:**
  - **Crashed gateway process:** the container restarts and re-takes its own lease at once. Its
    players are lost.
  - **Dead or hung node:** a standby takes the VIP when the lease expires (10 s). Those players
    are lost.
  - **Partitioned active gateway:** if it can't renew for 2/3 of the lease, it takes its VIP
    down before anyone else may take it.
  - **Hung but not crashed (e.g. SIGSTOP):** it keeps answering ARP until it runs again, then
    steps down at its next tick. Lab: 11 s of overlap.

## Lab results (2026-10-23; VIP 192.168.100.250 on node2/node3, echo client on node1)

| Scenario | Result |
|---|---|
| 8 handoffs (`POST /vip/handoff`) and 3 DaemonSet rolling restarts, one client connection throughout | 0 errors; worst round trip 106 ms (1 KiB echo every 20 ms; 3 of ~5,500 above 100 ms) |
| Gateway work per handoff | 3-5 ms (freeze + dump; restore + VIP up) |
| Before relays could reach any gateway | 282-763 ms per handoff, 1.4 s in a rolling restart (waiting for the Service to follow the active label) |
| Active gateway frozen (SIGSTOP) 20 s | standby took the VIP after 9 s; the old one stepped down when resumed; new connections fine |
| Kernel-level move between two VIP namespaces (unit test, `unshare -rn`) | 27-57 ms, 10/10, bytes in flight intact |

## Deploying it

- **Manifest:** `lab/manifests/gateway-vip.yaml` (DaemonSet, ServiceAccount + Role for leases
  and pod labels, the relays' Service). For production:
  - pick a free LAN address for the VIP;
  - set `-vip-parent` to the nodes' LAN NIC (`enp0s7`) and `-vip-gateway 10.0.0.1`;
  - point the router's port-forward at the VIP;
  - add the gateway's Service to each server's relay `-gateway` list.
- **What it needs:** a kernel with ipvlan and `CONFIG_TCP_REPAIR` (both present on the prod
  nodes). The pod needs `NET_ADMIN` and `SYS_ADMIN` in the host network namespace.
- **The active node can't reach its own VIP** (ipvlan doesn't hairpin to its parent), so
  anything on the nodes that probes the VIP must allow for that. Players are elsewhere.
- **Not yet built:** metrics for the VIP controller (its state is on `/vip`), and alerts.
