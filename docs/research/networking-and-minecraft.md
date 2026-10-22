# Live migration of a Paper server on k3s + Cilium: networking and application constraints

*Research date: 2026-10-22. Source snapshots: CRIU `criu-dev` @ `c9f3f04` (2026-10-21), PaperMC/Paper `main` @ `2d602d6` (2026-10-21), Cilium `main` (`bpf/lib/lb.h`, fetched 2026-10-22), Linux `master` (`net/ipv4/tcp.c`, fetched 2026-10-22). Line numbers below refer to those snapshots.*

## TL;DR

1. **CRIU can only restore an established TCP socket if its original local IP can be bound on the target**, and the network has to stay "locked" (peer packets dropped, no RSTs) from dump until restore. A pod that comes back with a new pod IP therefore **cannot** restore the players' TCP sockets unless something gives the pod the old IP back.
2. **Cilium OSS cannot really move a pod IP between nodes.** The closest option is multi-pool IPAM with a `/32` `CiliumPodIPPool` selected by pod name. That only works stop-and-copy (old pod deleted before the new one gets the IP), and Cilium maintainers say IP mobility and live migration aren't supported. Commercial *Isovalent Networking for Virtualization* does offer IP preservation for KubeVirt. Kube-OVN, OVN-Kubernetes and Calico offer it in their own ways.
3. **The binding limit is the Minecraft client's 30 s netty `ReadTimeoutHandler`**, not TCP. A Linux client's TCP gives up after about 15 min. Windows aborts after 5 data retransmissions, which on a low-RTT link can be well under a minute. Paper's own keepalive limit is 30 s, and the watchdog is 60 s by default.
4. **Plain CRIU makes most of the freeze invisible to the JVM's monotonic clock.** It always restores into a fresh time namespace whose `CLOCK_MONOTONIC`/`CLOCK_BOOTTIME` continue from the values captured at dump time (`criu/timens.c`, `cr-restore.c:1579`). Server-side keepalive and watchdog timers, and Paper's tick catch-up, only see the dump tail plus restore time. Image transfer time is hidden. `System.currentTimeMillis()` still jumps forward, and **clients still see the full wall-clock blackout**.
5. **Recommended design:** a *stable front proxy* holds the players' TCP connections. A *relay process inside the migrated container* talks to Paper over loopback and is checkpointed with it. Only the relay-to-front link is torn down and re-dialled, with byte-offset resume. This needs no pod-IP preservation, no Cilium changes, and no MPTCP. The client never sees TCP-level retransmission backoff. The total client-visible blackout must stay well under 30 s (aim for ≤10 s).

---

## 1. CRIU TCP connection repair

### 1.1 Mechanism and options

CRIU uses the kernel's `TCP_REPAIR` mode (via `libsoccr`) to read and restore a socket's sequence numbers, queues, window and options without any packets going on the wire. The relevant options, from the CRIU man page ([`Documentation/criu.txt`](https://github.com/checkpoint-restore/criu/blob/criu-dev/Documentation/criu.txt)):

| Option | Dump | Restore |
|---|---|---|
| `--tcp-established` | "Checkpoint established TCP connections." | "Restore previously dumped established TCP connections. This implies that the network has been locked between dump and restore phases so other side of a connection simply notice a kind of lag." |
| `--tcp-close` | "Don't dump the state of, or block, established tcp connections (including the connection is once established but now closed). This is useful when tcp connections are not going to be restored." | "Restore connected TCP sockets in closed state." |
| `--skip-in-flight` | "skips in-flight TCP connections. If any TCP connections that are not yet completely established are found, criu ignores these connections, rather than errors out. The TCP stack on the client side is expected to handle the re-connect gracefully." | n/a |
| `--network-lock {iptables\|nftables\|skip}` | "Locking is done to ensure that tcp packets are dropped between dump and restore. This is done to avoid the kernel sending RST when a packet arrives destined for the dumped process." With `skip`, "if `--tcp-close` is not used, the network must be locked externally." | same |

Without `--tcp-established` or `--tcp-close`, CRIU refuses to dump any socket in ESTABLISHED, FIN_WAIT*, CLOSE_WAIT, LAST_ACK, CLOSING or SYN_SENT ("Connected TCP socket, consider using --tcp-established", `criu/sk-inet.c:193-203`). Both options are **global**: you can't keep some connections and close others. Any incidental outbound connection the container holds (a plugin's MySQL pool, a Discord websocket, an RCON client) is subject to the same rules as the player connections (see §1.7).

CRIU also handles half-closed states. It restores them as ESTABLISHED, calls `shutdown(SHUT_WR)` and injects fake FIN/ACKs as needed. It carries TCP timestamps across hosts with `TCP_TIMESTAMP` get/set ([criu.org/TCP_connection](https://criu.org/TCP_connection)).

When repair mode is switched off, Linux immediately sends a **window probe** (`tcp_send_window_probe(sk)` in the `TCP_REPAIR_OFF` branch, `net/ipv4/tcp.c:3991-3994`). So the restored side always emits one packet right away. That matters for the WireGuard-roaming variant in §3.5.

### 1.2 Must the local IP be identical?

**Yes.** The CRIU wiki says: "on restore there should be available the IP address, that was used by the connection. Of course this is so if restore happens on the same box, but in case of live migration this should be taken care of" ([criu.org/TCP_connection](https://criu.org/TCP_connection)). In code, `inet_bind()` (`criu/sk-inet.c:1039-1093`) does a plain `bind()` to the dumped `src_addr:src_port`. It only sets `IP_FREEBIND` temporarily for IPv6 (to get past DAD's tentative state), or when the application itself had it set. For IPv4, the kernel's bind check passes for a non-local address only if `freebind`, `transparent` or `net.ipv4.ip_nonlocal_bind` is set (default 0, [ip-sysctl docs](https://docs.kernel.org/networking/ip-sysctl.html)). So restoring into a pod netns that lacks the old IP fails with `EADDRNOTAVAIL`.

Even with the old IP added as an alias, or with `ip_nonlocal_bind=1`, the bind only *succeeds*. Peers still address their packets to the old IP, so the network has to deliver them to the new node, and Cilium has to accept the pod's egress packets with that source IP. Cilium validates endpoint source IPs, so in practice the IP must be the pod's real Cilium endpoint IP. The 4-tuple is fixed for the life of the connection.

The remote IP doesn't need to "exist" anywhere. It only matters that packets from it reach the socket.

### 1.3 Locking and avoiding RSTs

- If the dumped tree has its own netns (always the case for a container), CRIU switches into that netns and installs a netns-wide iptables/nftables drop lock. It also runs the `network-lock`/`network-unlock` action scripts (`network_lock()` / `network_lock_internal()`, `criu/net.c:3560-3700`).
- In Kubernetes **all containers of a pod share the sandbox netns**. The lock therefore also cuts off sidecars during the dump. Anything that has a loopback TCP connection to Paper must be inside the same checkpointed process tree (§1.5).
- On the **source**, nothing must answer the peer with RST after the dump. CRIU closes sockets while they're still in repair mode, which is silent, and leaves the lock in place. The *passt* project hit exactly this problem when it added KubeVirt live migration with `TCP_REPAIR`. Its fix keeps the migrated sockets open in repair mode on the source so that late packets don't trigger RSTs ([passt-dev, Jul 2025](https://lists.passt.top/hyperkitty/list/passt-dev@passt.top/message/2D4VRGTCWAIP3UDL3YD4LPUQF5JOITLB/)). Lesson: deleting the source pod while the VIP still routes to it is fine (Cilium drops traffic to a vanished endpoint). A live, unlocked netns that still owns the IP is not.
- On the **target**, the peer's retransmissions must not reach the netns before the sockets exist. Otherwise the kernel answers with RST and the connection dies. Keep the lock in place, or keep traffic routed away, until CRIU finishes.

### 1.4 What the peer sees during the freeze

From the peer's point of view the server has simply gone silent. Unacked data is retransmitted with exponential RTO backoff. Peers with nothing outstanding see nothing at TCP level unless keepalive fires, and TCP keepalive defaults are hours. A Minecraft client always has data in flight, because it sends movement packets every tick, so it will be retransmitting.

| Peer OS | Give-up rule | Approx. time to abort |
|---|---|---|
| Linux | `tcp_retries2=15`: "a hypothetical timeout of 924.6 seconds and is a lower bound for the effective timeout"; `tcp_rto_min_us`=200 000, `tcp_rto_max_ms`=120 000 ([ip-sysctl](https://docs.kernel.org/networking/ip-sysctl.html)) | ≥ ~15 min |
| Windows | `TcpMaxDataRetransmissions` default **5**, timer doubled per retransmission, initial RTO 3 s then SRTT-adapted ([MS KB170359](https://support.microsoft.com/kb/170359), [TechNet](https://technet.microsoft.com/en-us/library/cc938210)) | *Estimate:* with a low-RTT RTO of ~0.3–0.5 s, 5 doublings give roughly **20–30 s**; with RTO≈1 s about 60 s. **This can be shorter than Minecraft's own 30 s timeout.** Verify on current Windows 11. |
| macOS (XNU) | `TCP_MAXRXTSHIFT 12`, backoff table `{1,2,4,8,16,32,64,64,…}`, `TCPTV_REXMTMAX` 64 s ([xnu tcp_timer.h](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/netinet/tcp_timer.h), [tcp_timer.c](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/netinet/tcp_timer.c)) | several minutes |

**Extra stall after restore.** The peer's next retransmission is timer-driven. If the peer's backoff schedule is 0.2, 0.6, 1.4, 3.0, 6.2, 12.6, 25.4 s… and the restore finishes at t = 7 s, client→server traffic doesn't resume until about t = 12.6 s. The window probe from §1.1 makes the peer send a dup-ACK, but it doesn't reset the peer's retransmit timer. Server→client traffic resumes at once. This is one more reason not to expose the client's own TCP connection to the freeze.

On a Linux peer you control, such as a front proxy, cap the backoff with `net.ipv4.tcp_rto_max_ms` or the per-socket `TCP_RTO_MAX_MS` option (range 1–120 s). Both were added in early 2025 (Linux 6.15; [netdev patch](https://lists.openwall.net/netdev/2025/02/10/9)).

### 1.5 Loopback connections between two processes in the same dump

Supported. CRIU's test suite has `socket-tcp-local` (a symlink to `socket-tcp.c`, connecting to `localhost`) and `socket-tcp4v6-local`/`socket-tcp6-local`/`socket-tcpbuf6-local`, all run with `--tcp-established` (`test/zdtm/static/socket-tcp4v6-local.desc`: `{'opts': '--tcp-established', ...}`). Both endpoints are restored from the images. `127.0.0.1`/`::1` always exist in the new netns, so **the local-IP problem disappears for loopback**.

Requirements:

- **Both ends must be in the dumped tree.** If one end were in another container that isn't checkpointed, its half would be lost.
- `--tcp-established` must be passed.

Unix domain sockets between processes in the dump are also handled. Paper supports `server-ip=unix:/path`, but only when Velocity or BungeeCord forwarding is enabled ("Unix domain sockets require IPs to be forwarded from a proxy", Paper `DedicatedServer.java.patch`), so loopback TCP is the general choice.

### 1.6 Listening sockets

A `TCP_LISTEN` socket is dumped and re-created by bind+listen. If its accept queue is non-empty (`rqlen != 0`), the dump fails with "In-flight connection (l)" unless `--skip-in-flight` is given, which drops those pending connections (`criu/sk-inet.c:175-192`). `SYN_SENT` sockets fall under `--tcp-established`.

The same bind rule applies: a listener bound to a specific pod IP cannot be restored on a new IP. Bind Paper to `127.0.0.1` (relay design) or `0.0.0.0`.

### 1.7 Other CRIU networking gotchas found

- **MPTCP sockets are not supported.** `IPPROTO_MPTCP` falls into "Unsupported proto", and CRIU even prints the hint "For Go programs, consider using GODEBUG=multipathtcp=0 to disable MPTCP" (`criu/sk-inet.c:128-142`). Since Go 1.24, "ListenConfig now uses MPTCP by default on systems where it is supported" ([Go 1.24 release notes](https://go.dev/doc/go1.24)). **Any Go relay inside the checkpoint must disable MPTCP.**
- **io_uring has no handling in CRIU at all** (no matches in `criu/`). Paper uses Netty **epoll** (`EventLoopGroupHolder.java.patch`), which CRIU supports. Don't enable an io_uring transport.
- **UDP:** CRIU dumps UDP/UDPLite sockets. A socket bound to `0.0.0.0:port` restores anywhere, but one bound to a specific pod IP hits the same bind problem.
- **Incidental external TCP connections** (DB pools, webhooks, metrics push) also need their old local IP at restore, or restore aborts. Options:
  - Route them through the relay too.
  - Close them in a pre-dump hook.
  - Use the "alias trick": add the old pod IP as a `/32` on `lo` in the target netns so `bind()` succeeds. Those connections then restore as black-holed zombies (Cilium will drop egress with a foreign source IP), and the library's own reconnect logic recovers. Test this; it's a hack.
- **Kubernetes/containerd restore path (context, not networking).**
  - Kubelet checkpointing (KEP-2008) is beta since v1.30, but Kubernetes has no restore API ([KEP-2008](https://www.kubernetes.dev/resources/keps/2008/)).
  - containerd's implicit "restore via CreateContainer" is disabled by default in 2.2.7/2.3.4 (needs `enable_experimental_restore_via_create`) and is **removed in containerd 2.4**. The replacement is KEP-5823 "Pod-Level Checkpoint/Restore" (RestorePod API) ([sig-node announcement, 2026-08-12](https://groups.google.com/a/kubernetes.io/g/sig-node/c/tsdff1ptP-0)).
  - k3s embeds containerd, so check which version your k3s ships. Driving runc/CRIU directly is probably needed regardless.

---

## 2. Keeping the pod IP across nodes with Cilium

### 2.1 IPAM modes

| Mode | Where pod IPs come from | Can a pod get a specific IP / move it? |
|---|---|---|
| `cluster-pool` (default) | Operator carves per-node PodCIDRs from a cluster pool; agent allocates from its node's CIDR | No. The IP is always inside the current node's CIDR. |
| `kubernetes` | `Node.spec.podCIDR(s)` from kube-controller-manager (k3s default `10.42.0.0/16` → /24 per node) | No |
| `multi-pool` (beta in 1.14; **stable in 1.19**, now also with native routing and IPsec, [Cilium 1.19 coverage](https://www.cloudmagazin.com/en/wp-json/wp/v2/posts/47717)) | Several `CiliumPodIPPool`s; a node holds one or more CIDR blocks from each pool (block size = pool `maskSize`, immutable); pods select a pool by annotation `ipam.cilium.io/ip-pool` (or `ipv4-pool`/`ipv6-pool`), or by the pool's `podSelector`/`namespaceSelector`; `ipam.cilium.io/require-pool-match="true"` blocks allocation until a non-default pool matches ([multi-pool docs](https://docs.cilium.io/en/latest/network/concepts/ipam/multi-pool/)) | **Effectively yes, stop-and-copy only** (see 2.2) |
| ENI / Azure / GKE (cloud) | Cloud-provider VPC IPs on node NICs | Cloud-specific; irrelevant for bare-metal k3s (1.20 adds ENI IPv6 beta) |

There is **no `ipam.cilium.io/ips`-style "give me IP X" pod annotation in Cilium OSS.** The annotations are pool selectors, and they're "only considered when a pod is created" (multi-pool docs). LB-IPAM has its own annotation for Service VIPs (`lbipam.cilium.io/ips`), but that's for Services, not pods.

### 2.2 Static pod IP via a /32 pool

A Cilium IPAM maintainer described the workaround and its limit on the KubeVirt request ([cilium/cilium#38576](https://github.com/cilium/cilium/issues/38576), 2025-04-09):

> "Static IPs are supported with Multi-Pool IPAM. UX is a bit awkward, but you can create a /32 CiliumPodIPPool and assign that to the VM – but live migration still requires us to solve b) [two pods with the same IP running at once] which is much harder… Cilium has the pretty deeply held assumption that an IP uniquely identifies one endpoint… In particular if one also wants to preserve existing connections (because then we also have to migrate connection tracking state)."

The issue went stale and was auto-closed (Aug 2025). As of Oct 2026, users were still asking for it with no roadmap answer. To pin a pool to a single pod, use the synthetic labels Cilium adds for pool selection (`io.kubernetes.pod.namespace`, `io.kubernetes.pod.name`) in the pool's `podSelector`, with `maskSize: 32`.

**Does this fit CRIU?** Partly. CRIU migration is stop-and-copy, so unlike KubeVirt you don't need two live endpoints with the same IP. The sequence would be:

1. Freeze and dump.
2. Delete the source pod, so the agent releases the IP and then the `/32` block from `CiliumNode(A).spec.ipam.pools.allocated`. The docs say only that the agent "removes PodCIDRs it has released and is no longer using", with no stated latency.
3. The target node's agent requests the pool, and the operator assigns the `/32` to node B.
4. Create the target pod, and the CNI ADD gets the IP.
5. The endpoint and ipcache propagate cluster-wide; the route moves (native routing: `autoDirectNodeRoutes` or BGP advertisement of the pool's per-node CIDRs).
6. Restore.

**Every one of steps 2–5 lands inside the client-visible blackout.** It also depends on unmeasured operator/agent reconciliation latency. The pod name must be identical (or the selector must match both pods). This is a real but fragile option.

### 2.3 Routing mode implications

- **Tunnel (VXLAN 8472/udp, Geneve 6081/udp).** The network only needs node-to-node reachability. Cilium chooses the remote node from its own state: per-node allocation prefixes plus per-endpoint ipcache entries ([routing docs](https://docs.cilium.io/en/latest/network/concepts/routing/)). Any IP that has a CiliumEndpoint is reachable wherever it lives, which makes this the easiest mode for IP mobility.
- **Native routing.** "The network connecting the cluster nodes must be capable of routing PodCIDRs." Use `autoDirectNodeRoutes` on a shared L2 segment, or BGP. With a `/32` pool, the `/32` is the "PodCIDR" that has to move, so route convergence is part of the blackout.
- **A /32 outside the node's main podCIDR:** possible only with multi-pool, where a node holds extra CIDRs from other pools. Not possible with `cluster-pool`/`kubernetes`.

### 2.4 Endpoint identity and datapath state

- **Security identity** comes from labels, so a recreated pod with the same labels gets the same numeric identity and policy follows. The endpoint ID is node-local and changes. Other nodes learn IP→identity/node through ipcache (CiliumEndpoint watch), and until that converges they send to the old node.
- **Connection tracking and NAT state live in per-node BPF maps** and do not move. On the new node the first mid-stream packet is a CT miss and is treated as a new flow. Policy must allow it, which is fine with default-allow or a port-25565 ingress rule. The maintainer's remark about having "to migrate connection tracking state" refers to this. Test it explicitly with `cilium-dbg monitor --type drop`.

### 2.5 Stable VIP: LB-IPAM, BGP, L2 announcements, and what happens when the backend moves

- **LB-IPAM** assigns a stable VIP to the `LoadBalancer` Service from a `CiliumLoadBalancerIPPool`.
- **BGP Control Plane** "advertises exact routes for the VIPs (/32 or /128)". With `externalTrafficPolicy: Local` it "keeps track of the endpoints for the service on the local node and stops advertisement when there's no local endpoint" ([BGP config docs](https://docs.cilium.io/en/latest/network/bgp-control-plane/bgp-control-plane-configuration/)). With eTP=Local the VIP route follows the pod, after the new endpoint becomes Ready and BGP converges, and the client source IP is preserved.
- **L2 announcements** use one lease per Service. Failover takes `leaseDuration ± leaseRenewDeadline` (defaults 15 s / 5 s, so 10–20 s), with gratuitous ARP on takeover. The feature "is incompatible with the `externalTrafficPolicy: Local`" ([L2 docs](https://docs.cilium.io/en/latest/network/l2-announcements/)). The VIP stays on whichever node holds the lease, and traffic is forwarded to the pod (eTP=Cluster).
- **Backend move with Cilium's datapath LB** (`bpf/lib/lb.h`, `lb4_local`, around lines 2304-2385):
  - **Backend deleted.** For an existing flow (CT_REPLY path), if the backend ID is gone, Cilium **re-selects a backend and rewrites the CT entry**. The source comment says: "If the lookup fails it means the user deleted the backend out from underneath us. To resolve this fall back to hash. If this is a TCP session we are likely to get a TCP RST." So established flows *are* re-steered to the new pod, and they survive only if the new pod has a socket with the same 4-tuple. With eTP=Cluster in SNAT mode, the ingress node keeps its SNAT entry, so the backend sees the same `nodeIP:port → podIP:25565` tuple as before. **This works only if the pod IP is unchanged.**
  - **Backend terminating.** If the old backend is merely *terminating*, existing TCP flows stay pinned to it ("Drain existing connections, but redirect new ones to only active backends"). It must be fully removed.
  - **Socket-LB.** In-cluster clients using socket-LB (connect-time translation) are connected straight to the backend IP and are never re-steered. `socketLB.hostNamespaceOnly=true` disables socket-LB inside pods; that setting exists for KubeVirt/Kata/gVisor/Istio ([kube-proxy-free docs](https://docs.cilium.io/en/latest/network/kubernetes/kubeproxy-free/)).
- **Net:** a stable VIP decouples the *client* from the node, but the *backend tuple* still contains the pod IP. A VIP alone doesn't solve migration. It solves it only together with pod-IP preservation (§2.2), or with a design where the client's TCP connection terminates at something that doesn't move (§3).

### 2.6 How KubeVirt live migration handles this, and what Cilium does

- **Cilium OSS:** no IP-preserving live migration (#38576 above). Related open pain point: after a KubeVirt migration with Cilium, a stale and a fresh EndpointSlice can reference the same reused IP, and Cilium parks the backend in "maintenance" ([kubevirt/kubevirt#19500](https://github.com/kubevirt/kubevirt/issues/19500), Oct 2026). KubeVirt's default masquerade/bridge bindings give the destination pod a new IP ([kubevirt#14320](https://github.com/kubevirt/kubevirt/issues/14320)).
- **Isovalent (Cisco) Networking for Virtualization (commercial, built on Cilium):** advertises "IP preservation across live migrations" using an "Isovalent Network Bridge… (ARP proxy, stateless NAT, Geneve/VXLAN), EVPN/BGP-based progressive network cutover" ([product page](https://isovalent.com/products/networking-for-virtualization/)). It's not in OSS Cilium.
- **Kube-OVN:** annotation `kubevirt.io/allow-pod-bridge-network-live-migration: "true"`. The target pod reuses the source's logical port (same IP). Traffic is copied to both source and target during migration (multi-chassis port binding), and the target port is activated by libvirt's RARP. They report "network interruption time can be controlled within 0.5 seconds, and TCP connections remain uninterrupted" ([CNCF blog, 2025-03-20](https://www.cncf.io/blog/2025/03/20/kubevirt-live-migration-mastery-network-transparency-with-kube-ovn/)).
- **OVN-Kubernetes:** persistent IPs (`enablePersistentIPs`, IPAMClaims) for KubeVirt. This is what KubeVirt maintainers pointed the Cilium user to (#38576).
- **Calico:** ties IP allocation to the VM identity rather than the pod, and L2 bridge networks keep the VM's address on a secondary interface ([Tigera on dev.to](https://dev.to/tigeraio/kubevirt-networking-how-to-preserve-vm-ip-addresses-during-migration-1fe9)).
- **passt binding:** passt migrates the *host-side* TCP sockets with `TCP_REPAIR`. Its lessons: avoid source-side RSTs (§1.3), and a timestamp-related bug ([RHEL-84192](https://issues.redhat.com/browse/RHEL-84192)).

The pattern across all of these: they preserve IPs either by making the CNI treat the IP as belonging to a *workload identity* rather than a pod, or by adding an L2/L3 indirection layer (proxy ARP, stateless NAT, overlay). With Cilium OSS on k3s you'd have to build that indirection yourself, which is what §3 does.

---

## 3. Alternative: stable front proxy plus a resumable backend link

### 3.1 Why

If the players' TCP connections terminate at a component that never moves:

- the client never experiences TCP retransmission or backoff (the proxy keeps ACKing), so the Windows retransmission limit is irrelevant;
- pod IPs, Cilium CT/NAT state and VIP routing are irrelevant to migration;
- the only thing that has to survive is the proxy↔server byte stream, which you control at both ends.

The client's **application** timeout (30 s of silence, §4.2) still applies. Under online-mode encryption the proxy cannot inject keepalives (§4.6).

### 3.2 Existing tools

| Tool | Byte-exact resume of a backend stream after the backend changes IP? | Notes |
|---|---|---|
| HAProxy / Envoy / nginx stream | **No** | Retries/redispatch only at connect time; once bytes flow, upstream failure closes the client side. Useful only as a dumb L4 front with long `timeout server`/`timeout tunnel`, in variants that keep the backend TCP connection alive (§3.5). |
| Velocity / BungeeCord | **No** | They terminate the Minecraft protocol (and encryption, in online mode). A dead backend means fallback to another server (Velocity `failover-on-unexpected-server-disconnect`, except on read timeouts) or a kick. `read-timeout = 30000` by default ([default-velocity.toml](https://github.com/PaperMC/Velocity/blob/dev/3.0.0/proxy/src/main/resources/default-velocity.toml)). Switching servers is a respawn/config-phase transition, not a transparent resume. |
| Eternal Terminal (`et`) | Yes, for its own channel | Reconnecting transport with backed reader/writer and sequence-number replay. Supports `--tunnel`/`-L` port forwards over it, though forwards "are not restored on attach" to a *new* session ([README](https://github.com/MisterTea/EternalTerminal)). SSH-bootstrapped and terminal-oriented: a proof of concept, not a building block. |
| mosh | No | SSP over UDP synchronises *terminal state*, not byte streams. Not applicable. |
| ResumeProxy | Yes | SOCKS5 tunnel that "survives TCP connection loss… every byte continues exactly where it stopped" ([erfanara/resumeproxy](https://github.com/erfanara/resumeproxy)). Educational, 1 star, Oct 2026. Good reference design, not production. |
| BAMF `pkg/tunnel` | Yes | Go "reliable stream" with framing, sequence numbers, ACKs, retransmit buffer and `Reconnect()` ([pkg.go.dev](https://pkg.go.dev/github.com/mattrobinsonsre/bamf/pkg/tunnel)). Embedded in a larger access platform; could be vendored or used as a reference. |
| QUIC connection migration | Yes, *client-initiated* | RFC 9000 §9 only lets the **client** migrate (servers only via `preferred_address` at handshake; a server-migration draft exists, [draft-kozuka-quic-server-migration-00](https://www.ietf.org/ietf-ftp/internet-drafts/draft-kozuka-quic-server-migration-00.html), Jan 2026). quic-go added server-side path validation in v0.50.0 and client-side migration in **v0.51.0** ([releases](https://github.com/quic-go/quic-go/releases)), with fixes through v0.64.0 (2026-10-16). See §3.3 for the in-pod "relay is the QUIC client" variant. |
| MPTCP (RFC 8684) | Yes in principle | See §3.4. Ruled out inside the checkpoint. |
| WireGuard | Indirectly | Built-in roaming: a peer's endpoint is updated to the source of the latest authenticated packet. Gives a **stable inner IP**. See §3.5. |

**Conclusion:** there's no off-the-shelf, production-grade "resumable TCP relay" that fits. The protocol is small (a few hundred lines), and ResumeProxy and BAMF are good references. QUIC with client migration is the closest standard alternative.

### 3.3 Relay inside the migrated container (recommended)

```
 player ──TCP──► Front proxy F (stable pod, LB-IPAM VIP) ══ resumable link ══► Relay R ──loopback TCP──► Paper 127.0.0.1:25565
                 holds client sockets, per-stream buffers      (re-dialled         └──── same container, same CRIU dump ────┘
                                                                after restore)
```

- **R lives in the same container/process tree as Java.** Use a tiny init that starts both (e.g. `tini` → R and `java`). Both ends of every R↔Paper loopback connection are then in the dump and are restored by `--tcp-established` with `127.0.0.1` (§1.5).
- **R dials F**, outbound to F's ClusterIP/VIP. Nothing about the pod's own IP matters. Socket-LB pinning is harmless because the link is re-dialled.
- **Before the final dump, R detaches.** The orchestrator signals R (control socket, or a CRIU `pre-dump` action script). R sends `PAUSE`, flushes, and closes the F link, so the only established TCP sockets left are loopback ones. Otherwise `--tcp-established` would also try to restore the R↔F socket on the old pod IP and **abort the restore**. Alternatively, run the R↔F link over UDP/QUIC: a `0.0.0.0`-bound UDP socket restores fine, and the QUIC client's address change looks like NAT rebinding to the server.
- **Protocol sketch:**
  - Frames: `HELLO{session, epoch, [(stream, rx_offset)…]}`, `OPEN{stream, client_addr}`, `DATA{stream, offset, bytes}`, `ACK{stream, offset}`, `FIN/RST{stream}`.
  - Each side keeps an unacked send buffer per stream. On reconnect, each side resends from the peer's `rx_offset`.
  - Bound buffers (a few MB per player) and use them for backpressure.
- **Real client IPs:** Paper sees every player as `127.0.0.1`. Have R prepend a **PROXY protocol v2** header on each loopback connection and enable Paper's `proxies.proxy-protocol: true` (`GlobalConfiguration.java:137`; Paper inserts `HAProxyMessageDecoder` after the timeout handler, `ServerConnectionListener.java.patch`). Bind Paper to `127.0.0.1` so the PROXY header can't be spoofed from outside.
- **Language:** if R is written in Go, set `GODEBUG=multipathtcp=0` or `ListenConfig.SetMultipathTCP(false)` (§1.7). Rust or C are equally fine. Avoid io_uring runtimes.
- **Timing advantage:** with CRIU's time namespace (§5.2), R barely notices the freeze and re-dials immediately after restore. There's no RTO backoff on any client-facing connection. The blackout is freeze + transfer + restore + one RTT.

### 3.4 MPTCP

- Linux supports make-before-break subflows (add a subflow from a new address, then drop the old one), which is exactly the semantics wanted.
- But CRIU **cannot checkpoint `IPPROTO_MPTCP` sockets** (§1.7).
- Java has no MPTCP socket option, so the vanilla client won't use it.
- Placing an MPTCP endpoint *outside* the checkpoint just moves the problem: that process would itself have to migrate.

**Verdict:** not viable for this use case.

### 3.5 Stable VIP on a dummy or overlay interface inside the pod

The idea: give Paper's sockets a local IP that is identical in every incarnation, so CRIU can rebind it on any node.

- **Dummy interface plus a Cilium-routed VIP:** doesn't work as-is. Cilium only delivers packets to a pod for IPs it knows as that endpoint's IP. Service VIPs are DNATed to the *pod IP* before delivery, so the socket's local address would still be the pod IP.
- **Overlay with roaming (WireGuard):** works on paper.
  1. Create a `wg0` in the pod netns with a fixed inner IP (e.g. `10.99.0.2`) and a fixed key, peering with F (`10.99.0.1`).
  2. F's backend TCP connection goes to `10.99.0.2:25565`.
  3. On restore, re-create `wg0` (same key and IP) in the *new* pod netns before running CRIU. This needs NET_ADMIN in an init container, or a chained CNI plugin.
  4. The restored sockets bind `10.99.0.2` successfully.
  5. The window probe sent on `TCP_REPAIR_OFF` (§1.1) is the first authenticated packet from the new pod IP, and WireGuard roaming updates F's endpoint automatically.

  Caveats:
  - Don't let the new `wg0` handshake (persistent keepalive) before the restore completes, or F's retransmissions will hit an empty netns and get RST (§1.3).
  - F's backend socket still experiences RTO backoff. Cap it with `TCP_RTO_MAX_MS` ≥ 1 s on F (Linux ≥ 6.15).
  - F needs long backend timeouts (e.g. HAProxy `timeout server`/`timeout tunnel` ≥ 60 s).
  - No custom protocol code; an off-the-shelf L4 proxy works as F.

  A reasonable **fallback design** if you'd rather not write the resume protocol.

---

## 4. Minecraft protocol timing (vanilla and Paper)

### 4.1 Keepalive

- **Vanilla:** `keepConnectionAlive()` runs each tick. If `now - keepAliveTime >= 15000` and a keepalive is still pending, the client is disconnected. Otherwise a new keepalive (ID = current millis) is sent. Effective rules: a keepalive every 15 s, kick if the answer hasn't arrived by the next 15 s mark. A wrong ID also disconnects. The wiki says: "If the client does not respond to a Keep Alive packet within 15 seconds after it was sent, the server kicks the client" ([wiki: Keep Alive](https://minecraft.wiki/w/Java_Edition_protocol/Packets#Keep_Alive_(clientbound))).
- **Paper:** keeps the 15 s send interval but only kicks once `elapsedTime >= KEEPALIVE_LIMIT`, where `KEEPALIVE_LIMIT = Long.getLong("paper.playerconnection.keepalive", 30) * 1000`. That's a **30 s default**, tunable with `-Dpaper.playerconnection.keepalive=<s>` (`ServerCommonPacketListenerImpl.java.patch`, lines ~20 and ~184-197).
- Both use `Util.getMillis()`, which derives from `System.nanoTime()` (monotonic).

### 4.2 Read timeouts

- **Both client and server** install a Netty `ReadTimeoutHandler` in their channel initialisers: client `Connection$1` (Yarn `ClientConnection$1`) and server `ServerConnectionListener$1` (Yarn `ServerNetworkIo$1`). The TimeOutOut mod patches exactly that constructor argument in both ([mixin source](https://github.com/PotatoPresident/TimeOutOut/blob/HEAD/src/main/java/us/potatoboy/timeoutout/mixin/ChannelInitializerMixin.java)), describing it as "raised to 120 seconds from the vanilla value of **30 seconds**" ([TimeOutOut](https://www.curseforge.com/minecraft/mc-mods/timeoutout-fabric)). Paper references the handler as `HandlerNames.TIMEOUT`.
- **Semantics:** the handler fires after 30 s with *no inbound bytes at all*. A server normally sends something every tick, so on the client this is "30 s of server silence". The wiki states a stricter "if the server does not send any keep-alives for 20 seconds, the client will disconnect". Treat **~20 s as the conservative design ceiling** until it's verified against a current client.
- **The client cannot be helped by server-side settings.** Without client mods, the client-side limit is the hard ceiling on the *wall-clock* blackout.

### 4.3 Tick loop after a pause

- **Vanilla:** if the server is behind by more than `OVERLOADED_THRESHOLD` (1 s) + 20 ticks (≈2 s in total), it logs "Can't keep up! Is the server overloaded? Running Xms or N ticks behind" and **skips** those ticks (`nextTickTimeNanos += ticks * tickNanos`). CraftBukkit raises the base threshold to 1.5 s.
- **Paper:** replaces this with a `tickSchedule`. It runs at most **`misc.catchup-ticks` (default 5)** extra ticks back-to-back and drops the rest (`MinecraftServer.java.patch`, "improve tick loop"; `GlobalConfiguration.catchupTicks` → `.or(5)`).
- Either way there's no long burst of catch-up ticks, and game time is tick-based, so it doesn't jump.

### 4.4 Watchdog

Paper's `org.spigotmc.WatchdogThread`:

- **Clock:** `monotonicMillis() = System.nanoTime()/1_000_000`. The main thread stamps `lastTick` every tick via `WatchdogThread.tick()`.
- **Loop:** the watchdog thread wakes **every 1000 ms**.
- **Early warning:** a short thread dump if no tick for `watchdog.early-warning-delay` (default **10 000 ms**), repeated every `early-warning-every` (default **5 000 ms**).
- **Long timeout:** if `currentTime > lastTick + timeoutTime`, where `timeoutTime` is spigot.yml `settings.timeout-time`, default **60 s** (`SpigotConfig.java:177`), it logs "The server has stopped responding!", dumps all threads, and calls `server.halt(false, restart)` with `restart-on-crash: true` by default, which runs the restart script.
- **Kill switch:** `-Ddisable.watchdog=true` disables it.

**Would a CRIU freeze trip it?** Both the watchdog thread and the main thread are frozen together, so during the freeze nothing runs. After restore, the watchdog's next check compares the monotonic clock with the last `lastTick`. With raw host clocks across nodes, `CLOCK_MONOTONIC` is "time since that host booted", so the jump could be arbitrarily large or even negative: an instant halt, or a watchdog that's blind for days. With CRIU's automatic time namespace (§5.2), the apparent gap is only dump tail + restore duration, normally well under 10 s, so not even the early warning fires. Even so, as defence in depth, set `timeout-time` ≥ 120 and consider `-Ddisable.watchdog=true` during experiments.

### 4.5 What the server perceives under CRIU (time namespace)

| Timer | Clock | Perceived gap after restore (CRIU timens) |
|---|---|---|
| Keepalive (15 s / Paper 30 s) | nanoTime | dump-tail + restore |
| Netty `ReadTimeoutHandler(30)` server side | nanoTime (Netty ticker) | dump-tail + restore |
| Paper watchdog (10 s warn / 60 s halt) | nanoTime | dump-tail + restore |
| Tick scheduler | nanoTime | dump-tail + restore → ≤5 catch-up ticks |
| Anything using `System.currentTimeMillis()` / `Instant.now()` (plugins, `java.util.Timer`, signed-chat timestamp checks) | `CLOCK_REALTIME` (not namespaced) | **full wall-clock blackout** |

The server side is therefore safe for any realistic freeze. **The client side, with ~20–30 s of wall-clock silence, is the binding constraint.**

### 4.6 Encryption and compression through a byte-transparent proxy

- **Online-mode encryption:** after Encryption Response, both sides use AES/CFB8 with key = IV = shared secret; "everything is encrypted, including the length field"; and "the AES cipher is updated continuously, not finished and restarted every packet" ([wiki: Encryption](https://minecraft.wiki/w/Java_Edition_protocol/Encryption)).
  - A proxy that copies bytes exactly, in order, with no loss or duplication is fully transparent. The cipher state lives in the client and in Paper's JVM heap, and CRIU checkpoints the latter.
  - A **single** lost, duplicated or reordered byte desynchronises the CFB8 stream and the framing, causing garbage and a disconnect. Hence "byte-exact resume".
  - It also means the proxy **cannot inject or filter packets** (e.g. fake keepalives) in online mode.
- **Compression:** per-packet zlib after `Set Compression` ([wiki: packet format](https://minecraft.wiki/w/Java_Edition_protocol/Packets)). It's irrelevant to a byte-level relay.
- The PROXY protocol header is sent by R on the *loopback* connection before any Minecraft bytes, so it doesn't interfere.

### 4.7 Velocity and BungeeCord

- With Velocity in front, **players' TCP and encryption terminate at Velocity**. The Velocity↔Paper connection is a separate one, unencrypted with modern forwarding.
- Migrating Paper then means keeping *that* connection alive. It's the same problem, just with both ends under your control. Velocity's `read-timeout` (default 30 s) applies to the backend connection, and a read timeout disconnects the player rather than failing over.
- Velocity can run the relay pattern too: Velocity → R over a resumable link. Velocity, being a plaintext Minecraft endpoint, could in principle send keepalives to clients during a backend stall, but that would need a custom plugin and keepalive-ID bookkeeping.
- Without a hard requirement for a network of servers, the plain byte relay is simpler.

### 4.8 Blackout budget

| Component | Limit | Notes |
|---|---|---|
| Client read timeout | 30 s (wiki claims 20 s for keepalives) | Hard ceiling on wall-clock silence |
| Windows client TCP (direct designs only) | ~20–60 s (estimate) | Avoided by front proxy |
| Paper keepalive | 30 s (vanilla 15–30 s) | Perceived time only (timens) |
| Paper watchdog | 10 s warning, 60 s halt | Perceived time only |
| **Target** | **≤ 10 s freeze→resume, wall clock** | Leaves margin for RTO tail and retries |

---

## 5. JVM specifics under CRIU

### 5.1 CPU features

- HotSpot picks JIT intrinsics and instruction sets (AVX2/AVX-512, AES-NI, SHA, BMI…) once at startup. glibc picks `memcpy`/`strlen` ifunc variants at load time.
- Restoring on a CPU lacking a feature gives `SIGILL`. CRIU checks this by default: `--cpu-cap` defaults to **`fpu` and `ins`** on dump ("check if CPU supports all instructions required"), and restore refuses mismatches unless `--cpu-cap=none` ([criu.txt](https://github.com/checkpoint-restore/criu/blob/criu-dev/Documentation/criu.txt)).
- **Mitigations:**
  - Homogeneous node CPUs, the simplest. Or pin the lowest common denominator:
    - HotSpot: `-XX:UseAVX=2`, `-XX:-UseAVX512…`, etc.
    - glibc: `GLIBC_TUNABLES=glibc.cpu.hwcaps=-AVX512F,…`.
  - A CRaC-enabled JDK's `-XX:CPUFeatures=generic` (or the bitwise AND of the nodes' feature masks) ([CRaC CPU features](https://github.com/CRaC/docs/blob/master/cpu-features.md), [Azul docs](https://docs.azul.com/core/crac/cpu-features)).
  - OpenJ9 with `-XX:+EnableCRIUSupport` "generates only those compiled codes that are portable" ([OpenJ9 docs](https://eclipse.dev/openj9/docs/xxenablecriusupport/)).

### 5.2 Clocks: `System.nanoTime()` across hosts

- `System.nanoTime()` on Linux is `CLOCK_MONOTONIC`, which is time since that host booted, so it's meaningless across hosts.
- **CRIU handles this automatically.** At dump it records `CLOCK_MONOTONIC` and `CLOCK_BOOTTIME` (`dump_time_ns()`, `criu/timens.c`). It does so even when the tree isn't in a time namespace (`cr-dump.c:2176`: `if ((root_ns_mask & CLONE_NEWTIME) == 0) dump_time_ns(0)`).
- At restore, if the kernel supports time namespaces (≥ 5.6) and CRIU isn't running `--unprivileged`, the root task does `unshare(CLONE_NEWTIME)` and writes `/proc/self/timens_offsets` so the clocks resume **from the dumped values** (`prepare_timens()`, called at `cr-restore.c:1579-1585`). The vDSO honours the namespace, so the JVM's fast path sees the shifted clock.
- **Consequences:**
  1. Monotonic time is continuous across hosts; it never goes backwards and never takes a boot-time-sized jump.
  2. The freeze is *mostly hidden*. The process sees only the time between the clock capture during dump and its resumption after restore (dump tail + restore work). Transfer and pod-creation time are invisible.
  3. `CLOCK_REALTIME` is not namespaced and jumps by the full blackout.
- **containerd/runc and Kubernetes don't create a time namespace for containers.** runc does support `linux.timeOffsets` / timens ([runc timens tests](https://cos.googlesource.com/third_party/runc/+/refs/heads/release-R129/tests/integration/timens.bats)), but kubelet never requests it. It doesn't matter, because CRIU creates one on restore itself.
- **Verify after restore:** `readlink /proc/<pid>/ns/time` differs from the host's, and `cat /proc/<pid>/timens_offsets` shows the offsets.
- **Contrast with CRaC and OpenJ9.** CRaC adjusts nanoTime in the JVM: it "records the wall clock time before checkpoint and after restore and tries to adjust the value provided by nanotime()" ([crac PR #53](https://mail.openjdk.org/pipermail/crac-dev/2023-March/000568.html)), which makes the downtime *visible*. OpenJ9 "compensates for the downtime… for elapsed time sensitive APIs such as `System.nanotime()` and `java.util.Timer`". Both confirm the problem is real. For keeping players connected, the CRIU-timens behaviour (downtime hidden from server timeouts) is the desirable one.

### 5.3 GC threads, JIT, heap

- GC and JIT thread counts and `availableProcessors()` are fixed at JVM start from the container's cgroup limits. Keep the target pod's CPU and memory requests/limits identical.
- Heap size is fixed too. Dump size is roughly the resident heap: `-XX:+AlwaysPreTouch` makes the whole `-Xmx` resident and enlarges dumps.
- To keep the final freeze short, use **iterative pre-dump** (`criu pre-dump` with dirty tracking, plus `--page-server`) so the last dump only carries recently dirtied pages. `--lazy-pages` (post-copy via userfaultfd) shrinks the freeze further, at the cost of a remote-fault dependency on the source.
- The JIT's code cache is just memory and restores fine. Only CPU features matter (§5.1).

### 5.4 Files and locks

- **World directory:** must be identical on the target, via shared/replicated storage or rsync during pre-copy.
- **`session.lock`:** Minecraft holds a file lock on it. Dump and restore with `--file-locks` (`-l`).
- **`/tmp/hsperfdata_<user>/<pid>`:** the JVM mmaps this file. Ship it with the rootfs diff, or run with `-XX:-UsePerfData` or `-XX:+PerfDisableSharedMem` to avoid it.
- PIDs are restored inside the container's PID namespace.

### 5.5 What CRaC teaches

- **Explicit boundary instead of transparency.** CRaC is OpenJDK's coordinated C/R project, using CRIU as one engine. It "requires that there are no open file handles and sockets when you create a checkpoint", and offers a Resource API (`beforeCheckpoint()`/`afterRestore()`) to close and reopen them ([Azul CRaC intro](https://docs.azul.com/core/crac/crac-introduction)). The goal is restoring one image many times anywhere, so CRaC deliberately avoids carrying sockets, CPU features or clocks across implicitly. The relay design is the same idea: give the checkpoint a clean network boundary (only loopback inside, re-dial outside).
- **Explicit handling of environment drift.** CRaC treats CPU features and monotonic time as first-class problems.
- **Adopt the transparent half; borrow the rest.** CRaC itself can't keep live sockets, so it isn't a drop-in. Transparent CRIU with the relay boundary gets you the live-socket property CRaC gives up, while borrowing CRaC's CPU-feature discipline.

---

## 6. Recommended network design

### 6.1 Design

1. **Cilium stays vanilla.** Use `cluster-pool` or `kubernetes` IPAM, tunnel or native routing, kube-proxy replacement as you like. Nothing depends on pod IPs.
2. **Front proxy F**, a small purpose-built TCP relay (Go or Rust):
   - Runs as a Deployment with 1–2 replicas, pinned to nodes that aren't routinely drained (or outside the cluster).
   - Exposed by a `LoadBalancer` Service with an **LB-IPAM VIP**, announced by the **BGP Control Plane** (eTP=Local preserves client IPs and follows F) or by **L2 announcements** (eTP=Cluster, 10–20 s failover).
   - F terminates player TCP and multiplexes streams to R over a resumable link with per-stream byte offsets and bounded buffers.
3. **Relay R inside the Paper container**, started by the same init as Java and therefore in the same CRIU dump:
   - Connects to Paper over `127.0.0.1:25565`, sending a PROXY v2 header with the real client address.
   - Paper settings: `server-ip=127.0.0.1`, `proxies.proxy-protocol: true`.
   - Dials F outbound and resumes from offsets after any reconnect.
4. **Migration sequence:**
   1. Pre-create the target pod/sandbox on node B.
   2. Run iterative `criu pre-dump` (page server to B) while also syncing the world.
   3. Signal R to *detach*: PAUSE, flush, close the F link. F keeps all client sockets and buffers client→server bytes.
   4. Final `criu dump --tcp-established --file-locks` (only loopback TCP remains, plus `--skip-in-flight` for a racing join).
   5. Transfer the delta, then `criu restore --tcp-established --file-locks` into B's pod netns. CRIU puts the tree in a fresh time namespace.
   6. R re-dials F, `HELLO` exchanges offsets, buffers are replayed, and play continues.
   7. Delete the source pod.
5. **Hardening:**
   - `timeout-time: 120` in spigot.yml during rollout.
   - Leave the Paper keepalive at 30 s.
   - Homogeneous CPUs (or pinned features); identical pod resources.
   - Go relays use `GODEBUG=multipathtcp=0`; no io_uring.
   - Incidental outbound connections in plugins either go through R, are closed by a pre-dump hook, or rely on the old-IP-on-`lo` alias trick.
   - Alert if freeze→resume exceeds 10 s.

### 6.2 Trade-offs

| Design | Pros | Cons | Verdict |
|---|---|---|---|
| **A. Direct client TCP plus a preserved pod IP** (multi-pool `/32` by pod name, stable VIP via BGP/L2, CRIU repairs player sockets) | No extra hop; real end-to-end TCP | Not supported by Cilium OSS for live migration (maintainer statement); IP release/realloc, endpoint, ipcache, route and BGP convergence all inside the blackout; CT/NAT state not migrated; old backend must be *deleted* for re-steer; client TCP backoff adds seconds after restore; Windows clients may abort within ~20–60 s; restore aborts on any IP mismatch | **Not recommended** |
| **B. Stable front proxy plus a WireGuard overlay IP in the pod** (CRIU repairs the F↔Paper socket on a fixed inner IP; WG roaming re-points F) | No custom protocol; off-the-shelf L4 proxy works; pod IP irrelevant | NET_ADMIN or a CNI hook to create `wg0` per pod; must keep the target netns quiet until restore; F's socket suffers RTO backoff (cap with `TCP_RTO_MAX_MS`); kernel-level moving parts | **Good fallback** |
| **C. Stable front proxy plus in-pod relay with resumable link (recommended)** | No dependency on CNI/IPAM/pod IP; checkpoint boundary is loopback only (CRIU's best-tested path); deterministic, immediate re-dial with no TCP backoff; real client IPs via PROXY v2; works identically with any CNI; optional QUIC (quic-go client migration) instead of a custom TCP resume | Custom component, though small and testable in isolation; one extra userspace hop (negligible for Minecraft bandwidth); F is a new SPOF that can't itself be migrated this way (keep it stable or redundant); still bounded by the client's 20–30 s silence limit | **Recommended** |
| **D. Velocity front plus server switch** (no CRIU of connections; move players to a fresh backend) | Standard tooling | Visible reload, loses transient state; not live migration | Only as a fallback when a migration fails |

### 6.3 Open questions to validate experimentally

1. **Real client silence limit:** measure the vanilla client's actual timeout under a full silent server (20 s per the wiki vs 30 s from `ReadTimeoutHandler`).
2. **Time namespace under runc/containerd:** confirm CRIU's automatic timens works when CRIU is driven by runc/containerd in k3s (`/proc/<pid>/timens_offsets` after restore).
3. **Freeze budget for your heap:** end-to-end freeze→resume time with pre-dump for a realistic heap (e.g. 6–10 GB) on your network.
4. **Design A only:** Cilium multi-pool `/32` release-and-reassign latency between nodes, and whether mid-stream packets after the move pass Cilium policy and CT.
5. **Windows TCP abort timing:** only relevant if you ever expose clients directly (design A).
