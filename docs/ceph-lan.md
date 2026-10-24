> **Status (2026-10-24):** research draft, nothing applied. Written from the homelab repo and an older
> nixos-dots checkout (the fork's master, which predates upstream #16-#24: e.g. `rollout.nix` exists
> upstream), so re-check the repo facts in section 0 before acting.
>
> **Measured on production the same night** (read-only or test-scoped):
> - Node to node over the tailnet: 136 MB/s single stream, 149 MB/s with 4 streams (iperf3,
>   arquitens -> munificent). The plain LAN did ~273 MB/s earlier (the LAN firewall only admits tcp/22,
>   so it wasn't re-measured).
> - AES-128-GCM on a node: ~4.6 GB/s per core (`openssl speed`), so TLS or Ceph's secure mode costs
>   little next to the LAN's ~270 MB/s.
> - CephFS from a pod on arquitens (fresh 5 GiB volume, fio): 4 KiB random writes with fsync each
>   121 IOPS (~8 ms per synced write); 1 MiB sequential writes 24.5 MB/s (one fsync at the end) and
>   12.2 MB/s (fsync each); 1 MiB sequential reads 35.9 MB/s. Reads go to each object's primary, so
>   read affinity (section 3) would help reads directly.
> - **`sync-chunk-writes` decides save stalls on CephFS for modded servers.** A fresh world with 1024
>   force-loaded chunks, then a timed `save-all flush` (pod on arquitens, new 5 GiB CephFS volume):
>
>   | server | `sync-chunk-writes=true` | `false` |
>   |---|---|---|
>   | Fabric 1.21.10 (vanilla chunk I/O, like Forge/NeoForge) | 25.3 s | 3.5 s |
>   | Paper 1.21.10 (async chunk I/O) | 2.6 s | 3.0 s |
>
>   With `true`, vanilla fsyncs every region-file write, and a synced write costs ~8 ms here. The
>   modded packs inherited `true` (the default) from recusant's local disks. Setting it to `false` in
>   each pack's `server.properties` is the cheapest Ceph fix there is.
>
>   The tradeoff, from the 1.21.10 bytecode (`RegionFile`): `true` opens region files with `O_DSYNC`.
>   A chunk save writes the data to fresh sectors, then the header that points at them, then frees the
>   old sectors. `O_DSYNC` is what keeps those writes in that order on disk. With `false` they sit in
>   the node's page cache, which can write them back in any order. Losing that cache can therefore
>   leave a header pointing at sectors that never got written or now hold another chunk. The server
>   logs it and regenerates the chunk from the seed, so builds in that chunk are lost: that's
>   corruption of individual chunks, not of the whole world.
>   - Losing the cache means a node crash or power loss, or the MDS evicting the node's CephFS client.
>     A JVM crash, an OOM kill, a pod kill or a migration loses nothing: the page cache outlives the
>     process, and CephFS flushes dirty pages before another node opens the file.
>   - Only chunks written in roughly the last 30 s (writeback expiry) are at risk.
>   - A clean stop and `save-all flush` still fsync.
>   - Even with `true`, chunk, entity and POI files aren't crash-consistent with each other. Velero
>     backs up every world volume.
> - lcm's own transfers can already go over the LAN encrypted: `lcm-agent -tls-listen :7791` plus
>   `lcm migrate --transfer-network 10.0.0.0/24` (TLS 1.3 keyed from the agent token; see
>   docs/operations.md), tested in the lab. That needs tcp/7791 open on the LAN interface.
>
> **Measured 2026-10-24, afternoon (arquitens -> munificent, 20 s runs, all test state removed):**
>
> | path | 1 stream | 4 streams | notes |
> |---|---|---|---|
> | plain LAN TCP | 251 MB/s | 250 MB/s | 2.5GbE macvtap |
> | TLS 1.3 over plain LAN | 267 MB/s | 262 MB/s | Python ssl, AES-256-GCM: line rate |
> | tailnet (tailscaled) | 139 MB/s | 133 MB/s | |
> | kernel WireGuard over LAN | 109 MB/s | 116 MB/s | in a netns, MTU 1420, 218/1325 retransmits |
> | **pod network** (host -> pod on another node) | **40 MB/s** | 42 MB/s | Cilium VXLAN inside the tailnet, pod MTU 1230; 32 MB/s reversed |
> | pod -> other node's tailnet IP (no VXLAN, masqueraded) | 130 MB/s | 145 MB/s | what native routing over the tailnet would carry; 127 MB/s reversed |
>
> `rados bench` from the toolbox (pod network, size-3 pool, quiet cluster, OSD commit latency
> 2-3 ms): 4 MiB writes 14.7 MB/s with 1 in flight (271 ms each) and 26 MB/s with 16 (avg 2.3 s,
> max 9.9 s); 4 MiB reads 40 / 63 MB/s; 4 KiB writes 142 IOPS (7 ms).
>
> What this changes:
> - **The bottleneck is the pod network, not the tailnet or the disks.**
>   - VXLAN packets reach tailscale0 as individual UDP datagrams. They lose the TCP segmentation
>     offload that lets host TCP over the tailnet run at 139 MB/s, so Ceph gets ~40 MB/s.
>   - Every Ceph hop is pod to pod (client -> primary OSD -> replicas), and a 4 MiB write costs
>     ~12 MiB on that path, which matches the 271 ms.
> - **(d1) is out.** Kernel WireGuard is slower than tailscaled here. Any tunnel that sends
>   ~1.4 KB packets through the crosvm virtio path hits the same packets-per-second limit.
> - **Encryption over plain TCP runs at LAN line rate**, because 64 KB segments go through virtio.
>   That is (b), Ceph msgr2 secure on host networking over the LAN: roughly 6x today's Ceph path.
> - **Faster pod network for everything, Ceph included:** Cilium native routing over the tailnet
>   instead of VXLAN.
>   - Each node advertises its pod /24 as a Tailscale subnet route (no SNAT) and accepts the
>     others'. Cilium runs `routingMode: native` with `ipv4NativeRoutingCIDR: 10.42.0.0/16` and no
>     autoDirectNodeRoutes.
>   - Pod TCP then reaches tailscale0 as TCP and keeps the offloads: measured 130 MB/s vs 40.
>   - Ceph speeds up about 3x without touching Ceph, and it all stays encrypted by Tailscale.
>   - The catch is the switch itself: nodes in different routing modes can't reach each other's
>     pods, so all three agents change in one short window.
> - **A smaller alternative:** Ceph on host networking over the tailnet IPs. It needs no msgr2
>   secure (tailscaled already encrypts) and no LAN firewall change, gets ~139 MB/s (3.5x), but
>   still needs the same mon IP migration as (b).

> **Production after the changes (re-measured 2026-10-24 ~20:20Z).** Ceph runs msgr2 secure on host
> networking over the LAN (mons g/h/i on v2:10.0.0.x), and Cilium routes natively over the tailnet.
> Measured from arquitens (to munificent where it's node to node):
>
> | | morning | now |
> |---|---|---|
> | plain LAN TCP (1 / 4 streams) | 251 / 250 MB/s | 240 / 246 MB/s |
> | TLS 1.3 over LAN | 267 / 262 MB/s | 258 / 246 MB/s |
> | tailnet | 139 / 133 MB/s | 144 / 155 MB/s |
> | **pod to pod across nodes** | **40 / 42 MB/s** (VXLAN) | **130 / 130 MB/s** (native) |
> | rados 4M write t=1 | 14.7 MB/s, 271 ms | **62 MB/s, 64 ms** |
> | rados 4M write t=16 | 26 MB/s, avg 2.3 s, max 9.9 s | **168 MB/s, avg 0.38 s, max 1.3 s** |
> | rados 4M read t=1 / t=16 | 40 / 63 MB/s | **121 / 431 MB/s** |
> | rados 4K write t=1 | 142 IOPS, 7.0 ms | **220 IOPS, 4.5 ms** |
> | CephFS fio 4k randwrite fsync each | 121 IOPS | **178 IOPS** |
> | CephFS fio 1M write, fsync at end | 24.5 MB/s | **143 MB/s** |
> | CephFS fio 1M write, fsync each | 12.2 MB/s | **41 MB/s** |
> | CephFS fio 1M read | 35.9 MB/s | **105 MB/s** |
>
> Chunk saves (same synctest: 1024 force-loaded chunks, timed `save-all flush`, fresh 5 GiB
> CephFS volume; the evening runs were on munificent because arquitens had no CPU requests left):
>
> | server | `sync-chunk-writes=true` morning -> now | `false` morning -> now |
> |---|---|---|
> | Fabric 1.21.10 | 25.3 s -> **6.0 s** | 3.5 s -> **1.4 s** |
> | Paper 1.21.10 | 2.6 s -> **0.9 s** | 3.0 s -> **0.9 s** |
>
> lcm migration (lcm-test Paper 1.21.10, ~1.4 GB, TLS LAN transport; CPU request lowered to 250m
> so it fits on arquitens):
>
> | | 2026-10-23 (tailnet) | munificent -> arquitens | arquitens -> munificent |
> |---|---|---|---|
> | pre-copy round 1 | 3.67 GB at 55 MB/s | 1.38 GB, shipped in 5.5 s (**253 MB/s**) | 1.45 GB in 5.9 s (**248 MB/s**) |
> | final dump / ship / restore | | 1.2 s / 0.5 s / 2.9 s | 1.1 s / 0.1 s / 1.5 s |
> | **blackout** | 14 s | **4.7 s** | **5.0 s** |
>
> A first run munificent -> carrack failed at CRIU restore, in 0.03 s: `mnt-v2: Can't stat mountpoint
> .../proc/scsi`. runc masks `/proc/scsi` only if it exists, and it exists only once `scsi_mod` is
> loaded. On these nodes that happens whenever the `fwupd-refresh` timer first fires after boot
> (fwupd pulls in `modprobe@sd_mod`), at a random time: 27, 54 and 59 minutes after the 12:57 boot.
> munificent had it, so the source container carried a `/proc/scsi` mask; carrack didn't yet, and
> a mountpoint can't be created inside procfs. lcm rolled back correctly, with an 80 s blackout.
> The same migration retried after carrack loaded the module: **3.2 s blackout** (1.76 GB pre-copy
> in 6.5 s, final dump 1.1 s, restore 2.0 s).

> The morning `rados bench` ran from the toolbox on the pod network. The toolbox is host-networked
> now, so it measures the LAN path the kernel clients use. The fio pod sits on the pod network, but
> its CephFS mount is the node's kernel client, so both reach Ceph over the LAN.

> **Rehearsed in the lab 2026-10-24: option (b), Ceph msgr2 secure plus host networking on the
> LAN.** Lab Ceph mirrors production: Rook 1.20.9, ceph-csi-drivers 1.0.4, Ceph v19.2.7-20261009,
> 3 mons, an OSD per node, CephFS with a hot standby MDS (lab/rook/). The cluster starts on the
> pod network, and node InternalIPs stay on a tunnel as production's do on the tailnet. The "LAN" is
> a separate subnet on the plain NIC. A CephFS writer fsyncs every 200 ms throughout
> (lab/rook/writer.yaml).
>
> 1. **`network.connections.encryption.enabled: true` + `requireMsgr2: true`** (one Helm upgrade).
>    - Rook rolls mons, MDS, mgr and OSDs. The writer's worst pause was about 1 s per OSD restart.
>    - Existing v1 mounts were **not** disconnected, despite Rook's warning. The mons keep their
>      v1 address and `ms_bind_msgr1` stays true, so those mounts carry on unencrypted.
>    - New mounts come up `ms_mode=secure` with `mon_addr=...:3300`.
> 2. **Remount every consumer** (pod restart or lcm migration). Production's mounts today are all
>    v1 (`mon_addr=<mon ClusterIPs>:6789`).
>    - This step is mandatory before step 3. After it, the mons and OSDs are v2-only, and a v1
>      mount can't reach them.
>    - Check every node: each `ceph` line in /proc/mounts has `ms_mode=secure`. Check RBD mappings
>      the same way; RBD wasn't in the rehearsal.
> 3. **`network.provider: host` + `addressRanges` (public and cluster = the LAN)**, after
>    annotating each node with `network.rook.io/mon-ip=<its LAN IP>`.
>    - OSDs, MDS and mgr restart onto host networking and bind the LAN for public and cluster
>      traffic. The writer's worst pause was 2 s (MDS restart), about 1 s per OSD.
>    - **Rook then migrates the mons by itself**, one at a time, from its "fail over list". Each
>      takes a ~20 s election, and the writer saw no stall.
>    - **Do not fail mons over by hand.** I did, in the same second Rook started on another mon.
>      Two of three mons were down, quorum was lost, and I/O froze for 10 minutes until a mon came
>      back. The kernel client then resumed with no errors (one write took 593 s). Set
>      `healthCheck.daemonHealth.mon.timeout` lower only if you want Rook to move faster.
> 4. **Result:**
>    - The monmap is `v2:<LAN IP>:3300` only, and the CSI connection object lists the LAN mons.
>    - Mounted clients follow the monmap even though their mount options still name the old
>      ClusterIPs.
>    - The toolbox, on the pod network, still reaches everything through masquerading.
>    - **Encryption is verified on the wire:** ~12 MB of a marker string written to CephFS
>      appeared 0 times in a capture of the LAN, while a plaintext control on the same capture was
>      found.
>    - Lab throughput didn't change (54 vs 49 MB/s) because the lab is bound by its qcow2 disks.
>      Production's numbers above (pod path 40 MB/s, TLS over LAN 267 MB/s) are what to expect.
>
> **Production runbook for (b)** (a Flux PR per step; `ceph osd set noout` around steps 1 and 3):
> - **Before step 3:** nixos-dots opens tcp 3300 and 6800-7568 on enp0s7 for 10.0.0.10-12 only
>   (like the 7791 rule). Annotate the nodes: arquitens 10.0.0.10, carrack 10.0.0.11,
>   munificent 10.0.0.12.
> - **Steps 1-3 as above.** Step 2 can take days: let pods restart and LivePods migrate, then
>   force the stragglers.
>   - Step 3 needs a node-by-node check first: no line in /proc/mounts lacks `ms_mode=secure`,
>     and RBD mappings likewise.
> - **Not rehearsed:**
>   - RGW (it moves to host networking too; its Service keeps working for pods).
>   - RBD clients.
>   - Rolling back from host networking (another unsupported provider change).
>   - A real 3-node cluster with the production data size, where OSD restarts take longer.
>   Native routing and this change are independent; either can go first.

# Ceph (and live migration) onto the LAN, encrypted: options and a staged plan

Status: research and planning only. Nothing was run against any node or cluster. Written 2026-10-24.

## 0. What the repos say (confirmed facts, and what is still unknown)

From the NixOS checkout (the nixos-dots checkout, master ec03169, which is **older than upstream**: #16/#17/#19-21
and d051fdb are not in it, and there is **no `rollout.nix`** in it):

- k3s servers, all three: `--node-ip = --advertise-address = --bind-address = <tailnet IP>`
  (arquitens 100.126.30.73, carrack 100.103.225.29, munificent 100.65.16.13). carrack and munificent join with
  `serverAddr = https://100.126.30.73:6443`. The `--tls-san` list holds only the tailnet IPs and api.k8s.rooty.dev.
  The tailscale-serve svc:k8s forwards 6443 to `--bind-address` (see the module's assertion).
- LAN: the guest NIC is `enp0s7`. It is a macvtap (bridge mode) on the hypervisor's USB 2.5GbE dongle (r8152),
  with the dongle's MAC and a DHCP lease: arquitens 10.0.0.10, carrack .11, munificent .12. DHCP hands out .2-.199,
  Cilium LB IPAM uses .200-.209, and the VIP is .250.
  - **macvtap bridge mode only delivers unicast to the VM's own MAC.** A macvlan created inside a guest (new MAC)
    won't get LAN unicast. ipvlan (same MAC, new IPs) is the same pattern as the 2026-10-22 carrack outage
    (`ip addr add 10.0.0.249`).
- `kernel.nix`: **`install esp4 /bin/false` / `install esp6 /bin/false`** (commit "properly mitigate"). The IPsec
  ESP modules can't load, so **Cilium IPsec and any other IPsec option are out** unless that mitigation is reversed.
  WireGuard is the only kernel-tunnel option.
- Node kernel `linuxPackages_7_2`: msgr2 secure in the kernel client needs 5.11+, read_from_replica/crush_location
  need 5.8+. Both are fine.
- `networking.firewall.enable = mkForce false` on the *-node guests in this checkout. My memory notes from 2026-10-23
  say the LAN is firewalled except tcp/22, udp/41641 and udp/6666, so upstream has probably turned the firewall on
  since then. **Confirm.** Every option below needs new LAN ports opened.
- NetworkManager runs on the nodes, and tailscale0 is a trusted interface. rp_filter is loose (2) with
  src_valid_mark=1, which the policy-routing option below relies on.
- Each OSD is a Micron 5300 PRO (PLP, write cache off) behind host LUKS, then LVM, then Rook raw mode, passed through
  virtio-blk with O_DIRECT.
- The lcm repo already has `--transfer-network CIDR` (`LCM_TRANSFER_NETWORK`). It ships over plain `http://` to an
  agent address in that CIDR (internal/controller/migrate.go `transferURL`).

From homelab-ro (Flux):

- Rook chart **1.20.9**, Ceph **v19.2.7 (Squid)**, with CSI through ceph-csi-operator (ceph-csi-drivers 1.0.4).
- `cephClusterSpec` has **no `network:` and no `csi:` block**, so the chart defaults apply: pod network,
  `encryption: false`, `compression: false`, **`requireMsgr2: false`**, read affinity off.
- CephFS is the chart default `ceph-filesystem`: data pool `data0`, replicated 3, failureDomain host, 1 active MDS
  plus active standby. The Minecraft PVCs use `ceph-filesystem`. The RGW object store is replicated 3.
- No NetworkPolicy references rook-ceph. The only clusterwide policy is `deny-pod-nfs`.
- **Cilium is installed with the cilium CLI and isn't in GitOps.** The CiliumL2AnnouncementPolicy comment says its
  devices are `enp0s7` and `tailscale0`. You have to read the actual Cilium values off the cluster.

Still to confirm on the live cluster (read-only commands):

| What | Command | Why it matters |
|---|---|---|
| Cilium config | `cilium config view` (routing-mode, tunnel-protocol, devices, mtu, ipam + pool CIDR, enable-wireguard, kube-proxy-replacement, cni-exclusive, node-encryption-opt-out-labels) | Options a and c. If IPAM were cluster-pool with the default 10.0.0.0/8, it would overlap the LAN. Pods appear to be 10.42/16, but check. |
| Policy routing | `ip rule; ip route show table 52` on a node | d051fdb ("Cilium-marked packets via Tailscale table") adds an fwmark rule. The rule priority for option d1 has to come before it. |
| How CephFS is mounted today | `grep ceph /proc/mounts` on a node: `ms_mode=`, mon ports `:6789` vs `:3300` | With requireMsgr2=false, the CSI mounts are probably **msgr1 (legacy)**. msgr1 can't be encrypted, so encryption needs `requireMsgr2` plus a remount. |
| Mon addresses | `ceph mon dump` (v1+v2?), `ceph config dump \| grep ms_` | Option b |
| ClientProfile Rook generates | `kubectl -n rook-ceph get clientprofiles,cephconnections -o yaml` | Where kernel mount options and read affinity end up in Rook 1.20 |
| Is Tailscale direct? | `tailscale status` / `tailscale ping <peer>` → `direct 10.0.0.x:41641` | If it is DERP-relayed, that alone explains a lot. |
| DHCP reservations for .10-.12 | router | Node IPs on the LAN must never change. |
| Node firewall on enp0s7 | `nft list ruleset` | Ports listed per option |

## 1. Why it's slow (model used for all estimates)

On a size=3 pool with 3 hosts, one client write of S bytes goes client → primary OSD (S), then primary → 2 replicas
(2S, both leaving the primary's one uplink). It is acked after all three commit. For one 4 MiB object in flight:

    T ≈ S/B (client→primary) + 2S/B (primary uplink) + commit  =  12 MiB / B + ~6 ms

The current effective node-to-node bandwidth for VXLAN inside tailscaled is B ≈ 45–60 MB/s, which gives
1/(12/55 + 0.006) × 4 ≈ 18 MB/s. That fits the measured 8–17 MB/s single stream: **large writes are bandwidth-bound
on the tailnet.** Small fsync'd writes are mostly latency: 2 network hops + 4–8 ms commit, plus an MDS round trip for
creates and renames.

With buffered writes (many objects in flight), the client node's uplink carries
2/3·1 (remote primary) + 1/3·2 (local primary sends both copies) = **1.33 bytes per byte written**.
So the ceiling is ≈ 0.75·B: about 45 MB/s today, about 190 MB/s on a 250 MB/s path.

Where the bandwidth goes today (memory notes from 2026-10-23): the physical path is already the LAN, because
Tailscale peers connect directly over 10.0.0.x. What is slow is userspace WireGuard (tailscaled about 1.4 cores at
60 MB/s) plus **packet-rate loss in the crosvm virtio path**: any WireGuard turns traffic into MTU-sized UDP packets,
while plain TCP crosses virtio as 64 KB TSO frames. #17 (vhost-net, mrg-rxbuf, txqueuelen 4096) targeted that loss,
and the "raw ~270 MB/s" figure suggests it helped. So "move Ceph to the LAN" really means: **stop paying for userspace
WireGuard and small packets.** The options differ mainly in whether traffic stays in WireGuard (small packets) or uses
TLS-like encryption over plain TCP (big TSO frames).

## 2. Options for getting Ceph traffic onto the LAN, encrypted

### (a) Cilium on LAN node IPs, with Cilium WireGuard (IPsec excluded: esp4/esp6 blocked)

What it is: k3s `--node-ip` on 10.0.0.x, so Cilium's tunnel and WireGuard endpoints become LAN addresses (Cilium takes
both from the Node InternalIP). Cilium transparent encryption is `type: wireguard` with `nodeEncryption: true`. Tunnel
mode can stay (VXLAN is encapsulated first, then WireGuard). Native routing with `autoDirectNodeRoutes` would also work,
since all nodes share one L2, but switching routing mode on a live cluster disrupts every cross-node flow and isn't
needed.

```yaml
# cilium CLI / helm values (1.20). Set encryption FIRST, while node IPs are still tailnet.
encryption:
  enabled: true
  type: wireguard
  nodeEncryption: true          # beta. Without it, host<->pod traffic is NOT encrypted.
  strictMode: {enabled: false}  # leave off during the transition
# All three nodes are control-plane. Cilium opts nodes labelled node-role.kubernetes.io/control-plane OUT of node
# encryption by default, which would leave kernel CephFS clients (host netns -> mon/OSD pods) and lcm (host->host)
# in clear. Clear the opt-out (agent flag --node-encryption-opt-out-labels=""; check the helm key name for 1.20).
devices: "enp0s7 tailscale0"    # already these, per the L2 policy. Keep enp0s7.
```

```nix
# per node, e.g. carrack-node: change only once Cilium WG is on
extraFlags = [
  "--node-ip=10.0.0.11"
  "--advertise-address=10.0.0.11"
  "--bind-address=10.0.0.11"   # svc:k8s's 6443 forward follows bindFlag
];
# k3s module: add --tls-san=10.0.0.10/.11/.12 on all servers first; carrack/munificent serverAddr -> a LAN IP at the end.
```

What changing node IPs means for embedded etcd: k3s expects a server's IP to stay fixed for its life, and the etcd
peer URL comes from the node IP. The k3s docs' only remedy is `--cluster-reset`. Node by node, the safe path is:
`k3s etcd-snapshot save`; stop k3s on node X (containers keep running, because k3s uses KillMode=process); from a
healthy server, `etcdctl member remove <X>`; move `/var/lib/rancher/k3s/server/db` aside; start k3s with the new
flags so it rejoins as a new member with peer URL `https://10.0.0.x:2380`. Until X is back, etcd runs **2 of 2**: any
second failure takes the API down. Mixed state works: old members' 100.x peer URLs are reachable over the tailnet, and
the new LAN peer URL needs tcp/2379-2380 open on enp0s7 (etcd peer traffic is mTLS, so it is fine in clear on the
LAN). Kubelet reports the new InternalIP, and Cilium on the other nodes updates the tunnel/WireGuard peer from the
CiliumNode. Repeat for each node, then optionally drop tailscale0 from Cilium's devices.

- Gain: one encryption layer (kernel WireGuard) instead of userspace WireGuard, and MTU 1500→1370 inner instead of
  1280→1230. Still WireGuard-sized UDP packets through virtio. Expect roughly kernel-WireGuard iperf numbers
  (measure, §4). Likely 2–4× today's, still below raw TCP.
- CPU: kernel WireGuard (ChaCha20-Poly1305, multi-core) is about 0.5–1 core per Gbit/s per side on Zen 3 with steal.
  That is far cheaper than tailscaled.
- Kernel client: unaffected (no Ceph change).
- Downtime: none for workloads if it goes right. Brief packet loss when Cilium (re)configures WireGuard and when
  each node's IP flips. Three etcd member re-adds.
- Rollback: the same re-add procedure backwards, per node. Heavy.
- Risks: etcd 2-of-2 windows. Touches every component that knows node IPs (TLS SANs, serverAddr, tailscale serve,
  netconsole, lcm, VictoriaMetrics scrape targets on :2381, the recusant NFS export, which allows "its node's tailnet
  address" for masqueraded pod traffic). Changing addresses on prod node networking caused the carrack outage
  (cause unknown). nodeEncryption is beta. **Not recommended unless the goal is to take Kubernetes off Tailscale
  entirely.**

### (b) Ceph msgr2 secure plus Ceph on host networking (or Multus)

msgr2 "secure" is AES-128-GCM per connection, with AES-NI in both the OSD (OpenSSL) and the kernel client (gcm(aes)).
It runs over **plain TCP**, so it keeps TSO/GRO: big frames through virtio, which avoids the pps problem entirely.
This is the option most likely to reach close to raw LAN speed (~250 MB/s).

Rook facts (sources in §6):

- CRD doc: "Changing networking configuration after a Ceph cluster has been deployed is only supported for the
  network encryption settings. Changing other network settings is NOT supported…". The network-providers page is
  softer: changes are "allowed … with great care". When host networking changes, "existing mons will remain running
  with the same network settings", and to finish you **fail over the mons**. If the public network isn't the node
  IP, set the node annotation `network.rook.io/mon-ip`. Without it, host-network mons would bind the **tailnet**
  InternalIP.
- Encryption requires kernel 5.11+ for cephfs, and when it's on, "the mons only accept secure connections".
  `requireMsgr2: true` drops port 6789.

Step 1, the supported change (do it first, still on the pod network):

```yaml
cephClusterSpec:
  network:
    connections:
      encryption: {enabled: true}   # ms_{cluster,service,client}_mode=secure
      requireMsgr2: true            # msgr1 can't encrypt. Today's mounts are probably v1 (check /proc/mounts).
  csi:
    cephfs:
      kernelMountOptions: "ms_mode=secure"   # only if the generated ClientProfile doesn't already carry it
```

Existing kernel mounts keep their mount-time protocol and mode. A legacy (v1) or crc mount won't become secure, and
once the mons require secure it fails to reconnect. **Every CephFS mount has to be remounted**: drain node by node, or
live-migrate each LivePod (the destination gets a fresh CSI mount). For a gentler transition, first set the server
modes to `secure crc` (a preference list, through `cephConfig.global.ms_*_mode`), mount with `ms_mode=prefer-secure`,
roll all pods, check `ceph tell mds.* session ls` / `ceph daemon mon.* sessions` for remaining crc/v1 sessions, then
switch to Rook's `encryption: true`. Whether flipping `requireMsgr2` counts as "supported" is ambiguous in the docs.
**Rehearse in the KVM lab on Rook 1.20.9 / Ceph 19.2.7.**

Step 2, unsupported (maintenance window, lab-rehearsed):

```yaml
  network:
    provider: host
    addressRanges:
      public:  ["10.0.0.0/24"]
      cluster: ["10.0.0.0/24"]
    connections: {encryption: {enabled: true}, requireMsgr2: true}
```

```sh
kubectl annotate node arquitens  network.rook.io/mon-ip=10.0.0.10
kubectl annotate node carrack    network.rook.io/mon-ip=10.0.0.11
kubectl annotate node munificent network.rook.io/mon-ip=10.0.0.12
# OSD/MDS/mgr/RGW roll onto hostNetwork as Rook updates them (one OSD at a time, ok-to-stop).
# Then fail over mons one at a time: scale rook-ceph-mon-a to 0 and wait out the failover timeout
# (default 10 min; lower spec.healthCheck.daemonHealth.mon.timeout for the window). The new mon (d, e, f…) comes up
# on host networking at 10.0.0.x. Wait for quorum and HEALTH_OK, then do the next.
# With 3 nodes and allowMultiplePerNode=false, the replacement may have nowhere to schedule until the old one is gone.
# Rehearse; consider allowMultiplePerNode: true for the window (no port clash: the old mon is in pod netns).
```

Mixed-state reachability: pod-network daemon → 10.0.0.x on another node is BPF-masqueraded out enp0s7. That's in
clear at the IP layer but encrypted by msgr2. Host-network daemon → 10.42.x pod goes over VXLAN/tailnet. Both work.
Existing kernel mounts follow monmap changes while at least one mon stays reachable, which is why mons go one at a
time. The CSI config and the `rook-ceph-active-mons` endpoints follow automatically. LAN firewall: tcp 3300 and
6800-7568 on enp0s7 (6789 not needed with requireMsgr2).

- Gain: Ceph runs at near raw-LAN speed. Single 4 MiB stream ≈ 1/(12/250+0.006)×4 ≈ 70–80 MB/s; buffered ≈ 190 MB/s.
  Latency loses two userspace WireGuard hops and the VXLAN. Only Ceph benefits: etcd, other pods and lcm stay on
  the tailnet.
- CPU: AES-GCM is about 3–6 GB/s per core on Zen 3, so about 0.1 core per side at 250 MB/s. Ceph's own benchmark
  (100GbE/NVMe) saw secure mode cost mostly *single-client large-IO* throughput (and up to −35 % on multi-client 4 MB
  writes combined with LUKS). At 2.5GbE that ceiling is far away.
- Multus variant instead of host networking: possible, but on these VMs the attachment has to be ipvlan on enp0s7
  (macvlan breaks behind macvtap) with whereabouts IPs from the LAN. Those are new IPs ARPing from a VM MAC, the
  carrack-incident pattern. Cilium would also need `cni.exclusive=false`. Not worth it here.
- Rollback: step 1 reverts by setting encryption off (supported). Step 2 reverts the same unsupported way, with mon
  failover back. Keep a monmap/config backup and an etcd snapshot.
- Risks: officially unsupported. Mon identity and IP churn. Every CephFS client remounts. Ceph ports become exposed
  on the LAN (cephx plus secure mode, but more attack surface than a pod network).

### (c) Pod networking for clients, OSD replication only on the LAN (Ceph cluster network)

Feasibility with Rook: yes, but only through Multus. With `provider: multus` and only the `cluster` selector set,
"If `public` is left unspecified, Rook will configure Ceph to use the Kubernetes pod network for Ceph client traffic"
and "Only OSD pods will have both public and cluster networks attached". The host provider can't split like this:
it puts every daemon on host networking.

```yaml
apiVersion: k8s.cni.cncf.io/v1
kind: NetworkAttachmentDefinition
metadata: {name: ceph-cluster, namespace: rook-ceph}
spec:
  config: '{"cniVersion":"0.4.0","type":"ipvlan","master":"enp0s7","mode":"l2",
            "ipam":{"type":"whereabouts","range":"10.0.0.0/24",
                    "range_start":"10.0.0.220","range_end":"10.0.0.229"}}'
---
# cephClusterSpec
network:
  provider: multus
  selectors: {cluster: rook-ceph/ceph-cluster}
  connections: {encryption: {enabled: true}}   # otherwise replication crosses the LAN in clear
```

- Gain: moves 2 of every 3 written bytes, the replication. The client→primary hop and all reads stay on the
  tailnet: T ≈ 4/55 + 8/250 + 6 ms, about 35–40 MB/s single stream. **Roughly half of what (b) gives.**
- Downtime: changing `cluster_network` while running means OSDs on different cluster networks can't heartbeat each
  other. Restart all three OSDs together under `noout`/`nodown`, which pauses I/O for about a minute. Changing the
  provider is officially unsupported, as in (b).
- Risks: installing Multus next to Cilium (cni.exclusive). ipvlan addresses on the LAN (carrack incident). The extra
  addresses must stay outside DHCP .2-199, LB .200-209 and the VIP .250. **Not recommended:** it carries all of (b)'s
  risk for about half the gain.

### (d) A kernel WireGuard mesh on the LAN

**d1: route the tailnet IPs over a kernel WireGuard link on the LAN (recommended first step).** Each node gets a
`wg-lan` interface with **no IP address**. Each peer's AllowedIPs is that peer's *tailnet* /32, its endpoint is the
peer's LAN IP, and a policy rule placed before Tailscale's rules (5210–5270) sends traffic for a peer's 100.x address
into wg-lan with the node's own 100.x as source. No Kubernetes, etcd, Cilium or Rook identity changes. Every
node-to-node flow moves off tailscaled and onto in-kernel WireGuard: Cilium VXLAN (and with it all of Ceph), etcd, the
apiserver, kubelet and lcm transfers. That makes migration memory transfers faster with no lcm change.

```nix
# sketch: modules/system/k3s/lan-underlay.nix
{ config, lib, pkgs, ... }:
let
  nodes = {
    arquitens  = { lan = "10.0.0.10"; ts = "100.126.30.73";  pub = "<pubkey>"; };
    carrack    = { lan = "10.0.0.11"; ts = "100.103.225.29"; pub = "<pubkey>"; };
    munificent = { lan = "10.0.0.12"; ts = "100.65.16.13";   pub = "<pubkey>"; };
  };
  me = nodes.${config.networking.hostName};
  peers = lib.filterAttrs (n: _: n != config.networking.hostName) nodes;
  ip = "${pkgs.iproute2}/bin/ip";
in {
  networking.wireguard.interfaces.wg-lan = {
    privateKeyFile = "/persist/wg-lan.key";   # generated per node, like the Ceph LUKS key
    listenPort = 51820;                        # 51871 stays Cilium's
    ips = [ ];                                 # no address: nothing new for Cilium device detection or the router
    allowedIPsAsRoutes = false;                # routes/rules below
    mtu = 1420;
    peers = lib.mapAttrsToList (_: p: {
      publicKey = p.pub;
      endpoint = "${p.lan}:51820";
      allowedIPs = [ "${p.ts}/32" ];
      persistentKeepalive = 25;
    }) peers;
  };
  networking.networkmanager.unmanaged = [ "wg-lan" ];
  networking.firewall.trustedInterfaces = [ "wg-lan" ];
  networking.firewall.interfaces.enp0s7.allowedUDPPorts = [ 51820 ];
  # Tailscale's ts-input chain DROPs 100.64.0.0/10 sources arriving on any interface but tailscale0.
  # nodivert keeps its chains but stops jumping to them. tailscale0 is trusted by the NixOS firewall anyway.
  services.tailscale.extraSetFlags = [ "--netfilter-mode=nodivert" ];

  systemd.services.wg-lan-routes = {
    after = [ "wireguard-wg-lan.service" "tailscaled.service" ];
    requires = [ "wireguard-wg-lan.service" ];
    wantedBy = [ "multi-user.target" ];
    serviceConfig = { Type = "oneshot"; RemainAfterExit = true; };
    script = lib.concatMapStrings (p: ''
      ${ip} route replace ${p.ts}/32 dev wg-lan src ${me.ts} table 51820
      ${ip} rule add to ${p.ts}/32 lookup 51820 priority 5000 2>/dev/null || true
    '') (lib.attrValues peers);
    preStop = lib.concatMapStrings (p: ''
      ${ip} rule del to ${p.ts}/32 lookup 51820 priority 5000 || true
    '') (lib.attrValues peers);
  };
  # Plus a 10 s timer: if `wg show wg-lan latest-handshakes` for a peer is older than ~150 s, delete that peer's rule
  # (traffic falls back to Tailscale). Re-add it when the handshake is fresh. WireGuard has no carrier, so without
  # this a dead peer black-holes traffic.
}
```

Why it works node by node: the rule only steers what *this* node sends. A node without it still replies over
tailscale0, which is asymmetric but fine with rp_filter=2. Every node needs wg-lan up (with peers configured) before
any node adds its rule, so do it in two passes: pass 1 deploys wg-lan with the routes service disabled, pass 2 enables
the rules one node at a time. **Rollback is instant: `systemctl stop wg-lan-routes` (delete the rule).**

Checks before relying on it:
- Rule priority relative to d051fdb's fwmark rule. Cilium-marked packets must also take wg-lan, so rule 5000 has to
  come before that one.
- Cilium's VXLAN decap accepts outer source 100.B arriving on a non-Cilium device. It should: outer UDP 8472 is
  delivered to cilium_vxlan by the kernel, and 100.B is a known node IP.
- Cilium device auto-detection ignores wg-lan (no global address). Better: pin `devices=enp0s7,tailscale0` explicitly.
- The inner MTU stays at Cilium's 1230-ish (sized for tailscale0) inside wg-lan's 1420, so no fragmentation. Raising
  Cilium's MTU later (`mtu: 1370`, which takes effect for pods only after they restart) cuts the packet rate by about
  10 %, but then the Tailscale fallback can't carry full-size packets. Do it only once d1 is trusted.

- Gain: replaces userspace WireGuard with kernel WireGuard. Packet count stays about the same, so whether this helps
  2× or 4× depends on whether #17 fixed the virtio pps loss. **Measure first (§4).** Expected: Ceph single stream
  about 30–60 MB/s, buffered about 90–150 MB/s.
- CPU: much lower than tailscaled.
- Downtime: none. Reversible in seconds.
- Risks: a new failure mode (black-holing if wg-lan breaks), covered by the watchdog. The ts-input change. Touching
  prod node networking: no addresses are added, but run it at an agreed time with Jacob, one node first (see the
  prod-network-tests note).
- Node config reaches the guests from the hypervisor's store. In this checkout a guest update is a VM restart, i.e. a
  reboot. If upstream's rollout tooling (rollout.nix / node-rollout without `--reboot`) can switch a guest live, d1
  goes out without reboots. Otherwise apply it imperatively at an agreed time (same commands) and let the NixOS
  version land with each node's next planned restart.

**d2: a WireGuard mesh with its own subnet (e.g. 10.99.0.0/24) as the underlay.** As k3s node IPs, it has all of
(a)'s etcd cost. As Ceph's `addressRanges` with host networking, it's (b) with WireGuard-sized packets, so worse than
(b)'s plain-TCP msgr2. Its only advantage over d1 is cleanliness. Not recommended.

**Live-migration transfers:** d1 covers them automatically, because lcm ships to the agent's InternalIP. Without d1,
the encrypted LAN choices are `LCM_TRANSFER_NETWORK=<wg mesh CIDR>` (needs d2's addresses), or adding TLS to the lcm
agent's transfer endpoint and then using `--transfer-network 10.0.0.0/24`. Go TLS AES-GCM runs over plain TCP with
TSO, at the same speed class as (b), and is a self-contained lcm change.

### Summary

| Option | Encrypted by | Packets through virtio | Est. Ceph single stream / buffered | Reboots? | Rollback | Rook-supported? |
|---|---|---|---|---|---|---|
| today | tailscaled (userspace) | ~1.3 KB | 8–17 / ~45 MB/s | – | – | – |
| (a) Cilium WG on LAN node IPs | kernel WG | ~1.4 KB | 40–70 / 100–170 | k3s restarts plus 3 etcd re-adds | heavy | n/a |
| (b) host network + msgr2 secure | msgr2 AES-GCM | 64 KB TSO | 70–80 / ~190 | none (pod restarts, mon failovers) | unsupported reverse | step 1 yes, step 2 no |
| (c) Multus cluster net only | msgr2 secure | mixed | 35–40 / ~70 | none (all-OSD restart, I/O pause) | unsupported | no |
| (d1) tailnet IPs over kernel WG | kernel WG | ~1.3 KB | 30–60 / 90–150 | none if a live switch is possible | seconds | n/a (no Ceph change) |

The bandwidth estimates assume the §1 model and must be replaced by the §4 measurements.

## 3. CephFS locality

**What can't be avoided:** with size=3, failure domain host and 3 hosts, every object has a copy on every node. A
write is acked only after all 3 commit, so at least 2 copies cross the network on every write, whatever the
placement. The floor:
- Local primary: 2·S bytes on the network, 1 network stage (primary → both replicas in parallel, from one uplink).
- Remote primary: 3·S bytes, 2 serial stages (client → primary, then primary → 2 replicas).

So locality saves at most one stage and a third of the bytes. It can't make writes local.

- **CRUSH primary affinity** (`ceph osd primary-affinity osd.N 0..1`) is **per OSD and global across all PGs and
  pools**. It can't follow a volume. With 3 OSDs, giving one OSD affinity 1 and the others 0 makes that node primary
  for everything: good for pods on that node, worse for everyone else.
  - Large-write latency: 12S/B → 8S/B, a third faster.
  - Small sync writes: saves one network hop, about 0.3–1 ms of a 6–10 ms op (5–15 %).
  - It **raises** the client node's uplink load from 1.33 to 2 bytes per byte, so buffered throughput drops from
    0.75·B to 0.5·B.
  - Net: not worth it.
- **Reef's per-PG primary (`pg-upmap-primary`)** would be the right tool, but the docs say it "is not yet supported by
  the kernel client", and kernel clients route to the wrong OSD and hang. **Do not use it** with kernel CephFS.
- **Per-node pools plus directory layouts:**
  - Make three data pools, each with a "local host first" rule. Because CRUSH doesn't de-duplicate across emit
    steps, link the other two hosts into a separate root:
    `ceph osd crush add-bucket not-carrack root; ceph osd crush link arquitens root=not-carrack; ceph osd crush link munificent root=not-carrack`
  - The rule for carrack: `take carrack; chooseleaf firstn 1 type host; emit; take not-carrack; chooseleaf firstn -1 type host; emit`.
  - Then `ceph fs add_data_pool ceph-filesystem data-carrack` and
    `setfattr -n ceph.dir.layout.pool -v data-carrack <subvolume dir>`, or a per-node StorageClass with the CephFS
    `pool:` parameter.
  - Caveats:
    - A layout applies only to **new files**. Existing files keep their pool, and moving them means rewriting them.
    - Rook reconciles the crush_rule of pools in its spec, so these pools would have to be created outside the
      CephFilesystem CR (confirm that Rook leaves unknown data pools alone).
    - Gains are the same as primary affinity, but per volume.
  - **When the pod moves** (which is the whole point of live migration), the primaries stay on the old node. The pod
    is then worse off than with random primaries (0 % local vs 1/3), until the data is rewritten into the new node's
    pool: tens of GB for a world. **This conflicts with live migration. Not recommended.**
- **`read_from_replica=localize` + `crush_location=host:<node>`** (kernel 5.8+, safe since Octopus): because every
  node has a replica, **all reads become local** from whichever node the pod is on, after any migration, with no data
  movement. Reads of objects with in-flight writes bounce to the primary.
  - Rook/ceph-csi does this for CephFS kernel mounts through `csi.readAffinity`:
    ```yaml
    cephClusterSpec:
      csi:
        readAffinity:
          enabled: true
          crushLocationLabels: [kubernetes.io/hostname]   # Rook names CRUSH host buckets after the node
    ```
  - Applies when a node stages the volume afresh: a migration to another node does, but a pod restarting
    where the volume is still staged keeps the old mount options (it takes an unstage, i.e. no pod on that node
    using the volume for a moment). Proposed in uorux/homelab#403 (verified to propagate through Rook 1.20.9's
    CephConnection and ceph-csi-operator 1.0.4 to ceph-csi 3.16.3; Rook turns it off on Ceph 20.2.0, which isn't
    deployed).
  - Saves 100 % of read network traffic and one RTT plus transfer per read miss. Reads come in at the OSD's local
    speed (hundreds of MB/s) instead of tailnet speed, which helps chunk loading, backups, restic/velero reads and
    lcm's shared-file warm-up.
  - Rook warns the OSDs "may consume some extra memory".
  - Writes are unchanged. **Recommended: cheap and migration-friendly.**

Separate from the network: Minecraft world saves are dominated by sync writes. Vanilla, Forge/NeoForge and Fabric
default to `sync-chunk-writes=true` in server.properties, which makes every chunk write a synchronous Ceph round trip
(2 network hops + a 4–8 ms commit). Paper disables it by default; verify for each server type. Setting it to `false`
for the modded and Fabric servers turns a save into buffered writes plus a final flush. Measured (see the top): a
1024-chunk `save-all flush` on Fabric took 25.3 s with `true` and 3.5 s with `false`, and Paper didn't care. The trade-off is losing un-flushed chunks if the node crashes. The 4–8 ms OSD
commit on PLP SSDs is also high (crosvm virtio-blk plus LUKS). Once the network is fixed, it becomes the next floor.

## 4. Measure these first (in this order, all read-only or test-scoped)

1. **State checks** (read-only): the confirm table in §0.
2. **Network paths, node A→B**, each as iperf3 TCP `-P1` and `-P4` 30 s, `ping -c100` RTT, plus `mpstat`/`top` for
   tailscaled, ksoftirqd and steal during the run:
   - a. tailnet 100.x (the baseline after #17)
   - b. plain LAN 10.0.0.x (needs a LAN port, or the TCP simultaneous-open trick from the 2026-10-23 tests)
   - c. **kernel WireGuard**, using the netns trick so the root namespace gets no new address or route:
     ```sh
     ip link add wgt0 type wireguard                     # born in root ns: its UDP socket stays there
     wg set wgt0 listen-port 51999 private-key /run/wgt.key \
        peer <B-pub> endpoint 10.0.0.11:51999 allowed-ips 192.168.77.2/32
     ip netns add wgtest; ip link set wgt0 netns wgtest
     ip -n wgtest addr add 192.168.77.1/24 dev wgt0; ip -n wgtest link set wgt0 mtu 1420 up
     ip netns exec wgtest iperf3 -c 192.168.77.2         # mirror on B
     ```
     This needs udp/51999 open on the LAN. It's still prod node networking, so get Jacob's go-ahead, or run it
     first between two KVM-lab VMs or between two *-hv machines for an upper bound.
   - d. pod-to-pod across nodes (iperf3 pods with anti-affinity): what Ceph actually gets today.
   - e. UDP pps loss at 1420 B (`iperf3 -u -l 1392 -b …`) to see if #17 fixed the loss knee (was ~2 % at 50 kpps).
3. **Crypto cost:** `openssl speed -evp aes-128-gcm` and `-evp chacha20-poly1305` (single core) on a node.
4. **RADOS from the toolbox pod** (pod network, i.e. the current Ceph path), on a throwaway size-3 pool:
   - `rados bench -p bench 30 write -b 4M -t 1 --no-cleanup` (single-stream model check)
   - `-t 16` (buffered ceiling)
   - `rados bench -p bench 30 seq -t 16`
   - `rados bench -p bench 30 write -b 4K -t 1` (latency)
   - `ceph osd perf` during the runs
   - `ceph tell osd.N bench` only in a quiet window (it writes 1 GB to the OSD)
   - Compare `-t 1` 4M against `12 MiB / B_measured(2d) + commit`.
5. **CephFS fio** on a test PVC (ceph-filesystem), pod on node A:
   - `fio --rw=write --bs=4M --size=4G --end_fsync=1` (buffered)
   - `--rw=write --bs=4M --size=1G --fsync=1` (sync large)
   - `--rw=randwrite --bs=4k --iodepth=1 --fsync=1 --runtime=60` (sync small)
   - `--rw=randread --bs=64k --size=2G` after dropping caches (for the read-affinity before/after)
   - a create/fsync/rename loop of 1000 × 16 KB files (MDS path)
6. **Workload:** time `save-all flush` on a representative world with `sync-chunk-writes` true vs false (lcm-test
   Paper with the flag forced, or a modded test server).

Decision rule:
- If 2c (kernel WG) ≥ about 2× 2a and close to 2b, d1 is enough. Stop there, plus read affinity.
- If 2c stays far below 2b (the pps limit persists), Ceph needs plain-TCP encryption: (b).
- If 5 shows small-sync latency dominated by commit, not network, prioritise `sync-chunk-writes` and the disk path
  over (b).

## 5. Recommended staged plan

**Stage 0 (now, no changes):** the §0 confirmations and the §4 measurements. Rehearse everything below in the KVM lab
on Rook 1.20.9 / Ceph 19.2.7 / Cilium 1.20 with the same topology. Plain VMs are fine for the Ceph steps; macvtap
matters only for (c).

**Stage 1 (no reboots, no Ceph risk):**
1. `sync-chunk-writes=false` for the modded/Fabric servers, if step 6 confirms it (homelab config,
   gen-minecraft.py).
2. `csi.readAffinity` in the CephCluster (Flux PR). It takes effect as pods restart or migrate. Verify with
   `grep read_from_replica /proc/mounts`.

**Stage 2 (d1, node by node):**
1. Generate wg-lan keys. Deploy wg-lan without routes (pass 1). Pin Cilium `devices` explicitly. Set Tailscale
   `--netfilter-mode=nodivert`. Add the watchdog timer.
2. Enable the rule on one node (munificent first, not carrack). Watch etcd, `cilium status`, `ceph -s`, Hubble drops
   and the tailscale0/wg-lan counters for a day.
3. Then the other two. Re-measure 2d, 4 and 5.
4. lcm needs no change: migrations follow InternalIPs.
5. If guests can't take a live config switch, apply it imperatively at an agreed time and codify it for each node's
   next planned restart.

**Stage 3 (only if Stage 2 numbers aren't enough; maintenance window): (b)**
1. Encryption + requireMsgr2 + secure mounts (supported). Roll all CephFS consumers node by node or by live migration.
   Confirm no crc/v1 sessions remain.
2. Open LAN tcp 3300 and 6800-7568 on enp0s7. Annotate the `network.rook.io/mon-ip` values, set provider host and the
   addressRanges, let the OSD/MDS/mgr/RGW roll, then fail mons over one at a time (lowered timeout,
   `allowMultiplePerNode` as rehearsed).
3. Rollback plan: the same in reverse. Take an etcd snapshot and a Ceph mon/config backup before starting.

**Not recommended:** (a) (three etcd re-adds and beta node encryption for a gain d1 gets more cheaply), (c) (all of
(b)'s risk for half the gain, plus ipvlan on the LAN), per-node pools and primary affinity (they fight live migration
or hurt throughput), pg-upmap-primary (kernel client unsupported), anything IPsec (esp4/esp6 deliberately blocked).

## 6. Sources

- Rook network providers (changing networks, host conversion needs mon failover, mon-ip annotation, Multus
  public/cluster semantics): https://rook.io/docs/rook/latest/CRDs/Cluster/network-providers/
- Rook CephCluster CRD (network settings, "only supported for the network encryption settings", 5.11 kernel):
  https://rook.io/docs/rook/latest/CRDs/Cluster/ceph-cluster-crd/
- Rook cluster.yaml comments (encryption/requireMsgr2, Multus selector behaviour, readAffinity):
  https://raw.githubusercontent.com/rook/rook/master/deploy/examples/cluster.yaml
- Rook chart 1.20.9 defaults (requireMsgr2 false, ceph-filesystem/data0):
  https://raw.githubusercontent.com/rook/rook/v1.20.9/deploy/charts/rook-ceph-cluster/values.yaml
- Rook mon health/failover: https://rook.io/docs/rook/latest/Storage-Configuration/Advanced/ceph-mon-health/
- Rook CSI read affinity for RBD and CephFS:
  https://rook.io/docs/rook/latest/Storage-Configuration/Ceph-CSI/ceph-csi-drivers/
- mount.ceph options (ms_mode, read_from_replica, crush_location): https://docs.ceph.com/en/latest/man/8/mount.ceph/
- Ceph read balancer (pg-upmap-primary not supported by the kernel client):
  https://docs.ceph.com/en/latest/rados/operations/read-balancer/
- Ceph Reef encryption performance (msgr2 secure overhead):
  https://ceph.io/en/news/blog/2023/ceph-encryption-performance/
- Cilium WireGuard (1.20.4: tunnel-then-WG, port 51871, nodeEncryption, control-plane opt-out):
  https://docs.cilium.io/en/stable/security/network/encryption-wireguard/
- k3s embedded etcd IP changes (static IPs expected, `--cluster-reset`):
  https://forums.suse.com/t/k3s-w-etcd-fails-to-restart-after-ip-change/42496 and https://github.com/k3s-io/k3s/issues/2345
