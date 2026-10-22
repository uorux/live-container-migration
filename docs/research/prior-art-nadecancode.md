# Prior art: nadecancode/live-container-migration

- Repo: https://github.com/nadecancode/live-container-migration (default branch `master`, 127 commits, 2024-02-05 to 2024-03-21, plus the report PDF uploaded 2024-08-11). The `conntrack` branch is fully merged into `master`. There are no GitHub issues or PRs.
- Origin: final project for UCSD CSE 291 (Virtualization & Cloud Computing, Winter 2024). Authors are Allen Zhang ("nade"/"allen") and Jacob Root. Commits by `Jacob Root <me@rooty.dev>` cover the networking (WireGuard, conntrack, iptables) and the test harness.
- Writeup: `CSE 291 - Final Project Report (1).pdf` (7 pages, in the repo root). The graphs are in `graphs/`. Raw data and notebooks are in https://github.com/nadecancode/cse291-analysis, which holds only `visualizer.ipynb` and `data/`.
- Local clone for reference: `/tmp/claude-1001/-home-jrt-Documents-live-container-migration/6aa04806-2dec-424f-aed9-482e8bdb06a7/scratchpad/prior-art/live-container-migration`. It was read only. Nothing in it was executed.

## TL;DR

This project is **not Kubernetes**. It has no CRDs, no operator, no kubelet and no containerd. It migrates a **single Podman container**, using runc and netavark, between two **standalone Linux hosts** on AWS EC2 (Arch Linux). Two Python scripts drive Podman's REST API: a Flask "server" agent on the destination and an interactive "client" script on the source. The project's real contribution is **keeping established inbound TCP connections alive** across the move. It does this with CRIU `--tcp-established` (TCP repair) plus a hand-built **forwarding chain**: the source host's conntrack DNAT entries are rewritten to point into a WireGuard tunnel to the destination, and fwmark policy routing on the destination sends replies back through the tunnel. **Traffic stays tromboned through the original host for the life of each migrated connection**, and through every intermediate host after repeated migrations. The pod-IP problem is avoided rather than solved: Podman restores the container with the *same* private bridge IP on the new host, and clients only ever saw the host's public IP and published port. Storage is not addressed. Downtime is about 0.85 s for a tiny echo container on warm hosts, and about 24 s cold. This is proof-of-concept code. The concepts are reusable but the code is not.

## 1. Architecture

| Component | Path | What it is |
|---|---|---|
| Source agent ("client") | `agent-client/main.py`, `client.py`, `exporter.py` | Interactive CLI built with the `bullet` library: you pick the destination host/port and the running container. Flag `--use-cli-args --host --port --container_num` skips the prompts. A single linear script that runs the whole migration. |
| Destination agent ("server") | `agent-server/main.py`, `server.py` | Flask HTTP server bound to `0.0.0.0:50051` (`agent-server/main.py:19`). Long-running. Receives uploads, sets up WireGuard and routing on request, and runs `podman container restore`. |
| Shared networking logic | `common/net.py` | WireGuard tunnel setup and bookkeeping, iptables, policy routing, tc netem, and conntrack dump/rewrite/insert, all shelled out with `subprocess.run(..., shell=True)`. |
| Test harness | `wg_migration_test.py` | Uses SSH to start the agents on 3 nodes and an iperf3 server container. Runs a client on a 4th node and migrates node0→node1 at t≈20 s, then node1→node2 at t≈40 s. |
| Test containers | `container/example/{simple,io,io-openfd,net-in,net-out}` | Toy Python containers. The `net-*` examples are unfinished. The real network tests used `docker.io/cjimti/go-echo` and `docker.io/networkstatic/iperf3`. |
| Abandoned early design | git history before `5dfa61d` (`agent/grpc/protos/agent.proto`) | A gRPC `LCMAgent` service with streamed file chunks. This was replaced by Flask/HTTP. |

- Language: Python 3.11, managed with Poetry (`pyproject.toml`). Dependencies are `podman` 4.9 bindings, `flask`, `requests==2.25.0` (pinned so `requests-unixsocket` works), `filelock` and `netifaces`.
- Talks to Podman over its REST API on `/run/podman/podman.sock` (`agent-client/util.py`), plus the `podman` CLI for `inspect` and `restore`.
- There is no controller, no scheduler, no CRD and no persisted migration state beyond `/tmp/tunnels.json`. Migrations are triggered by a human running `python agent-client/main.py` on the source host.

### End-to-end flow (`agent-client/main.py`)

1. `POST /start` with container id (`main.py:83`, `server.py:53`).
2. **WireGuard tunnel bring-up/reuse** (`main.py:96-177`). If both sides already have a complete tunnel it is reused. Otherwise both sides tear down and run a full handshake: exchange `ip -o addr` lists, randomly pick a non-overlapping `10.x.y.z/30` (`net.py:21-48`), exchange WG ports and public keys, create the interface `wg-<b>-<c>-<d>` with `allowed-ips 0.0.0.0/0` (`net.py:345-350`), and install migration routing on the destination (`net.py:88-99`).
3. `podman inspect` to get the container's bridge IP and published ports (`main.py:180-191`).
4. **One pre-dump**: `POST /libpod/containers/{id}/checkpoint?export=true&preCheckpoint=true` (`exporter.py:29-40`). The tarball is written to `/tmp/exports`, then uploaded (`main.py:213-221`).
5. **Start of the network cutover window** on the source:
   - `tc qdisc add dev <wg> root netem delay 60000ms` (`main.py:235`, `net.py:132`). This buffers packets headed into the tunnel instead of dropping them.
   - `iptables -I FORWARD -s <container_ip> -j DROP` for tcp and udp (`net.py:298-302`). This drops everything the still-running old container emits, so it cannot recreate conntrack entries during the rewrite.
   - For each published port, dump the conntrack entries and rewrite them to DNAT to the destination's WG IP instead of the container IP (`main.py:242-249`, `net.py:145-271`).
6. **Final dump**: `checkpoint?export=true&tcpEstablished=true&keep=true&withPrevious=true` (`exporter.py:16-27`). `leaveRunning` is not passed, so the container stops here. (The report says `--leave-running`; the code does not.)
7. Upload the final checkpoint (`main.py:275`). This is a serial upload in 1 MiB HTTP POST chunks with a `Range` header (`client.py:34-72`, `server.py:72-85`).
8. `POST /restore` with the source's WG IP and the *original* conntrack entries (`main.py:290`). The destination inserts the conntrack entries, then runs `podman container restore --tcp-established -i /tmp/checkpoint.tar.gz --import-previous=/tmp/pre-checkpoint.tar.gz` (`server.py:102-122`).
9. Remove the netem delay (`main.py:297`), `POST /complete`, `podman rm -f` the source container (`main.py:309`), delete the netem qdisc and the FORWARD drop rule (`main.py:311`). The conntrack rewrite and the WG tunnel stay in place, because they *are* the forwarding path from then on.

## 2. Checkpoint / restore mechanism

- **Runtime:** Podman 4.9 with **runc**. `setup.sh` notes switching Podman's OCI runtime to runc, and the test uses `podman run --runtime=runc --privileged`. Networking is netavark's default bridge (`podman0`, 10.88.0.0/16). Podman calls CRIU through runc.
- **Checkpoint:** the Podman libpod REST endpoint `POST /v4.9.0/libpod/containers/{id}/checkpoint` with `export=true`. The response body is the checkpoint tarball, saved as a file (`agent-client/exporter.py`). Kubelet, crictl, ctr and direct CRIU calls are not used.
- **Restore:** `podman container restore --import` (shell CLI) on the destination. Podman recreates the container from the exported tarball. The tarball contains the CRIU images, the container config, the rootfs diff and, by default, named-volume contents (see §5). The image must be present or pullable on the destination. No custom shim, no OCI checkpoint image and no patched runtime are involved.
- **Pre-copy:** exactly **one** CRIU pre-dump (Podman `--pre-checkpoint`), followed by a final dump with `--with-previous`. Restore uses `--import-previous`. There is no iteration until dirty pages converge. The report (§VII.F) says they "leaned more into the network aspect than the minimizing downtime aspect … running multiple checkpoints until the sizes converge". Pre-dump requires kernel soft-dirty tracking. The EC2-optimized Arch kernel lacked it, as well as WireGuard, so they used the stock `linux` kernel (report §VII.A.2, §VII.E.2).
- **Ordering:** dump to a local file, then transfer, then restore, all strictly serial. There is no streaming, no CRIU page server and no lazy pages (`criu lazy-pages`/userfaultfd). The report lists "Disk-less migration" and "Iterative migration" on criu.org only as related work.

## 3. Moving checkpoint data

- Custom HTTP upload from source to destination over the hosts' normal network, not the WG tunnel (`client.py:34-72`). Fixed destination paths `/tmp/checkpoint.tar.gz` and `/tmp/pre-checkpoint.tar.gz`. No auth, no checksum, no compression beyond Podman's own export compression.
- No registry, no rsync and no shared storage.
- Bugs in the upload loop: `start` is advanced *before* the POST, so a chunk that raised an exception is skipped on retry and the file is silently corrupted. A non-OK response never increments `sent_chunk_count`, so the loop can spin forever. The destination's `/start` deletes only the stale checkpoint file and not the stale pre-checkpoint file (`server.py:63`), and partial writes reopen with `rb+`.

## 4. Networking (the important part)

### What the setup looks like

- The container sits on a per-host Podman bridge with a private IP (10.88.x.x) and a **published host port** (`-p 5201:5201`). The client connects to `<source-host-public-IP>:<hostPort>`, and the host's netfilter DNATs that to `<container_ip>:<containerPort>`.
- On restore Podman **keeps the same container IP** on the destination's bridge (report §IV.B: "Since podman keeps the container's IP the same, this works"). Inside the container's netns the TCP socket's local address is therefore unchanged, and CRIU TCP repair (`--tcp-established`) can rebuild the socket with the same 4-tuple, sequence numbers and queues. The code does not check whether that IP is already taken on the destination's bridge.

### How established connections survive

All of this is in `common/net.py`, with flow details in report §IV.B–C.

1. **Tunnel:** a point-to-point WireGuard `/30` between the two hosts, one interface per peer pair.
2. **Source host, inbound path:** each conntrack entry for `-g <container_ip> --dport <hostPort>` (the DNAT target) is deleted and re-added with the DNAT target changed to the destination's WG IP and `--reply-port-src` changed to the host port (`rewrite_source_conntrack_entries`, `net.py:236-271`). Client packets for an existing flow now get DNAT'd to `<dest-WG-IP>:<hostPort>` and routed into the tunnel by the connected `/30` route. This uses the `conntrack` CLI, so it is a delete followed by a re-add, which is not atomic. The FORWARD DROP rule exists to keep the old container from racing with it.
3. **Destination host, inbound path:** the same entries are inserted with orig-dst set to the dest WG IP, the DNAT target still set to the (unchanged) container IP, and a conntrack mark of `mark+1` (`add_dest_conntrack_entries`, `net.py:276-295`). This happens *before* `podman restore`, so the first packets after restore are DNAT'd into the container's veth.
4. **Destination host, reply path (fwmark reflection):** these rules come from a unix.stackexchange answer cited as [17] in the report.
   - Global: `iptables -t mangle -A PREROUTING -j CONNMARK --restore-mark` (`net.py:390`).
   - Per tunnel: packets entering from `wg` get `MARK mark+1` plus `CONNMARK --save-mark`. A packet carrying `mark+1` (a container reply whose connmark was restored) is re-marked to `mark`. `ip rule fwmark mark lookup table` with `default dev wg` sends it back into the tunnel (`net.py:88-99`).
   - Back on the source, the rewritten conntrack entry reverse-NATs the reply so it appears to come from `<source-public-IP>:<hostPort>`, which is what the client expects.
5. **Cutover buffering:** netem delays packets in the source→dest tunnel by 60 s while the final dump, transfer and restore run. The delay is then set to 0 so queued packets flush, and TCP retransmission covers whatever is lost. netem's default queue limit (1000 packets) is a plausible cause of the high-pps UDP failures. The report hypothesises this and cites [6].
6. **Multi-hop:** after A→B→C, packets go client→A→wg→B→wg→C and replies go back the same way. Marks are host-local, so the scheme chains (report §IV.C, commit `0871e00 Fix multihop conmark fallthrough`). One debugging story: creating the `--restore-mark` rule per tunnel instead of once per host broke the live demo when two tunnels ended on the same node (report §VII.A.1, commit `af19024`).

### What it does NOT solve

- **The source host remains a permanent data-plane dependency** for every migrated connection, and so does each intermediate host after chained migrations. If the source host goes away, those connections die. In the report's own words: "migrated network connections currently have to go through the original node" (§VII.E.4). In the iperf tests, TCP throughput dropped from about 2–3.5 Gbit/s to about 0.5–1 Gbit/s after one hop and about 0.3–0.5 Gbit/s after two (`graphs/figure-7.png`), and RTT and jitter increased.
- **New connections** after migration must go to the *destination* host's IP. Nothing updates DNS or any service endpoint. The optional DNAT rule that forwards new connections arriving at the old host (`net.py:114-125`) is commented out in `main.py:249`, because it polluted the source host and blocked reuse of the host port there.
- **Only inbound connections to published ports** are migrated, because entries are selected with `-g <container_ip> --dport <hostPort>`. Outbound connections from the container (SNAT/MASQUERADE) are not handled, and the report says SNAT made live migration impossible in their first attempt (§VII.C.1). Container-to-container traffic and connections from the same host are also not handled.
- **There is no notion of a cluster network, Service, kube-proxy, overlay or CNI.** The report itself says the "correct" approach is an overlay network that gives each container a unique, routable IP and re-routes it after migration. They considered eBPF/DPDK and deliberately avoided both, and avoided macvlan/ipvlan because of AWS anti-spoofing (§VII.D.1).
- **Cilium is not mentioned anywhere.** All of the mechanisms are iptables-legacy-style netfilter: `nat`, `mangle`, `CONNMARK`, conntrack and ip rules. In a Cilium cluster, especially with kube-proxy replacement or eBPF host routing, pod traffic largely bypasses iptables and netfilter conntrack: Cilium has its own BPF conntrack and NAT maps. **The core trick does not carry over as is.**
- UDP is "somewhat working": at high pps the restored iperf container stopped receiving, and at high throughput a socket error ended the test (§VII.E.5). IPv4 only.

## 5. Volumes / persistent storage

- **The code does not address storage.** The proposal planned a "volume content patching" step to sync writes made during migration, but it was dropped. The reasoning was that the final dump stops the container, so nothing changes after it (§VII.D.2).
- **Implicit behaviour (inferred from Podman, not tested by the project):** `podman container checkpoint --export` includes the container's rootfs diff and, unless `--ignore-volumes` is set, the **contents of named local volumes** in the tarball. Bind mounts are not included. The exporter passes neither `ignoreVolumes` nor `ignoreRootFS`, so named volumes travel inside the final checkpoint during the downtime window, uncompressed by any delta logic. For a multi-GB Minecraft world this would put the entire world copy in the downtime window. Bind-mounted world directories would need to exist on the destination at the same path, and nothing ensures that.
- No shared or replicated storage, no PV handling and no pre-sync of volume data were used.

## 6. Limitations, failure modes, measurements, versions

**Measured (report §VI.C, single small container `cjimti/go-echo`):**
- Warm ("hot") migrations: **1.14 s total, 0.85 s downtime** on average over 10 runs.
- Cold (first migration after reboot): **about 49 s total, about 24 s downtime** over 5 runs, mostly in restore. The authors attributed this to "some kind of caching". The likely cause is image pull or first-use overhead on the destination; in the README's words, "migrate another instance of the same container image first".
- iperf (`graphs/*.png`): a 1–2 s throughput hole at each migration, then permanently lower throughput because of the trombone. UDP loss spikes at migration and jitter grows with hops. Only runs where the first migration finished in under 20 s were kept (`wg_migration_test.py` labels the results "bad"/"notbad").
- **Nothing was measured for large memory footprints.** The test containers have tiny RSS, so these numbers say almost nothing about a multi-GB JVM heap.

**Versions and environment:**
- Arch Linux on EC2 with the stock `linux` kernel (the EC2-optimized kernel lacks soft-dirty/pre-dump support and WireGuard). They tried Ubuntu first and gave up on version skew (§VII.A.2).
- Podman 4.9 (the API path is hard-coded as `/v4.9.0/`), runc, netavark, conntrack-tools, iproute2, wireguard-tools, Python ≥ 3.11.
- The exact CRIU and kernel versions are not recorded. Arch in March 2024 would imply roughly CRIU 3.19 and kernel 6.7/6.8 (inference).
- Requires root, privileged containers (`--privileged` in the test) and host-level iptables, tc and wg control. The report says this will not change (§VII.E.1).

**Failure modes and design hazards:**
- Non-atomic conntrack delete/re-add via the CLI. Creating "conflicting" entries is refused with EPERM. They suggest using libnetfilter_conntrack directly (§VII.A.3). Parsing depends on the exact `conntrack -L` output format (`net.py:145-231`).
- No rollback. If restore fails after the conntrack rewrite, `net_cleanup()` removes netem and the DROP rule, but the source conntrack entries still point into the tunnel and the source container is already stopped (the checkpoint ran without `leaveRunning`), so connections are lost.
- One migration at a time per host, fixed `/tmp` paths, `rm -rf /tmp/exports` on each run (`main.py:203-205`), and weak state checks (upload and restore endpoints never check `state.status`).
- **Security:** the Flask agent on `0.0.0.0:50051` has no authentication. Anyone who can reach it can upload a checkpoint and have it restored as root, reconfigure WireGuard, or tear down tunnels. All commands are shell-interpolated. The repo also contains a committed OpenSSH private key (`credential/dest_key`).
- netem with a 60 s delay can drop packets once its queue limit is exceeded, and deleting the qdisc drops anything still queued.

**Minecraft / JVM specific:**
- None. There is no JVM, Java or game-server content anywhere in the repo or report. Considerations for our case that the prior art does not address:
  - Final-dump size scales with the dirty heap. A single pre-dump is unlikely to be enough for an actively ticking server, so iterative pre-dump or lazy pages are needed.
  - The vanilla client drops the connection after about 30 s with no traffic, and the server kicks clients whose keep-alive responses are late (verify the exact thresholds for the target version). Downtime and the wall-clock jump the JVM sees after restore must stay under those limits.
  - The JVM will log "Can't keep up!" and attempt tick catch-up after the clock jump.

## 7. Code quality / maturity

- About 1,100 lines of Python written in about 6 weeks for a class deadline. Commit messages show heavy trial and error ("fix parse" ×4, "dbg" ×5, ":sob:"). There are no unit tests, typing or logging, and errors are printed and handled with `sys.exit(-1)`. Every system interaction is `subprocess.run(f"...", shell=True)` with unchecked return codes.
- **Reusable as ideas:**
  - The conntrack-rewrite plus fwmark-reflection recipe (`net.py:88-99`, `236-295`) for forwarding established flows from an old node to a new one.
  - Buffering in-flight packets with a qdisc during cutover instead of dropping them.
  - Dropping the old container's egress during the conntrack rewrite.
  - The overall ordering: pre-dump → transfer → freeze network → final dump with `--with-previous` → transfer → install destination state → restore with `--tcp-established` → release the buffer.
  - The confirmation that CRIU TCP repair works across hosts *as long as the in-netns IP is preserved*.
- **Not reusable as code** for a k3s/Cilium system: it is Podman-only, assumes a host bridge with port publishing, uses iptables/conntrack, has no Kubernetes integration, and has the security and robustness problems listed above.

## 8. Implications for a k3s + Cilium design (our analysis, not from the repo)

- **Checkpoint:** k3s embeds containerd, so the analogue of Podman's export is the kubelet checkpoint API (`POST /checkpoint/{ns}/{pod}/{container}`, `ContainerCheckpoint` feature gate) or driving containerd/runc/CRIU directly from a node agent. The prior art gives no guidance here.
- **Restore:** this is the hard part, and the prior art avoids it entirely (Podman's `restore --import` has no equivalent in kubelet). Options to evaluate: an OCI "checkpoint image" consumed by the runtime (CRI-O supports this; check containerd's current state), or a node agent and runtime hook that restores into a pod sandbox the kubelet created.
- **Pod IP:** CRIU TCP repair requires the restored socket's local IP to exist in the new netns. The prior art got this for free from Podman reusing the bridge IP. With Cilium, the destination pod is normally given an IP from the *destination node's* podCIDR (cluster-pool IPAM). To keep connections alive you need one of:
  - (a) move or pin the pod IP across nodes. This needs a Cilium IPAM mode or feature that allows it; investigate multi-pool, static-IP annotations, or routing a /32 to the new node.
  - (b) restore with the old IP added inside the netns and NAT between old and new pod IP on the destination, which means BPF or iptables inside the pod netns.
  - (c) a prior-art-style trombone through the source node. Under Cilium this would need to be re-implemented in BPF or at the Service/LB layer, not with netfilter conntrack.
- **Client-facing address:** Minecraft clients connect to a stable Service address (LoadBalancer, NodePort or hostPort). Whether established flows survive depends on where NAT state lives. With Cilium kube-proxy replacement that is BPF CT/NAT maps on the node that handled the flow, so the node that NAT'd the client flow must keep forwarding to the new backend, or the client must hit the IP directly. This is the Cilium equivalent of the prior art's conntrack rewrite and has to be designed fresh.
- **Downtime:** expect to need iterative pre-dump or lazy pages, and volume data kept out of the dump (shared or replicated PV, or a pre-synced volume plus a final delta) to stay within Minecraft's keep-alive budget.
