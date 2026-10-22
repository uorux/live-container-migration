# Checkpoint/restore of containers in Kubernetes: state of the art (2026-10-22)

Scope: live-migrating a running Minecraft Java server (JVM, players connected) between nodes of a k3s cluster (embedded containerd, Cilium, NixOS, kernel 6.x/7.x).

Status labels used below:

- **RELEASED**: in a tagged release you can install today.
- **MERGED**: on a main branch or in an API, but not yet usable end to end.
- **PROPOSED**: a KEP, PR or issue only.

Versions and dates come from GitHub release metadata, KEP `kep.yaml` files and source code at the tags named, unless noted otherwise.

---

## TL;DR (decision-relevant)

1. **Checkpoint is easy.** The kubelet `POST /checkpoint/{ns}/{pod}/{container}` API (KEP-2008) is **Beta and on by default since Kubernetes v1.30, and still Beta in v1.37**. It never went GA, although the KEP once targeted v1.33. On k3s it works through containerd's `CheckpointContainer` (containerd ≥ 2.0) as long as a `criu` binary (≥ 3.16) is on containerd's PATH. **However, it always leaves the container running** (`leaveRunning=true`, hard-coded in containerd). It also passes no `--tcp-established` or `--file-locks`. On its own it is a forensic snapshot primitive, not a migration primitive.
2. **Restore is the hard part, and the ground moved in August 2026.** Kubernetes has never had a restore API. The only "Kubernetes-native" restore is an *implicit* runtime trick: put an OCI image with a checkpoint annotation in `image:`, and the runtime restores instead of creating. containerd added this in **v2.1.0 (May 2025, PR #10365)**. After three CVEs, containerd **disabled it by default in v2.2.7 and v2.3.4 (2026-08-12)**, behind `enable_experimental_restore_via_create = true`. It **removed it entirely in v2.4.0 (2026-09-16)**. CRI-O likewise now defaults to `checkpoint_only` (merged 2026-09-04, in 1.37).
3. **Current k3s releases (2026-09-30)** ship containerd **2.3.4-k3s1** (v1.36.5, v1.37.1) or **2.2.7-k3s1** (v1.34.12, v1.35.9). Both still contain restore-via-CreateContainer, but it is **off by default**. You must opt in with a containerd drop-in, which is easy on k3s because of the `config-v3.toml.d/*.toml` import (k3s PR #13680, 2026-03). **When k3s moves to containerd 2.4, this path disappears.** containerd 2.3 is an LTS line with EOL 2028-04-30.
4. **The future official path is KEP-5823 (Pod-level checkpoint/restore):** a `PodCheckpoint` object, `pod.spec.restoreFrom`, and CRI `CheckpointPod`/`RestorePod`. The CRI RPCs were merged for v1.37 (k/k PR #140366, 2026-07-22). The kubelet/API alpha targets **v1.38**. Alpha is **same-node only**, closes TCP connections, and leaves the source running. Cross-node restore and live migration are explicit non-goals deferred to future KEPs. containerd (PR #13822) and CRI-O (PR #10313) implementations are open PRs.
5. **Every downtime-reduction feature is unreachable through kubelet or CRI**: pre-dump/iterative, page-server, lazy-pages, and `--tcp-established`. containerd's task API gained only `parent_path` (v2.4.0). To get low downtime you must drive **runc/CRIU directly** on the node (k3s runc root `/run/containerd/runc/k8s.io`), or put a **runc wrapper behind a RuntimeClass**. Some CRIU options *can* be injected globally via **`/etc/criu/runc.conf`**, which runc always passes to CRIU (`tcp-established`, `tcp-close`, `file-locks`, `skip-in-flight`, `ghost-limit`, ...).
6. **TCP continuity needs the same pod IP on the destination.** Cilium does not support that for migration: two endpoints with one IP, conntrack handover. A /32 `CiliumPodIPPool` gives a static IP but not overlap (Cilium issue #38576, closed "not planned"). KubeVirt solves this only with CNIs that implement IP handover: Kube-OVN, OVN-K, and Calico ≥ 3.3x in BGP mode without overlay. With Cilium, plan for **players reconnecting**, or put a non-migrating proxy in front.

---

## 1. Kubelet ContainerCheckpoint API (KEP-2008)

### Status

| Item | Value | Source |
|---|---|---|
| KEP | 2008 "Forensic Container Checkpointing", sig-node, owner @adrianreber | [KEP README](https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/2008-forensic-container-checkpointing) |
| `kep.yaml` | `stage: beta`, `latest-milestone: v1.30`, milestones alpha v1.25 / beta v1.30 / stable **v1.33 (planned, did not happen)** | [kep.yaml](https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/2008-forensic-container-checkpointing/kep.yaml) |
| Feature gate | `ContainerCheckpoint` (kubelet). Alpha, default off, 1.25–1.29. **Beta, default on, 1.30 → current (1.37)** | [feature-gates](https://kubernetes.io/docs/reference/command-line-tools-reference/feature-gates/) |
| GA criteria | Runtime support in CRI-O and containerd (both done; containerd [#6965](https://github.com/containerd/containerd/pull/6965), merged 2024-03-07, containerd 2.0) plus e2e with both | KEP |
| Restore in KEP-2008 | Explicitly out of scope. A restore RPC was removed from the CRI API on 2022-05-17 | KEP history |

You do **not** need to set any feature gate on k3s ≥ 1.30. Setting `--kubelet-arg=feature-gates=ContainerCheckpoint=true` is harmless.

### How to call it

```
POST https://<node>:10250/checkpoint/{namespace}/{pod}/{container}[?timeout=<seconds>]
```

- Response `200` returns JSON `{"items":["/var/lib/kubelet/checkpoints/checkpoint-<pod>_<ns>-<container>-<RFC3339 timestamp>.tar"]}`. containerd's own e2e script parses `.items[0]` ([checkpoint-restore-kubernetes-test.sh](https://github.com/containerd/containerd/blob/release/2.3/contrib/checkpoint/checkpoint-restore-kubernetes-test.sh)).
- `404` means the gate is off or the pod/container was not found. `500` means a CRI or CRIU failure, and the message carries the CRIU error.
- The default timeout is the kubelet CRI timeout (2 min). Pass `?timeout=` for multi-GB heaps.
- **AuthZ:** kubelet maps `/checkpoint/*` to resource `nodes`, subresource **`checkpoint`**, verb `create` ([kubelet authn/authz](https://kubernetes.io/docs/reference/access-authn-authz/kubelet-authn-authz/)). It does *not* fall back to `nodes/proxy`. `KubeletFineGrainedAuthz` is GA in v1.36. On k3s the kubelet uses webhook authn/authz, so either:
  - use a ServiceAccount token with a ClusterRole granting `create` on `nodes/checkpoint`: `curl -k -H "Authorization: Bearer $TOKEN" -X POST https://NODE_IP:10250/checkpoint/ns/pod/ctr`; or
  - use the admin client cert on a server node (`/var/lib/rancher/k3s/server/tls/client-admin.{crt,key}`).

  Going through the apiserver node proxy (`/api/v1/nodes/<n>/proxy/checkpoint/...`) needs the very broad `nodes/proxy` permission.
- The archive lands in `<kubelet --root-dir>/checkpoints`. On k3s that is `/var/lib/kubelet/checkpoints`. It is mode 0600 root, and it contains **all memory**, including secrets. Nothing garbage-collects it; the [checkpoint-restore-operator](https://github.com/checkpoint-restore/checkpoint-restore-operator) exists for that.

### What containerd actually does (v2.3 source, `internal/cri/server/container_checkpoint_linux.go`)

1. `checkCriu()` checks three things:
   - `enable_criu` is true (the default);
   - `criu` is found, first in the `PATH=` from `[plugins.'io.containerd.shim.v1.manager'] env` if set, otherwise via `exec.LookPath` on containerd's own PATH;
   - the version is ≥ `PodCriuVersion` = **31600 (CRIU 3.16)**.
2. Only containers in `CONTAINER_RUNNING` state can be checkpointed.
3. It calls `task.Checkpoint` with runc options `Exit = false` (**leave running, hard-coded**) and `WorkPath = <container dir>/ctrd-checkpoint`. **No** `OpenTcp`, `FileLocks`, `ExternalUnixSockets` or `ParentPath` is passed.
4. It converts containerd's checkpoint image (an index of CRIU tar, rw-layer diff and spec) into a flat tar.

### Archive layout (containerd)

```
checkpoint/          CRIU image files (pages-*.img, pagemap-*.img, core-*.img, mm-*.img, files.img, ...)
rootfs-diff.tar      tar of the container's writable overlay layer (changes vs. image), gzip-layer extracted
spec.dump            the OCI runtime spec (config.json) of the checkpointed container
config.dump          checkpointctl ContainerConfig JSON: id, name, rootfsImageName, rootfsImageRef (NAME@DIGEST), OCI runtime, checkpointedAt
status.dump          CRI ContainerStatus JSON (labels, annotations, image, mounts, ...)
status               containerd internal status file (used by checkpointctl)
container.log        copy of the CRI log file
dump.log, stats-dump CRIU work files
```

CRI-O archives use the same metadata names (from [checkpointctl `lib/metadata.go`](https://github.com/checkpoint-restore/checkpointctl/tree/main/lib)) and may add more: `network.status`, `deleted.files`, `devshm-checkpoint.tar`, `bind.mounts`, `volumes/` (Podman), `pod.options`/`pod.dump` (pod checkpoints). **Volume contents are never included by kubelet/containerd.** Inspect any archive with `checkpointctl show|inspect|memparse <tar>`.

---

## 2. Restore

### 2.1 Is there a Kubernetes restore API?

- **No, as of v1.37.** KEP-2008 has none.
- **KEP-5823 "Pod-Level Checkpoint/Restore"** ([dir](https://github.com/kubernetes/enhancements/tree/master/keps/sig-node/5823-pod-level-checkpoint-restore)) is the official path. Authors: @rst0git, @viktoriaas, @adrianreber, @andreyvelich. KEP opened 2026-01-29.
  - `kep.yaml`: `status: implementable`, `stage: alpha`, **alpha milestone v1.38**, gate **`PodLevelCheckpointRestore`** (kube-apiserver, kube-controller-manager, kubelet).
  - **MERGED:** CRI `CheckpointPod` and `RestorePod` RPCs in `RuntimeService` ([k/k #140366](https://github.com/kubernetes/kubernetes/pull/140366), merged 2026-07-22, milestone v1.37). Only the CRI API; "the Kubernetes API objects, kubelet checkpoint orchestration, admission, and controller will be introduced in follow up pull requests." The v1.37 feature-gate list does **not** contain `PodLevelCheckpointRestore`. Some third-party "1.37 features" blogs claim alpha in 1.37; that is not what the KEP/gate list says.
  - **Design:**
    - The user creates a `PodCheckpoint` object (`sourcePodName`, optional `sourcePodUID`). The kubelet watches these and checkpoints the whole pod (freezing the pod cgroup) into a runtime-defined directory.
    - Restore: create a new Pod with `spec.restoreFrom: <PodCheckpoint name>`. The `PodRestoreAuthorization` admission plugin requires a `restore` verb on the PodCheckpoint and spec equality. Node affinity to the checkpoint's node is injected.
    - The [pod-snapshot-controller](https://github.com/checkpoint-restore/pod-snapshot-controller) is the reference controller.
  - **Explicit alpha non-goals:** cross-node restore, live migration, IP preservation, TCP preservation (alpha uses "TCP-close": all established connections are closed), stopping the source after checkpoint (source stays `Running`), shared pod resources (shm, volumes), devices.
  - **Runtimes:**
    - containerd: [issue #13979](https://github.com/containerd/containerd/issues/13979) and [PR #13822](https://github.com/containerd/containerd/pull/13822), open.
    - CRI-O: [PR #10313](https://github.com/cri-o/cri-o/pull/10313), open.
    - crictl `checkpointp`/`restorep`: [cri-tools #2169](https://github.com/kubernetes-sigs/cri-tools/pull/2169), draft.
    - gVisor and Kata have tracking issues.
  - Even when it lands, it will not do cross-node live migration for you in 2026/early 2027.

### 2.2 containerd: restore via CreateContainer with a checkpoint image

| containerd | Restore-via-CreateContainer | Source |
|---|---|---|
| 2.0.x | Checkpoint only (no CRI restore) | [#6965](https://github.com/containerd/containerd/pull/6965) |
| **2.1.0** (2025-05) | **Added**: "Support container restore through CRI/Kubernetes" | [#10365](https://github.com/containerd/containerd/pull/10365), merged 2025-03-11 |
| 2.2.0 – 2.2.6, 2.3.0 – 2.3.3 | On whenever CRIU is installed (2.2.6/2.3.3 and later also add `enable_criu`) | release notes |
| **2.2.7, 2.3.4** (2026-08-12) | **Deprecated and disabled by default.** Requires `enable_experimental_restore_via_create = true`. Also `enable_criu = true` (default) plus CRIU present. Emits deprecation warning `io.containerd.deprecation/cri-create-container-checkpoint-restore` | [#13913](https://github.com/containerd/containerd/pull/13913), [#13937](https://github.com/containerd/containerd/pull/13937), [SIG Node mail, 2026-08-12](https://groups.google.com/a/kubernetes.io/g/sig-node/c/tsdff1ptP-0) |
| **2.4.0** (2026-09-16) | **Removed** ("cri: remove restore in CreateContainer"). Adds `parent_path` to runc CheckpointOptions | [#13871](https://github.com/containerd/containerd/pull/13871), [#13699](https://github.com/containerd/containerd/pull/13699) |

The CVEs that triggered the removal are [GHSA-rgh6-rfwx-v388](https://github.com/containerd/containerd/security/advisories/GHSA-rgh6-rfwx-v388), [GHSA-cvxm-645q-p574](https://github.com/containerd/containerd/security/advisories/GHSA-cvxm-645q-p574) and [GHSA-33vj-92qq-66hc](https://github.com/containerd/containerd/security/advisories/GHSA-33vj-92qq-66hc). The rationale: "the implicit behavior means Kubernetes has no visibility of the restore or policy levers".

**How it works (2.3 source, `CRImportCheckpoint`):**

- On `CreateContainer`, if `image:` resolves to a local image whose top-level index/manifest **annotations** contain `org.criu.checkpoint.container.name`, containerd restores. This is checkpointctl's `CheckpointAnnotationName`. containerd 2.1–2.3 vendor checkpointctl 1.4/1.5, where the constant is `org.criu.checkpoint.container.name`. The older `io.kubernetes.cri-o.annotations.checkpoint.name` from the 2022 blog is not what containerd checks. **Set both** to be safe across runtimes.
- Logs show `Found checkpoint of container <name> in <ref>`.
- containerd also accepts `image:` being a path to a tar on the node. kubelet will not pass a path through image management, so in practice you use an OCI image.
- It reads `spec.dump`, `config.dump` and `status.dump`, then **pulls the base image `rootfsImageRef` (NAME@DIGEST)** if it is not present. The destination must be able to pull it, or have it preloaded.
- It creates the container in the **new pod's sandbox**, with the **new pod's container config** (mounts, env appended). Labels/annotations get pod name/UID fixups, and the annotation `restored=true` is added.
- It unpacks `checkpoint/` and `rootfs-diff.tar` into the container dir. On `StartContainer`, the shim runs `runc restore`: runc treats the pod netns as external, re-binds bind mounts to the new sources, and applies the rootfs diff.
- The container name comes from the checkpoint. **Use the same container name in the restore pod.**
- **Known issue:** [containerd #12108](https://github.com/containerd/containerd/issues/12108). Restore succeeds and then the kubelet kills the container. Reported 2025-07, still reproducible per a 2026-02 comment. Test this in the lab, watching probes and container hash annotations.

### 2.3 CRI-O

- Checkpoint has been supported since 1.25. Restore uses the same implicit image mechanism, and CRI-O also accepts archive paths.
- Config, newest first:
  - `[crio.checkpoint_restore] container_level_enabled = "none" | "checkpoint_only" | "checkpoint_restore"`. The default became **`checkpoint_only`** ([#10251](https://github.com/cri-o/cri-o/pull/10251), merged 2026-09-04; CRI-O 1.37.0 released 2026-09-15).
  - The old `enable_criu_support` is deprecated ([#10248](https://github.com/cri-o/cri-o/pull/10248)).
  - So **restore must be explicitly enabled on new CRI-O too**.
- CRI-O also had 2026 restore CVEs (CVE-2026-15801 metadata validation, CVE-2026-53492 CDI annotations in checkpoint metadata).
- The canonical image build (k8s blog 2022, containerd e2e script):

```bash
newcontainer=$(buildah from scratch)
buildah add "$newcontainer" /var/lib/kubelet/checkpoints/checkpoint-<pod>_<ns>-<ctr>-<ts>.tar /
buildah config --annotation=org.criu.checkpoint.container.name=<ctr> \
               --annotation=io.kubernetes.cri-o.annotations.checkpoint.name=<ctr> "$newcontainer"
buildah commit "$newcontainer" registry.example/checkpoint-<ctr>:<tag>   # OCI format (default); docker v2s2 manifests have no annotations
buildah rm "$newcontainer"
```

Notes:

- `checkpointctl` only inspects archives. It does not build images.
- Any tool that can write a single-layer OCI image with manifest annotations works, for example `crane append` + `crane mutate --annotation` (unverified for this exact use).

### 2.4 What people do today to restore on another node

1. Checkpoint via kubelet. The source keeps running.
2. Convert the tar into an OCI image (buildah), then push it to a registry or `ctr -n k8s.io images import` it on the destination node.
3. Delete the source pod (or keep it, for "clone" use cases).
4. Create a pod pinned to the destination (`nodeName`/affinity) with `image: <checkpoint image>`, the **same container name**, and the same volume mounts. If the base image is not cached, the destination pulls it.

This is the flow in the k8s docs, the containerd e2e test, the Red Hat/OpenShift demos and the third-party operators. Consequences:

- State changes between checkpoint and source deletion are lost.
- The restored pod has a **new pod IP**.
- Established TCP sockets either make the dump fail (CRIU refuses established sockets without `--tcp-established`/`--tcp-close`) or cannot be re-bound on the new IP.

For a Minecraft server with connected players, put **`tcp-close`** in `/etc/criu/runc.conf` (see §4.4). CRIU then dumps without connection state and the clients get disconnected.

---

## 3. k3s specifics

### 3.1 Embedded containerd/runc versions (k3s GitHub release notes)

| k3s release (date) | Kubernetes | containerd | runc |
|---|---|---|---|
| v1.37.1+k3s1 (2026-09-30) | 1.37.1 | **2.3.4-k3s1** | 1.4.2 |
| v1.36.5+k3s1 (2026-09-30) | 1.36.5 | **2.3.4-k3s1.36** | 1.4.2 |
| v1.35.9+k3s1 (2026-09-30) | 1.35.9 | **2.2.7-k3s1** | 1.4.2 |
| v1.34.12+k3s1 (2026-09-30) | 1.34.12 | **2.2.7-k3s1** | 1.4.2 |
| v1.36.3 / v1.35.7 (2026-08-04) | | 2.3.2-k3s2 / 2.2.5-k3s2 (restore on by default if CRIU present) | 1.4.2 |

nixpkgs (nixos-unstable, 2026-10) has `k3s_1_34`, `k3s_1_35`, `k3s_1_36` (= the default `k3s`, 1.36.5+k3s1, containerd 2.3.4-k3s1.36) and `k3s_1_37` (1.37.1+k3s1). **No k3s release ships containerd 2.4 yet.** Watch for it: it removes the image-based restore path.

### 3.2 CRIU on the host

- k3s does **not** bundle CRIU. The chain is k3s → containerd → `containerd-shim-runc-v2` → `runc` → `criu swrk`:
  - runc invokes `exec.Command("criu", "swrk", "3")`, i.e. a **PATH lookup**;
  - containerd's CRI plugin does its own `LookPath` for the version check.
- k3s passes its environment through to containerd: it prepends its own `/var/lib/rancher/k3s/data/<hash>/bin`, strips the `CONTAINERD_` prefix, and drops `NOTIFY_SOCKET`. So **criu must be on the k3s service's PATH**.
- Optionally, set `[plugins.'io.containerd.shim.v1.manager'] env = ["PATH=..."]` to pin it. containerd ≥ 2.2.7/2.3.4 uses that PATH for the CRIU check.
- CRIU also shells out to `iptables`/`nft` (network lock), `ip`, and `tar` in some paths. Your `lab/node.nix` already does `systemd.services.k3s.path = [ criu iptables nftables iproute2 kmod util-linux ]`; that is the right pattern. NixOS systemd units do **not** see `/run/current-system/sw/bin`.
- **PATH ordering gotcha:** k3s puts its own bin dir first, and that dir ships busybox `tar` among other tools. zeropod reports that this breaks CRIU and works around it by putting GNU tar ahead. Make sure the tools CRIU invokes resolve to GNU versions.
- Kernel: `CONFIG_CHECKPOINT_RESTORE`, `CONFIG_MEM_SOFT_DIRTY` (pre-dump), `CONFIG_USERFAULTFD` (lazy-pages), `*_diag` modules (`inet_diag`, `tcp_diag`, `unix_diag`, `netlink_diag`, `packet_diag`). Run `criu check --all` on every node.

### 3.3 Enabling restore-via-create on k3s (containerd 2.2.7 / 2.3.4)

- k3s ≥ the 2026-03 releases add `imports = ["/var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.d/*.toml"]` to the generated v3 config ([k3s #13680](https://github.com/k3s-io/k3s/pull/13680), merged 2026-03-02).
- Do not append the table to a `config-v3.toml.tmpl` that includes `{{ template "base" . }}`. The base template already defines `[plugins.'io.containerd.cri.v1.runtime']`, and a duplicate table is a TOML error. Use the drop-in instead:

```toml
# /var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.d/50-criu.toml
[plugins.'io.containerd.cri.v1.runtime']
  enable_criu = true
  enable_experimental_restore_via_create = true
```

- On NixOS, install it with `systemd.tmpfiles.settings` (an `L+` symlink to a store path) or an activation script.
- The NixOS `services.k3s.containerdConfigTemplate` option writes `config.toml.tmpl` (v2 format), which is the wrong tool here.
- Verify with `k3s crictl info | jq '.config.enableExperimentalRestoreViaCreate'`, or look for the deprecation warning in containerd.log.

### 3.4 Known k3s issues

- There are no k3s-specific checkpoint issues in k3s-io/k3s (search 2026-10). Problems reported against upstream containerd apply: #12108; restore requires the base image to be pullable; CRIU-not-found returns a 500.
- k3s's default `local-path` storage is node-local. A restored pod cannot see the world data unless it is on RWX/network storage or copied (see §5).

### 3.5 NixOS CRIU package

- `pkgs.criu` is **4.1.1** on nixos-26.05 and nixos-unstable (2026-10).
- Upstream releases:
  - 4.1 (2025-03-25): pidfd support, nftables lock-name fix.
  - 4.1.1 (2025-07-29): mount-ns restore fix for a kernel change.
  - **4.2 (2025-11-13)**: includes a fix for `SO_PASSCRED/SO_PASSSEC` getsockopt on **Linux 6.16+**.
  - 4.2.1 (2026-07-21), 4.2.2.
  - **4.3 (2026-10-19)**: LZ4 page compression, parallel memfd/shmem restore, AIO page restore, `--image-io-mode` O_DIRECT, TLS + compression for page-server, streaming over RPC with incremental streamed dumps, an `UPDATE_INETSK` hook for rewriting socket addresses on restore, and a TCP_REPAIR_WINDOW ordering fix.
- **On kernel ≥ 6.16 (you run 7.2), use ≥ 4.2.x.** Override `pkgs.criu` in an overlay (`version = "4.3"`, new hash; the two nixpkgs patches are probably already upstream). Then `criu check --all` on 7.x.

---

## 4. Downtime reduction

### 4.1 CRIU mechanisms

- **Pre-dump / iterative migration** ([criu.org/Iterative_migration](https://criu.org/Iterative_migration)):
  - `criu pre-dump -t PID -D A --track-mem` copies memory while the task keeps running.
  - Repeat with `--prev-images-dir ../A` (path relative to the new dir), so each round copies only pages dirtied since the last one. This uses kernel soft-dirty bits and needs `CONFIG_MEM_SOFT_DIRTY`.
  - The final `criu dump --prev-images-dir ../An --track-mem` freezes the task and writes the remaining dirty pages plus all non-memory state.
  - `--pre-dump-mode=read` (process_vm_readv) cuts frozen time compared with the default `splice`.
  - `--auto-dedup` punches holes in parent images.
- **Page server** (`criu page-server --address A --port P` on the destination; `--page-server --address A --port P` on pre-dump/dump at the source) streams pages straight to the destination instead of local disk. That avoids write-then-copy. 4.3 adds TLS and LZ4 compression.
- **Lazy pages / post-copy** ([criu.org/Lazy_migration](https://criu.org/Lazy_migration), userfaultfd):
  - Source: `criu dump --lazy-pages --address SRC --port P`. It does not write memory, and it serves pages until the destination has them all.
  - Destination: `criu lazy-pages --page-server --address SRC --port P` (daemon), then `criu restore --lazy-pages`.
  - The task resumes with only the non-memory state transferred. Pages are faulted in on demand and the rest are pushed in the background.
  - Risk: if the source or the network dies before all pages arrive, the restored process is lost. There is also a fault-latency storm right after resume.
  - Combine with pre-dumps: pre-copy most of the heap, then do the final dump lazily.
- **criu-image-streamer** ([repo](https://github.com/checkpoint-restore/criu-image-streamer)) with `--stream`: streams images through a pipe, for example over the network or into object storage. 4.3 adds incremental streamed dumps.

### 4.2 runc surface (v1.4.2, as embedded in k3s)

- `runc checkpoint` flags:
  - `--pre-dump`: a single pre-dump RPC. runc checks the `MemTrack` feature.
  - `--parent-path <dir>`: sets `ParentImg` and forces `TrackMem`.
  - `--page-server IP:PORT`, `--lazy-pages` with `--status-fd` (a 0 byte is written when the lazy server is ready).
  - `--leave-running`, `--tcp-established`, `--tcp-skip-in-flight`, `--file-locks`, `--ext-unix-sk`, `--link-remap`, `--manage-cgroups-mode soft|full|strict|ignore`, `--empty-ns`, `--auto-dedup`, `--image-path`, `--work-path`.
- `runc restore` flags: `--lazy-pages` (needs the `criu lazy-pages` daemon running), `--tcp-established`, `--file-locks`, `--manage-cgroups-mode` (use `ignore` on **both** sides to restore into a different cgroup, and change the container id or cgroupsPath), `--lsm-profile`, `--empty-ns`, `--bundle`, `-d`.
  - There is **no** `--page-server` on restore. Pages arrive either as files, or through the `lazy-pages` daemon.
- Iterative pattern with runc:

```bash
runc checkpoint --pre-dump --image-path /cp/1 <id>
runc checkpoint --pre-dump --image-path /cp/2 --parent-path ../1 <id>
runc checkpoint --image-path /cp/final --parent-path ../2 [--tcp-established --file-locks] <id>   # stops the container
```

  Add `--page-server dst:port` to stream each round to a `criu page-server` on the destination.
- Things runc does automatically:
  - adds bind mounts as CRIU external mounts (`addCriuDumpMount`);
  - marks an existing netns/pidns joined by path as external (`--external net[ino]:extRootNetNS`) and re-attaches it on restore via `--inherit-fd`;
  - passes `/etc/criu/runc.conf` as the CRIU config file. A container-specific file can be set with the `org.criu.config` annotation; an empty value disables the file.

### 4.3 What is reachable through each layer

| Feature | kubelet API | containerd CRI | containerd task API (`ctr t checkpoint`) | runc CLI | CRIU config file `/etc/criu/runc.conf` |
|---|---|---|---|---|---|
| Full dump, leave running | yes (forced) | yes (forced) | yes | yes | |
| Full dump + stop (`exit`) | no | no | yes (`--exit`) | yes (default) | |
| `--tcp-established` | no | no | yes (`open_tcp`) | yes | **yes** (`tcp-established`) |
| `--file-locks` | no | no | yes | yes | **yes** |
| Pre-dump | no | no | **no** (no `pre_dump` field) | yes | no (a different RPC type) |
| Parent path | no | no | **2.4.0+ only** (`parent_path`) | yes | |
| Page server | no | no | no | yes | possibly (`page-server`, `address`, `port`; untested) |
| Lazy pages | no | no | no | yes | no |
| Restore into k8s pod | n/a | image annotation (≤ 2.3.x, opt-in) | containerd-only container (kubelet won't know it) | outside containerd | |

Conclusion: **pre-dump, page-server and lazy-pages need runc/CRIU called directly.** On k3s you can run `runc --root /run/containerd/runc/k8s.io checkpoint ... <container-id>` on a container that containerd started. Two things to watch:

- With `--leave-running` the shim stays consistent. Without it, the container exits, which the kubelet sees as a container death and may restart in place. Set `restartPolicy: Never`, or delete the pod right after.
- The final image directory must then be restored into the destination pod. With containerd ≤ 2.3 you can still do that via a checkpoint OCI image: pack the `--parent-path` chain flattened. CRIU restore with parent images needs the parent dirs present, or run `criu dump` with `--auto-dedup` and copy everything. Alternatively, use a runc wrapper.

### 4.4 A future-proof injection point: a RuntimeClass wrapping runc (design idea, not an existing project)

- containerd's shim restores natively when `CreateTaskRequest.Checkpoint` is set, and runc's `restore` builds the container from the bundle plus images.
- A wrapper binary set as `BinaryName` for a dedicated runtime handler (RuntimeClass `criu-restore`) could intercept `runc create`. When an allow-listed pod annotation is present (pass it through with containerd `pod_annotations = ["migration.example.io/*"]`), it would:
  1. untar `rootfs-diff.tar` into the bundle rootfs;
  2. run `runc restore --detach --manage-cgroups-mode ignore --image-path <dir> [--lazy-pages] [--tcp-established] --bundle ...` in place of `create`;
  3. turn the following `start` into a no-op.
- Benefits: this survives the containerd 2.4 removal, keeps kubelet in the loop (the pod is a normal pod), and exposes every CRIU flag. Cost: you own the edge cases (pid file, console, exit status, cgroup path).

### 4.5 Typical numbers

See §4.6. These come from published measurements; your lab must confirm them. The rough model:

- Stop-and-copy downtime ≈ dump (freeze + write) + transfer + restore, scaling with *resident* memory.
- Pre-copy downtime ≈ final dirty set / bandwidth + fixed costs.
- Post-copy downtime ≈ fixed costs (≈ 100s of ms), followed by degraded performance while pages fault in.

### 4.6 Published downtime numbers

No published measurement covers a multi-GB JVM, and none covers Minecraft.

- **Guitart 2024, "Practicable live container migrations in HPC clouds: diskless, iterative, connection-persistent"** ([preprint](https://zenodo.org/records/11637685), DOI 10.1016/j.sysarc.2024.103157). Setup: runc 1.1.8, CRIU 3.18, kernel 5.19, 1 GbE, 4 GB VMs; NPB class A/B and Redis with 1–2 M keys; established TCP preserved via external netns plus `--tcp-established`. Downtime:

  | Mode | NPB | Redis |
  |---|---|---|
  | Stop-and-copy with page-server | 1.36–1.94 s | 2.15–2.48 s |
  | Pre-copy (1 pre-dump + page-server) | 1.26–1.73 s | 1.89–2.36 s |
  | Post-copy (lazy-pages) | 1.12–1.24 s, size-independent | same |

  - Post-copy pays afterwards: up to +8.5 s total runtime for memory-heavy NPB.
  - Restore-side fixed costs: about 240–270 ms to recreate the netns plus 110–230 ms for `runc restore`.
  - Page-server, which writes straight to the destination, matters far more than tmpfs vs disk.
  - `--auto-dedup` increases downtime.
  - A 3 s restore delay cost the TCP client more than 2 s extra to recover, because of retransmit backoff.
- **Sinha et al., mWarp (IEEE CLOUD 2019)** ([pdf](https://kartikgopalan.github.io/publications/sinha19mwarp.pdf)), CRIU baseline at 10 Gbps:
  - A 1 GB idle container took ≈ 2.56 s total, with ≈ 1.3 s frozen.
  - At a 1 GB/s dirty rate, total time exceeded 10 s.
  - Memory copying is more than 70% of dump-side and about 96% of restore-side frozen time.
- **Throughput ceilings:** criu-image-streamer reached checkpoint rates of 15 GB/s by load-balancing across several pipes, at 0.1 CPU-s/GB for checkpoint and 1.4 CPU-s/GB for restore ([LPC 2020](https://lpc.events/event/7/contributions/641/)). CRIU 4.3 adds LZ4 compression, compressed TLS page-server, AIO restore and parallel memfd/shmem restore.

**Estimates for a 2–6 GB RSS JVM** (my estimates; dump size ≈ resident anonymous memory, not `-Xmx`):

| Mode | 1 GbE (~115 MB/s) | 10 GbE (~1.1 GB/s) |
|---|---|---|
| Stop-and-copy, page-server to tmpfs | 20–55 s | 3–8 s + 1–3 s restore |
| Stock kubelet path (dump, then content store, then re-tar, then OCI image, then import) | minutes | 15–60 s |
| Pre-copy, final dump ≈ dirty set (eden + dirtied old regions ≈ 0.3–1.5 GB) | 5–15 s | 1.5–4 s |
| Lazy-pages | ≈ 1–2 s freeze, then remote page faults | same, faster fault service |

- **Kubelet path:** it copies memory several times before any transfer: CRIU images, then content store, then extraction, then tar. It is also leave-running, so effective downtime runs from dump start to restore end.
- **JVM under pre-copy:** the young generation is re-dirtied at the allocation rate, and Minecraft allocates heavily. Pre-copy therefore converges to roughly eden plus promotion. A smaller young generation and a GC right before the final dump help.
- **Post-copy:** each fault is a network RTT inside the 50 ms tick budget. Losing the source mid-transfer loses the process.
- **Client tolerance:** the Minecraft client's Netty read timeout is about 30 s (unverified, from decompiled source). In practice, TCP retransmit backoff makes anything over a few seconds feel worse than the raw number.

---

## 5. CRIU constraints relevant to a JVM / Minecraft

Checklist below (based on criu.org, the CRIU man page and source, runc and containerd source, and CRaC docs). Items marked *(unverified)* are inference.

| Area | Constraint / what to do |
|---|---|
| **CPU features** | The JIT, intrinsics and glibc IFUNCs choose AVX2/AVX-512/SHA etc. once at startup, so restoring on a CPU lacking them gives SIGILL. CRIU dump records fpu+ins by default, but **restore checks only `fpu` by default**. Put `cpu-cap ins` (or `all` for identical CPUs) in `/etc/criu/runc.conf`, and run `criu cpuinfo dump` / `criu cpuinfo check` on node pairs ahead of time. CRaC JDKs have `-XX:CPUFeatures=generic` ([CRaC docs](https://github.com/CRaC/docs/blob/master/cpu-features.md)); mainline HotSpot does not. With a plain JDK, use identical CPUs or cap the JIT (`-XX:UseAVX=N`, `-XX:-UseSHA`) and glibc hwcaps via `GLIBC_TUNABLES` *(glibc detail unverified)*. |
| **Options containerd doesn't pass** | CRI checkpoint sets only `Exit=false` and `WorkPath`. CRI-O always passes `--file-locks`. Use `/etc/criu/runc.conf` (runc always passes it to CRIU, and its settings override RPC options; a per-container file can be set via the `org.criu.config` annotation) for `tcp-established`/`tcp-close`, `file-locks`, `cpu-cap`, `ghost-limit`, `skip-in-flight`. |
| **Established TCP** | Without an option the dump fails with "Connected TCP socket, consider using --tcp-established option". `tcp-established` needs the same IP at restore plus network locking. `tcp-close` drops the connections. |
| **File locks** | Minecraft holds `session.lock` via NIO `FileLock` (a POSIX fcntl lock), so `--file-locks` is needed ([What cannot be checkpointed](https://criu.org/What_cannot_be_checkpointed)). On shared storage the source must be dead before restore re-acquires it. |
| **Open regular files / volumes** | CRIU validates the size of open regular files on restore. Files opened O_WRONLY/O_APPEND, such as logs, are exempt. Region `.mca` files are opened rw, so the world volume must be byte-identical at restore: RWX storage, or a copy made while frozen. runc turns all OCI bind mounts (volumes, `/etc/hosts`, `/dev/shm`) into external mounts, and their contents are never checkpointed. |
| **hsperfdata** | `/tmp/hsperfdata_<user>/<pid>` is a MAP_SHARED file. It travels in `rootfs-diff.tar` if `/tmp` is in the rootfs, but **not** if `/tmp` is an emptyDir, in which case restore fails. Use `-XX:-UsePerfData` or `-XX:+PerfDisableSharedMem`. CRaC special-cases it ([JDK-8328344 thread](https://mail.openjdk.org/pipermail/crac-dev/2024-March/001861.html)). |
| **Deleted files** | Open-but-unlinked files become ghost files up to `--ghost-limit` (default 1 MB); `--link-remap` is the alternative. Netty extracts its native epoll `.so` to /tmp and deletes it *(unverified)*. That should fit under 1 MB, but set `ghost-limit 16M` to be safe. |
| **io_uring** | Not supported in mainline CRIU. The 2021 GSoC prototype was never merged, and a 2026 LKML thread reports dumps hanging on iou-wrk threads. The JDK doesn't use io_uring by default; make sure no Netty io_uring transport or plugin does. epoll, eventfd, timerfd and signalfd are fine. |
| **perf events, ptrace, devices** | perf_event and other exotic anon-inode fds can't be dumped, so no async-profiler in perf mode at checkpoint time *(perf specifics unverified)*. No strace/gdb attached. Devices other than null/zero/tty/tun can't be dumped. |
| **pidfd / memfd / rseq / seccomp** | pidfd: since CRIU 4.1. memfd: supported, restored in parallel in 4.3. ZGC backs its heap with memfd; prefer G1/Parallel because pre-dump efficiency with ZGC's multi-mapping is unverified. rseq: supported (needed with glibc ≥ 2.35). seccomp filters are dumped and restored. |
| **cgroup v2** | Supported. runc's default `--manage-cgroups-mode` is soft. To restore into a new pod's cgroup, use `ignore` on both sides plus a changed cgroupsPath. containerd's CRI restore handles this for you. CRIU 4.3 clones cgroup2 mounts with OPEN_TREE_CLONE. |
| **AppArmor / SELinux** | CRIU restores the dumped LSM label, so the same profile (e.g. `cri-containerd.apparmor.d`) must be loaded on the destination. `runc restore --lsm-profile` overrides it. NixOS nodes without AppArmor are uniform. |
| **inotify/fanotify** | Restored by file handle; can fail on overlayfs. `--force-irmap` helps, and 4.3 adds an overlayfs fallback. Only matters if a plugin uses WatchService. |
| **Unix sockets** | Internal sockets are fine, including the JVM attach socket `/tmp/.java_pid<pid>`; don't run jcmd during a dump. An external peer needs `--ext-unix-sk`. |
| **SysV IPC / shm** | Pod-level IPC namespace. The JVM uses SysV shm only with `-XX:+UseSHM`. Pod `/dev/shm` is an external mount and its contents are lost. |
| **PID namespace** | Each Kubernetes container has its own PID namespace, so there are no PID collisions. Avoid `shareProcessNamespace: true`. |
| **Kernel / vDSO / THP** | Run the same kernel on all nodes. CRIU handles vDSO differences. On kernel ≥ 6.16 use CRIU ≥ 4.2.x. THP may need to re-collapse after restore *(unverified)*. |
| **Heap sizing / GC** | Dump size ≈ resident anonymous pages; reserved-but-untouched heap is not dumped. Size `-Xmx` tightly, never use `-XX:+AlwaysPreTouch`, allow uncommit (`G1PeriodicGCInterval`), GC right before the final dump, set `MALLOC_ARENA_MAX=2`, and keep the young generation modest. |
| **k3s PATH gotcha** | k3s puts its own bin dir (busybox `tar`, etc.) first on containerd's PATH. zeropod found that this breaks CRIU and symlinks GNU tar ahead of it. Make sure GNU `tar` resolves first, or test it. |

Additional Kubernetes/CRI-specific points (verified in source):

- **Established TCP:** containerd's CRI checkpoint passes no TCP option, so a server with connected players fails to dump unless `/etc/criu/runc.conf` has `tcp-established` or `tcp-close`. Restoring `tcp-established` sockets needs the **same local IP** in the destination netns. With a new pod IP, use `tcp-close`.
- **File locks:** Minecraft holds a `FileChannel.lock()` on `world/session.lock`, so `file-locks` is needed in runc.conf or on the runc CLI.
- **Regular files must match:** on restore CRIU validates every open regular file's **size** (`File %s has bad size`) and mode (`--skip-file-rwx-check` bypasses only the mode check). World region files (`.mca`), logs and the server jar must be byte-identical in size at the destination path. So copy or sync the world volume **after** the final freeze, or keep it on shared storage that both nodes mount at the same path.
- **Volumes:** runc maps bind mounts (all k8s volumes) as external mounts, so the restore pod must mount the **same container paths**. Contents are not checkpointed. `emptyDir`, including any `/tmp` mounted as emptyDir, is lost. Keep the JVM's `hsperfdata` out of it (`-XX:-UsePerfData`), or keep `/tmp` in the rootfs, which `rootfs-diff.tar` captures.
- **Pod IPC namespace:** containers join the sandbox's IPC namespace. SysV shm and POSIX mqueues are pod-level shared resources that KEP-5823 explicitly does not handle. The JVM does not use SysV shm by default.

---

## 6. KubeVirt live migration, and what transfers

### How it works

- **Components:** `virt-controller` (orchestration), `virt-handler` (privileged DaemonSet per node), `virt-launcher` pod (per VM; runs virtqemud + QEMU).
- **Target pre-created:** a `VirtualMachineInstanceMigration` makes virt-controller create a **new virt-launcher pod on the target**, with the same spec, the same RWX PVCs and network attachments. Once that pod is ready, the target virt-handler prepares an incoming domain.
- **Migration proxy:**
  - Source libvirt connects to a Unix socket exposed by the source virt-handler.
  - virt-handlers carry the stream over (by default) TLS-encrypted TCP to the target virt-handler, which pipes it into the target pod's virtqemud socket.
  - Ports 49152/49153 inside virt-launcher are reserved.
  - A dedicated migration network can be attached via a NetworkAttachmentDefinition.
  - 2026 CVEs: CVE-2026-13325 (`disableTLS` exposes an unauthenticated listener) and a symlink-following `/proc/<pid>/root` dial.
- **Memory:**
  - libvirt **pre-copy** by default.
  - **Auto-converge** throttles vCPUs when the dirty rate exceeds bandwidth.
  - **Post-copy** (`allowPostCopy`) switches when `completionTimeoutPerGiB` (default 800 s/GiB) is exceeded.
  - `bandwidthPerMigration` defaults to 64 MiB/s. `progressTimeout` defaults to 150 s.
  - Migration policies tune this per workload.
  - Storage must be RWX (or use storage migration).
- **Cutover:** the source pauses, the remaining state transfers, the target resumes and sends RARP/GARP, and the source pod is torn down. The VMI's `status.migrationState` records it.

Source: [KubeVirt live migration guide](https://kubevirt.io/user-guide/compute/live_migration/).

### Network continuity

- **masquerade binding:**
  - The guest keeps an internal IP (10.0.2.2) behind NAT in the pod; the pod IP **changes** on migration.
  - The guest does not notice: it keeps its IP until DHCP renew.
  - **Connections to the old pod IP break.** Clients that reach the VM via a Service survive at the Service level, but established flows reset.
- **bridge binding on the pod network:** live migration is **not allowed** by default. The guest would keep the old pod IP, which the CNI reassigns.
- **passt binding:** migratable. The target pod gets a new IP unless the CNI preserves it. "When using a CNI that supports retaining the virt-launcher pod IP through migration, passt ensures TCP connections continue."
- **CNIs that implement IP handover:**
  - **Kube-OVN** ([CNCF blog 2025-03](https://www.cncf.io/blog/2025/03/20/kubevirt-live-migration-mastery-network-transparency-with-kube-ovn/)): the target pod reuses the source's logical port (multi-chassis binding). Traffic is duplicated to both while the target port is inactive, and activation happens on libvirt's RARP. Reported interruption is <0.5 s with TCP surviving via retransmit.
  - **OVN-Kubernetes**: persistent IPs and primary UDN layer2.
  - **Calico Open Source 3.33**: bridge binding. IPAM keys the allocation on the *VM* identity, so source and target pods share the IP. Policy is programmed on the destination before activation (`policy_setup_timeout_seconds`, default 10 s), and route priority shifts to the new host for a 30 s convergence window. BGP only; no VXLAN/IPIP.
- **Cilium:**
  - No support. Maintainers state that a static IP is possible with Multi-Pool IPAM (a /32 `CiliumPodIPPool`), but "two pods with the same static IP in running state" breaks Cilium's assumption that an IP uniquely identifies an endpoint. Preserving connections would also require migrating BPF conntrack state ([cilium#38576](https://github.com/cilium/cilium/issues/38576), closed not-planned; +1s through 2025-12).
  - Red Hat lists Calico and Cilium as not supporting live migration (CNV-80885).

### Lessons for containers

1. **Pre-create and pre-validate the target pod** (scheduling, image pull, volumes, network ready) *before* freezing the source. KubeVirt does this, and the "restore pod with checkpoint image" flow can't do it because the image is only built after the dump. A runc-wrapper or "placeholder pod" design can.
2. **Use a node-to-node data path owned by a privileged per-node agent** (virt-handler ≈ your migration DaemonSet), not the pod network. Use a dedicated interface if possible; encrypt it.
3. **Pre-copy with convergence control, then optionally post-copy.** CRIU offers the same primitives (pre-dump rounds ≈ pre-copy, lazy-pages ≈ post-copy), but there is no auto-converge. You could emulate it by cgroup CPU throttling (`cpu.max`) during the last rounds.
4. **IP continuity is a CNI feature, not a runtime feature.** Without a CNI that can hand an IP from one endpoint to another, use masquerade-like indirection: clients talk to something stable (Service/LB/proxy) and accept that established flows reset. With Cilium today, TCP survival across nodes is not available off the shelf.
5. **Storage must be shared (RWX)** or explicitly migrated.

---

## 7. Other projects doing container live migration on Kubernetes

| Project | Status (2026-10) | Mechanism | Cross-node restore | Networking / TCP |
|---|---|---|---|---|
| **Upstream K8s C/R WG** (Adrian Reber, Radostin Stoyanov, Viktória Spišaková), KEP-5823 | WG formed 2025, first meeting Dec 2025, [FOSDEM 2026 talk](https://fosdem.org/2026/schedule/event/k8s-checkpoint-restore-wg). KEP alpha targets v1.38 | kubelet `PodCheckpoint` + `spec.restoreFrom`, CRI `CheckpointPod`/`RestorePod` | No (alpha non-goal) | TCP-close, new IP |
| **Red Hat Podman / OpenShift** | Podman `checkpoint --export/--create-image`, `restore --import` (cross-host, `--tcp-established`). OpenShift forensic checkpointing on by default since 4.17 | Podman → runc → CRIU | Podman yes, OpenShift no | Podman keeps TCP only if the IP is the same ([Red Hat blog](https://www.redhat.com/en/blog/container-migration-around-world)) |
| **[checkpoint-restore-operator](https://github.com/checkpoint-restore/checkpoint-restore-operator)** (CRIU org) | v0.1.0 2026-07-09, active | Checkpoint GC (`maxCheckpointsPerContainer`), `CheckpointSchedule`, and a **`PodRestore` CRD**. A per-node **CRI proxy** (kubelet `--container-runtime-endpoint` → `/run/cr-restore-proxy/cri-proxy.sock`) rewrites `image:` to the local checkpoint tar path, so the runtime takes its restore-via-create path | The tar must already be on the target node | None. Breaks with containerd 2.4; needs `enable_experimental_restore_via_create` on 2.2.7/2.3.4 |
| **[zeropod](https://github.com/laravel/zeropod)** (moved from ctrox to laravel) | v0.13.0 2026-08-23, active; k3s/RKE2 supported | containerd **shim** (RuntimeClass) driving runc/CRIU directly. Scale-to-zero, plus *experimental* `zeropod.ctrox.dev/live-migrate`: on pod deletion it runs `runc checkpoint --lazy-pages --page-server`, a `Migration` CR is claimed by the new pod's node, memory streams lazily via userfaultfd through a TLS page-server proxy between node daemons, and the overlay upper dir is copied. Optional pre-dump | Yes (Deployment-replacement style) | New IP. Ships `/etc/criu/default.conf` with `tcp-close`, `skip-in-flight`, `network-lock skip`, so connections drop. Bundles CRIU 4.2.1. **Closest open-source analogue to Path B** |
| **[Cedana](https://github.com/cedana/cedana)** | AGPL daemon, active; SaaS control plane | `cedana-helm` installs shim `cedana-shim-runc-v2` as RuntimeClass `cedana` (handles k3s templates). Checkpoints to S3/GCS; a rescheduled pod resumes from the latest checkpoint. Checkpoint-and-resume, not pre-copy live migration | Yes (via object storage) | Dumps with `TcpEstablished`, `TcpSkipInFlight`, `LinkRemap`. No IP preservation documented, so TCP to clients is lost on a node change |
| **[CAST AI Container Live Migration](https://docs.cast.ai/docs/clm-overview)** | Proprietary; launch blog dated 2026-08-11; EKS first, docs also mention GKE/AKS | CRIU + orchestrator, iterative pre-copy (x86), containerd ≥ 2, K8s ≥ 1.30, same instance family | Yes | **TCP preserved:** forked AWS VPC CNI keeps the pod IP, or tc/eBPF rewriting to a new IP (kernel ≥ 6.6), or Calico VXLAN with IP annotation. Rewrites socket addresses inside images. Cilium not listed. io_uring/MPTCP/eBPF/privileged workloads flagged |
| **[DevZero live migration](https://www.devzero.io/docs/operators/write-operator/workload-optimization/live-migration)** | Proprietary operator | Node agent freezes the pod with CRIU, pauses the controller (Deployment paused / STS OnDelete), restores on the destination | Yes | TCP_REPAIR plus netfilter drop during the freeze, then conntrack + SNAT/DNAT rewriting so peers reach the new IP. Claims 10–500 ms network freeze (no independent data) |
| **[SSU-DCN podmigration-operator](https://github.com/SSU-DCN/podmigration-operator)** | Dormant (last push 2022-11-08) | Forked kubelet + containerd-cri (K8s 1.19), CRIU 3.14, checkpoint to NFS, `kubectl migrate` | Yes | None |
| **[GKE Pod snapshots](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/pod-snapshots)** | GKE-only, GA 2026-05 (1.35.3-gke.1234000) | gVisor checkpoint (not CRIU), `PodSnapshotPolicy` CRDs, used by Agent Sandbox | Yes (new pod) | New IP. "Active network connections … are closed on restore". Requires the same machine series and CPUID flags |
| **[criu-image-streamer](https://github.com/checkpoint-restore/criu-image-streamer)** | Active | `criu --stream` through a UNIX socket, sharding, no disk | Building block | n/a |
| **[pod-snapshot-controller](https://github.com/checkpoint-restore/pod-snapshot-controller)** | Linked from KEP-5823 but 404 (not public yet) | KEP-5823 reference controller | No (alpha) | n/a |
| **SHADOW / MS2M** ([arXiv 2603.25484](https://arxiv.org/abs/2603.25484), 2026-03) | Research operator | ShadowPod restored from a CRIU checkpoint while the source serves; message-queue replay; StatefulSet identity swap | Yes | App/message-level, so not applicable to raw TCP game clients |
| **Guitart 2024** ([Zenodo](https://zenodo.org/records/11637685)) | Research, non-k8s | Diskless, iterative, lazy runc/CRIU migrations with external netns | Yes | `--tcp-established` with the IP kept |
| "MIGrate", "kube-migration" | Not found as k8s projects | | | |

**Who preserves TCP:**

- Only the proprietary CAST AI and DevZero, using CNI-level IP preservation or NAT/conntrack rewriting, plus research prototypes outside Kubernetes.
- Every open-source tool that runs on k3s drops connections: zeropod, Cedana, checkpoint-restore-operator, and KEP-5823 alpha.
- **No one supports TCP-preserving migration on Cilium.**
- Your own prior art ([nadecancode/live-container-migration](prior-art-nadecancode.md)) preserved TCP with conntrack DNAT rewriting into a WireGuard tunnel back to the destination. That is the same family as DevZero's approach, with traffic tromboning through the source host.

---

## 8. Recommended path for this project (k3s + containerd 2.3.4 + Cilium)

**Path A: works today with stock components; the restore is "cold" for TCP.**

1. Install CRIU ≥ 4.2 (overlay) on every node and put it on `systemd.services.k3s.path`, along with `iptables`/`nftables`/`iproute2`.
2. Add the containerd drop-in with `enable_experimental_restore_via_create = true`. Pin k3s to a containerd 2.3.x release (LTS to 2028-04-30) and **do not upgrade to a k3s with containerd 2.4**.
3. `/etc/criu/runc.conf` on all nodes: `tcp-close`, `file-locks`, plus `ghost-limit`/`link-remap` as needed.
4. Freeze the world in the app (`save-off`/`save-all flush`), or freeze the pod cgroup. Then call the kubelet checkpoint (leave running) and immediately kill the source pod. Alternatively use `runc checkpoint` (stop) directly.
5. Build the OCI image with both annotations, then push it to an in-cluster registry or import it on the destination with `ctr -n k8s.io images import`.
6. Sync the world volume, or use RWX storage. Create the destination pod with the same container name, same mounts, `image: <checkpoint image>`, and `nodeName: <dst>`.
7. Expect a new pod IP. Players reconnect via the Service.

**Path B: low downtime; DIY below the CRI.** Run runc pre-dump rounds with `--page-server` to the destination. Pre-create the destination pod with a RuntimeClass whose runc wrapper restores from the streamed image dir (optionally `--lazy-pages`). Do the final `runc checkpoint` (stop) on the source. This is not exposed by any upstream component in 2026.

**TCP survival:**

- Requires the same IP on the destination, plus CRIU `tcp-established` with network locking during the cutover. Cilium cannot provide IP handover. Options:
  - switch CNI (Calico BGP / Kube-OVN) for the migrating pods;
  - add a secondary interface (Multus + macvlan/ipvlan with a static IP that you move yourself, for example via GARP);
  - terminate player TCP on a non-migrating proxy and make the proxy→backend hop reconnect-tolerant.
- Minecraft has a protocol-level transfer packet (1.20.5+), and Velocity/BungeeCord can move players between backends. For players, that is often a better user-facing trick than TCP repair.
