#!/usr/bin/env bash
# Manual (pre-agent) migration, run as root on the SOURCE node.
# usage: migrate-src.sh <namespace> <pod> <container> <dest-node> <dest-pod> [predumps]
set -euo pipefail
NS=$1 POD=$2 CTR=$3 DEST=$4 DPOD=$5 NPRE=${6:-1}
# The runc containerd's shim uses (k3s's service PATH), not whatever is on ours.
RUNC_BIN=${RUNC_BIN:-$(PATH=$(tr '\0' '\n' < /proc/$(pgrep -o -f containerd-shim-runc-v2)/environ | sed -n 's/^PATH=//p') command -v runc)}
RUNC="$RUNC_BIN --root /run/containerd/runc/k8s.io"
KEY="${NS}_${DPOD}_${CTR}"
D=/var/lib/lcm/ckpt/$KEY; R=/var/lib/lcm/restore/$KEY
ms() { date +%s%3N; }
rm -rf "$D"; mkdir -p "$D"
PODID=$(k3s crictl pods --namespace "$NS" --name "^$POD\$" --state ready -q)
CID=$(k3s crictl ps --pod "$PODID" --name "^$CTR\$" -q)
UPPER=$(grep " /run/k3s/containerd/io.containerd.runtime.v2.task/k8s.io/$CID/rootfs " /proc/self/mountinfo | grep -o 'upperdir=[^,]*' | cut -d= -f2)
echo "container $CID upper $UPPER"
ssh "$DEST" "rm -rf $R; mkdir -p $R"
ship() { tar -C "$D" -cf - "$@" | ssh "$DEST" "tar -C $R -xf -"; }

parent=""
for i in $(seq 1 "$NPRE"); do
  t=$(ms)
  $RUNC checkpoint --pre-dump --image-path "$D/pre-$i" --work-path "$D/work-pre-$i" ${parent:+--parent-path ../$parent} "$CID"
  t2=$(ms); ship "pre-$i"; t3=$(ms)
  echo "pre-dump $i: dump $((t2-t))ms ship $((t3-t2))ms size $(du -sh "$D/pre-$i" | cut -f1)"
  parent=pre-$i
done

mkdir -p /var/lib/lcm/fence && touch "/var/lib/lcm/fence/${NS}_${POD}"
T0=$(ms)
if ! $RUNC checkpoint --image-path "$D/images" --work-path "$D/work" --tcp-established --file-locks ${parent:+--parent-path ../$parent} "$CID"; then
  # CRIU unfreezes the tree on a failed dump; lift the fence so the source keeps running normally.
  rm -f "/var/lib/lcm/fence/${NS}_${POD}"; echo "final dump failed; source left running"; exit 1
fi
T1=$(ms)
/lcm/bin/lcm-rootfsdiff capture "$UPPER" > "$D/rootfs-diff.tar"
T2=$(ms)
ship images rootfs-diff.tar
ssh "$DEST" "touch $R/READY"
T3=$(ms)
echo "final dump $((T1-T0))ms ($(du -sh "$D/images" | cut -f1)), rootfs diff $((T2-T1))ms ($(du -sh "$D/rootfs-diff.tar" | cut -f1)), ship $((T3-T2))ms; frozen-so-far $((T3-T0))ms"
echo "$T0" > "$D/freeze-start-ms"
