#!/usr/bin/env bash
# Start the lab on the host: a VDE switch plus nodeN VMs as transient systemd --user units.
# Usage: up.sh [node...]   (default: node1 node2 node3). Rebuilds the VM run scripts first.
set -euo pipefail
LAB=$(cd "$(dirname "$0")" && pwd)
STATE=$LAB/state; mkdir -p "$STATE"
NODES=("$@"); [ $# -eq 0 ] && NODES=(node1 node2 node3)
# paths.env (written by build.sh) maps tools/nodeN to prebuilt store paths; fall back to building here.
[ -f "$STATE/paths.env" ] && . "$STATE/paths.env"
path() { local v=${1}_PATH; [ -n "${!v:-}" ] && echo "${!v}" || nix build "$LAB#$1" --no-link --print-out-paths; }
TOOLS=$(path tools)
if ! systemctl --user is-active -q lcm-vde; then
  systemd-run --user --unit=lcm-vde --collect -p Type=forking -p PIDFile="$STATE/vde1.pid" \
    "$TOOLS/bin/vde_switch" -d -s "$STATE/vde1.ctl" -m 0700 -p "$STATE/vde1.pid"
  sleep 1
fi
for n in "${NODES[@]}"; do
  VM=$(path "$n")
  systemctl --user is-active -q "lcm-$n" && { echo "$n already running"; continue; }
  mkdir -p "$STATE/tmp-$n"
  systemd-run --user --unit="lcm-$n" --collect -p LimitNOFILE=1048576 \
    -E QEMU_VDE_SOCKET_1="$STATE/vde1.ctl" -E NIX_DISK_IMAGE="$STATE/$n.qcow2" -E TMPDIR="$STATE/tmp-$n" \
    -E PATH="$TOOLS/bin:/run/current-system/sw/bin" \
    -p StandardOutput=file:"$STATE/$n.log" -p StandardError=file:"$STATE/$n.log" -p StandardInput=null \
    "$VM/bin/run-$n-vm"
  echo "started $n"
done
