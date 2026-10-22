#!/usr/bin/env bash
# Re-activate lab nodes from the current lab flake, in place (no VM restart; kernel unchanged).
# usage: rebuild.sh [node...]
LAB=$(cd "$(dirname "$0")" && pwd)
NODES=("$@"); [ $# -eq 0 ] && NODES=(node1 node2 node3)
for n in "${NODES[@]}"; do
  ( "$LAB/ssh.sh" "$n" "cd /tmp && nixos-rebuild test --flake /lcm/lab#$n 2>&1 | grep -v 'Git tree' | tail -3" | sed "s/^/[$n] /" ) &
done
wait
