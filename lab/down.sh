#!/usr/bin/env bash
# Stop lab VMs (and the switch when stopping everything). Disk images in state/ are kept.
NODES=("$@"); [ $# -eq 0 ] && NODES=(node1 node2 node3)
for n in "${NODES[@]}"; do systemctl --user stop "lcm-$n" 2>/dev/null && echo "stopped $n"; done
[ $# -eq 0 ] && systemctl --user stop lcm-vde 2>/dev/null
true
