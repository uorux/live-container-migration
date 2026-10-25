#!/bin/sh
# vcpu-pin.sh plan|apply|keep|revert|measure  (on a node-VM hypervisor, as root)
#
# Pinning + isolation for the node VM's vCPUs:
#  - host work (every other service, user sessions, kernel unbound work, IRQs, crosvm's own
#    non-vCPU threads and device processes, vhost workers) on the first HOST_CORES physical cores,
#    both SMT threads;
#  - each vCPU N on its own thread of the remaining cores, vCPUs 2k and 2k+1 on sibling threads.
# apply arms an automatic revert after MINUTES (default 15); `keep` disarms it.
# All changes are runtime only (systemctl --runtime, taskset, sysfs): a reboot also undoes them.
set -eu
PATH=/run/current-system/sw/bin:/usr/bin:/bin:$PATH
SYS=${SYS:-/sys}               # overridable for dry runs
DRY=${DRY:-}                   # DRY=1: print what would be done
HOST_CORES=${HOST_CORES:-2}
MINUTES=${MINUTES:-15}
VM_NAME=${VM_NAME:-nv-node}
STATE=/run/vcpu-pin.state
SELF=$(readlink -f "$0")

run() { if [ -n "$DRY" ]; then echo "  would: $*"; else "$@"; fi; }

# Expand "0,8" / "0-1" / "0-3,8-11" into one CPU per line.
expand() { echo "$1" | tr ',' '\n' | awk -F- 'NF==2{for(i=$1;i<=$2;i++)print i; next}{print $1}'; }

# Physical cores, ordered by their lowest CPU; each line: the core's threads, space-separated.
cores() {
	for f in "$SYS"/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list; do
		expand "$(cat "$f")" | sort -n | tr '\n' ' '; echo
	done | sort -u | sort -n -k1,1
}

plan() {
	ALL=$(cores | tr ' ' '\n' | grep -v '^$' | sort -n | paste -sd, -)
	H=$(cores | head -n "$HOST_CORES" | tr ' ' '\n' | grep -v '^$' | sort -n | paste -sd, -)
	V=$(cores | tail -n +"$((HOST_CORES + 1))" | tr ' ' '\n' | grep -v '^$' | paste -sd' ' -)
	m=0; for c in $(echo "$H" | tr ',' ' '); do m=$((m | (1 << c))); done; HMASK=$(printf '%x' "$m")
}

vm_pids() {
	main=$(pgrep -f "crosvm.*--name $VM_NAME" | head -n1)
	[ -n "$main" ] || { echo "no crosvm for $VM_NAME" >&2; exit 1; }
	cat "$(echo /sys/fs/cgroup"$(awk -F:: '/^0::/{print $2}' /proc/"$main"/cgroup)")/cgroup.procs"
}

services() {
	systemctl list-units --type=service,scope --state=running --plain --no-legend | awk '{print $1}' |
		grep -v -E '^node-vm-' || true
}

case "${1:-plan}" in
plan)
	plan
	echo "cores (threads per physical core):"; cores | sed 's/^/  /'
	echo "host CPUs: $H (mask $HMASK)"
	echo "vCPU CPUs, vCPU 0..: $V"
	echo "all CPUs: $ALL"
	;;
apply)
	plan
	echo "host CPUs $H; vCPUs on $V"
	: > "$STATE"
	for u in $(services) user.slice; do run systemctl set-property --runtime "$u" AllowedCPUs="$H"; done
	run sh -c "echo $HMASK > /sys/devices/virtual/workqueue/cpumask"
	for i in /proc/irq/[0-9]*; do run sh -c "echo $H > $i/smp_affinity_list" 2>/dev/null || true; done
	# Unbound kernel threads (allowed everywhere) to the host CPUs; per-CPU ones stay.
	for p in $(ps -eo pid=,ppid= | awk '$2==2{print $1}'); do
		[ "$(awk '/Cpus_allowed_list/{print $2}' /proc/"$p"/status 2>/dev/null)" = "$ALL" ] || continue
		run taskset -pc "$H" "$p" >/dev/null 2>&1 && echo "kthread $p" >> "$STATE" || true
	done
	# The VM: vCPU N -> its own CPU, everything else in its processes -> host CPUs.
	for pid in $(vm_pids); do
		for t in $(ls /proc/"$pid"/task); do
			comm=$(cat /proc/"$pid"/task/"$t"/comm 2>/dev/null || true)
			case "$comm" in
			*vcpu[0-9]*)
				n=$(echo "$comm" | sed -E 's/.*vcpu([0-9]+).*/\1/')
				cpu=$(echo "$V" | cut -d' ' -f$((n + 1)))
				[ -n "$cpu" ] || { echo "no CPU left for $comm" >&2; continue; }
				run taskset -pc "$cpu" "$t" >/dev/null; echo "  $comm -> CPU $cpu";;
			*) run taskset -pc "$H" "$t" >/dev/null 2>&1 || true;;
			esac
		done
	done
	for p in $(pgrep '^vhost-' || true); do run taskset -pc "$H" "$p" >/dev/null 2>&1 || true; done
	if [ -z "$DRY" ]; then
		systemd-run --quiet --unit=vcpu-pin-revert --on-active="${MINUTES}m" /bin/sh "$SELF" revert
		echo "applied; auto-revert in $MINUTES min (run '$SELF keep' to keep it)"
	fi
	;;
keep)
	systemctl stop vcpu-pin-revert.timer 2>/dev/null && echo "auto-revert disarmed" || echo "no auto-revert armed"
	;;
revert)
	plan
	for u in $(services) user.slice; do systemctl set-property --runtime "$u" AllowedCPUs= 2>/dev/null || true; done
	printf '%x\n' $(( (1 << $(echo "$ALL" | tr ',' '\n' | wc -l)) - 1 )) > /sys/devices/virtual/workqueue/cpumask || true
	for i in /proc/irq/[0-9]*; do echo "$ALL" > "$i"/smp_affinity_list 2>/dev/null || true; done
	[ -f "$STATE" ] && awk '/^kthread/{print $2}' "$STATE" | while read -r p; do taskset -pc "$ALL" "$p" >/dev/null 2>&1 || true; done
	for pid in $(vm_pids); do for t in $(ls /proc/"$pid"/task); do taskset -pc "$ALL" "$t" >/dev/null 2>&1 || true; done; done
	for p in $(pgrep '^vhost-' || true); do taskset -pc "$ALL" "$p" >/dev/null 2>&1 || true; done
	systemctl stop vcpu-pin-revert.timer 2>/dev/null || true
	rm -f "$STATE"; echo "reverted: everything may run on $ALL again"
	;;
measure)
	main=$(pgrep -f "crosvm.*--name $VM_NAME" | head -n1)
	s() { for t in /proc/"$main"/task/*; do grep -q vcpu "$t"/comm 2>/dev/null && cat "$t"/schedstat; done | awk '{d+=$2; n+=$3} END {print d, n}'; }
	a=$(s); sleep 10; b=$(s)
	echo "$a $b" | awk '{printf "run delay %.0f us per wake-up, %.0f wake-ups/s\n", ($3-$1)/($4-$2)/1e3, ($4-$2)/10}'
	;;
*) echo "usage: $0 plan|apply|keep|revert|measure" >&2; exit 2;;
esac
