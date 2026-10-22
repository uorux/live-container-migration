#!/usr/bin/env bash
# Bots stay connected while the server (label app=<app>) hops between node2 and node3 HOPS times.
# usage: e2e-pingpong.sh [app] [hops] [bots] [pause-s]
set -uo pipefail
APP=${1:-mc} HOPS=${2:-4} N=${3:-5} PAUSE=${4:-15}
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
GW=$(kubectl get svc lcm-gateway -o jsonpath='{.spec.clusterIP}')
OUT=/root/pingpong-$(date +%H%M%S); mkdir -p "$OUT"
DUR=$(( HOPS * (PAUSE + 40) + 30 ))
(cd /root/mcbot && nix shell nixpkgs#nodejs_22 -c node mcbot.js --host "$GW" --port 25565 --count "$N" --prefix bot --duration "$DUR" --stagger-ms 300 > "$OUT/bots.jsonl" 2> "$OUT/bots.err") &
BOTS=$!
for i in $(seq 120); do grep -q all_connected "$OUT/bots.jsonl" 2>/dev/null && break; sleep 1; done
for h in $(seq "$HOPS"); do
  sleep "$PAUSE"
  read -r POD NODE < <(kubectl get pod -l app="$APP" --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name} {.items[0].spec.nodeName}')
  TO=node2; [ "$NODE" = node2 ] && TO=node3
  lcm migrate "$POD" --to "$TO" --token-file /etc/lcm/token 2>&1 | grep -E "^done|failed" | tee -a "$OUT/migrations.log"
done
kill -INT $BOTS 2>/dev/null; wait $BOTS
grep '"event":"gap"\|"event":"disconnect"' "$OUT/bots.jsonl" | awk -F'"gap_ms":' '{print substr($0,1,60) ($2 ? " gap_ms=" $2 : "")}' | head -30
grep '"event":"summary"' "$OUT/bots.jsonl"
