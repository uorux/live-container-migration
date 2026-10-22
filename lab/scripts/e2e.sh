#!/usr/bin/env bash
# End-to-end: bots play through the gateway while the server pod is live-migrated (run on node1).
# usage: e2e.sh <pod> <target-node> [bots] [duration-s]
set -uo pipefail
POD=$1 TO=$2 N=${3:-5} DUR=${4:-90}
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
GW=$(kubectl get svc lcm-gateway -o jsonpath='{.spec.clusterIP}')
OUT=/root/e2e-$(date +%H%M%S); mkdir -p "$OUT"
[ -d /root/mcbot/node_modules ] || { cp -r /lcm/tools/mcbot /root/mcbot && (cd /root/mcbot && nix shell nixpkgs#nodejs_22 -c npm ci --silent); }
(cd /root/mcbot && nix shell nixpkgs#nodejs_22 -c node mcbot.js --host "$GW" --port 25565 --count "$N" --prefix bot --duration "$DUR" --stagger-ms 300 > "$OUT/bots.jsonl" 2> "$OUT/bots.err") &
BOTS=$!
for i in $(seq 120); do grep -q all_connected "$OUT/bots.jsonl" 2>/dev/null && break; sleep 1; done
grep -q all_connected "$OUT/bots.jsonl" || { echo "bots never connected"; tail "$OUT/bots.err"; kill $BOTS; exit 1; }
sleep 10
lcm migrate "$POD" --to "$TO" --token-file /etc/lcm/token --json > "$OUT/migrate.json" 2> >(tee "$OUT/migrate.log" >&2)
echo "migrate exit $?"
wait $BOTS; echo "bots exit $?"
grep '"event":"gap"\|"event":"disconnect"\|"event":"kicked"' "$OUT/bots.jsonl" | head -20
grep '"event":"summary"' "$OUT/bots.jsonl"
echo "output in $OUT"
