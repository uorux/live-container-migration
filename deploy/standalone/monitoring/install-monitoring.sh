#!/bin/sh
# Ingress box monitoring (root): node_exporter + blackbox_exporter on the tailnet address,
# Vector shipping the journal to VictoriaLogs, HAProxy's Prometheus endpoint (tailnet only).
set -eu
NE=1.12.1 BB=0.29.1 VEC=0.60.0
TS=$(tailscale ip -4 | head -1)
[ -n "$TS" ] || { echo "no tailnet address" >&2; exit 1; }
work=$(mktemp -d); trap 'rm -rf "$work"' EXIT; cd "$work"
fetch() { curl -fsSL -m 120 -o "$2" "$1"; }
fetch https://github.com/prometheus/node_exporter/releases/download/v$NE/node_exporter-$NE.linux-amd64.tar.gz ne.tgz
fetch https://github.com/prometheus/node_exporter/releases/download/v$NE/sha256sums.txt ne.sums
fetch https://github.com/prometheus/blackbox_exporter/releases/download/v$BB/blackbox_exporter-$BB.linux-amd64.tar.gz bb.tgz
fetch https://github.com/prometheus/blackbox_exporter/releases/download/v$BB/sha256sums.txt bb.sums
fetch https://github.com/vectordotdev/vector/releases/download/v$VEC/vector-$VEC-x86_64-unknown-linux-musl.tar.gz vec.tgz
fetch https://github.com/vectordotdev/vector/releases/download/v$VEC/vector-$VEC-SHA256SUMS vec.sums
check() { want=$(grep " $2\$" "$3" | awk '{print $1}'); got=$(sha256sum "$1" | awk '{print $1}'); [ -n "$want" ] && [ "$want" = "$got" ] || { echo "checksum mismatch for $2" >&2; exit 1; }; }
check ne.tgz node_exporter-$NE.linux-amd64.tar.gz ne.sums
check bb.tgz blackbox_exporter-$BB.linux-amd64.tar.gz bb.sums
check vec.tgz vector-$VEC-x86_64-unknown-linux-musl.tar.gz vec.sums
tar xzf ne.tgz; tar xzf bb.tgz; tar xzf vec.tgz
install -d /opt/ingress-monitoring /etc/ingress-monitoring
install -m 0755 node_exporter-$NE.linux-amd64/node_exporter /opt/ingress-monitoring/node_exporter
install -m 0755 blackbox_exporter-$BB.linux-amd64/blackbox_exporter /opt/ingress-monitoring/blackbox_exporter
install -m 0755 "$(find . -path '*/bin/vector' -type f | head -1)" /opt/ingress-monitoring/vector
cd /tmp/lcm-deploy/mon
install -m 0644 blackbox.yml vector.yaml /etc/ingress-monitoring/
sed "s/@TS@/$TS/g" node-exporter.service > /etc/systemd/system/ingress-node-exporter.service
sed "s/@TS@/$TS/g" blackbox-exporter.service > /etc/systemd/system/ingress-blackbox-exporter.service
install -m 0644 vector.service /etc/systemd/system/ingress-vector.service
command -v restorecon >/dev/null && restorecon -R /opt/ingress-monitoring /etc/ingress-monitoring /etc/systemd/system/ingress-*.service
/opt/ingress-monitoring/vector validate --no-environment /etc/ingress-monitoring/vector.yaml >/dev/null
systemctl daemon-reload
systemctl enable --now ingress-node-exporter ingress-blackbox-exporter ingress-vector
sleep 3
for u in ingress-node-exporter ingress-blackbox-exporter ingress-vector; do echo "$u: $(systemctl is-active $u)"; done
printf 'node_exporter: '; curl -s -m 5 http://$TS:9100/metrics | grep -c '^node_' || true
printf 'probe router path: '; curl -s -m 15 "http://$TS:9115/probe?module=minecraft_status&target=71.34.78.8:25565" | grep -E '^probe_success|^probe_duration_seconds' | tr '\n' ' '; echo

# HAProxy's Prometheus endpoint; refuses everything but the tailnet and loopback.
cfg=/etc/haproxy/haproxy.cfg
if ! grep -q '^frontend prometheus' $cfg; then
  bak=$cfg.pre-prom-$(date +%Y%m%d%H%M%S); cp -p $cfg $bak
  cp -p $cfg $cfg.new
  cat >> $cfg.new <<'EOT'

# Prometheus metrics for VictoriaMetrics (live-container-migration deploy/standalone); tailnet only.
frontend prometheus
    mode http
    bind 0.0.0.0:8405
    tcp-request connection reject unless { src 100.64.0.0/10 127.0.0.1 }
    http-request use-service prometheus-exporter if { path /metrics }
    http-request return status 404
    no log
EOT
  if haproxy -c -q -f $cfg.new; then
    mv $cfg.new $cfg; command -v restorecon >/dev/null && restorecon $cfg
    systemctl reload haproxy; sleep 2
    printf 'haproxy metrics: '; curl -s -m 5 http://$TS:8405/metrics | grep -c '^haproxy_' || true
  else rm -f $cfg.new; echo "haproxy -c failed; prometheus frontend not added"; fi
fi
