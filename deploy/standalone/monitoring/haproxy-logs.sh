#!/bin/sh
# Ship HAProxy's logs through Vector (root). Vector first, then HAProxy's log target.
set -u
install -m 0644 /tmp/lcm-deploy/mon/vector.yaml /etc/ingress-monitoring/vector.yaml
command -v restorecon >/dev/null && restorecon /etc/ingress-monitoring/vector.yaml
/opt/ingress-monitoring/vector validate --no-environment /etc/ingress-monitoring/vector.yaml >/dev/null || { echo "vector config invalid"; exit 1; }
systemctl restart ingress-vector; sleep 3
echo "vector: $(systemctl is-active ingress-vector), udp: $(ss -lun | grep -c '127.0.0.1:5140')"
cfg=/etc/haproxy/haproxy.cfg
if ! grep -q '127.0.0.1:5140' $cfg; then
  bak=$cfg.pre-logship-$(date +%Y%m%d%H%M%S); cp -p $cfg $bak
  if grep -q '^[[:space:]]*log[[:space:]]\+/dev/log[[:space:]]\+local0[[:space:]]*$' $cfg; then
    # aeon: keep the local /var/log/haproxy.log (rsyslog) and also send to Vector.
    awk '{print} /^[[:space:]]*log[[:space:]]+\/dev\/log[[:space:]]+local0[[:space:]]*$/ && !d {print "\tlog 127.0.0.1:5140 local0   # to Vector -> VictoriaLogs"; d=1}' $bak > $cfg.new
  else
    # type-6a: 127.0.0.1:514 (UDP) had no listener; send to Vector instead.
    sed 's/^\([[:space:]]*\)log[[:space:]]\+127\.0\.0\.1[[:space:]]\+local2[[:space:]]*$/\1log 127.0.0.1:5140 local2   # to Vector -> VictoriaLogs/' $bak > $cfg.new
  fi
  grep -q '127.0.0.1:5140' $cfg.new || { rm -f $cfg.new; echo "no log line to change; unchanged"; exit 1; }
  if haproxy -c -q -f $cfg.new; then
    chmod --reference=$cfg $cfg.new; chown --reference=$cfg $cfg.new; mv $cfg.new $cfg
    command -v restorecon >/dev/null && restorecon $cfg
    diff $bak $cfg; systemctl reload haproxy; sleep 2
    echo "haproxy: $(systemctl is-active haproxy) (backup $bak)"
  else rm -f $cfg.new; echo "haproxy -c failed; unchanged"; exit 1; fi
fi
# Watch Vector while generating some traffic of our own.
(timeout 22 /opt/ingress-monitoring/vector tap --url http://127.0.0.1:8686/graphql --limit 3 --duration-ms 18000 --format json haproxy > /tmp/tap.out 2>&1 &)
sleep 4
curl -s -k -m 5 --resolve git.rooty.dev:443:127.0.0.1 -o /dev/null https://git.rooty.dev/
timeout 3 bash -c 'exec 3<>/dev/tcp/127.0.0.1/25565' 2>/dev/null
sleep 16; grep '^{' /tmp/tap.out | cut -c1-260
echo "journal: $(ls -d /var/log/journal 2>/dev/null || echo 'no /var/log/journal (volatile unless Storage=persistent)'); $(grep -hs '^Storage' /etc/systemd/journald.conf /etc/systemd/journald.conf.d/*.conf | tr '\n' ' ')"
