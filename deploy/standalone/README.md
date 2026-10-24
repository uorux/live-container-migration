# lcm-gateway on a standalone ingress box

For the VPS ingresses (not NixOS, not in k3s). Each box keeps its HAProxy, which hands every
player to a local lcm-gateway with a PROXY v2 header. The gateway holds the players' connections
and tunnels them to the servers' relays in k3s, so servers can live-migrate under the players.
Requirements: Linux amd64 (arm64 needs a cross-built binary; ask), systemd ≥ 250, Tailscale,
`curl`.

```
player ──▶ HAProxy :25565 ──send-proxy-v2──▶ 127.0.0.1:25566 lcm-gateway ◀──tunnel :7000── relays (k3s pods, over the tailnet)
```

## Install (once per box, as root)

1. **Binary.** Copy `lcm-gateway-update` to `/usr/local/sbin/` and run it. It downloads the
   gateway build the k3s agents serve (over the tailnet, from any node's `:7790/v1/bin`) into
   `/opt/lcm-gateway/releases/<version>/` and points `/opt/lcm-gateway/current` at it. On the
   first run there's no service to restart yet, so you'll see a harmless "not healthy" message;
   carry on.
2. **Config.**
   ```sh
   install -d /etc/lcm-gateway
   install -m 0644 gateway.env.example /etc/lcm-gateway/gateway.env   # identical on every gateway
   install -m 0644 local.env.example   /etc/lcm-gateway/local.env     # this box
   ```
   `gateway.env` must list the same backends as the in-cluster VIP gateway
   (`deploy/homelab/minecraft-gw/gateway-vip.yaml`).
3. **Whitelists.** Only listed players can wake an asleep server (everyone else gets "not
   white-listed" before anything starts). Copy the lists from the cluster, and again after each
   whitelist change; the gateway picks up edits within seconds, no restart:
   ```sh
   for k in backend global bmc ops-global ops-bmc; do
     kubectl -n minecraft-gw get configmap mc-whitelist -o jsonpath="{.data.$k\.json}" \
       | ssh root@BOX "install -D -m 0644 /dev/stdin /etc/lcm-gateway/lists/$k.json"
   done
   ```
4. **Secrets**, run from your own machine with the 1Password CLI signed in, so values only ever
   travel through a pipe. They are stored encrypted on the box.
   ```sh
   # The tunnel token: the same value as the k8s Secret lcm-tunnel (1Password item minecraft-tunnel).
   op read 'op://k8s/minecraft-tunnel/token' \
     | ssh root@BOX 'install -d -m 0700 /etc/credstore.encrypted && systemd-creds encrypt --name=tunnel-token - /etc/credstore.encrypted/lcm-gateway.tunnel-token'
   # The wake token: derived on the k3s nodes from the cluster token (identical on all three).
   ssh root@arquitens cat /run/lcm/wake-token \
     | ssh root@BOX 'systemd-creds encrypt --name=wake-token - /etc/credstore.encrypted/lcm-gateway.wake-token'
   ```
   On systemd < 250, write the files with `install -m 0400 /dev/stdin /etc/lcm-gateway/creds/…`
   and use `LoadCredential=` instead of `LoadCredentialEncrypted=` in the unit.
5. **Service.**
   ```sh
   install -m 0644 lcm-gateway.service /etc/systemd/system/
   systemctl daemon-reload && systemctl enable --now lcm-gateway
   curl -s http://127.0.0.1:7001/status    # backends listed; "connected" once relays dial in
   ```
6. **Firewall:** 7000 (relays) and 7001 (admin, metrics) must only be reachable over the tailnet.
   The unit already refuses anything else (`IPAddressAllow`); keep the host firewall closed for
   them too. Tailnet ACL: allow the k3s nodes → this box on tcp 7000 and 7001 (pod traffic
   arrives as the node's tailnet IP), and this box → the nodes on tcp 7790 (wake).
7. **HAProxy, the cutover:** point the Minecraft backend at the gateway.
   ```haproxy
   backend mc
       # before: server recusant <recusant-tailnet-ip>:25601 send-proxy-v2
       server lcm 127.0.0.1:25566 send-proxy-v2
   ```
   `systemctl reload haproxy` keeps established connections on the old worker. Rollback is the
   same one-line edit back. No `check` on that server line: the gateway expects a PROXY header on
   every connection from 127.0.0.1.

## Monitoring

Scrape `http://<box-tailnet-ip>:7001/metrics` with a static VictoriaMetrics target
(job `lcm-gateway`, label `gateway=<box>`). The lcm dashboard and alerts pick it up:
`LcmGatewayDown` names the instance, and `LcmGatewayPlayersWaiting` / `LcmGatewayBackendDown`
work per backend.

## Upgrades

```sh
lcm-gateway-update            # refuses if players are connected
lcm-gateway-update --force    # restart anyway (drops this box's players; they reconnect)
lcm-gateway-update --rollback
```

The script takes the version every reachable agent reports (and waits while they disagree, i.e.
during a node rollout), verifies the binary's `-version`, keeps the newest three releases, and
checks `/healthz` after the restart. A restart of a standalone gateway drops its players: unlike
the in-cluster VIP gateway, there's no standby to hand them to. Upgrade only when gateway code
changed, preferably when nobody is on. The tunnel protocol is versioned: gateways and relays of
different lcm builds interoperate as long as the protocol version matches (it has been 1 since
the start).

## Logs

`journalctl -u lcm-gateway`. The gateway logs player source addresses on connect: keep journald
retention bounded (`SystemMaxUse=`).

## Host monitoring and log shipping

`monitoring/install-monitoring.sh` (as root, from a copy of `monitoring/` at
`/tmp/lcm-deploy/mon`) installs, from pinned upstream releases checked against their checksums:

- node_exporter on `<tailnet-ip>:9100`;
- blackbox_exporter on `<tailnet-ip>:9115`, with module `minecraft_status` (a server-list ping
  that never wakes anything), which VictoriaMetrics uses to probe every entry point from outside;
- Vector, shipping this box's journal (lcm-gateway included) to VictoriaLogs through the
  insert-only `https://logs-ingest.k8s.rooty.dev/insert/jsonline` (homelab `apps/infra/logs`);
  memory-capped at 128M, and it starts from "now" (no backfill);
- HAProxy's Prometheus endpoint on `:8405`, refusing everything but the tailnet and loopback.

The homelab side is `deploy/homelab/monitoring/ingress-monitoring.yaml` (scrapes, probes,
alerts) and `deploy/homelab/logs/ingest.yaml`. The tailnet policy must let the k3s nodes reach
the boxes on tcp 9100, 9115 and 8405.
