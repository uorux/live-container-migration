#!/usr/bin/env python3
"""Generate the Grafana dashboard for live container migration.

Writes deploy/homelab/monitoring/lcm-dashboard.json and a grafana_dashboard="1" ConfigMap
(lcm-dashboard.yaml). Prints every panel query with --queries (to validate against a TSDB)."""
import json, sys

DS = {"type": "prometheus", "uid": "prometheus"}
panels, y = [], 0

def row(title):
    global y
    panels.append({"type": "row", "title": title, "collapsed": False, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})
    y += 1

def panel(kind, title, targets, x, w, h=7, unit=None, desc="", **extra):
    p = {"type": kind, "title": title, "description": desc, "datasource": DS,
         "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "targets": [{"datasource": DS, "expr": e, "legendFormat": l, "refId": chr(65 + i)} for i, (e, l) in enumerate(targets)],
         "fieldConfig": {"defaults": {}, "overrides": []}, "options": {}}
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    p.update(extra)
    panels.append(p)

def nextrow(h=7):
    global y
    y += h

# The homelab's nodes, each with a color: timelines show "which node" as a color band, so a
# migration or a VIP handoff reads as a color change. A node missing here shows as "other".
NODES = [("arquitens", "blue"), ("carrack", "orange"), ("munificent", "purple")]
NODE_MAPPINGS = [{"type": "value", "options": {str(i + 1): {"text": n, "color": c, "index": i} for i, (n, c) in enumerate(NODES)}}]

def by_node(series, by):
    """Encode which node a 0/1 series is 1 on as 1..len(NODES), one result per `by` group.
    `series` is a format string with {node} for the node filter."""
    return " or ".join(f"(max by ({by}) ({series.format(node=n)}) == 1) * {i + 1}" for i, (n, _) in enumerate(NODES))

# lcm_livepod_running is scraped from the agents, whose target `node` label wins over the
# metric's: the pod's node ends up in exported_node (or in node, if the scrape ever honors labels).
LIVEPOD_ON = 'lcm_livepod_running{{exported_node="{node}"}} or lcm_livepod_running{{exported_node="", node="{node}"}}'
# The VIP gateways run on host networking: their series carry pod and a tailnet IP, and
# kube-state-metrics says which node a pod is on.
VIP_ON = ('lcm_gateway_vip_active{{namespace="minecraft-gw"}} * on (namespace, pod) group_left(node) '
          'max by (namespace, pod, node) (kube_pod_info{{namespace="minecraft-gw", node="{node}"}})')

row("Migrations")
panel("stat", "Migrations (range)", [('sum by (outcome) (increase(lcm_migrations_total[$__range]))', "{{outcome}}")], 0, 6,
      desc="Finished migrations by outcome: success, rolled_back (restored on the source after a failed restore), aborted (source kept running), failed (needs an operator).",
      options={"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background", "textMode": "value_and_name"})
panel("timeseries", "Blackout (workload stopped)", [
    ('histogram_quantile(0.5, sum by (le) (rate(lcm_migration_blackout_seconds_bucket[$__rate_interval])))', "p50"),
    ('histogram_quantile(0.95, sum by (le) (rate(lcm_migration_blackout_seconds_bucket[$__rate_interval])))', "p95")],
    6, 9, unit="s", desc="Final dump start to restore finished. Clients time out at 30 s; LcmBlackoutSlow fires above 12 s.")
panel("timeseries", "Player-visible freeze (gateway tunnel outages)", [
    ('histogram_quantile(0.95, sum by (le, namespace) (rate(lcm_gateway_tunnel_outage_seconds_bucket[$__rate_interval])))', "p95 {{namespace}}"),
    ('max by (namespace) (lcm_gateway_tunnel_down_seconds)', "down now {{namespace}}")],
    15, 9, unit="s", desc="How long players' connections were stalled, measured at the gateway.")
nextrow()
panel("timeseries", "Phase durations (p50)", [
    ('histogram_quantile(0.5, sum by (le, phase) (rate(lcm_checkpoint_phase_seconds_bucket[$__rate_interval])))', "{{phase}}"),
    ('histogram_quantile(0.5, sum by (le, part) (rate(lcm_restore_seconds_bucket[$__rate_interval])))', "restore {{part}}")],
    0, 12, unit="s", desc="predump_* happen while the workload runs; final_dump, final_ship, frozen and restore are inside the blackout.")
panel("timeseries", "Checkpoint bytes shipped", [('sum by (kind) (rate(lcm_checkpoint_bytes_total[$__rate_interval]))', "{{kind}}")],
      12, 6, unit="Bps")
panel("timeseries", "Restores / checkpoints", [
    ('sum by (result) (increase(lcm_restores_total[$__rate_interval]))', "restore {{result}}"),
    ('sum by (result) (increase(lcm_checkpoints_total[$__rate_interval]))', "checkpoint {{result}}")],
    18, 6, desc="Failures here correspond to rolled_back/failed migrations; see lcm history.")
nextrow()

row("Players (lcm-gateway)")
panel("timeseries", "Players", [('sum by (backend) (lcm_gateway_players)', "connected {{backend}}"),
                                ('sum by (backend) (lcm_gateway_held_players)', "held {{backend}}")], 0, 8,
      desc="Summed over gateways (one per ingress). Held: waiting for a migration, or for an on-demand server to boot.")
panel("state-timeline", "Server connected to gateway", [('min by (backend) (lcm_gateway_tunnel_up)', "{{backend}}")], 8, 8,
      desc="1 while the relay inside the server is connected to every gateway; drops during each migration and while asleep.")
panel("timeseries", "Player connections", [('sum by (result) (increase(lcm_gateway_player_connections_total[$__rate_interval]))', "{{result}}")],
      16, 8, desc="session_lost = the server restarted fresh instead of resuming (a failed migration or a crash).")
nextrow()
panel("timeseries", "Traffic", [('sum by (direction) (rate(lcm_gateway_bytes_total[$__rate_interval]))', "{{direction}}")], 0, 12, unit="Bps")
panel("timeseries", "Bytes buffered in the gateway", [('sum by (namespace) (lcm_gateway_buffered_bytes)', "{{namespace}}")], 12, 12, unit="bytes",
      desc="Grows during a migration (players keep sending), returns to ~0 after.")
nextrow()
panel("timeseries", "Players by ingress", [
    ('sum(lcm_gateway_players{job="lcm-gateway-vip-admin"})', "router VIP"),
    ('sum by (gateway) (lcm_gateway_players{job="lcm-gateway"})', "{{gateway}}")], 0, 24,
    desc="Where players come in: the router's port-forward to the VIP (handed between nodes without dropping them), or a VPS ingress's gateway (a restart of that gateway drops its players).")
nextrow()

row("Nodes (lcm-agent)")
panel("stat", "Agents up", [('up{job="lcm-agent"}', "{{instance}}")], 0, 6,
      options={"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background"},
      fieldConfig={"defaults": {"mappings": [{"type": "value", "options": {"0": {"text": "DOWN", "color": "red"}, "1": {"text": "up", "color": "green"}}}]}, "overrides": []})
panel("timeseries", "Checkpoint images in RAM", [('lcm_staging_bytes', "{{instance}} {{dir}}")], 6, 9, unit="bytes",
      desc="tmpfs staging; must return to 0 after each migration (LcmStagingNotCleaned).")
panel("table", "Node state", [('lcm_fences', "fences"), ('lcm_staged_restores', "staged"), ('lcm_agent_inflight_checkpoints', "in flight"),
                               ('lcm_agent_build_info', "version")], 15, 9,
      transformations=[{"id": "merge"}], options={"showHeader": True})
nextrow()

row("LivePods")
panel("state-timeline", "Where servers run", [(
    by_node(LIVEPOD_ON, "namespace, livepod")
    + ' or (max by (namespace, livepod) (lcm_livepod_running{phase=~"Asleep|Sleeping"}) * 0)'
    + ' or (max by (namespace, livepod) (lcm_livepod_running) * 0 - 1)', "{{livepod}}")], 0, 24, h=9,
    desc="One row per LivePod, colored by the node its pod runs on: a live migration shows as a color change. Grey: asleep (on-demand, idle). Yellow: starting, stopping or between pods.",
    fieldConfig={"defaults": {"color": {"mode": "fixed", "fixedColor": "text"}, "mappings": NODE_MAPPINGS + [
        {"type": "value", "options": {"0": {"text": "asleep", "color": "#5c5c5c", "index": 10}, "-1": {"text": "starting / moving", "color": "yellow", "index": 11}}},
        {"type": "range", "options": {"from": len(NODES) + 1, "to": 100, "result": {"text": "other node", "color": "red", "index": 12}}}]},
        "overrides": []},
    options={"showValue": "auto", "mergeValues": True, "rowHeight": 0.85, "legend": {"showLegend": False}})
nextrow(9)
panel("table", "Where each LivePod runs", [
    ('max by (namespace, livepod, node, phase) (label_replace(lcm_livepod_running, "node", "$1", "exported_node", "(.+)"))', "")], 0, 12,
      desc="Reported by the leading controller.", options={"showHeader": True},
      transformations=[{"id": "labelsToFields"}, {"id": "organize", "options": {"excludeByName": {"Time": True, "Value": True}}}])
panel("timeseries", "On-demand wakes", [('sum by (backend, result) (increase(lcm_gateway_wakes_total[$__rate_interval]))', "{{backend}} {{result}}")], 12, 12,
      desc="A player arrived for an asleep server and a gateway asked the agents to start it. Errors: no agent with the LivePod controller reachable, or no on-demand LivePod for that backend.")
nextrow()
panel("timeseries", "Server menu", [('sum by (result) (increase(lcm_gateway_menu_total[$__rate_interval]))', "{{result}}")], 0, 12,
      desc="Players on the menu address (lcm-gateway -menu): shown, then transferred / quit / left / timeout. unsupported = a client older than 1.21.6 (or newer than this lcm knows) got the address list; arrived = a transferred player reached its server.")
panel("timeseries", "Menu transfers by server", [('sum by (backend) (increase(lcm_gateway_menu_transfers_total[$__rate_interval]))', "{{backend}}")], 12, 12,
      desc="Players the menu sent to each server.")
nextrow()
panel("state-timeline", "Router-path VIP: serving node", [
    # No "unserved" fallback: the gateways are scraped a few seconds apart, so every handoff would
    # flash one. LcmVIPUnserved covers a VIP nobody serves.
    (by_node(VIP_ON, "namespace"), "VIP"),
    ('max by (node) (' + VIP_ON.format(node=".+").replace('node=".+"', 'node=~".+"') + ')', "{{node}}")], 0, 12,
      desc="Top row: the node whose lcm-gateway -vip serves the VIP, as a color (a handoff is a color change). Below: each node's gateway, serving or standby. See docs/router-path.md.",
      fieldConfig={"defaults": {"color": {"mode": "fixed", "fixedColor": "text"}}, "overrides": [
          {"matcher": {"id": "byName", "options": "VIP"}, "properties": [{"id": "mappings", "value": NODE_MAPPINGS}]},
          {"matcher": {"id": "byRegexp", "options": "^(?!VIP$).*"}, "properties": [{"id": "mappings", "value": [
              {"type": "value", "options": {"1": {"text": "serving", "color": "green", "index": 0}, "0": {"text": "standby", "color": "#5c5c5c", "index": 1}}}]}]}]},
      options={"showValue": "auto", "mergeValues": True, "rowHeight": 0.85, "legend": {"showLegend": False}})
panel("timeseries", "VIP handoffs", [('sum by (direction, result) (increase(lcm_gateway_vip_handoffs_total[$__rate_interval]))', "{{direction}} {{result}}"),
                                    ('histogram_quantile(0.9, sum by (le) (rate(lcm_gateway_vip_handoff_seconds_bucket{direction="out"}[$__rate_interval])))', "p90 seconds (out)")], 12, 12,
      desc="Handoffs of the VIP with its players' connections (node drain, gateway upgrade). rolled_back: no standby took it and the old gateway restored everything. The player-visible pause adds the relay's reconnect (~0.1 s).")
nextrow()

dash = {"title": "Live container migration", "uid": "lcm", "tags": ["lcm", "minecraft"], "timezone": "browser",
        "schemaVersion": 39, "refresh": "30s", "time": {"from": "now-24h", "to": "now"}, "panels": panels,
        "description": "uorux/live-container-migration — runbook: docs/operations.md"}

if "--queries" in sys.argv:
    for p in panels:
        for t in p.get("targets", []):
            print(t["expr"].replace("$__rate_interval", "5m").replace("$__range", "1h"))
    sys.exit()
out = json.dumps(dash, indent=2)
open("deploy/homelab/monitoring/lcm-dashboard.json", "w").write(out + "\n")
with open("deploy/homelab/monitoring/lcm-dashboard.yaml", "w") as f:
    f.write("# Generated by tools/gen-dashboard.py — edit that, not this.\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: lcm-dashboard\n  namespace: monitoring\n  labels: {grafana_dashboard: \"1\"}\ndata:\n  lcm.json: |\n")
    for line in out.splitlines():
        f.write("    " + line + "\n")
