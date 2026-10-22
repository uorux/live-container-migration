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
panel("timeseries", "Players", [('sum by (namespace) (lcm_gateway_players)', "connected {{namespace}}"),
                                ('sum by (namespace) (lcm_gateway_held_players)', "held {{namespace}}")], 0, 8)
panel("state-timeline", "Server connected to gateway", [('max by (namespace) (lcm_gateway_tunnel_up)', "{{namespace}}")], 8, 8,
      desc="1 while the relay inside the server is connected; drops during each migration.")
panel("timeseries", "Player connections", [('sum by (result) (increase(lcm_gateway_player_connections_total[$__rate_interval]))', "{{result}}")],
      16, 8, desc="session_lost = the server restarted fresh instead of resuming (a failed migration or a crash).")
nextrow()
panel("timeseries", "Traffic", [('sum by (direction) (rate(lcm_gateway_bytes_total[$__rate_interval]))', "{{direction}}")], 0, 12, unit="Bps")
panel("timeseries", "Bytes buffered in the gateway", [('sum by (namespace) (lcm_gateway_buffered_bytes)', "{{namespace}}")], 12, 12, unit="bytes",
      desc="Grows during a migration (players keep sending), returns to ~0 after.")
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
