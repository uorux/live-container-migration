#!/usr/bin/env bash
# On the host: run build.sh as a transient user unit (outlives short-lived exec sessions); log to state/build.log.
LAB=$(cd "$(dirname "$0")" && pwd); mkdir -p "$LAB/state"
systemctl --user reset-failed lcm-build 2>/dev/null
systemd-run --user --unit=lcm-build --collect -p StandardOutput=file:"$LAB/state/build.log" -p StandardError=file:"$LAB/state/build.log" \
  "$LAB/build.sh"
