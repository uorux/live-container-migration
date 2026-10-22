#!/usr/bin/env bash
# ssh.sh nodeN [cmd...] — root ssh into a lab VM via its forwarded localhost port.
LAB=$(cd "$(dirname "$0")" && pwd)
n=$1; shift; port=$((2220 + ${n#node}))
exec ssh -q -i "$LAB/keys/lab_ed25519" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -o LogLevel=ERROR -p "$port" root@127.0.0.1 "$@"
