#!/usr/bin/env bash
# Build the lab VMs + tools and record their store paths in state/paths.env for up.sh.
set -euo pipefail
LAB=$(cd "$(dirname "$0")" && pwd); mkdir -p "$LAB/state"
: > "$LAB/state/paths.env.new"
for a in tools node1 node2 node3; do
  echo "${a}_PATH=$(nix build "$LAB#$a" --no-link --print-out-paths)" >> "$LAB/state/paths.env.new"
done
mv "$LAB/state/paths.env.new" "$LAB/state/paths.env"; cat "$LAB/state/paths.env"
