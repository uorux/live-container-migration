#!/usr/bin/env bash
# import-images.sh IMAGE... — fetch images once on the host (fast) and import them into every lab
# VM's containerd over the ssh forwards, instead of each VM pulling through QEMU's slow user-net.
# Run on the host (it needs skopeo from nixpkgs and the lab ssh key).
set -euo pipefail
LAB=$(cd "$(dirname "$0")/.." && pwd)
SKOPEO=$(nix build --no-link --print-out-paths nixpkgs#skopeo.out | head -1)/bin/skopeo
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
for img in "$@"; do
  f="$TMP/$(echo "$img" | tr '/:@' '___').tar"
  "$SKOPEO" --insecure-policy copy --quiet "docker://$img" "docker-archive:$f:$img"
  for n in node1 node2 node3; do
    "$LAB/ssh.sh" "$n" 'k3s ctr -n k8s.io images import --no-unpack=false - >/dev/null' < "$f" &
  done
  wait
  echo "imported $img ($(du -h "$f" | cut -f1))"
done
