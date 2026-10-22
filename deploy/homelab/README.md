# Homelab manifests (for uorux/homelab)

Apply only after the nixos-dots change (services.lcm on the node VMs) is deployed: pods using the
`lcm` RuntimeClass fail to start on a node without the handler.

- `runtimeclass.yaml` — cluster-scoped `RuntimeClass lcm`.
- `lcm-test/` — a namespace to prove migration on the real nodes before touching a real server:
  - `gateway.yaml` — lcm-gateway (players connect here; LoadBalancer IP from Cilium LB-IPAM) and the
    tunnel Service the relay dials. The tunnel token Secret `lcm-tunnel` (key `token`) must be
    provided the homelab's usual way (1Password/ExternalSecret), not committed.
  - `smoke.yaml` — tiny Python workload (loopback pair + outbound connection) for a first
    migration between each node pair.
  - `paper.yaml` — throwaway Paper server on a CephFS (RWX) PVC behind the gateway, for bot tests
    and for Jacob to join with a real client.

Binaries (lcm-gateway, lcm-relay) are fetched by init containers from the node's lcm-agent
(`http://$HOST_IP:7790/v1/bin/...`), so no image registry is involved.
