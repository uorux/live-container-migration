{ lib, buildGoModule, rev ? "dev" }:
buildGoModule rec {
  pname = "lcm";
  # The git revision is part of the version: agents report it (lcm status, lcm_agent_build_info),
  # which is how version skew between nodes is noticed (LcmAgentVersionSkew).
  version = "0.1.0-${rev}";
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [ ../go.mod ../go.sum ../cmd ../internal ];
  };
  # Changes whenever the set of imported packages changes (go mod vendor copies only those);
  # the version carries the git revision, so a stale cached vendor tree can't mask that.
  vendorHash = "sha256-yhv8F7jqj1Kk66LTiuKm1QRBRngRF+xKdehyZiAs30k=";
  env.CGO_ENABLED = 0;
  ldflags = [ "-s" "-w" "-X main.version=${version}" "-X lcm/internal/agent.Version=${version}" ];
  # The tunnel/relay suites need -race (cgo) and real sockets; run them with `go test` instead.
  doCheck = false;
  meta.description = "Live migration of running containers between k3s nodes (CRIU)";
}
