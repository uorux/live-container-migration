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
  vendorHash = "sha256-+PzGXatuTJxxA5IxRcvnDhb+xM+NMbmiW7f33mVEink=";
  env.CGO_ENABLED = 0;
  ldflags = [ "-s" "-w" "-X main.version=${version}" "-X lcm/internal/agent.Version=${version}" ];
  # The tunnel/relay suites need -race (cgo) and real sockets; run them with `go test` instead.
  doCheck = false;
  meta.description = "Live migration of running containers between k3s nodes (CRIU)";
}
