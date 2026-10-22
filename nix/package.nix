{ lib, buildGoModule }:
buildGoModule rec {
  pname = "lcm";
  version = "0.1.0";
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [ ../go.mod ../go.sum ../cmd ../internal ];
  };
  vendorHash = "sha256-5HKiQF1jj6KeVcYc6zqYNIUz9Fe0u+MnTy3btGv3w5M=";
  env.CGO_ENABLED = 0;
  ldflags = [ "-s" "-w" "-X main.version=${version}" "-X lcm/internal/agent.Version=${version}" ];
  # The tunnel/relay suites need -race (cgo) and real sockets; run them with `go test` instead.
  doCheck = false;
  meta.description = "Live migration of running containers between k3s nodes (CRIU)";
}
