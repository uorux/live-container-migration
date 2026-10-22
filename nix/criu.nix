# CRIU 4.3. nixpkgs ships 4.1.1, which fails every dump on kernels >= 7.1 (UDP-Lite removal, criu#3002).
# A no-op when given a CRIU that is already new enough (e.g. through nix/overlay.nix).
{ criu, fetchFromGitHub, lib, protobuf }:
if lib.versionAtLeast criu.version "4.2" then criu else
criu.overrideAttrs (old: rec {
  version = "4.3";
  src = fetchFromGitHub {
    owner = "checkpoint-restore";
    repo = "criu";
    rev = "v${version}";
    hash = "sha256-+wh5rAgp0TmYxSCL9UPrTwVCLMUEogRn8cTaiNJeH28=";
  };
  patches = [ ]; # nixpkgs' backports are upstream as of 4.2
  # 4.3 compiles descriptor.proto itself and assumes protobuf lives under /usr/include.
  postPatch = old.postPatch + ''
    substituteInPlace images/Makefile \
      --replace-fail "--proto_path=/usr/include" "--proto_path=${lib.getDev protobuf}/include"
  '';
})
