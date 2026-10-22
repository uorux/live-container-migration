# Packages the migration stack needs that nixpkgs lacks or ships too old.
final: prev: {
  # nixpkgs has CRIU 4.1.1, which fails every dump on kernels >= 7.1 (UDP-Lite removed; criu#3002).
  criu = prev.criu.overrideAttrs (old: rec {
    patches = [ ]; # nixpkgs' rseq patch is upstream as of 4.2
    version = "4.3";
    src = final.fetchFromGitHub {
      owner = "checkpoint-restore";
      repo = "criu";
      rev = "v${version}";
      hash = "sha256-+wh5rAgp0TmYxSCL9UPrTwVCLMUEogRn8cTaiNJeH28=";
    };
    # 4.3 compiles descriptor.proto itself and assumes protobuf lives under /usr/include.
    postPatch = old.postPatch + ''
      substituteInPlace images/Makefile \
        --replace-fail "--proto_path=/usr/include" "--proto_path=${final.lib.getDev final.protobuf}/include"
    '';
  });
}
