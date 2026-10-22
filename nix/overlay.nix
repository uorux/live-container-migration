# Adds the migration stack to nixpkgs: pkgs.criu (4.3) and pkgs.lcm.
final: prev: {
  criu = final.callPackage ./criu.nix { criu = prev.criu; };
  lcm = final.callPackage ./package.nix { };
}
