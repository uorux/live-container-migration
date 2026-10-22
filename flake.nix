{
  description = "Live migration of running containers (CRIU) between k3s nodes";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; overlays = [ self.overlays.default ]; };
      rev = self.shortRev or self.dirtyShortRev or "dev";
    in
    {
      overlays.default = final: prev: import ./nix/overlay.nix final prev // {
        lcm = final.callPackage ./nix/package.nix { inherit rev; };
      };
      nixosModules.default = { pkgs, lib, ... }: {
        imports = [ ./nix/module.nix ];
        services.lcm.package = lib.mkDefault (pkgs.callPackage ./nix/package.nix { inherit rev; });
      };
      packages.${system} = {
        default = pkgs.lcm;
        inherit (pkgs) lcm criu;
      };
    };
}
