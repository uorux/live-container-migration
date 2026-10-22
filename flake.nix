{
  description = "Live migration of running containers (CRIU) between k3s nodes";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; overlays = [ self.overlays.default ]; };
    in
    {
      overlays.default = import ./nix/overlay.nix;
      nixosModules.default = ./nix/module.nix;
      packages.${system} = {
        default = pkgs.lcm;
        inherit (pkgs) lcm criu;
      };
    };
}
