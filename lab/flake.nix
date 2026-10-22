{
  description = "Local KVM lab for live container migration: NixOS VMs running k3s + Cilium + CRIU";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};

      # Lab nodes. node1 is the k3s server (and also schedulable); the rest are agents.
      nodes = {
        node1 = { ip = "192.168.100.11"; sshPort = 2221; role = "server"; };
        node2 = { ip = "192.168.100.12"; sshPort = 2222; role = "agent"; };
        node3 = { ip = "192.168.100.13"; sshPort = 2223; role = "agent"; };
      };

      mkNode = name: n: nixpkgs.lib.nixosSystem {
        inherit system;
        modules = [
          "${nixpkgs}/nixos/modules/virtualisation/qemu-vm.nix"
          ./node.nix
          {
            networking.hostName = name;
            lab = { inherit (n) ip sshPort role; serverIp = nodes.node1.ip; };
          }
        ];
      };
    in
    {
      nixosConfigurations = builtins.mapAttrs mkNode nodes;

      packages.${system} = (builtins.mapAttrs (name: _: self.nixosConfigurations.${name}.config.system.build.vm) nodes) // {
        tools = pkgs.buildEnv {
          name = "lab-tools";
          paths = with pkgs; [ vde2 qemu_kvm openssh kubectl cilium-cli ];
        };
      };
    };
}
