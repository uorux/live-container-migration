{ config, lib, pkgs, ... }:
let
  cfg = config.lab;
  labKey = builtins.readFile ./lab_ed25519.pub;
in
{
  options.lab = {
    ip = lib.mkOption { type = lib.types.str; };
    serverIp = lib.mkOption { type = lib.types.str; };
    sshPort = lib.mkOption { type = lib.types.port; };
    role = lib.mkOption { type = lib.types.enum [ "server" "agent" ]; };
  };

  imports = [ ../nix/module.nix ];

  config = {
    services.lcm = {
      enable = true;
      tokenFile = "/etc/lcm/token";
      deriveTokenFromK3s = false; # node2/3 are k3s agents without a server token
      stagingSize = "60%";
      # Exercise log shipping in the lab: lab/manifests/victorialogs.yaml runs a VictoriaLogs NodePort.
      logShipping.victoriaLogsUrl = "http://127.0.0.1:30428";
      # Only the k3s server (node1) has the admin kubeconfig the LivePod controller uses.
      livePods.enable = cfg.role == "server";
      wakeTokenFile = "/etc/lcm/wake-token";
      # The leading controller keeps this Secret holding the wake token (lab gateways can mount it).
      livePods.wakeTokenSecrets = [ "default/lcm-wake" ];
    };
    environment.etc."lcm/token".text = "lab-token-not-secret"; # lab only: lands in the store
    environment.etc."lcm/wake-token".text = "lab-wake-token-not-secret";
    system.stateVersion = "26.05";
    nixpkgs.overlays = [ (import ../nix/overlay.nix) ];
    # Lets me iterate with `nixos-rebuild switch --flake /lcm/lab#nodeN` inside the VM (builds land on the VM disk).
    nix.settings.experimental-features = [ "nix-command" "flakes" ];
    documentation.enable = false;

    boot.kernelPackages = pkgs.linuxPackages_latest;
    # sock_diag modules CRIU uses to dump sockets (TCP repair needs tcp_diag).
    boot.kernelModules = [ "inet_diag" "tcp_diag" "udp_diag" "unix_diag" "netlink_diag" "packet_diag" "nf_tables" "br_netfilter" ];

    virtualisation = {
      memorySize = 6144;
      cores = 4;
      diskSize = 40960;
      graphics = false;
      writableStoreUseTmpfs = false;
      forwardPorts = [{ from = "host"; host.address = "127.0.0.1"; host.port = cfg.sshPort; guest.port = 22; }];
      # Second NIC on a VDE switch is the private "cluster" network shared by all lab nodes (eth1).
      qemu.options = [
        "-cpu host"
        "-device virtio-net-pci,netdev=vlan1,mac=52:54:00:12:01:${lib.last (lib.splitString "." cfg.ip)}"
        "-netdev vde,id=vlan1,sock=\"$QEMU_VDE_SOCKET_1\""
      ];
      sharedDirectories.lcm = { source = "/home/jrt/Documents/live-container-migration"; target = "/lcm"; };
    };

    networking = {
      usePredictableInterfaceNames = false;
      firewall.enable = false;
      interfaces.eth1.ipv4.addresses = [{ address = cfg.ip; prefixLength = 24; }];
      extraHosts = ''
        192.168.100.11 node1
        192.168.100.12 node2
        192.168.100.13 node3
      '';
    };

    services.openssh = { enable = true; settings.PermitRootLogin = "prohibit-password"; };
    users.users.root.openssh.authorizedKeys.keys = [ labKey ];

    services.k3s = {
      enable = true;
      role = cfg.role;
      token = "lcm-lab-token";
      serverAddr = lib.mkIf (cfg.role == "agent") "https://${cfg.serverIp}:6443";
      extraFlags = [
        "--node-ip=${cfg.ip}"
        "--kubelet-arg=feature-gates=ContainerCheckpoint=true"
      ] ++ lib.optionals (cfg.role == "server") [
        "--flannel-backend=none"
        "--disable-network-policy"
        "--disable-kube-proxy"
        "--disable=traefik,servicelb,metrics-server"
        "--write-kubeconfig-mode=644"
        "--tls-san=${cfg.ip}"
      ];
    };

    environment.systemPackages = with pkgs; [
      cilium-cli kubectl kubernetes-helm jq tcpdump strace conntrack-tools bpftools
      skopeo buildah jdk21_headless python3 socat nfs-utils curl htop runc rsync
    ];

    # NFS server on node1 stands in for shared (RWX) storage in the lab.
    services.nfs.server = lib.mkIf (cfg.role == "server") {
      enable = true;
      exports = "/srv/nfs 192.168.100.0/24(rw,sync,no_subtree_check,no_root_squash)";
    };
    systemd.tmpfiles.rules = [ "d /srv/nfs 0777 root root -" ];
    boot.supportedFilesystems = [ "nfs" ];
  };
}
