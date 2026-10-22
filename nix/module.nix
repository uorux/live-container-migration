# NixOS module: everything a k3s node needs to send and receive live migrations.
#
#   imports = [ "${lcm-src}/nix/module.nix" ];
#   services.lcm = { enable = true; tokenFile = "/run/secrets/lcm-token"; };
#
# Pods opt in with `runtimeClassName: lcm` (deploy/runtimeclass.yaml).
{ config, lib, pkgs, ... }:
let
  cfg = config.services.lcm;
  runtimeDropin = pkgs.writeText "lcm-containerd-runtime.toml" ''
    [plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.lcm]
      runtime_type = "io.containerd.runc.v2"
    [plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.lcm.options]
      BinaryName = "/var/lib/lcm/bin/lcm-runc"
      SystemdCgroup = true
  '';
  inherit (lib) mkOption mkEnableOption mkIf types;
  stagingMount = where: {
    what = "tmpfs";
    inherit where;
    type = "tmpfs";
    options = "mode=0700,size=${cfg.stagingSize}";
    wantedBy = [ "local-fs.target" ];
  };
in
{
  options.services.lcm = {
    enable = mkEnableOption "live container migration (lcm-agent, lcm runtime handler, CRIU)";
    package = mkOption {
      type = types.package;
      default = pkgs.callPackage ./package.nix { };
      description = "The lcm package (lcm-agent, lcm-runc, lcm-relay, ...).";
    };
    criuPackage = mkOption {
      type = types.package;
      default = pkgs.callPackage ./criu.nix { };
      description = "CRIU >= 4.2 (nixpkgs' 4.1.1 cannot dump on kernels >= 7.1).";
    };
    tokenFile = mkOption {
      type = types.path;
      description = "File with the bearer token agents and the lcm CLI share. Keep it out of the store.";
    };
    listenAddress = mkOption {
      type = types.str;
      default = "0.0.0.0";
      description = "Address lcm-agent binds. Agents reach each other on the nodes' InternalIPs.";
    };
    port = mkOption { type = types.port; default = 7790; };
    firewallInterfaces = mkOption {
      type = types.listOf types.str;
      default = [ ];
      example = [ "tailscale0" ];
      description = "Interfaces on which to open the agent port.";
    };
    stagingSize = mkOption {
      type = types.str;
      default = "50%";
      description = ''
        Size cap of the tmpfs mounts holding checkpoint images (/var/lib/lcm/{ckpt,restore}).
        Images live in RAM because restore speed is bound by reading them; a migration needs
        roughly the workload's RSS on both ends while it runs.
      '';
    };
    containerdConfigDir = mkOption {
      type = types.str;
      default = "/var/lib/rancher/k3s/agent/etc/containerd/config-v3.toml.d";
      description = "Drop-in directory k3s' containerd config imports.";
    };
  };

  config = mkIf cfg.enable {
    # CRIU dumps sockets via sock_diag; TCP repair needs tcp_diag.
    boot.kernelModules = [ "inet_diag" "tcp_diag" "udp_diag" "unix_diag" "netlink_diag" "packet_diag" ];

    # Applied to every CRIU run runc makes.
    #  ghost-limit: deleted-but-open files on the rootfs (e.g. JNA's unpacked natives) are carried
    #    inside the image up to this size.
    #  pre-dump-mode read: copy pages with process_vm_readv after unfreezing, instead of splicing
    #    them while frozen, so pre-copy rounds don't stall the workload.
    #  network-lock nftables: block the netns while dumping/restoring TCP via libnftables rather
    #    than shelling out to iptables-restore.
    environment.etc."criu/runc.conf".text = ''
      ghost-limit 64M
      pre-dump-mode read
      network-lock nftables
    '';

    # runc (spawned by containerd's shim, spawned by k3s) finds criu on k3s' PATH.
    systemd.services.k3s.path = [ cfg.criuPackage pkgs.iptables pkgs.nftables ];
    # containerd only reads drop-ins at startup. Restarting k3s leaves running pods alone (their
    # shims keep running).
    systemd.services.k3s.restartTriggers = [ runtimeDropin ];

    # A copy, not a symlink: containerd refuses to start on a dangling drop-in, and a copy stays
    # valid TOML even if this module is later removed.
    systemd.tmpfiles.settings.lcm = {
      "${cfg.containerdConfigDir}/60-lcm-runtime.toml"."C+".argument = toString runtimeDropin;
      "/var/lib/lcm".d.mode = "0755";
      "/var/lib/lcm/fence".d.mode = "0755";
      # Stable path for containerd: every shim remembers the BinaryName its container was created
      # with, so a store path would break stop/kill of existing containers once it is GC'd.
      "/var/lib/lcm/bin/lcm-runc"."L+".argument = "${cfg.package}/bin/lcm-runc";
    };
    systemd.mounts = [ (stagingMount "/var/lib/lcm/ckpt") (stagingMount "/var/lib/lcm/restore") ];

    systemd.services.lcm-agent = {
      description = "Live container migration agent";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" "var-lib-lcm-ckpt.mount" "var-lib-lcm-restore.mount" ];
      wants = [ "network-online.target" ];
      requires = [ "var-lib-lcm-ckpt.mount" "var-lib-lcm-restore.mount" ];
      path = [ cfg.criuPackage pkgs.iptables pkgs.nftables ];
      environment.LCM_BIN_DIR = "${cfg.package}/bin";
      serviceConfig = {
        ExecStart = "${cfg.package}/bin/lcm-agent serve -listen ${cfg.listenAddress}:${toString cfg.port} -token-file ${cfg.tokenFile}";
        Restart = "always";
        RestartSec = 2;
        # Root: it drives runc/CRIU against containerd's containers.
      };
    };

    environment.systemPackages = [ cfg.package cfg.criuPackage ];
    networking.firewall.interfaces = lib.genAttrs cfg.firewallInterfaces (_: { allowedTCPPorts = [ cfg.port ]; });
  };
}
