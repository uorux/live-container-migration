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
      default = "/run/lcm/token";
      description = ''
        File with the bearer token agents and the lcm CLI share. Keep it out of the store. The
        default is derived from the k3s cluster token (see deriveTokenFromK3s).
      '';
    };
    deriveTokenFromK3s = mkOption {
      type = types.bool;
      default = true;
      description = ''
        Write tokenFile as HMAC-SHA256(k3s server token, "lcm-agent-v1") at boot, so every server
        node gets the same agent token without a new secret to distribute (and the cluster token
        itself never leaves the node).
      '';
    };
    k3sTokenFile = mkOption {
      type = types.str;
      default = "/var/lib/rancher/k3s/server/token";
    };
    listenAddress = mkOption {
      type = types.str;
      default = "0.0.0.0";
      description = "Address lcm-agent binds. Agents reach each other on the nodes' InternalIPs.";
    };
    port = mkOption { type = types.port; default = 7790; };
    allowedCIDRs = mkOption {
      type = types.listOf types.str;
      default = [ "10.42.0.0/16" ];
      description = ''
        Source networks allowed to reach the agent port regardless of interface. Must include the
        pod CIDR: the destination pod's wait init container and the relay-injecting init
        containers call the agent on the node's IP from the pod network.
      '';
    };
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
    logShipping.victoriaLogsUrl = mkOption {
      type = types.nullOr types.str;
      default = null;
      example = "http://10.0.0.11:30428";
      description = ''
        When set, lcm-agent ships its own structured logs and lcm-runc's log to this VictoriaLogs
        base URL (jsonline ingest), with stream fields host and unit. Query e.g.
        `host:arquitens unit:lcm-agent`. Best effort: a bounded queue; drops are exported as
        lcm_logship_lines{state="dropped"}.
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
    #  file-validation filesize: don't read every open file looking for an ELF build-id (on
    #    CephFS each read is a round trip, cold ones stalled restores ~5 s per file); size checks
    #    still catch a file that changed.
    environment.etc."criu/runc.conf".text = ''
      ghost-limit 64M
      pre-dump-mode read
      network-lock nftables
      file-validation filesize
    '';

    # runc (spawned by containerd's shim, spawned by k3s) finds criu on k3s' PATH.
    systemd.services.k3s.path = [ cfg.criuPackage pkgs.iptables pkgs.nftables ];
    # containerd only reads drop-ins at startup. Restarting k3s leaves running pods alone (their
    # shims keep running).
    systemd.services.k3s.restartTriggers = [ runtimeDropin ];
    # A copy, not a symlink: containerd refuses to start on a dangling drop-in, and a copy stays
    # valid TOML even if this module is later removed. Installed by a unit ordered after the
    # mount holding the drop-in dir (not an activation script: with impermanence,
    # /var/lib/rancher is bind-mounted after activation runs at boot, hiding what it wrote).
    systemd.services.lcm-containerd-dropin = {
      description = "Install the lcm containerd runtime handler drop-in";
      wantedBy = [ "k3s.service" ];
      before = [ "k3s.service" ];
      unitConfig.RequiresMountsFor = [ cfg.containerdConfigDir ];
      restartTriggers = [ runtimeDropin ];
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        ExecStart = "${pkgs.coreutils}/bin/install -D -m 0644 ${runtimeDropin} ${cfg.containerdConfigDir}/60-lcm-runtime.toml";
      };
    };

    systemd.tmpfiles.settings.lcm = {
      "/var/lib/lcm".d.mode = "0755";
      "/var/lib/lcm/fence".d.mode = "0755";
      # Stable path for containerd: every shim remembers the BinaryName its container was created
      # with, so a store path would break stop/kill of existing containers once it is GC'd.
      "/var/lib/lcm/bin/lcm-runc"."L+".argument = "${cfg.package}/bin/lcm-runc";
      # Tools lcm-runc and the agent use to handle the pod IP change (see internal/netaddr).
      "/var/lib/lcm/bin/ip"."L+".argument = "${pkgs.iproute2}/bin/ip";
      "/var/lib/lcm/bin/ss"."L+".argument = "${pkgs.iproute2}/bin/ss";
      "/var/lib/lcm/bin/nsenter"."L+".argument = "${pkgs.util-linux}/bin/nsenter";
    };
    systemd.mounts = [ (stagingMount "/var/lib/lcm/ckpt") (stagingMount "/var/lib/lcm/restore") ];

    systemd.services.lcm-token = lib.mkIf cfg.deriveTokenFromK3s {
      description = "Derive the lcm agent token from the k3s cluster token";
      wantedBy = [ "multi-user.target" ];
      before = [ "lcm-agent.service" ];
      unitConfig.ConditionPathExists = cfg.k3sTokenFile;
      serviceConfig = { Type = "oneshot"; RemainAfterExit = true; UMask = "0077"; };
      path = [ pkgs.openssl pkgs.coreutils ];
      script = ''
        mkdir -p "$(dirname ${cfg.tokenFile})"
        printf %s lcm-agent-v1 | openssl dgst -sha256 -hmac "$(tr -d '\n' < ${cfg.k3sTokenFile})" -r | cut -d' ' -f1 > ${cfg.tokenFile}.tmp
        mv ${cfg.tokenFile}.tmp ${cfg.tokenFile}
      '';
    };

    systemd.services.lcm-agent = {
      description = "Live container migration agent";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" "var-lib-lcm-ckpt.mount" "var-lib-lcm-restore.mount" "lcm-token.service" ];
      wants = [ "network-online.target" ] ++ lib.optional cfg.deriveTokenFromK3s "lcm-token.service";
      requires = [ "var-lib-lcm-ckpt.mount" "var-lib-lcm-restore.mount" ];
      # k3s for `k3s crictl pull` (pre-pulling a migrating pod's images on the destination).
      path = [ cfg.criuPackage pkgs.iptables pkgs.nftables ] ++ lib.optional config.services.k3s.enable config.services.k3s.package;
      environment = {
        LCM_BIN_DIR = "${cfg.package}/bin";
      } // lib.optionalAttrs (cfg.logShipping.victoriaLogsUrl != null) {
        LCM_VICTORIALOGS_URL = cfg.logShipping.victoriaLogsUrl;
      };
      serviceConfig = {
        ExecStart = "${cfg.package}/bin/lcm-agent serve -listen ${cfg.listenAddress}:${toString cfg.port} -token-file ${cfg.tokenFile}";
        Restart = "always";
        RestartSec = 2;
        # On restart (including NixOS activation), stop only the agent itself: a runc/CRIU it
        # spawned may be in the middle of dumping a workload; the restarted agent picks up the
        # result (Recover).
        KillMode = "process";
        # Root: it drives runc/CRIU against containerd's containers.
      };
    };

    environment.systemPackages = [ cfg.package cfg.criuPackage ];
    networking.firewall.interfaces = lib.genAttrs cfg.firewallInterfaces (_: { allowedTCPPorts = [ cfg.port ]; });
    networking.firewall.extraInputRules = lib.mkIf (config.networking.nftables.enable && cfg.allowedCIDRs != [ ])
      "ip saddr { ${lib.concatStringsSep ", " cfg.allowedCIDRs} } tcp dport ${toString cfg.port} accept";
    networking.firewall.extraCommands = lib.mkIf (!config.networking.nftables.enable) (lib.concatMapStrings
      (c: "iptables -w -A nixos-fw -p tcp -s ${c} --dport ${toString cfg.port} -j nixos-fw-accept\n")
      cfg.allowedCIDRs);
  };
}
