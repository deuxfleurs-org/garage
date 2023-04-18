{ config, lib, pkgs, ... }:
let
  unstable = import ./unstable.nix;
  addressMap =
    {
      "n1" = { localAddress = "10.233.0.101"; hostAddress = "10.233.1.101"; };
      "n2" = { localAddress = "10.233.0.102"; hostAddress = "10.233.1.102"; };
      "n3" = { localAddress = "10.233.0.103"; hostAddress = "10.233.1.103"; };
      "n4" = { localAddress = "10.233.0.104"; hostAddress = "10.233.1.104"; };
      "n5" = { localAddress = "10.233.0.105"; hostAddress = "10.233.1.105"; };
    };
  toHostsEntry = name: { localAddress, ... }: "${localAddress} ${name}";
  extraHosts =
    builtins.concatStringsSep "\n"
      (lib.attrsets.mapAttrsToList toHostsEntry addressMap);
  nodeConfig = hostName: { localAddress, hostAddress }: {
    inherit localAddress hostAddress;

    ephemeral = true;
    autoStart = true;
    privateNetwork = true;

    config = { config, pkgs, ... }:
      {
        networking = {
          inherit hostName extraHosts;
        };

        services.openssh = {
          enable = true;
          permitRootLogin = "yes";
        };
        users.users.root.initialPassword = "root";

        system.stateVersion = "22.11";

        services.garage = {
          enable = true;
          logLevel = "debug";
          settings.replication_mode = "3";
        };

        # Workaround for nixos-container issue
        # (see https://github.com/NixOS/nixpkgs/issues/67265 and
        # https://github.com/NixOS/nixpkgs/pull/81371#issuecomment-605526099).
        # The etcd service is of type "notify", which means that
        # etcd would not be considered started until etcd is fully online;
        # however, since NixOS container networking only works sometime *after*
        # multi-user.target, we forgo etcd's notification entirely.
        systemd.services.etcd.serviceConfig.Type = lib.mkForce "exec";

        systemd.services.etcd.serviceConfig.StandardOutput = "file:/var/log/etcd.log";
        systemd.services.etcd.serviceConfig.StandardError = "file:/var/log/etcd.log";

        networking.firewall.allowedTCPPorts = [ 2379 2380 ];
      };
  };
in
{
  containers = lib.attrsets.mapAttrs nodeConfig addressMap;
  networking = {
    inherit extraHosts;
  };
}
