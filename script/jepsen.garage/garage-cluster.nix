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
          kexAlgorithms = [ "curve25519-sha256@libssh.org" "ecdh-sha2-nistp256" "ecdh-sha2-nistp384" "ecdh-sha2-nistp521" "diffie-hellman-group-exchange-sha256" "diffie-hellman-group14-sha1" "diffie-hellman-group-exchange-sha1" "diffie-hellman-group1-sha1" ];
        };
        users.users.root.initialPassword = "root";
        users.users.root.openssh.authorizedKeys.keys = [
          "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJpaBZdYxHqMxhv2RExAOa7nkKhPBOHupMP3mYaZ73w9"
        ];

        system.stateVersion = "22.11";

        services.garage = {
          enable = true;
          logLevel = "debug";
          settings = {
            replication_mode = "3";
            db_engine = "lmdb";
            rpc_secret = "b597bb28ebdc90cdc4f15712733ca678cfb9a7e0311e0b9e93db9610fc3685e6";
            rpc_bind_addr = "0.0.0.0:3901";
            s3_api = {
              region = "garage";
              api_bind_addr = "0.0.0.0:3900";
            };
            k2v_api.api_bind_addr = "0.0.0.0:3902";
            admin = {
              api_bind_addr = "0.0.0.0:3903";
              admin_token = "icanhazadmin";
            };
          };
        };

        networking.firewall.allowedTCPPorts = [ 3901 3900 3902 3903 ];
      };
  };
in
{
  containers = lib.attrsets.mapAttrs nodeConfig addressMap;
  networking = {
    inherit extraHosts;
  };
}
