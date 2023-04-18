# Example flake.nix
{
  inputs.nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
  inputs.microvm.url = "github:astro/microvm.nix";
  inputs.microvm.inputs.nixpkgs.follows = "nixpkgs";

  outputs = { self, nixpkgs, microvm }:
  with nixpkgs.lib;
  let
    addressMap =
      {
        "n1" = { ip = "10.1.0.10"; mac = "02:00:00:00:99:01"; };
        "n2" = { ip = "10.2.0.10"; mac = "02:00:00:00:99:02"; };
        "n3" = { ip = "10.3.0.10"; mac = "02:00:00:00:99:03"; };
        "n4" = { ip = "10.4.0.10"; mac = "02:00:00:00:99:04"; };
        "n5" = { ip = "10.5.0.10"; mac = "02:00:00:00:99:05"; };
      };
    toHostsEntry = name: { ip, ... }: "${ip} ${name}";
    extraHosts =
      builtins.concatStringsSep "\n"
        (attrsets.mapAttrsToList toHostsEntry addressMap);

    nodeConfig = hostName: { ip, mac }: nixosSystem {
      system = "x86_64-linux";
      modules = [
        # Include the microvm module
        microvm.nixosModules.microvm
        # Add more modules here
        {
          networking = {
            inherit hostName extraHosts;
          };

          microvm.hypervisor = "cloud-hypervisor";
          microvm.interfaces = [ {
            inherit mac;
            type = "tap";
            id = "microvm-${hostName}";
          } ];

          services.openssh = {
            enable = true;
            permitRootLogin = "yes";
          };
          users.users.root.initialPassword = "root";

          #services.garage = {
          #  enable = true;
          #  logLevel = "debug";
          #  settings.replication_mode = "3";
          #};
        }
      ];
    };
  in
  {
    nixosConfigurations = mapAttrs nodeConfig addressMap;
  };
}
