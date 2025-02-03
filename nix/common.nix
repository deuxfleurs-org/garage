let
  lock = builtins.fromJSON (builtins.readFile ../flake.lock);

  inherit (lock.nodes.flake-compat.locked) owner repo rev narHash;

  flake-compat = fetchTarball {
    url = "https://github.com/${owner}/${repo}/archive/${rev}.tar.gz";
    sha256 = narHash;
  };

  flake = (import flake-compat { system = builtins.currentSystem; src = ../.; });
in

{
  flake = flake.defaultNix;
  nixpkgs = flake.defaultNix.inputs.nixpkgs;
  devShells = flake.defaultNix.devShells.${builtins.currentSystem};
}
