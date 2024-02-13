let
  lock = builtins.fromJSON (builtins.readFile ../flake.lock);

  inherit (lock.nodes.flake-compat.locked) owner repo rev narHash;

  flake-compat = fetchTarball {
    url = "https://github.com/${owner}/${repo}/archive/${rev}.tar.gz";
    sha256 = narHash;
  };

  flake = (import flake-compat { system = builtins.currentSystem; src = ../.; });
in
rec {
  pkgsSrc = flake.defaultNix.inputs.nixpkgs;
  cargo2nix = flake.defaultNix.inputs.cargo2nix;
  cargo2nixOverlay = cargo2nix.overlays.default;
  devShells = builtins.getAttr builtins.currentSystem flake.defaultNix.devShells;
}
