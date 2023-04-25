let
  lock = builtins.fromJSON (builtins.readFile ../flake.lock);
  flakeCompatRev = lock.nodes.flake-compat.locked.rev;
  flakeCompat = fetchTarball {
    url =
      "https://github.com/edolstra/flake-compat/archive/${flakeCompatRev}.tar.gz";
    sha256 = lock.nodes.flake-compat.locked.narHash;
  };
  flake = ((import flakeCompat) { src = ../.; }).defaultNix;
in rec {
  pkgsSrc = flake.inputs.nixpkgs;
  cargo2nix = flake.inputs.cargo2nix;
  cargo2nixOverlay = cargo2nix.overlays.default;
}
