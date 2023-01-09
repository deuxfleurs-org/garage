rec {
  /*
   * Fixed dependencies
   */
  pkgsSrc = fetchTarball {
    # As of 2023-01-09
    url = "https://github.com/NixOS/nixpkgs/archive/baed728abe983508cabc99d05cccc164fe748744.zip";
    sha256 = "1m7kmcjhnj3lx7xqs0nh262c4nxs5n8p7k9g6kc4gv1k5nrcrnpf";
  };
  cargo2nixSrc = fetchGit {
    # As of 2022-10-18: two small patches over unstable branch, one for clippy and one to fix feature detection
    url = "https://github.com/Alexis211/cargo2nix";
    ref = "custom_unstable";
    rev = "505caa32110d42ee03bd68b47031142eff9c827b";
  };

  /*
   * Shared objects
   */
  cargo2nix = import cargo2nixSrc;
  cargo2nixOverlay = cargo2nix.overlays.default;
}
