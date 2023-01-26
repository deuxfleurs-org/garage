rec {
  # * Fixed dependencies
  pkgsSrc = fetchTarball {
    # As of 2022-10-13
    url =
      "https://github.com/NixOS/nixpkgs/archive/a3073c49bc0163fea6a121c276f526837672b555.zip";
    sha256 = "1bz632psfbpmicyzjb8b4265y50shylccvfm6ry6mgnv5hvz324s";
  };
  cargo2nixSrc = fetchGit {
    # As of 2022-10-18: two small patches over unstable branch, one for clippy and one to fix feature detection
    url = "https://github.com/Alexis211/cargo2nix";
    ref = "custom_unstable";
    rev = "a7a61179b66054904ef6a195d8da736eaaa06c36";
  };

  # * Shared objects
  cargo2nix = import cargo2nixSrc;
  cargo2nixOverlay = cargo2nix.overlays.default;
}
