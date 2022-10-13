rec {
  /*
   * Fixed dependencies
   */
  pkgsSrc = fetchTarball {
    # As of 2021-10-04
    #url = "https://github.com/NixOS/nixpkgs/archive/b27d18a412b071f5d7991d1648cfe78ee7afe68a.tar.gz";
    #sha256 = "1xy9zpypqfxs5gcq5dcla4bfkhxmh5nzn9dyqkr03lqycm9wg5cr";

    # NixOS 22.05
    url = "https://github.com/NixOS/nixpkgs/archive/refs/tags/22.05.zip";
    sha256 = "0d643wp3l77hv2pmg2fi7vyxn4rwy0iyr8djcw1h5x72315ck9ik";

    # As of 2022-10-13
    #url = "https://github.com/NixOS/nixpkgs/archive/a3073c49bc0163fea6a121c276f526837672b555.zip";
    #sha256 = "1bz632psfbpmicyzjb8b4265y50shylccvfm6ry6mgnv5hvz324s";
  };
  cargo2nixSrc = fetchGit {
    # As of 2022-10-14: (TODO)
    url = "https://github.com/Alexis211/cargo2nix";
    ref = "custom_unstable";
    rev = "15543df35485bef9e2092391ecafa78eae5fa740";
  };

  /*
   * Shared objects
   */
  cargo2nix = import cargo2nixSrc;
  cargo2nixOverlay = cargo2nix.overlays.default;
}
