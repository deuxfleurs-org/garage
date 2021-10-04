rec {
  /*
   * Fixed dependencies
   */
  pkgsSrc = fetchTarball {
    # As of 2021-10-04
    url ="https://github.com/NixOS/nixpkgs/archive/b27d18a412b071f5d7991d1648cfe78ee7afe68a.tar.gz";
    sha256 = "1xy9zpypqfxs5gcq5dcla4bfkhxmh5nzn9dyqkr03lqycm9wg5cr";
  };
  cargo2nixSrc = fetchGit {
    # As of 2021-10-06
    url = "https://github.com/superboum/cargo2nix";
    rev = "1364752cd784764db2ef5b1e1248727cebfae2ce";
  };
  rustOverlaySrc = fetchGit {
    # As of 2021-10-05
    url = "https://github.com/oxalica/rust-overlay";
    rev = "9c2fc6a62ccbc6f420d71ecac6bf0b84dbbee64f";
  };

  /*
   * Shared objects
   */
  rustOverlay = import rustOverlaySrc;
  cargo2nix = import cargo2nixSrc;
  cargo2nixOverlay = import "${cargo2nixSrc}/overlay";
}
