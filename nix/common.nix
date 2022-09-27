rec {
  /*
   * Fixed dependencies
   */
  pkgsSrc = fetchTarball {
    # As of 2021-10-04
    url = "https://github.com/NixOS/nixpkgs/archive/b27d18a412b071f5d7991d1648cfe78ee7afe68a.tar.gz";
    sha256 = "1xy9zpypqfxs5gcq5dcla4bfkhxmh5nzn9dyqkr03lqycm9wg5cr";
  };
  cargo2nixSrc = fetchGit {
    # As of 2022-08-29, stacking two patches: superboum@dedup_propagate and Alexis211@fix_fetchcrategit
    url = "https://github.com/superboum/cargo2nix";
    ref = "dedup_propagate";
    rev = "486675c67249e735dd7eb68e1b9feac9db102be7";
  };

  /*
   * Shared objects
   */
  cargo2nix = import cargo2nixSrc;
  cargo2nixOverlay = import "${cargo2nixSrc}/overlay";
}
