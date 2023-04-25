{
  description =
    "Garage, an S3-compatible distributed object store for self-hosted deployments";

  # Nixpkgs unstable as of 2023-04-25, has rustc v1.68
  inputs.nixpkgs.url =
    "github:NixOS/nixpkgs/94517a501434a627c5d9e72ac6e7f26174b978d3";

  inputs.cargo2nix = {
    # As of 2022-10-18: two small patches over unstable branch, one for clippy and one to fix feature detection
    url = "github:Alexis211/cargo2nix/a7a61179b66054904ef6a195d8da736eaaa06c36";

    # As of 2023-04-25:
    # - my two patches were merged into unstable (one for clippy and one to "fix" feature detection)
    # - rustc v1.66
    # url = "github:cargo2nix/cargo2nix/8fb57a670f7993bfc24099c33eb9c5abb51f29a2";

    # Rust overlay as of 2023-04-25
    inputs.rust-overlay.url =
      "github:oxalica/rust-overlay/74f1a64dd28faeeb85ef081f32cad2989850322c";

    inputs.nixpkgs.follows = "nixpkgs";
  };

  inputs.flake-utils.follows = "cargo2nix/flake-utils";
  inputs.flake-compat.follows = "cargo2nix/flake-compat";

  outputs = { self, nixpkgs, cargo2nix, flake-utils, ... }:
    let
      git_version = self.lastModifiedDate;
      compile = import ./nix/compile.nix;
    in flake-utils.lib.eachDefaultSystem (system:
      let pkgs = nixpkgs.legacyPackages.${system};
      in {
        packages = {
          default = (compile {
            inherit system git_version;
            pkgsSrc = nixpkgs;
            cargo2nixOverlay = cargo2nix.overlays.default;
            release = true;
          }).workspace.garage { compileMode = "build"; };
        };
        devShell = (compile {
          inherit system git_version;
          pkgsSrc = nixpkgs;
          cargo2nixOverlay = cargo2nix.overlays.default;
          release = false;
        }).workspaceShell { packages = [ pkgs.rustfmt ]; };
      });
}
