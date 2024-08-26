{
  description =
    "Garage, an S3-compatible distributed object store for self-hosted deployments";

  # Nixpkgs 24.05 as of 2024-08-26 has rustc v1.77
  inputs.nixpkgs.url =
    "github:NixOS/nixpkgs/0239aeb2f82ea27ccd6b61582b8f7fb8750eeada";

  inputs.flake-compat.url = "github:nix-community/flake-compat";

  inputs.cargo2nix = {
    # As of 2022-10-18: two small patches over unstable branch, one for clippy and one to fix feature detection
    url = "github:Alexis211/cargo2nix/a7a61179b66054904ef6a195d8da736eaaa06c36";

    # As of 2023-04-25:
    # - my two patches were merged into unstable (one for clippy and one to "fix" feature detection)
    # - rustc v1.66
    # url = "github:cargo2nix/cargo2nix/8fb57a670f7993bfc24099c33eb9c5abb51f29a2";

    # Rust overlay as of 2024-08-26
    inputs.rust-overlay.url =
      "github:oxalica/rust-overlay/19b70f147b9c67a759e35824b241f1ed92e46694";

    inputs.nixpkgs.follows = "nixpkgs";
    inputs.flake-compat.follows = "flake-compat";
  };

  inputs.flake-utils.follows = "cargo2nix/flake-utils";

  outputs = { self, nixpkgs, cargo2nix, flake-utils, ... }:
    let
      git_version = self.lastModifiedDate;
      compile = import ./nix/compile.nix;
    in
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        packages =
          let
            packageFor = target: (compile {
              inherit system git_version target;
              pkgsSrc = nixpkgs;
              cargo2nixOverlay = cargo2nix.overlays.default;
              release = true;
            }).workspace.garage { compileMode = "build"; };
          in
          {
            # default = native release build
            default = packageFor null;
            # other = cross-compiled, statically-linked builds
            amd64 = packageFor "x86_64-unknown-linux-musl";
            i386 = packageFor "i686-unknown-linux-musl";
            arm64 = packageFor "aarch64-unknown-linux-musl";
            arm = packageFor "armv6l-unknown-linux-musl";
          };

        # ---- developpment shell, for making native builds only ----
        devShells =
          let
            shellWithPackages = (packages: (compile {
              inherit system git_version;
              pkgsSrc = nixpkgs;
              cargo2nixOverlay = cargo2nix.overlays.default;
            }).workspaceShell { inherit packages; });
          in
          {
            default = shellWithPackages
              (with pkgs; [
                rustfmt
                clang
                mold
              ]);

            # import the full shell using `nix develop .#full`
            full = shellWithPackages (with pkgs; [
              rustfmt
              rust-analyzer
              clang
              mold
              # ---- extra packages for dev tasks ----
              cargo-audit
              cargo-outdated
              cargo-machete
              nixpkgs-fmt
            ]);
          };
      });
}
