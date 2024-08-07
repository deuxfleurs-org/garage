{
  description =
    "Garage, an S3-compatible distributed object store for self-hosted deployments";

  # Nixpkgs 24.05 as of 2024-08-07, has rustc v1.77.2
  inputs.nixpkgs.url =
    "github:NixOS/nixpkgs/63dacb46bf939521bdc93981b4cbb7ecb58427a0";

  inputs.flake-compat.url = "github:nix-community/flake-compat";

  inputs.cargo2nix = {
    # Top of unreleased 0.12 branch, include the two patches from Alex, nix 23.11 rust 1.75.0
    url = "github:cargo2nix/cargo2nix/da5f5d796af00fe818aa12f3b2d46a4800e6fec8";

    # Rust overlay as of 2024-08-07
    inputs.rust-overlay.url =
      "github:oxalica/rust-overlay/7df2ac544c203d21b63aac23bfaec7f9b919a733";

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
