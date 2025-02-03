{
  description =
    "Garage, an S3-compatible distributed object store for self-hosted deployments";

  # Nixpkgs 24.11 as of 2025-01-12
  inputs.nixpkgs.url =
    "github:NixOS/nixpkgs/7c4869c47090dd7f9f1bdfb49a22aea026996815";

  # Rust overlay as of 2025-02-03
  inputs.rust-overlay.url =
  "github:oxalica/rust-overlay/35c6f8c4352f995ecd53896200769f80a3e8f22d";
  inputs.rust-overlay.inputs.nixpkgs.follows = "nixpkgs";

  inputs.crane.url = "github:ipetkov/crane";

  inputs.flake-compat.url = "github:nix-community/flake-compat";
  inputs.flake-utils.url = "github:numtide/flake-utils";

  outputs = { self, nixpkgs, flake-utils, crane, rust-overlay, ... }:
    let
      compile = import ./nix/compile.nix;
    in
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        packageFor = target: release: (compile {
          inherit system target nixpkgs crane rust-overlay release;
        }).garage;
        testWith = extraTestEnv: (compile {
          inherit system nixpkgs crane rust-overlay extraTestEnv;
          release = false;
        }).garage-test;
      in
      {
        packages = {
          # default = native release build
          default = packageFor null true;

          # <arch> = cross-compiled, statically-linked release builds
          amd64 = packageFor "x86_64-unknown-linux-musl" true;
          i386 = packageFor "i686-unknown-linux-musl" true;
          arm64 = packageFor "aarch64-unknown-linux-musl" true;
          arm = packageFor "armv6l-unknown-linux-musl" true;

          # dev = native dev build
          dev = packageFor null false;

          # test = cargo test
          tests = testWith {};
          tests-lmdb = testWith {
            GARAGE_TEST_INTEGRATION_DB_ENGINE = "lmdb";
          };
          tests-sqlite = testWith {
            GARAGE_TEST_INTEGRATION_DB_ENGINE = "sqlite";
          };
        };

        # ---- developpment shell, for making native builds only ----
        devShells =
          let
            targets = compile {
              inherit system nixpkgs crane rust-overlay;
            };
          in
          {
            default = targets.devShell;

            # import the full shell using `nix develop .#full`
            full = pkgs.mkShell {
              buildInputs = with pkgs; [
                targets.toolchain
                protobuf
                clang
                mold
                # ---- extra packages for dev tasks ----
                rust-analyzer
                cargo-audit
                cargo-outdated
                cargo-machete
                nixpkgs-fmt
              ];
            };
          };
      });
}
