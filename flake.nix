{
  description =
    "Garage, an S3-compatible distributed object store for self-hosted deployments";

  # Nixpkgs 25.05 as of 2025-11-24
  inputs.nixpkgs.url =
    "github:NixOS/nixpkgs/cfe2c7d5b5d3032862254e68c37a6576b633d632";

  # Rust overlay as of 2026-04-23
  inputs.rust-overlay.url =
  "github:oxalica/rust-overlay/2d35c4358d7de3a0e606a6e8b27925d981c01cc3";
  inputs.rust-overlay.inputs.nixpkgs.follows = "nixpkgs";

  # Crane as of 2025-01-24
  inputs.crane.url = "github:ipetkov/crane/6fe74265bbb6d016d663b1091f015e2976c4a527";

  inputs.flake-compat.url = "github:nix-community/flake-compat";
  inputs.flake-utils.url = "github:numtide/flake-utils";

  outputs = { self, nixpkgs, flake-utils, crane, rust-overlay, ... }:
    let
      compile = import ./nix/compile.nix;
    in
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        webadmin = pkgs.callPackage ./nix/webadmin.nix {};
        packageFor = target: release: (compile {
          inherit system target nixpkgs crane rust-overlay release webadmin;
        }).garage;
        testWith = extraTestEnv: (compile {
          inherit system nixpkgs crane rust-overlay extraTestEnv;
          release = false;
        }).garage-test;
        lints = (compile {
          inherit system nixpkgs crane rust-overlay;
          release = false;
        });
      in
      {
        packages = {
          # default = native release build
          default = packageFor null true;

          # webadmin = distfiles for webadmin configured for embedding within Garage binary
          inherit webadmin;

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
          tests-fjall = testWith {
            GARAGE_TEST_INTEGRATION_DB_ENGINE = "fjall";
          };

          # lints (fmt, clippy)
          fmt = lints.garage-cargo-fmt;
          clippy = lints.garage-cargo-clippy;
        };

        # ---- development shell, for making native builds only ----
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
                openssl
                socat
                killall
              ];
            };

            # dev shell for fuzzing
            fuzz = pkgs.mkShell {
              buildInputs = with pkgs; [
                targets.toolchainNightly
                cargo-fuzz
              ];
            };
          };
      });
}
