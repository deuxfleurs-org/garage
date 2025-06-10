{
  /* build inputs */
  nixpkgs,
  crane,
  rust-overlay,

  /* parameters */
  system,
  git_version ? null,
  target ? null,
  release ? false,
  features ? null,
  extraTestEnv ? {}
}:

let
  log = v: builtins.trace v v;

  # NixOS and Rust/Cargo triples do not match for ARM, fix it here.
  rustTarget = if target == "armv6l-unknown-linux-musleabihf" then
    "arm-unknown-linux-musleabihf"
  else
    target;

  rustTargetEnvMap = {
    "x86_64-unknown-linux-musl" = "X86_64_UNKNOWN_LINUX_MUSL";
    "aarch64-unknown-linux-musl" = "AARCH64_UNKNOWN_LINUX_MUSL";
    "i686-unknown-linux-musl" = "I686_UNKNOWN_LINUX_MUSL";
    "arm-unknown-linux-musleabihf" = "ARM_UNKNOWN_LINUX_MUSLEABIHF";
  };

  pkgsNative = import nixpkgs {
    inherit system;
    overlays = [ (import rust-overlay) ];
  };

  pkgs = if target != null then
    import nixpkgs {
      inherit system;
      crossSystem = {
        config = target;
        isStatic = true;
      };
      overlays = [ (import rust-overlay) ];
    }
  else
    pkgsNative;

  inherit (pkgs) lib stdenv;

  toolchainFn = (p: p.rust-bin.stable."1.82.0".default.override {
    targets = lib.optionals (target != null) [ rustTarget ];
    extensions = [
      "rust-src"
      "rustfmt"
    ];
  });

  craneLib = (crane.mkLib pkgs).overrideToolchain toolchainFn;

  src = craneLib.cleanCargoSource ../.;

  /* We ship some parts of the code disabled by default by putting them behind a flag.
     It speeds up the compilation (when the feature is not required) and released crates have less dependency by default (less attack surface, disk space, etc.).
     But we want to ship these additional features when we release Garage.
     In the end, we chose to exclude all features from debug builds while putting (all of) them in the release builds.
  */
  rootFeatures = if features != null then
    features
  else
    ([ "bundled-libs" "lmdb" "sqlite" "k2v" ] ++ (lib.optionals release [
      "consul-discovery"
      "kubernetes-discovery"
      "metrics"
      "telemetry-otlp"
      "syslog"
      "journald"
    ]));

  featuresStr = lib.concatStringsSep "," rootFeatures;

  /* We compile fully static binaries with musl to simplify deployment on most systems.
     When possible, we reactivate PIE hardening (see above).

     Also, if you set the RUSTFLAGS environment variable, the following parameters will
     be ignored.

     For more information on static builds, please refer to Rust's RFC 1721.
     https://rust-lang.github.io/rfcs/1721-crt-static.html#specifying-dynamicstatic-c-runtime-linkage
  */
  codegenOptsMap = {
    "x86_64-unknown-linux-musl" =
      [ "target-feature=+crt-static" "link-arg=-static-pie" ];
    "aarch64-unknown-linux-musl" = [
      "target-feature=+crt-static"
      "link-arg=-static"
    ]; # segfault with static-pie
    "i686-unknown-linux-musl" = [
      "target-feature=+crt-static"
      "link-arg=-static"
    ]; # segfault with static-pie
    "armv6l-unknown-linux-musleabihf" = [
      "target-feature=+crt-static"
      "link-arg=-static"
    ]; # compile as dynamic with static-pie
  };

  codegenOpts = if target != null then codegenOptsMap.${target} else [
    "link-arg=-fuse-ld=mold"
  ];

  commonArgs =
    {
      inherit src;
      pname = "garage";
      version = "dev";

      strictDeps = true;
      cargoExtraArgs = "--locked --features ${featuresStr}";
      cargoTestExtraArgs = "--workspace";

      nativeBuildInputs = [
        pkgsNative.protobuf
        pkgs.stdenv.cc
      ] ++ lib.optionals (target == null) [
        pkgs.clang
        pkgs.mold
      ];

      CARGO_PROFILE = if release then "release" else "dev";
      CARGO_BUILD_RUSTFLAGS =
        lib.concatStringsSep
          " "
          (builtins.map (flag: "-C ${flag}") codegenOpts);
    }
  //
    (if rustTarget != null then {
      CARGO_BUILD_TARGET = rustTarget;

      "CARGO_TARGET_${rustTargetEnvMap.${rustTarget}}_LINKER" = "${stdenv.cc.targetPrefix}cc";

      HOST_CC = "${stdenv.cc.nativePrefix}cc";
      TARGET_CC = "${stdenv.cc.targetPrefix}cc";
    } else {
      CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER = "clang";
    });

in rec {
  toolchain = toolchainFn pkgs;

  devShell = pkgs.mkShell {
    buildInputs = [
      toolchain
    ] ++ (with pkgs; [
      protobuf
      clang
      mold
    ]);
  };

  # ---- building garage ----

  garage-deps = craneLib.buildDepsOnly commonArgs;

  garage = craneLib.buildPackage (commonArgs // {
    cargoArtifacts = garage-deps;

    doCheck = false;
  } //
    (if git_version != null then {
      version = git_version;
      GIT_VERSION = git_version;
    } else {}));

  # ---- testing garage ----

  garage-test-bin = craneLib.cargoBuild (commonArgs // {
    cargoArtifacts = garage-deps;

    pname = "garage-tests";

    CARGO_PROFILE = "test";
    cargoExtraArgs = "${commonArgs.cargoExtraArgs} --tests --workspace";
    doCheck = false;
  });

  garage-test = craneLib.cargoTest (commonArgs // {
    cargoArtifacts = garage-test-bin;
    nativeBuildInputs = commonArgs.nativeBuildInputs ++ [
      pkgs.cacert
    ];
  } // extraTestEnv);
}
