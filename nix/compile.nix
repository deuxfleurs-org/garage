{ system, target ? null, pkgsSrc, cargo2nixOverlay
, release ? false, git_version ? null, features ? null, }:

let
  log = v: builtins.trace v v;

  pkgs = if target != null then
    import pkgsSrc {
      inherit system;
      crossSystem = {
        config = target;
        isStatic = true;
      };
      overlays = [ cargo2nixOverlay ];
    }
  else
    import pkgsSrc {
      inherit system;
      overlays = [ cargo2nixOverlay ];
    };

  toolchainOptions = {
    rustVersion = "1.78.0";
    extraRustComponents = [ "clippy" ];
  };

  /* Cargo2nix provides many overrides by default, you can take inspiration from them:
     https://github.com/cargo2nix/cargo2nix/blob/master/overlay/overrides.nix

     You can have a complete list of the available options by looking at the overriden object, mkcrate:
     https://github.com/cargo2nix/cargo2nix/blob/master/overlay/mkcrate.nix
  */
  packageOverrides = pkgs:
    pkgs.rustBuilder.overrides.all ++ [
      /* [1] We need to alter Nix hardening to make static binaries: PIE,
         Position Independent Executables seems to be supported only on amd64. Having
         this flag set either 1. make our executables crash or 2. compile as dynamic on some platforms.
         Here, we deactivate it. Later (find `codegenOpts`), we reactivate it for supported targets
         (only amd64 curently) through the `-static-pie` flag.
         PIE is a feature used by ASLR, which helps mitigate security issues.
         Learn more about Nix Hardening at: https://github.com/NixOS/nixpkgs/blob/master/pkgs/build-support/cc-wrapper/add-hardening.sh

         [2] We want to inject the git version while keeping the build deterministic.
         As we do not want to consider the .git folder as part of the input source,
         we ask the user (the CI often) to pass the value to Nix.

         [3] We don't want libsodium-sys and zstd-sys to try to use pkgconfig to build against a system library.
         However the features to do so get activated for some reason (due to a bug in cargo2nix?),
         so disable them manually here.
      */
      (pkgs.rustBuilder.rustLib.makeOverride {
        name = "garage";
        overrideAttrs = drv:
          (if git_version != null then {
            # [2]
            preConfigure = ''
              ${drv.preConfigure or ""}
              export GIT_VERSION="${git_version}"
            '';
          } else
            { }) // {
              # [1]
              hardeningDisable = [ "pie" ];
            };
      })

      (pkgs.rustBuilder.rustLib.makeOverride {
        name = "libsodium-sys";
        overrideArgs = old: {
          features = [ ]; # [3]
        };
      })

      (pkgs.rustBuilder.rustLib.makeOverride {
        name = "zstd-sys";
        overrideArgs = old: {
          features = [ ]; # [3]
        };
      })
    ];

  /* We ship some parts of the code disabled by default by putting them behind a flag.
     It speeds up the compilation (when the feature is not required) and released crates have less dependency by default (less attack surface, disk space, etc.).
     But we want to ship these additional features when we release Garage.
     In the end, we chose to exclude all features from debug builds while putting (all of) them in the release builds.
  */
  rootFeatures = if features != null then
    features
  else
    ([ "garage/bundled-libs" "garage/lmdb" "garage/sqlite" "garage/k2v" ] ++ (if release then [
      "garage/consul-discovery"
      "garage/kubernetes-discovery"
      "garage/metrics"
      "garage/telemetry-otlp"
      "garage/syslog"
    ] else
      [ ]));

  packageFun = import ../Cargo.nix;

  /* We compile fully static binaries with musl to simplify deployment on most systems.
     When possible, we reactivate PIE hardening (see above).

     Also, if you set the RUSTFLAGS environment variable, the following parameters will
     be ignored.

     For more information on static builds, please refer to Rust's RFC 1721.
     https://rust-lang.github.io/rfcs/1721-crt-static.html#specifying-dynamicstatic-c-runtime-linkage
  */

  codegenOpts = {
    "armv6l-unknown-linux-musleabihf" = [
      "target-feature=+crt-static"
      "link-arg=-static"
    ]; # compile as dynamic with static-pie
    "aarch64-unknown-linux-musl" = [
      "target-feature=+crt-static"
      "link-arg=-static"
    ]; # segfault with static-pie
    "i686-unknown-linux-musl" = [
      "target-feature=+crt-static"
      "link-arg=-static"
    ]; # segfault with static-pie
    "x86_64-unknown-linux-musl" =
      [ "target-feature=+crt-static" "link-arg=-static-pie" ];
  };

  # NixOS and Rust/Cargo triples do not match for ARM, fix it here.
  rustTarget = if target == "armv6l-unknown-linux-musleabihf" then
    "arm-unknown-linux-musleabihf"
  else
    target;

in pkgs.rustBuilder.makePackageSet ({
  inherit release packageFun packageOverrides codegenOpts rootFeatures;
  target = rustTarget;
  workspaceSrc = pkgs.lib.cleanSource ../.;
} // toolchainOptions)
