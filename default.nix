{
  system ? builtins.currentSystem,
  release ? false,
  target ? "x86_64-unknown-linux-musl",
  compileMode ? null,
  git_version ? null,
}:

with import ./nix/common.nix;

let
  crossSystem = { config = target; };
in let
  log = v: builtins.trace v v;

  pkgs = import pkgsSrc {
    inherit system crossSystem;
    overlays = [ cargo2nixOverlay ];
  };


  /*
   Rust and Nix triples are not the same. Cargo2nix has a dedicated library
   to convert Nix triples to Rust ones. We need this conversion as we want to
   set later options linked to our (rust) target in a generic way. Not only
   the triple terminology is different, but also the "roles" are named differently.
   Nix uses a build/host/target terminology where Nix's "host" maps to Cargo's "target".
  */
  rustTarget = log (pkgs.rustBuilder.rustLib.rustTriple pkgs.stdenv.hostPlatform);

  /*
   Cargo2nix is built for rustOverlay which installs Rust from Mozilla releases.
   We want our own Rust to avoid incompatibilities, like we had with musl 1.2.0.
   rustc was built with musl < 1.2.0 and nix shipped musl >= 1.2.0 which lead to compilation breakage.
   So we want a Rust release that is bound to our Nix repository to avoid these problems.
   See here for more info: https://musl.libc.org/time64.html
   Because Cargo2nix does not support the Rust environment shipped by NixOS,
   we emulate the structure of the Rust object created by rustOverlay.
   In practise, rustOverlay ships rustc+cargo in a single derivation while
   NixOS ships them in separate ones. We reunite them with symlinkJoin.
   */
  rustChannel = pkgs.symlinkJoin {
    name ="rust-channel";
    paths = [ 
      pkgs.rustPlatform.rust.rustc
      pkgs.rustPlatform.rust.cargo
    ];
  };

  /*
   Cargo2nix provides many overrides by default, you can take inspiration from them:
   https://github.com/cargo2nix/cargo2nix/blob/master/overlay/overrides.nix

   You can have a complete list of the available options by looking at the overriden object, mkcrate:
   https://github.com/cargo2nix/cargo2nix/blob/master/overlay/mkcrate.nix
  */
  overrides = pkgs.rustBuilder.overrides.all ++ [
    /*
     [1] We need to alter Nix hardening to be able to statically compile: PIE,
     Position Independent Executables seems to be supported only on amd64. Having
     this flags set either make our executables crash or compile as dynamic on many platforms.
     In the following section codegenOpts, we reactive it for the supported targets
     (only amd64 curently) through the `-static-pie` flag. PIE is a feature used
     by ASLR, which helps mitigate security issues.
     Learn more about Nix Hardening: https://github.com/NixOS/nixpkgs/blob/master/pkgs/build-support/cc-wrapper/add-hardening.sh

     [2] We want to inject the git version while keeping the build deterministic.
     As we do not want to consider the .git folder as part of the input source,
     we ask the user (the CI often) to pass the value to Nix.
    */
    (pkgs.rustBuilder.rustLib.makeOverride {
      name = "garage_rpc";
      overrideAttrs = drv:
        /* [1] */ { hardeningDisable = [ "pie" ]; }
        //
        /* [2] */ (if git_version != null then {
          preConfigure = ''
            ${drv.preConfigure or ""}
            export GIT_VERSION="${git_version}"
          '';
        } else {});
    })

    /*
     We ship some parts of the code disabled by default by putting them behind a flag.
     It speeds up the compilation (when the feature is not required) and released crates have less dependency by default (less attack surface, disk space, etc.).
     But we want to ship these additional features when we release Garage.
     In the end, we chose to exclude all features from debug builds while putting (all of) them in the release builds.
     Currently, the only feature of Garage is kubernetes-discovery from the garage_rpc crate.
    */
    (pkgs.rustBuilder.rustLib.makeOverride {
      name = "garage_rpc";
      overrideArgs = old:
        {
          features = if release then [ "kubernetes-discovery" ] else [];
        };
    })
  ];

  packageFun = import ./Cargo.nix;

  /*
    We compile fully static binaries with musl to simplify deployment on most systems.
    When possible, we reactivate PIE hardening (see above).

    Also, if you set the RUSTFLAGS environment variable, the following parameters will
    be ignored.

    For more information on static builds, please refer to Rust's RFC 1721.
    https://rust-lang.github.io/rfcs/1721-crt-static.html#specifying-dynamicstatic-c-runtime-linkage
  */

  codegenOpts = {
   "armv6l-unknown-linux-musleabihf" = [ "target-feature=+crt-static" "link-arg=-static" ]; /* compile as dynamic with static-pie */
   "aarch64-unknown-linux-musl" = [ "target-feature=+crt-static" "link-arg=-static" ]; /* segfault with static-pie */
   "i686-unknown-linux-musl" = [ "target-feature=+crt-static" "link-arg=-static" ]; /* segfault with static-pie */
   "x86_64-unknown-linux-musl" = [ "target-feature=+crt-static" "link-arg=-static-pie" ];
  };

  /*
   The following definition is not elegant as we use a low level function of Cargo2nix
   that enables us to pass our custom rustChannel object. We need this low level definition
   to pass Nix's Rust toolchains instead of Mozilla's one.

   target is mandatory but must be kept to null to allow cargo2nix to set it to the appropriate value
   for each crate.
  */ 
  rustPkgs = pkgs.rustBuilder.makePackageSet {
    inherit packageFun rustChannel release codegenOpts;
    packageOverrides = overrides;
    target = null;

    buildRustPackages = pkgs.buildPackages.rustBuilder.makePackageSet {
      inherit rustChannel packageFun codegenOpts;
      packageOverrides = overrides;
      target = null;
    };
  };


in
  if compileMode == "test"
    then pkgs.symlinkJoin {
      name ="garage-tests";
      paths = builtins.map (key: rustPkgs.workspace.${key} { inherit compileMode; }) (builtins.attrNames rustPkgs.workspace);
    }
    else rustPkgs.workspace.garage { inherit compileMode; }
