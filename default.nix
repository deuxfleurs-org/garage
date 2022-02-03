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
  pkgs = import pkgsSrc {
    inherit system crossSystem;
    overlays = [ cargo2nixOverlay ];
  };

  /*
   Cargo2nix is built for rustOverlay which installs Rust from Mozilla releases.
   We want our own Rust to avoir incompatibilities, like we had with musl 1.2.0.
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

  overrides = pkgs.rustBuilder.overrides.all ++ [
    /*
     We want to inject the git version while keeping the build deterministic.
     As we do not want to consider the .git folder as part of the input source,
     we ask the user (the CI often) to pass the value to Nix.
    */
    (pkgs.rustBuilder.rustLib.makeOverride {
      name = "garage";
      overrideAttrs = drv: if git_version != null then {
        preConfigure = ''
          ${drv.preConfigure or ""}
          export GIT_VERSION="${git_version}"
        '';
      } else {};
    })

    /*
     On a sandbox pure NixOS environment, /usr/bin/file is not available.
     This is a known problem: https://github.com/NixOS/nixpkgs/issues/98440
     We simply patch the file as suggested
    */
    /*(pkgs.rustBuilder.rustLib.makeOverride {
      name = "libsodium-sys";
      overrideAttrs = drv: {
        preConfigure = ''
          ${drv.preConfigure or ""}
          sed -i 's,/usr/bin/file,${file}/bin/file,g' ./configure
        '';
      }
    })*/
  ];

  packageFun = import ./Cargo.nix;

  /*
   The following definition is not elegant as we use a low level function of Cargo2nix
   that enables us to pass our custom rustChannel object
  */ 
  rustPkgs = pkgs.rustBuilder.makePackageSet {
    inherit packageFun rustChannel release;
    packageOverrides = overrides;
    target = null; /* we set target to null because we want that cargo2nix computes it automatically */

    buildRustPackages = pkgs.buildPackages.rustBuilder.makePackageSet {
      inherit rustChannel packageFun;
      packageOverrides = overrides;
      target = null; /* we set target to null because we want that cargo2nix computes it automatically */ 
    };
  };


in
  if compileMode == "test"
    then pkgs.symlinkJoin {
      name ="garage-tests";
      paths = builtins.map (key: rustPkgs.workspace.${key} { inherit compileMode; }) (builtins.attrNames rustPkgs.workspace);
    }
    else rustPkgs.workspace.garage { inherit compileMode; }
