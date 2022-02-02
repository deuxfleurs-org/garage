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
   The following complexity should be abstracted by makePackageSet' (note the final quote).
   However its code uses deprecated features of rust-overlay that can lead to bug.
   Instead, we build our own rustChannel object with the recommended API of rust-overlay.
   */
  rustChannel = pkgs.rustPlatform.rust;

  overrides = pkgs.buildPackages.rustBuilder.overrides.all ++ [
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

  rustPkgs = pkgs.rustBuilder.makePackageSet {
    inherit packageFun rustChannel release;
    packageOverrides = overrides;

    buildRustPackages = pkgs.buildPackages.rustBuilder.makePackageSet {
      inherit rustChannel packageFun;
      packageOverrides = overrides;
    };

    localPatterns = [
      /*
       The way the default rules are written make think we match recursively, on full path, but the rules are misleading.
       In fact, the regex is only called on root elements of the crate (and not recursively).
       This behavior does not work well with our nested modules.
       We tried to build a "deny list" but negative lookup ahead are not supported on Nix.
       As a workaround, we have to register all our submodules in this allow list...
       */
       ''^(src|tests)''          # fixed default
       ''.*\.(rs|toml)$''        # fixed default
       ''^(crdt|replication|cli|helper|signature|common|ext)''   # our crate submodules
    ];
  };

in
  if compileMode == "test"
    then builtins.mapAttrs (name: value: rustPkgs.workspace.${name} { inherit compileMode; }) rustPkgs.workspace
    else rustPkgs.workspace.garage { inherit compileMode; }
