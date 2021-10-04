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
    overlays = [ cargo2nixOverlay rustOverlay ];
  };
  rustDist = pkgs.buildPackages.rust-bin.stable.latest.default;

  /*
   The following complexity should be abstracted by makePackageSet' (note the final quote).
   However its code uses deprecated features of rust-overlay that can lead to bug.
   Instead, we build our own rustChannel object with the recommended API of rust-overlay.
   */
  rustChannel = rustDist // {
    cargo = rustDist;
    rustc = rustDist.override {
      targets = [
        (pkgs.rustBuilder.rustLib.realHostTriple pkgs.stdenv.targetPlatform)
      ];
    };
  };

  overrides = pkgs.buildPackages.rustBuilder.overrides.all ++ [
    (pkgs.rustBuilder.rustLib.makeOverride {
      name = "garage";
      overrideAttrs = drv: if git_version != null then {
        preConfigure = ''
          ${drv.preConfigure or ""}
          export GIT_VERSION="${git_version}"
        '';
      } else {};
    })
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
       ''^(crdt|replication)''   # our crate submodules
    ];
  };

in
  if compileMode == "test"
    then builtins.mapAttrs (name: value: rustPkgs.workspace.${name} { inherit compileMode; }) rustPkgs.workspace
    else rustPkgs.workspace.garage { inherit compileMode; }
