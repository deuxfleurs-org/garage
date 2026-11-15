{ system ? builtins.currentSystem, git_version ? null, }:

with import ./nix/common.nix;

let
  pkgs = import nixpkgs { };
  compile = import ./nix/compile.nix;
  webadmin = pkgs.callPackage ./nix/webadmin.nix {};

  build_release = target: (compile {
    inherit target system git_version nixpkgs webadmin;
    crane = flake.inputs.crane;
    rust-overlay = flake.inputs.rust-overlay;
    release = true;
  }).garage;

in {
  releasePackages = {
    amd64 = build_release "x86_64-unknown-linux-musl";
    i386 = build_release "i686-unknown-linux-musl";
    arm64 = build_release "aarch64-unknown-linux-musl";
    arm = build_release "armv6l-unknown-linux-musleabihf";
  };
  flakePackages = flake.packages.${system};
}
