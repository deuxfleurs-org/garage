{
  description = "Garage, an S3-compatible distributed object store for self-hosted deployments";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/baed728abe983508cabc99d05cccc164fe748744";
  inputs.cargo2nix = {
    # As of 2022-10-18: two small patches over unstable branch, one for clippy and one to fix feature detection
    url = "github:Alexis211/cargo2nix/505caa32110d42ee03bd68b47031142eff9c827b";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, cargo2nix }: let
    git_version = self.lastModifiedDate;
    compile = import ./nix/compile.nix;
    forAllSystems = nixpkgs.lib.genAttrs nixpkgs.lib.systems.flakeExposed;
  in
  {
    packages = forAllSystems (system: {
      default = (compile {
        inherit system git_version;
        pkgsSrc = nixpkgs;
        cargo2nixOverlay = cargo2nix.overlays.default;
        release = true;
      }).workspace.garage {
        compileMode = "build";
      };
    });
  };
}
