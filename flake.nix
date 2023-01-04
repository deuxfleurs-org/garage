{
  description = "Garage, an S3-compatible distributed object store for self-hosted deployments";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/a3073c49bc0163fea6a121c276f526837672b555";
  inputs.cargo2nix = {
    # As of 2022-10-18: two small patches over unstable branch, one for clippy and one to fix feature detection
    url = "github:Alexis211/cargo2nix/a7a61179b66054904ef6a195d8da736eaaa06c36";
    inputs.nixpkgs.follows = "nixpkgs";
  };
  inputs.flake-utils.url = "github:numtide/flake-utils";

  outputs = { self, nixpkgs, cargo2nix, flake-utils }:
    let
      git_version = self.lastModifiedDate;
      compile = import ./nix/compile.nix;
    in flake-utils.lib.eachDefaultSystem (system:
      let pkgs = nixpkgs.legacyPackages.${system};
      in {
        packages = {
          default = (compile {
            inherit system git_version;
            pkgsSrc = nixpkgs;
            cargo2nixOverlay = cargo2nix.overlays.default;
            release = true;
          }).workspace.garage { compileMode = "build"; };
        };
        devShell = ((compile {
          inherit system git_version;
          pkgsSrc = nixpkgs;
          cargo2nixOverlay = cargo2nix.overlays.default;
          release = false;
        }).workspaceShell {
          packages = [ pkgs.rustfmt ];
        });
      });
}
