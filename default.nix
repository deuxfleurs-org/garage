{
  system ? builtins.currentSystem,
  git_version ? null,
}:

with import ./nix/common.nix;

let 
  pkgs = import pkgsSrc { };
  compile = import ./nix/compile.nix;
  build_debug_and_release = (target: {
    debug = (compile { inherit target; release = false; }).workspace.garage { compileMode = "build"; }; 
    release = (compile { inherit target; release = true; }).workspace.garage { compileMode = "build"; }; 
  });
  test = (rustPkgs: pkgs.symlinkJoin {
    name ="garage-tests";
    paths = builtins.map (key: rustPkgs.workspace.${key} { compileMode = "test"; }) (builtins.attrNames rustPkgs.workspace);
  });

in {
  pkgs = {
    amd64 = build_debug_and_release "x86_64-unknown-linux-musl";
    i386 = build_debug_and_release "i686-unknown-linux-musl";
    arm64 = build_debug_and_release "aarch64-unknown-linux-musl";
    arm = build_debug_and_release "armv6l-unknown-linux-musleabihf";
  };
  test = {
    amd64 = test (compile { target = "x86_64-unknown-linux-musl"; });
  };
  clippy = {
    amd64 = (compile { compiler = "clippy"; }).workspace.garage { compileMode = "build"; } ;
  };
}
