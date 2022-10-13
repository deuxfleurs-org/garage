{
  system ? builtins.currentSystem,
  git_version ? null,
}:

with import ./nix/common.nix;

let
  pkgs = import pkgsSrc { };
  compile = import ./nix/compile.nix;

  build_debug_and_release = (target: {
    debug = (compile {
      inherit target git_version;
      release = false;
    }).workspace.garage {
      compileMode = "build";
    };

    release = (compile {
      inherit target git_version;
      release = true;
    }).workspace.garage {
      compileMode = "build";
    };
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
    amd64 = test (compile {
      inherit git_version;
      target = "x86_64-unknown-linux-musl";
      features = [
        "garage/bundled-libs"
        "garage/sled"
        "garage/k2v"
        "garage/lmdb"
        "garage/sqlite"
      ];
    });
  };
  clippy = {
    amd64 = (compile {
      inherit git_version;
      target = "x86_64-unknown-linux-musl";
      compiler = "clippy";
    }).workspace.garage {} ;
  };
}
