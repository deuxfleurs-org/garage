{ system ? builtins.currentSystem, }:

with import ./common.nix;

let
  pkgsHost = import pkgsSrc { };
  kaniko = (import ./kaniko.nix) pkgsHost;
  winscp = (import ./winscp.nix) pkgsHost;
  manifestTool = (import ./manifest-tool.nix) pkgsHost;
in [ kaniko winscp manifestTool ]

