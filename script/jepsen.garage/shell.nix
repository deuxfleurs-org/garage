{ pkgs ? import <nixpkgs> { } }:
pkgs.mkShell {
  nativeBuildInputs = with pkgs; [
    leiningen
    vagrant
    gnuplot
  ];
}
