{ pkgs ? import <nixpkgs> {
  overlays = [
    (self: super: {
      jdk = super.jdk11;
      jre = super.jre11;
    })
  ];
} }:
pkgs.mkShell {
  nativeBuildInputs = with pkgs; [
    leiningen
    jdk
    jna
    vagrant
    gnuplot
    graphviz
  ];
}
