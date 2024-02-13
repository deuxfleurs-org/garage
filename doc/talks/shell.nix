{ pkgs ? import <nixpkgs> { } }:
let
  latex = (pkgs.texlive.combine {
    inherit (pkgs.texlive)
    scheme-basic 
    beamer amsmath mathtools breqn
    environ 
    multirow graphics import adjustbox tabu vwcol stmaryrd ulem ragged2e textpos
    dvisvgm dvipng wrapfig hyperref capt-of;
  });
in pkgs.mkShell { nativeBuildInputs = [ pkgs.gnumake latex ]; }

