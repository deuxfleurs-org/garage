pkgs:

pkgs.stdenv.mkDerivation rec {
  pname = "winscp";
  version = "5.19.6";

  src = pkgs.fetchzip {
    url = "https://winscp.net/download/WinSCP-${version}-Portable.zip";
    sha256 = "sha256-8+6JuT0b1fFQ6etaFTMSjIKvDGzmJoHAuByXiqCBzu0=";
    stripRoot = false;
  };

  buildPhase = ''
        cat > winscp <<EOF
    #!${pkgs.bash}/bin/bash

    WINEDEBUG=-all
    ${pkgs.winePackages.minimal}/bin/wine $out/opt/WinSCP.com
    EOF
  '';

  installPhase = ''
    mkdir -p $out/{bin,opt}
    cp {WinSCP.com,WinSCP.exe} $out/opt
    cp winscp $out/bin
    chmod +x $out/bin/winscp
  '';
}
