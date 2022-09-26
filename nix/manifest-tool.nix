pkgs:
pkgs.buildGoModule rec {
  pname = "manifest-tool";
  version = "2.0.5";

  src = pkgs.fetchFromGitHub {
    owner = "estesp";
    repo = "manifest-tool";
    rev = "v${version}";
    sha256 = "hjCGKnE0yrlnF/VIzOwcDzmQX3Wft+21KCny/opqdLg=";
  } + "/v2";

  vendorSha256 = null;

  checkPhase = "true";

  meta = with pkgs.lib; {
    description = "Command line tool to create and query container image manifest list/indexes";
    homepage = "https://github.com/estesp/manifest-tool";
    license = licenses.asl20;
    platforms = platforms.linux;
  };
}
