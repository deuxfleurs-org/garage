pkgs:
pkgs.buildGoModule rec {
  pname = "kaniko";
  version = "1.6.0";

  src = pkgs.fetchFromGitHub {
    owner = "GoogleContainerTools";
    repo = "kaniko";
    rev = "v${version}";
    sha256 = "1fnclr556avxay6pvgw5ya3xbxfnf2gv4njq2hr4fd6fcjyslq5h";
  };

  vendorSha256 = null;

  checkPhase = "true";

  meta = with pkgs.lib; {
    description = "kaniko is a tool to build container images from a Dockerfile, inside a container or Kubernetes cluster.";
    homepage = "https://github.com/GoogleContainerTools/kaniko";
    license = licenses.asl20;
    platforms = platforms.linux;
  };
}
