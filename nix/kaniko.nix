pkgs:
pkgs.buildGoModule rec {
  pname = "kaniko";
  version = "1.9.2";

  src = pkgs.fetchFromGitHub {
    owner = "GoogleContainerTools";
    repo = "kaniko";
    rev = "v${version}";
    sha256 = "dXQ0/o1qISv+sjNVIpfF85bkbM9sGOGwqVbWZpMWfMY=";
  };

  vendorSha256 = null;

  checkPhase = "true";

  meta = with pkgs.lib; {
    description =
      "kaniko is a tool to build container images from a Dockerfile, inside a container or Kubernetes cluster.";
    homepage = "https://github.com/GoogleContainerTools/kaniko";
    license = licenses.asl20;
    platforms = platforms.linux;
  };
}
