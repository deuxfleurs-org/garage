{
  system ? builtins.currentSystem,
  crossSystem ? null,
  rust ? true,
  integration ? true,
  release ? true,
}:

with import ./nix/common.nix;

let
  pkgs = import pkgsSrc {
    inherit system crossSystem;
    overlays = [ cargo2nixOverlay rustOverlay ];
  };
  rustDist = pkgs.buildPackages.rust-bin.stable.latest.default;
  kaniko = (import ./nix/kaniko.nix) pkgs;

in

pkgs.mkShell {
  shellHook = ''
function to_s3 {
  aws \
      --endpoint-url https://garage.deuxfleurs.fr \
      --region garage \
    s3 cp \
      ./result/bin/garage \
      s3://garagehq.deuxfleurs.fr/_releases/''${DRONE_TAG:-$DRONE_COMMIT}/''${TARGET}/garage
}

function to_docker {
  executor  \
    --force \
    --customPlatform="''${DOCKER_PLATFORM}" \
    --destination "''${CONTAINER_NAME}:''${CONTAINER_TAG}" \
    --context dir://`pwd` \
    --verbosity=debug
}

function refresh_index {
  aws \
      --endpoint-url https://garage.deuxfleurs.fr \
      --region garage \
    s3 ls \
      --recursive \
      s3://garagehq.deuxfleurs.fr/_releases/ \
  > aws-list.txt

  nix-build nix/build_index.nix

  aws \
      --endpoint-url https://garage.deuxfleurs.fr \
      --region garage \
    s3 cp \
      --content-type "text/html" \
      result \
      s3://garagehq.deuxfleurs.fr/_releases.html
}
  '';

  nativeBuildInputs = 
   (if rust then [ rustDist (pkgs.callPackage cargo2nix {}).package ] else [])
   ++
   (if integration then [ pkgs.s3cmd pkgs.awscli2 pkgs.minio-client pkgs.rclone pkgs.socat pkgs.psmisc pkgs.which ] else [])
   ++
   (if release then [ pkgs.awscli2 kaniko ] else [])
   ;
}
