{
  system ? builtins.currentSystem,
}:

with import ./nix/common.nix;

let
  pkgs = import pkgsSrc {
    inherit system;
    overlays = [ cargo2nixOverlay ];
  };
  kaniko = (import ./nix/kaniko.nix) pkgs;
  winscp = (import ./nix/winscp.nix) pkgs;

in
  {

    /* --- Rust Shell ---
     * Use it to compile Garage
     */
    rust = pkgs.mkShell {
      shellHook = ''
function refresh_toolchain {
  nix copy \
    --to 's3://nix?endpoint=garage.deuxfleurs.fr&region=garage&secret-key=/etc/nix/signing-key.sec' \
    $(nix-store -qR \
      $(nix-build --quiet --no-build-output --no-out-link nix/toolchain.nix))
}
      '';

      nativeBuildInputs = [
        #pkgs.rustPlatform.rust.rustc
        pkgs.rustPlatform.rust.cargo
        #pkgs.clippy
        pkgs.rustfmt
        #pkgs.perl
        #pkgs.protobuf
        #pkgs.pkg-config
        #pkgs.openssl
        pkgs.file
        #cargo2nix.packages.x86_64-linux.cargo2nix
      ];
    };

    /* --- Integration shell ---
     * Use it to test Garage with common S3 clients
     */
    integration = pkgs.mkShell {
      nativeBuildInputs = [
        winscp
        pkgs.s3cmd
        pkgs.awscli2
        pkgs.minio-client
        pkgs.rclone
        pkgs.socat
        pkgs.psmisc
        pkgs.which
        pkgs.openssl
        pkgs.curl
        pkgs.jq
      ];
    };

    /* --- Release shell ---
     * A shell built to make releasing easier
     */
    release = pkgs.mkShell {
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
      result/share/_releases.json \
      s3://garagehq.deuxfleurs.fr/

  aws \
      --endpoint-url https://garage.deuxfleurs.fr \
      --region garage \
    s3 cp \
      result/share/_releases.html \
      s3://garagehq.deuxfleurs.fr/
}
        '';
      nativeBuildInputs = [
        pkgs.awscli2
        kaniko 
      ];
    };
  }


