{ system ? builtins.currentSystem, }:

with import ./nix/common.nix;

let
  pkgs = import nixpkgs {
    inherit system;
  };
  winscp = (import ./nix/winscp.nix) pkgs;
in
{
  # --- Dev shell inherited from flake.nix ---
  devShell = devShells.default;
  devShellFull = devShells.full;

  # --- Continuous integration shell ---
  # The shell used for all CI jobs (along with devShell)
  ci = pkgs.mkShell {
    nativeBuildInputs = with pkgs; [
      winscp

      kaniko
      manifest-tool
      awscli2
      file
      s3cmd
      minio-client
      rclone
      socat
      psmisc
      which
      openssl
      curl
      jq
    ];
    shellHook = ''
      function to_s3 {
        aws \
            --endpoint-url https://garage.deuxfleurs.fr \
            --region garage \
          s3 cp \
            ./result/bin/garage \
            s3://garagehq.deuxfleurs.fr/_releases/''${CI_COMMIT_TAG:-$CI_COMMIT_SHA}/''${TARGET}/garage
      }

      function to_docker {
        executor  \
          --force \
          --customPlatform="$(echo "''${DOCKER_PLATFORM}" | sed 's/i386/386/')" \
          --destination "$(echo "''${CONTAINER_NAME}" | sed 's/i386/386/'):''${CONTAINER_TAG}" \
          --context dir://`pwd` \
          --verbosity=debug
      }

      function multiarch_docker {
              manifest-tool push from-spec <(cat <<EOF
      image: dxflrs/garage:''${CONTAINER_TAG}
      manifests:
        -
          image: dxflrs/arm64_garage:''${CONTAINER_TAG}
          platform:
            architecture: arm64
            os: linux
        -
          image: dxflrs/amd64_garage:''${CONTAINER_TAG}
          platform:
            architecture: amd64
            os: linux
        -
          image: dxflrs/386_garage:''${CONTAINER_TAG}
          platform:
            architecture: 386
            os: linux
        -
          image: dxflrs/arm_garage:''${CONTAINER_TAG}
          platform:
            architecture: arm
            os: linux
      EOF
              )
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

  };

  # --- Cache shell ---
  # A shell for refreshing caches
  cache = pkgs.mkShell {
    shellHook = ''
      function refresh_cache {
        pass show deuxfleurs/nix_priv_key > /tmp/nix-signing-key.sec
        for attr in pkgs.amd64.debug test.amd64 pkgs.{amd64,i386,arm,arm64}.release; do
          echo "Updating cache for ''${attr}"
          nix copy -j8 \
            --to 's3://nix?endpoint=garage.deuxfleurs.fr&region=garage&secret-key=/tmp/nix-signing-key.sec' \
            $(nix path-info ''${attr} --file default.nix --derivation --recursive | sed 's/\.drv$/.drv^*/')

        done
        rm /tmp/nix-signing-key.sec
      }
    '';
  };
}

