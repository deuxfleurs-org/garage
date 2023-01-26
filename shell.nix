{ system ? builtins.currentSystem, }:

with import ./nix/common.nix;

let
  pkgs = import pkgsSrc {
    inherit system;
    overlays = [ cargo2nixOverlay ];
  };
  kaniko = (import ./nix/kaniko.nix) pkgs;
  manifest-tool = (import ./nix/manifest-tool.nix) pkgs;
  winscp = (import ./nix/winscp.nix) pkgs;

in {
  # --- Rust Shell ---
  # Use it to compile Garage
  rust = pkgs.mkShell {
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

  # --- Integration shell ---
  # Use it to test Garage with common S3 clients
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

  # --- Release shell ---
  # A shell built to make releasing easier
  release = pkgs.mkShell {
    shellHook = ''
      function refresh_toolchain {
        pass show deuxfleurs/nix_priv_key > /tmp/nix-signing-key.sec
        nix copy \
          --to 's3://nix?endpoint=garage.deuxfleurs.fr&region=garage&secret-key=/tmp/nix-signing-key.sec' \
          $(nix-store -qR \
            $(nix-build --no-build-output --no-out-link nix/toolchain.nix))
        rm /tmp/nix-signing-key.sec
      }

      function refresh_cache {
        pass show deuxfleurs/nix_priv_key > /tmp/nix-signing-key.sec
        for attr in clippy.amd64 test.amd64 pkgs.{amd64,i386,arm,arm64}.{debug,release}; do
          echo "Updating cache for ''${attr}"
          derivation=$(nix-instantiate --attr ''${attr})
          nix copy -j8 \
            --to 's3://nix?endpoint=garage.deuxfleurs.fr&region=garage&secret-key=/tmp/nix-signing-key.sec' \
            $(nix-store -qR ''${derivation%\!bin})
        done
        rm /tmp/nix-signing-key.sec
      }

      function refresh_flake_cache {
        pass show deuxfleurs/nix_priv_key > /tmp/nix-signing-key.sec
        for attr in packages.x86_64-linux.default devShell.x86_64-linux; do
          echo "Updating cache for ''${attr}"
          derivation=$(nix path-info --derivation ".#''${attr}")
          nix copy -j8 \
            --to 's3://nix?endpoint=garage.deuxfleurs.fr&region=garage&secret-key=/tmp/nix-signing-key.sec' \
            $(nix-store -qR ''${derivation})
        done
        rm /tmp/nix-signing-key.sec
      }

      function to_s3 {
        aws \
            --endpoint-url https://garage.deuxfleurs.fr \
            --region garage \
          s3 cp \
            ./result-bin/bin/garage \
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
    nativeBuildInputs = [ pkgs.awscli2 kaniko manifest-tool ];
  };
}

