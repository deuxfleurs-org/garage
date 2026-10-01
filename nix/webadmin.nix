{
  lib,
  nodejs_24,
  fetchgit,
  fetchNpmDeps,
  importNpmLock,
  buildNpmPackage,
}:
buildNpmPackage (finalAttrs: {
  pname = "garage-webadmin";
  version = "0.1.0";

  nodejs = nodejs_24;
  src = fetchgit {
    url = "https://git.deuxfleurs.fr/Deuxfleurs/garage-webadmin";
    # rev = "v${finalAttrs.version}";
    # temporary: use unreleased version
    rev = "a2b75ef6bb99d67df1d20685733a1f44b98208dd";
    sha256 = "sha256-CMHAQ4Vb7hEOWd+XFm9II4cuaSIxuE571Iu6Io8wz7I=";
  };

  npmDepsHash = "sha256-Fp6e/p3lzryaQrLV6WS8AuQ7rtU/+L1CGo6Z9OEUfBE=";

  buildPhase = ''
    npm run build:integrated
  '';
  installPhase = ''
    cp -rv dist $out
  '';

  meta = {
    description = "Web admin UI for Garage";
    homepage = "https://git.deuxfleurs.fr/Deuxfleurs/garage-webadmin";
  };
})
  
