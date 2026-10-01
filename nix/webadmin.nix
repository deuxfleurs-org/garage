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
    rev = "v${finalAttrs.version}";
    sha256 = "sha256-o/Ww3dEE0ofKqZrVugnM7FJLdzOO+cTWHASILELaJZc=";
  };

  npmDepsHash = "sha256-EBXekK7lglNWDTR294C9/McWWSVd4x68b/h7NBHnmtg=";

  buildPhase = ''
    npx vite build --base=/ui/
  '';
  installPhase = ''
    cp -rv dist $out
  '';

  VITE_API_HOST = "/";
  VITE_FORCE_API_HOST = "true";

  meta = {
    description = "Web admin UI for Garage";
    homepage = "https://git.deuxfleurs.fr/Deuxfleurs/garage-webadmin";
  };
})
  
