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
  version = "0.2.2";

  nodejs = nodejs_24;
  src = fetchgit {
    url = "https://git.deuxfleurs.fr/Deuxfleurs/garage-webadmin";
    rev = "v${finalAttrs.version}";
    sha256 = "sha256-8Q4Pc1XKXK86kG2o1MAvgdmAzOpzIpq5Kg2iiJIAN2I=";
  };

  npmDepsHash = "sha256-G/ODrxE87F2aS3l0ypLIQC6hvgCFOmm5oo+aQNvJbnc=";

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
  
