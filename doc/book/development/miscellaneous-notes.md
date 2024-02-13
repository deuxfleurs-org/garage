+++
title = "Miscellaneous notes"
weight = 20
+++

## Quirks about cargo2nix/rust in Nix

If you use submodules in your crate (like `crdt` and `replication` in `garage_table`), you must list them in `default.nix`

The Windows target does not work. it might be solvable through [overrides](https://github.com/cargo2nix/cargo2nix/blob/master/overlay/overrides.nix). Indeed, we pass `x86_64-pc-windows-gnu` but mingw need `x86_64-w64-mingw32`

We have a simple [PR on cargo2nix](https://github.com/cargo2nix/cargo2nix/pull/201) that fixes critical bugs but the project does not seem very active currently. We must use [my patched version of cargo2nix](https://github.com/superboum/cargo2nix) to enable i686 and armv6l compilation. We might need to contribute to cargo2nix in the future.


## Nix

Nix has no armv7 + musl toolchains but armv7l is backward compatible with armv6l.

```bash
cat > $HOME/.awsrc <<EOF
export AWS_ACCESS_KEY_ID="xxx"
export AWS_SECRET_ACCESS_KEY="xxx"
EOF

# source each time you want to send on the cache
source ~/.awsrc

# copy garage build dependencies (and not only the output)
nix-build
nix-store -qR --include-outputs $(nix-instantiate default.nix) 
  | xargs nix copy --to 's3://nix?endpoint=garage.deuxfleurs.fr&region=garage'
  
# copy shell dependencies
nix-build shell.nix -A inputDerivation
nix copy $(nix-store -qR result/) --to 's3://nix?endpoint=garage.deuxfleurs.fr&region=garage' 
```

More example of nix-copy

```
# nix-build produces a result/ symlink
nix copy result/ --to 's3://nix?endpoint=garage.deuxfleurs.fr&region=garage'

# alternative ways to use nix copy
nix copy nixpkgs.garage --to ...
nix copy /nix/store/3rbb9qsc2w6xl5xccz5ncfhy33nzv3dp-crate-garage-0.3.0 --to ...
```
    

Clear the cache:

```bash
mc rm --recursive --force garage/nix/
```

---

A desirable `nix.conf` for a consumer:

```toml
substituters = https://cache.nixos.org https://nix.web.deuxfleurs.fr
trusted-public-keys = cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY= nix.web.deuxfleurs.fr:eTGL6kvaQn6cDR/F9lDYUIP9nCVR/kkshYfLDJf1yKs=
```

And now, whenever you run a command like:

```
nix-shell
nix-build
```

Our cache will be checked.

### Some references about Nix


 - https://doc.rust-lang.org/nightly/rustc/platform-support.html
 - https://nix.dev/tutorials/cross-compilation
 - https://nixos.org/manual/nix/unstable/package-management/s3-substituter.html
 - https://fzakaria.com/2020/09/28/nix-copy-closure-your-nix-shell.html
 - http://www.lpenz.org/articles/nixchannel/index.html


## Woodpecker

Woodpecker can do parallelism both at the step and the pipeline level. At the step level, parallelism is restricted to the same runner.

## Building Docker containers

We were:
  - Unable to use the official Docker plugin because
    - it requires to mount docker socket in the container but it is not recommended
    - you cant set the platform when building
  - Unable to use buildah because it needs `CLONE_USERNS` capability
  - Unable to use the kaniko plugin for Drone as we can't set the target platform
  - Unable to use the kaniko container provided by Google as we can't run arbitrary logic: we need to put our secret in .docker/config.json.

Finally we chose to build kaniko through nix and use it in a `nix-shell`.
We then switched to using kaniko from nixpkgs when it was packaged.
