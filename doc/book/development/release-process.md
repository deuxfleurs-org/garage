+++
title = "Release process"
weight = 15
+++

Before releasing a new version of Garage, our code pass through a succession of checks and transformations.
We define them as our release process.

## Trigger and classify a release

While we run some tests on every commits, we do not make a release for all of them.

A release can be triggered manually by "promoting" a successful build.
Otherwise, every weeks, a release build is triggered on the `main` branch.

If the build is from a tag following the regex: `v[0-9]+\.[0-9]+\.[0-9]+`, it will be listed as stable.
If it is a tag but with a different format, it will be listed as Extra.
Otherwise, if it is a commit, it will be listed as development.
This logic is defined in `nix/build_index.nix`.

## Testing

For each commit, we first pass the code to a formatter (rustfmt) and a linter (clippy).
Then we try to build it in debug mode and run both unit tests and our integration tests.

Additionnaly, when releasing, our integration tests are run on the release build for amd64 and i686.

## Generated Artifacts

We generate the following binary artifacts for now:
  - **architecture**: amd64, i686, aarch64, armv6
  - **os**: linux
  - **format**: static binary, docker container

Additionnaly we also build two web pages and one JSON document:
  - the documentation (this website)
  - [the release page](https://garagehq.deuxfleurs.fr/_releases.html)
  - [the release list in JSON format](https://garagehq.deuxfleurs.fr/_releases.json)

We publish the static binaries on our own garage cluster (you can access them through the releases page)
and the docker containers on Docker Hub.

## Automation

We automated our release process with Nix and Drone to make it more reliable.
Here we describe how we have done in case you want to debug or improve it.

### Caching build steps

To speed up the CI, we use the caching feature provided by Nix.

You can benefit from it by using our provided `nix.conf` as recommended or by simply adding the following lines to your file:

```toml
substituters = https://cache.nixos.org https://nix.web.deuxfleurs.fr
trusted-public-keys = cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY= nix.web.deuxfleurs.fr:eTGL6kvaQn6cDR/F9lDYUIP9nCVR/kkshYfLDJf1yKs=
```

Sending to the cache is done through `nix copy`, for example:

```bash
nix copy --to 's3://nix?endpoint=garage.deuxfleurs.fr&region=garage&secret-key=/etc/nix/signing-key.sec' result
```

*Note that you need the signing key. In our case, it is stored as a secret in Drone.*

The previous command will only send the built packet and not its dependencies.
To send its dependency, a tool named `nix-copy-closure` has been created but it is not compatible with the S3 protocol.

Instead, you can use the following commands to list all the runtime dependencies:

```bash
nix copy \
  --to 's3://nix?endpoint=garage.deuxfleurs.fr&region=garage&secret-key=/etc/nix/signing-key.sec' \
  $(nix-store -qR result/)
```

*We could also write this expression with xargs but this tool is not available in our container.*

But in certain cases, we want to cache compile time dependencies also.
For example, the Nix project does not provide binaries for cross compiling to i686 and thus we need to compile gcc on our own.
We do not want to compile gcc each time, so even if it is a compile time dependency, we want to cache it.

This time, the command is a bit more involved:

```bash
nix copy --to \
  's3://nix?endpoint=garage.deuxfleurs.fr&region=garage&secret-key=/etc/nix/signing-key.sec' \
   $(nix-store -qR --include-outputs \
     $(nix-instantiate))
```

This is the command we use in our CI as we expect the final binary to change, so we mainly focus on
caching our development dependencies.

*Currently there is no automatic garbage collection of the cache: we should monitor its growth.
Hopefully, we can erase it totally without breaking any build, the next build will only be slower.*

In practise, we concluded that we do not want to cache all the compilation dependencies.
Instead, we want to cache the toolchain we use to build Garage each time we change it.
So we removed from Drone any automatic update of the cache and instead handle them manually with:

```
source ~/.awsrc
nix-shell --run 'refresh_toolchain'
```

Internally, it will run `nix-build` on  `nix/toolchain.nix` and send the output plus its depedencies to the cache.

To erase the cache:

```
mc rm --recursive --force 'garage/nix/'
```

### Publishing Garage

We defined our publishing logic in Nix, mostly as shell hooks.
You can inspect them in `shell.nix` to see exactly how.
Here, we will give a quick explanation on how to use them to manually publish a release.

Supposing you just have built garage as follow:

```bash
nix-build --arg release true
```

To publish a static binary in `result/bin` on garagehq, run:

```bash
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx
export DRONE_TAG=handcrafted-1.0.0 # or DRONE_COMMIT
export TARGET=x86_64-unknown-linux-musl

nix-shell --run to_s3
```

To create and publish a docker container, run:

```bash
export DOCKER_AUTH='{ "auths": { "https://index.docker.io/v1/": { "auth": "xxxx" }}}'
export DOCKER_PLATFORM='linux/amd64' # check GOARCH and GOOS from golang.org
export CONTAINER_NAME='me/amd64_garage'
export CONTAINER_TAG='handcrafted-1.0.0'

nix-shell --run to_docker
```

To rebuild the release page, run:
```bash
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx

nix-shell --run refresh_index
```

If you want to compile for different architectures, you will need to repeat all these commands for each architecture.

**In practise, and except for debugging, you will never directly run these commands. Release is handled by drone**

### Drone

Our instance is available at [https://drone.deuxfleurs.fr](https://drone.deuxfleurs.fr).  
You need an account on [https://git.deuxfleurs.fr](https://git.deuxfleurs.fr) to use it.

**Drone CLI** - Drone has a CLI tool to interact with.
It can be downloaded from its Github [release page](https://github.com/drone/drone-cli/releases).

To communicate with our instance, you must setup some environment variables.
You can get them from your [Account Settings](https://drone.deuxfleurs.fr/account).

To make drone easier to use, you could create a `~/.dronerc` that you could source each time you want to use it.

```
export DRONE_SERVER=https://drone.deuxfleurs.fr
export DRONE_TOKEN=xxx
drone info
```

The CLI tool is very self-discoverable, just append `--help` to each subcommands.
Start with:

```bash
drone --help
```

**.drone.yml** - The builds steps are defined in `.drone.yml`.  
You can not edit this file without resigning it.

To sign it, you must be a maintainer and then run:

```bash
drone sign --save Deuxfleurs/garage
```

Looking at the file, you will see that most of the commands are `nix-shell` and `nix-build` commands with various parameters.


