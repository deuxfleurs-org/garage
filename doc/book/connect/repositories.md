+++
title = "Repositories (Docker, Nix, Git...)"
weight = 15
+++

Whether you need to store and serve binary packages or source code, you may want to deploy a tool referred as a repository or registry.
Garage can also help you serve this content.

| Name | Status | Note |
|------|--------|------|
| [Gitea](#gitea)     | ✅       |   |
| [Docker](#docker)     | ✅        | Requires garage >= v0.6.0   |
| [Nix](#nix)     | ✅        |     |
| [Gitlab](#gitlab)     |  ❓        |  Not yet tested    |



## Gitea

You can use Garage with Gitea to store your [git LFS](https://git-lfs.github.com/) data, your users' avatar, and their attachments.
You can configure a different target for each data type (check `[lfs]` and `[attachment]` sections of the Gitea documentation) and you can provide a default one through the `[storage]` section.

Let's start by creating a key and a bucket (your key id and secret will be needed later, keep them somewhere):

```bash
garage key create gitea-key
garage bucket create gitea
garage bucket allow gitea --read --write --key gitea-key
```

Then you can edit your configuration (by default `/etc/gitea/conf/app.ini`):

```ini
[storage]
STORAGE_TYPE=minio
MINIO_ENDPOINT=localhost:3900
MINIO_ACCESS_KEY_ID=GKxxx
MINIO_SECRET_ACCESS_KEY=xxxx
MINIO_BUCKET=gitea
MINIO_LOCATION=garage
MINIO_USE_SSL=false
```

You can also pass this configuration through environment variables:

```bash
GITEA__storage__STORAGE_TYPE=minio
GITEA__storage__MINIO_ENDPOINT=localhost:3900
GITEA__storage__MINIO_ACCESS_KEY_ID=GKxxx
GITEA__storage__MINIO_SECRET_ACCESS_KEY=xxxx
GITEA__storage__MINIO_BUCKET=gitea
GITEA__storage__MINIO_LOCATION=garage
GITEA__storage__MINIO_USE_SSL=false
```

Then restart your gitea instance and try to upload a custom avatar.
If it worked, you should see some content in your gitea bucket (you must configure your `aws` command before):

```
$ aws s3 ls s3://gitea/avatars/
2021-11-10 12:35:47     190034 616ba79ae2b84f565c33d72c2ec50861
```


*External link:* [Gitea Documentation > Configuration Cheat Sheet](https://docs.gitea.io/en-us/config-cheat-sheet/)

## Docker

Create a bucket and a key for your docker registry, then create `config.yml` with the following content:

```yml
version: 0.1
http:
  addr: 0.0.0.0:5000
  secret: asecretforlocaldevelopment
  debug:
    addr: localhost:5001
storage:
  s3:
    accesskey: GKxxxx
    secretkey: yyyyy
    region: garage
    regionendpoint: http://localhost:3900
    bucket: docker
    secure: false
    v4auth: true
    rootdirectory: /
```

Replace the `accesskey`, `secretkey`, `bucket`, `regionendpoint` and `secure` values by the one fitting your deployment.

Then simply run the docker registry:

```bash
docker run \
  --net=host \
  -v `pwd`/config.yml:/etc/docker/registry/config.yml \
  registry:2
```

*We started a plain text registry but docker clients require encrypted registries. You must either [setup TLS](https://docs.docker.com/registry/deploying/#run-an-externally-accessible-registry) on your registry or add `--insecure-registry=localhost:5000` to your docker daemon parameters.*


*External link:* [Docker Documentation > Registry storage drivers > S3 storage driver](https://docs.docker.com/registry/storage-drivers/s3/)

## Nix

Nix has no repository in its terminology: instead, it breaks down this concept in 2 parts: binary cache and channel.

**A channel** is a set of `.nix` definitions that generate definitions for all the software you want to serve.

Because we do not want all our clients to compile all these derivations by themselves,
we can compile them once and then serve them as part of our **binary cache**.

It is possible to use a **binary cache** without a channel, you only need to serve your nix definitions
through another support, like a git repository.

As a first step, we will need to create a bucket on Garage and enabling website access on it:

```bash
garage key create nix-key
garage bucket create nix.example.com
garage bucket allow nix.example.com --read --write --key nix-key
garage bucket website nix.example.com --allow
```

If you need more information about exposing buckets as websites on Garage,
check [Exposing buckets as websites](@/documentation/cookbook/exposing-websites.md)
 and [Configuring a reverse proxy](@/documentation/cookbook/reverse-proxy.md).

Next, we want to check that our bucket works:

```bash
echo nix repo > /tmp/index.html
mc cp /tmp/index.html garage/nix/
rm /tmp/index.html

curl https://nix.example.com
# output: nix repo
```

### Binary cache

To serve binaries as part of your cache, you need to sign them with a key specific to nix.
You can generate the keypair as follow: 

```bash
nix-store --generate-binary-cache-key <name> cache-priv-key.pem cache-pub-key.pem
```

You can then manually sign the packages of your store with the following command:

```bash
nix sign-paths --all -k cache-priv-key.pem
```

Setting a key in `nix.conf` will do the signature at build time automatically without additional commands.
Edit the `nix.conf` of your builder:

```toml
secret-key-files = /etc/nix/cache-priv-key.pem
```

Now that your content is signed, you can copy a derivation to your cache.
For example, if you want to copy a specific derivation of your store:

```bash
nix copy /nix/store/wadmyilr414n7bimxysbny876i2vlm5r-bash-5.1-p8 --to 's3://nix?endpoint=garage.example.com&region=garage'
```

*Note that if you have not signed your packages, you can append to the end of your S3 URL `&secret-key=/etc/nix/cache-priv-key.pem`.*

Sometimes you don't want to hardcode this store path in your script.
Let suppose that you are working on a codebase that you build with `nix-build`, you can then run:

```bash
nix copy $(nix-build) --to 's3://nix?endpoint=garage.example.com&region=garage'
```

*This command works because the only thing that `nix-build` outputs on stdout is the paths of the built derivations in your nix store.*

You can include your derivation dependencies:

```bash
nix copy $(nix-store -qR $(nix-build)) --to 's3://nix?endpoint=garage.example.com&region=garage'
```

Now, your binary cache stores your derivation and all its dependencies.
Just inform your users that they must update their `nix.conf` file with the following lines:

```toml
substituters = https://cache.nixos.org https://nix.example.com
trusted-public-keys = cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY= nix.example.com:eTGL6kvaQn6cDR/F9lDYUIP9nCVR/kkshYfLDJf1yKs=
```

*You must re-add cache.nixorg.org because redeclaring these keys override the previous configuration instead of extending it.*

Now, when your clients will run `nix-build` or any command that generates a derivation for which a hash is already present
on the binary cache, the client will download the result from the cache instead of compiling it, saving lot of time and CPU!


### Channels

Channels additionnaly serve Nix definitions, ie. a `.nix` file referencing
all the derivations you want to serve.

## Gitlab

*External link:* [Gitlab Documentation > Object storage](https://docs.gitlab.com/ee/administration/object_storage.html)


