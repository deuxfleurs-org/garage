# Repositories (Docker, Nix, Git...)

## Sourcehut

## Gitlab

## Gitea & Gogs


## Docker

Not yet compatible, follow [#103](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/103).

## Nix

Nix has no repository in its terminology: instead, it breaks down this concept in 2 parts: binary cache and channel.

**A channel** is a set of `.nix` definitions that generate definitions for all the software you want to serve.

Because we do not want all our clients to compile all these derivations by themselves,
we can compile them once and then serve them as part of our **binary cache**.

It is possible to use a **binary cache** without a channel, you only need to serve your nix definitions
through another support, like a git repository.

As a first step, we will need to create a bucket on Garage and enabling website access on it:

```bash
garage key new --name nix-key
garage bucket create nix.example.com
garage bucket allow nix.example.com --read --write --key nix-key
garage bucket website nix.example.com --allow
```

If you need more information about exposing buckets as websites on Garage,
check [Exposing buckets as websites](/cookbook/exposing_websites.html)
 and [Configuring a reverse proxy](/cookbook/reverse_proxy.html).

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
