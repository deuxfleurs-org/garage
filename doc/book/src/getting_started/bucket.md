# Create buckets and keys

*We use a command named `garagectl` which is in fact an alias you must define as explained in the [Control the daemon](./daemon.md) section.*

In this section, we will suppose that we want to create a bucket named `nextcloud-bucket`
that will be accessed through a key named `nextcloud-app-key`.

Don't forget that `help` command and `--help` subcommands can help you anywhere, the CLI tool is self-documented! Two examples:

```
garagectl help
garagectl bucket allow --help
```

## Create a bucket

Fine, now let's create a bucket (we imagine that you want to deploy nextcloud):

```
garagectl bucket create nextcloud-bucket
```

Check that everything went well:

```
garagectl bucket list
garagectl bucket info nextcloud-bucket
```

## Create an API key

Now we will generate an API key to access this bucket.
Note that API keys are independent of buckets: one key can access multiple buckets, multiple keys can access one bucket.

Now, let's start by creating a key only for our PHP application:

```
garagectl key new --name nextcloud-app-key
```

You will have the following output (this one is fake, `key_id` and `secret_key` were generated with the openssl CLI tool):

```
Key name: nextcloud-app-key
Key ID: GK3515373e4c851ebaad366558
Secret key: 7d37d093435a41f2aab8f13c19ba067d9776c90215f56614adad6ece597dbb34
Authorized buckets:
```

Check that everything works as intended:

```
garagectl key list
garagectl key info nextcloud-app-key
```

## Allow a key to access a bucket

Now that we have a bucket and a key, we need to give permissions to the key on the bucket!

```
garagectl bucket allow \
  --read \
  --write 
  nextcloud-bucket \
  --key nextcloud-app-key
```

You can check at any times allowed keys on your bucket with:

```
garagectl bucket info nextcloud-bucket
```

