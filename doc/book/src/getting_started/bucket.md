# Create buckets and keys

First, chances are that your garage deployment is secured by TLS.
All your commands must be prefixed with their certificates.
I will define an alias once and for all to ease future commands.
Please adapt the path of the binary and certificates to your installation!

```
alias grg="/garage/garage --ca-cert /secrets/garage-ca.crt --client-cert /secrets/garage.crt --client-key /secrets/garage.key"
```

Now we can check that everything is going well by checking our cluster status:

```
grg status
```

Don't forget that `help` command and `--help` subcommands can help you anywhere, the CLI tool is self-documented! Two examples:

```
grg help
grg bucket allow --help
```

Fine, now let's create a bucket (we imagine that you want to deploy nextcloud):

```
grg bucket create nextcloud-bucket
```

Check that everything went well:

```
grg bucket list
grg bucket info nextcloud-bucket
```

Now we will generate an API key to access this bucket.
Note that API keys are independent of buckets: one key can access multiple buckets, multiple keys can access one bucket.

Now, let's start by creating a key only for our PHP application:

```
grg key new --name nextcloud-app-key
```

You will have the following output (this one is fake, `key_id` and `secret_key` were generated with the openssl CLI tool):

```
Key { key_id: "GK3515373e4c851ebaad366558", secret_key: "7d37d093435a41f2aab8f13c19ba067d9776c90215f56614adad6ece597dbb34", name: "nextcloud-app-key", name_timestamp: 1603280506694, deleted: false, authorized_buckets: [] }
```

Check that everything works as intended (be careful, info works only with your key identifier and not with its friendly name!):

```
grg key list
grg key info GK3515373e4c851ebaad366558
```

Now that we have a bucket and a key, we need to give permissions to the key on the bucket!

```
grg bucket allow --read --write nextcloud-bucket --key GK3515373e4c851ebaad366558
```

You can check at any times allowed keys on your bucket with:

```
grg bucket info nextcloud-bucket
```

