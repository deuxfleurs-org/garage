# Quickstart on an existing deployment

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

Now, let's move to the S3 API!
We will use the `s3cmd` CLI tool.
You can install it via your favorite package manager.
Otherwise, check [their website](https://s3tools.org/s3cmd)

We will configure `s3cmd` with its interactive configuration tool, be careful not all endpoints are implemented!
Especially, the test run at the end does not work (yet).

```
$ s3cmd --configure

Enter new values or accept defaults in brackets with Enter.
Refer to user manual for detailed description of all options.

Access key and Secret key are your identifiers for Amazon S3. Leave them empty for using the env variables.
Access Key: GK3515373e4c851ebaad366558
Secret Key: 7d37d093435a41f2aab8f13c19ba067d9776c90215f56614adad6ece597dbb34
Default Region [US]: garage

Use "s3.amazonaws.com" for S3 Endpoint and not modify it to the target Amazon S3.
S3 Endpoint [s3.amazonaws.com]: garage.deuxfleurs.fr

Use "%(bucket)s.s3.amazonaws.com" to the target Amazon S3. "%(bucket)s" and "%(location)s" vars can be used
if the target S3 system supports dns based buckets.
DNS-style bucket+hostname:port template for accessing a bucket [%(bucket)s.s3.amazonaws.com]: garage.deuxfleurs.fr

Encryption password is used to protect your files from reading
by unauthorized persons while in transfer to S3
Encryption password: 
Path to GPG program [/usr/bin/gpg]: 

When using secure HTTPS protocol all communication with Amazon S3
servers is protected from 3rd party eavesdropping. This method is
slower than plain HTTP, and can only be proxied with Python 2.7 or newer
Use HTTPS protocol [Yes]: 

On some networks all internet access must go through a HTTP proxy.
Try setting it here if you can't connect to S3 directly
HTTP Proxy server name: 

New settings:
  Access Key: GK3515373e4c851ebaad366558
  Secret Key: 7d37d093435a41f2aab8f13c19ba067d9776c90215f56614adad6ece597dbb34
  Default Region: garage
  S3 Endpoint: garage.deuxfleurs.fr
  DNS-style bucket+hostname:port template for accessing a bucket: garage.deuxfleurs.fr
  Encryption password: 
  Path to GPG program: /usr/bin/gpg
  Use HTTPS protocol: True
  HTTP Proxy server name: 
  HTTP Proxy server port: 0

Test access with supplied credentials? [Y/n] n

Save settings? [y/N] y
Configuration saved to '/home/quentin/.s3cfg'
```

Now, if everything works, the following commands should work:

```
echo hello world > hello.txt
s3cmd put hello.txt s3://nextcloud-bucket
s3cmd ls s3://nextcloud-bucket
s3cmd rm s3://nextcloud-bucket/hello.txt
```

That's all for now!

