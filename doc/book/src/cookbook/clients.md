# Configuring S3 clients to interact with Garage

To configure an S3 client to interact with Garage, you will need the following
parameters:

- An **API endpoint**: this corresponds to the HTTP or HTTPS address
  used to contact the Garage server. When runing Garage locally this will usually
  be `http://127.0.0.1:3900`. In a real-world setting, you would usually have a reverse-proxy
  that adds TLS support and makes your Garage server available under a public hostname
  such as `https://garage.example.com`.

- An **API access key** and its associated **secret key**. These usually look something
  like this: `GK3515373e4c851ebaad366558` (access key),
  `7d37d093435a41f2aab8f13c19ba067d9776c90215f56614adad6ece597dbb34` (secret key).
  These keys are created and managed using the `garage` CLI, as explained in the
  [quick start](../quick_start/index.md) guide.

Most S3 clients can be configured easily with these parameters,
provided that you follow the following guidelines:

- **Force path style:** Garage does not support DNS-style buckets, which are now by default
  on Amazon S3. Instead, Garage uses the legacy path-style bucket addressing.
  Remember to configure your client to acknowledge this fact.

- **Configuring the S3 region:** Garage requires your client to talk to the correct "S3 region",
  which is set in the configuration file. This is often set just to `garage`.
  If this is not configured explicitly, clients usually try to talk to region `us-east-1`.
  Garage should normally redirect your client to the correct region,
  but in case your client does not support this you might have to configure it manually.

We will now provide example configurations for the most common S3 clients.

## AWS CLI

Export the following environment variables:

```bash
export AWS_ACCESS_KEY_ID=<access key>
export AWS_SECRET_ACCESS_KEY=<secret key>
export AWS_DEFAULT_REGION=<region>
```

Now invoke `aws` as follows:

```bash
aws --endpoint-url <endpoint> s3 <command...>
```

For instance: `aws --endpoint-url http://127.0.0.1:3901 s3 ls s3://my-bucket/`.

## Minio client

Use the following command to set an "alias", i.e. define a new S3 server to be
used by the Minio client:

```bash
mc alias set \
  garage \
  <endpoint> \
  <access key> \
  <secret key> \
  --api S3v4
```

Remember that `mc` is sometimes called `mcli` (such as on Arch Linux), to avoid conflicts
with the Midnight Commander.


## `rclone`

`rclone` can be configured using the interactive assistant invoked using `rclone configure`.

You can also configure `rclone` by writing directly its configuration file.
Here is a template `rclone.ini` configuration file:

```ini
[garage]
type = s3
provider = Other
env_auth = false
access_key_id = <access key>
secret_access_key = <secret key>
region = <region>
endpoint = <endpoint>
force_path_style = true
acl = private
bucket_acl = private
```

## Cyberduck

TODO

## `s3cmd`

Here is a template for the `s3cmd.cfg` file to talk with Garage:

```ini
[default]
access_key = <access key>
secret_key = <secret key>
host_base = <endpoint without http(s)://>
host_bucket = <same as host_base>
use_https = False | True
```
