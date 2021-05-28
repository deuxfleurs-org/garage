# Handle files

We recommend the use of MinIO Client to interact with Garage files (`mc`).
Instructions to install it and use it are provided on the [MinIO website](https://docs.min.io/docs/minio-client-quickstart-guide.html).
Before reading the following, you need a working `mc` command on your path.

Note that on certain Linux distributions such as Arch Linux, the Minio client binary
is called `mcli` instead of `mc` (to avoid name clashes with the Midnight Commander).

## Configure `mc`

You need your access key and secret key created in the [previous section](bucket.md).
You also need to set the endpoint: it must match the IP address of one of the node of the cluster and the API port (3900 by default).
For this whole configuration, you must set an alias name: we chose `my-garage`, that you will used for all commands.

Adapt the following command accordingly and run it:

```bash
mc alias set \
  my-garage \
  http://172.20.0.101:3900 \
  <access key> \
  <secret key> \
  --api S3v4
```

You must also add an environment variable to your configuration to inform MinIO of our region (`garage` by default).
The best way is to add the following snippet to your `$HOME/.bash_profile` or `$HOME/.bashrc` file:

```bash
export MC_REGION=garage
```

## Use `mc`

You can not list buckets from `mc` currently.

But the following commands and many more should work:

```bash
mc cp image.png my-garage/nextcloud-bucket
mc cp my-garage/nextcloud-bucket/image.png .
mc ls my-garage/nextcloud-bucket
mc mirror localdir/ my-garage/another-bucket
```
