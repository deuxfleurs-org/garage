+++
title = "CLI tools"
weight = 20
+++

CLI tools allow you to query the S3 API without too many abstractions.
These tools are particularly suitable for debug, backups, website deployments or any scripted task that need to handle data.

## Minio client (recommended)

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
with Midnight Commander.

Some commands:

```bash
# list buckets
mc ls garage/

# list objets in a bucket
mc ls garage/my_files

# copy from your filesystem to garage
mc cp /proc/cpuinfo garage/my_files/cpuinfo.txt

# copy from garage to your filesystem
mc cp garage/my_files/cpuinfo.txt /tmp/cpuinfo.txt

# mirror a folder from your filesystem to garage
mc mirror --overwrite ./book garage/garagehq.deuxfleurs.fr
```


## AWS CLI

Create a file named `~/.aws/credentials` and put:

```toml
[default]
aws_access_key_id=xxxx
aws_secret_access_key=xxxx
```

Then a file named `~/.aws/config` and put:

```toml
[default]
region=garage
```

Now, supposing Garage is listening on `http://127.0.0.1:3900`, you can list your buckets with:

```bash
aws --endpoint-url http://127.0.0.1:3900 s3 ls
```

Passing the `--endpoint-url` parameter to each command is annoying but AWS developers do not provide a corresponding configuration entry.
As a workaround, you can redefine the aws command by editing the file `~/.bashrc`:

```
function aws { command aws --endpoint-url http://127.0.0.1:3900 $@ ; }
```

*Do not forget to run `source ~/.bashrc` or to start a new terminal before running the next commands.*

Now you can simply run:

```bash
# list buckets
aws s3 ls

# list objects of a bucket
aws s3 ls s3://my_files

# copy from your filesystem to garage
aws s3 cp /proc/cpuinfo s3://my_files/cpuinfo.txt

# copy from garage to your filesystem
aws s3 cp s3/my_files/cpuinfo.txt /tmp/cpuinfo.txt
```

## `rclone`

`rclone` can be configured using the interactive assistant invoked using `rclone config`.

You can also configure `rclone` by writing directly its configuration file.
Here is a template `rclone.ini` configuration file (mine is located at `~/.config/rclone/rclone.conf`):

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

Now you can run:

```bash
# list buckets
rclone lsd garage:

# list objects of a bucket aggregated in directories
rclone lsd garage:my-bucket

# copy from your filesystem to garage
echo hello world > /tmp/hello.txt
rclone copy /tmp/hello.txt garage:my-bucket/

# copy from garage to your filesystem
rclone copy garage:quentin.divers/hello.txt .

# see all available subcommands
rclone help
```

**Advice with rclone:** use the `--fast-list` option when accessing buckets with large amounts of objects.
This will tremendously accelerate operations such as `rclone sync` or `rclone ncdu` by reducing the number
of ListObjects calls that are made.


## `s3cmd`

Here is a template for the `s3cmd.cfg` file to talk with Garage:

```ini
[default]
access_key = <access key>
secret_key = <secret key>
host_base = <endpoint without http(s)://>
host_bucket = <same as host_base>
use_https = <False or True>
```

And use it as follow:

```bash
# List buckets
s3cmd ls

# s3cmd objects inside a bucket
s3cmd ls s3://my-bucket

# copy from your filesystem to garage
echo hello world > /tmp/hello.txt
s3cmd put /tmp/hello.txt s3://my-bucket/

# copy from garage to your filesystem
s3cmd get s3://my-bucket/hello.txt hello.txt
```

## Cyberduck & duck

TODO


