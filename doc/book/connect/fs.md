+++
title = "FUSE (s3fs, goofys, s3backer...)"
weight = 25
+++

**WARNING! Garage is not POSIX compatible.
Mounting S3 buckets as filesystems will not provide POSIX compatibility.
If you are not careful, you will lose or corrupt your data.**

Do not use these FUSE filesystems to store any database files (eg. MySQL, Postgresql, Mongo or sqlite),
any daemon cache (dovecot, openldap, gitea, etc.),
and more generally any software that use locking, advanced filesystems features or make any synchronisation assumption.
Ideally, avoid these solutions at all for any serious or production use.

## rclone mount

rclone uses the same configuration when used [in CLI](@/documentation/connect/cli.md) and mount mode.
We suppose you have the following entry in your `rclone.ini` (mine is located in `~/.config/rclone/rclone.conf`):

```toml
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

Then you can mount and access any bucket as follow:

```bash
# mount the bucket
mkdir /tmp/my-bucket
rclone mount --daemon garage:my-bucket /tmp/my-bucket

# set your working directory to the bucket
cd /tmp/my-bucket

# create a file
echo hello world > hello.txt

# access the file
cat hello.txt

# unmount the bucket
cd
fusermount -u /tmp/my-bucket
```

*External link:* [rclone documentation > rclone mount](https://rclone.org/commands/rclone_mount/)

## s3fs

*External link:* [s3fs github > README.md](https://github.com/s3fs-fuse/s3fs-fuse#user-content-examples)

## goofys

*External link:* [goofys github > README.md](https://github.com/kahing/goofys#user-content-usage)

## s3backer

*External link:* [s3backer github > manpage](https://github.com/archiecobbs/s3backer/wiki/ManPage)

## csi-s3

*External link:* [csi-s3 Github > README.md](https://github.com/ctrox/csi-s3)
