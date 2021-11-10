# FUSE (s3fs, goofys, s3backer...)

**WARNING! Garage is not POSIX compatible.
Mounting S3 buckets as filesystems will not provide POSIX compatibility.
If you are not careful, you will lose or corrupt your data.**

Do not use these FUSE filesystems to store any database files (eg. MySQL, Postgresql, Mongo or sqlite),
any daemon cache (dovecot, openldap, gitea, etc.),
and more generally any software that use locking, advanced filesystems features or make any synchronisation assumption.
Ideally, avoid these solutions at all for any serious or production use.

## rclone mount

*External link:* [rclone documentation > rclone mount](https://rclone.org/commands/rclone_mount/)

## s3fs

*External link:* [s3fs github > README.md](https://github.com/s3fs-fuse/s3fs-fuse#examples)

## goofys

*External link:* [goofys github > README.md](https://github.com/kahing/goofys#usage)

## s3backer

*External link:* [s3backer github > manpage](https://github.com/archiecobbs/s3backer/wiki/ManPage)

## csi-s3

*External link:* [csi-s3 Github > README.md](https://github.com/ctrox/csi-s3)
