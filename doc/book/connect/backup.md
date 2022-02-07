+++
title = "Backups (restic, duplicity...)"
weight = 25
+++


Backups are essential for disaster recovery but they are not trivial to manage.
Using Garage as your backup target will enable you to scale your storage as needed while ensuring high availability.

## Borg Backup

Borg Backup is very popular among the backup tools but it is not yet compatible with the S3 API.
We recommend using any other tool listed in this guide because they are all compatible with the S3 API.
If you still want to use Borg, you can use it with `rclone mount`.



## Restic

*External links:* [Restic Documentation > Amazon S3](https://restic.readthedocs.io/en/stable/030_preparing_a_new_repo.html#amazon-s3)

## Duplicity

*External links:* [Duplicity > man](https://duplicity.gitlab.io/duplicity-web/vers8/duplicity.1.html) (scroll to "URL Format" and "A note on Amazon S3")

## Duplicati

*External links:* [Duplicati Documentation > Storage Providers](https://github.com/kees-z/DuplicatiDocs/blob/master/docs/05-storage-providers.md#user-content-s3-compatible)

## knoxite

*External links:* [Knoxite Documentation > Storage Backends](https://knoxite.com/docs/storage-backends/#amazon-s3)

## kopia

*External links:* [Kopia Documentation > Repositories](https://kopia.io/docs/repositories/#amazon-s3)

