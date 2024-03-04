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

## git-annex

[git-annex](https://git-annex.branchable.com/) supports synchronizing files
with its [S3 special remote](https://git-annex.branchable.com/special_remotes/S3/).

Note that `git-annex` requires to be compiled with Haskell package version
`aws-0.24` to work with Garage.

```bash
garage key new --name my-key
garage bucket create my-git-annex
garage bucket allow my-git-annex --read --write --key my-key
```

Register your Key ID and Secret key in your environment:

```bash
export AWS_ACCESS_KEY_ID=GKxxx
export AWS_SECRET_ACCESS_KEY=xxxx
```

Within a git-annex enabled repository, configure your Garage S3 endpoint with
the following command:

```bash
git annex initremote garage type=S3 encryption=none host=my-garage-instance.mydomain.tld protocol=https bucket=my-git-annex requeststyle=path region=garage signature=v4
```

Files can now be synchronized using the usual `git-annex` `copy` or `get`
commands.

Note that for simplicity - this example does not enable encryption for the files
sent to Garage - please refer to the
[git-annex encryption page](https://git-annex.branchable.com/encryption/) for
how to configure this.

## Restic

Create your key and bucket:

```bash
garage key create my-key
garage bucket create backups
garage bucket allow backups --read --write --key my-key
```

Then register your Key ID and Secret key in your environment:

```bash
export AWS_ACCESS_KEY_ID=GKxxx
export AWS_SECRET_ACCESS_KEY=xxxx
```

Configure restic from environment too:

```bash
export RESTIC_REPOSITORY="s3:http://localhost:3900/backups"

echo "Generated password (save it safely): $(openssl rand -base64 32)"
export RESTIC_PASSWORD=xxx # copy paste your generated password here
```

Do not forget to save your password safely (in your password manager or print it). It will be needed to decrypt your backups.

Now you can use restic:

```bash
# Initialize the bucket, must be run once
restic init

# Backup your PostgreSQL database
# (We suppose your PostgreSQL daemon is stopped for all commands)
restic backup /var/lib/postgresql

# Show backup history
restic snapshots

# Backup again your PostgreSQL database, it will be faster as only changes will be uploaded
restic backup /var/lib/postgresql

# Show backup history (again)
restic snapshots

# Restore a backup
# (79766175 is the ID of the snapshot you want to restore)
mv /var/lib/postgresql /var/lib/postgresql.broken
restic restore 79766175 --target /var/lib/postgresql
```

Restic has way more features than the ones presented here.
You can discover all of them by accessing its documentation from the link below.

Files on Android devices can also be backed up with [restic-android](https://github.com/lhns/restic-android).

*External links:* [Restic Documentation > Amazon S3](https://restic.readthedocs.io/en/stable/030_preparing_a_new_repo.html#amazon-s3)

## Duplicity

*External links:* [Duplicity > man](https://duplicity.gitlab.io/duplicity-web/vers8/duplicity.1.html) (scroll to "URL Format" and "A note on Amazon S3")

## Duplicati

*External links:* [Duplicati Documentation > Storage Providers](https://duplicati.readthedocs.io/en/latest/05-storage-providers/#s3-compatible)

The following fields need to be specified:
```
Storage Type: S3 Compatible
Use SSL: [ ] # Only if you have SSL
Server: Custom server url (s3.garage.localhost:3900)
Bucket name: bucket-name
Bucket create region: Custom region value (garage) # Or as you've specified in garage.toml
AWS Access ID: Key ID from "garage key info key-name"
AWS Access Key: Secret key from "garage key info key-name"
Client Library to use: Minio SDK
```

Click `Test connection` and then no when asked `The bucket name should start with your username, prepend automatically?`. Then it should say `Connection worked!`.


## knoxite

*External links:* [Knoxite Documentation > Storage Backends](https://knoxite.com/docs/storage-backends/#amazon-s3)

## kopia

*External links:* [Kopia Documentation > Repositories](https://kopia.io/docs/repositories/#amazon-s3)

To create the Kopia repository, you need to specify the region, the HTTP(S) endpoint, the bucket name and the access keys.
For instance, if you have an instance of garage running on `https://garage.example.com`:

```
kopia repository create s3 --region=garage --bucket=mybackups --access-key=KEY_ID --secret-access-key=SECRET_KEY --endpoint=garage.example.com
```

Or if you have an instance running on localhost, without TLS:

```
kopia repository create s3 --region=garage --bucket=mybackups --access-key=KEY_ID --secret-access-key=SECRET_KEY --endpoint=localhost:3900 --disable-tls
```

After the repository has been created, check that everything works as expected:

```
kopia repository validate-provider
```

You can then run all the standard kopia commands: `kopia snapshot create`, `kopia mount`...
Everything should work out-of-the-box.
