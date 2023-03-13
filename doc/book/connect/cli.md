+++
title = "Browsing tools"
weight = 20
+++

Browsing tools allow you to query the S3 API without too many abstractions.
These tools are particularly suitable for debug, backups, website deployments or any scripted task that need to handle data.

| Name | Status | Note |
|------|--------|------|
| [Minio client](#minio-client)     | ✅       |  Recommended  |
| [AWS CLI](#aws-cli)     | ✅       | Recommended   |
| [rclone](#rclone)     | ✅       |    |
| [s3cmd](#s3cmd)     | ✅       |    |
| [s5cmd](#s5cmd)     | ✅       |    |
| [(Cyber)duck](#cyberduck)     | ✅       |    |
| [WinSCP (libs3)](#winscp)     | ✅       | CLI instructions only   |
| [sftpgo](#sftpgo)     | ✅       |    |


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

## `s5cmd`

Configure a credentials file as follows:

```bash
export AWS_ACCESS_KEY_ID=GK...
export AWS_SECRET_ACCESS_KEY=
export AWS_DEFAULT_REGION='garage'
export AWS_ENDPOINT='http://localhost:3900'
```

After adding these environment variables in your shell, `s5cmd` can be used
with:

```bash
s5cmd --endpoint-url=$AWS_ENDPOINT ls
```

See its usage output for other commands available.

## Cyberduck & duck {#cyberduck}

Both Cyberduck (the GUI) and duck (the CLI) have a concept of "Connection Profiles" that contain some presets for a specific provider.
We wrote the following connection profile for Garage:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
    <dict>
        <key>Protocol</key>
        <string>s3</string>
        <key>Vendor</key>
        <string>garage</string>
        <key>Scheme</key>
        <string>https</string>
        <key>Description</key>
        <string>GarageS3</string>
        <key>Default Hostname</key>
        <string>127.0.0.1</string>
        <key>Default Port</key>
        <string>4443</string>
        <key>Hostname Configurable</key>
        <false/>
        <key>Port Configurable</key>
        <false/>
        <key>Username Configurable</key>
        <true/>
        <key>Username Placeholder</key>
        <string>Access Key ID (GK...)</string>
        <key>Password Placeholder</key>
        <string>Secret Key</string>
        <key>Properties</key>
        <array>
            <string>s3service.disable-dns-buckets=true</string>
        </array>
        <key>Region</key>
        <string>garage</string>
        <key>Regions</key>
        <array>
            <string>garage</string>
        </array>
    </dict>
</plist>
```

*Note: If your garage instance is configured with vhost access style, you can remove `s3service.disable-dns-buckets=true`.*

### Instructions for the GUI

Copy the connection profile, and save it anywhere as `garage.cyberduckprofile`.
Then find this file with your file explorer and double click on it: Cyberduck will open a connection wizard for this profile.
Simply follow the wizard and you should be done!

### Instuctions for the CLI

To configure duck (Cyberduck's CLI tool), start by creating its folder hierarchy:

```
mkdir -p ~/.duck/profiles/
```

Then, save the connection profile for Garage in `~/.duck/profiles/garage.cyberduckprofile`.
To set your credentials in `~/.duck/credentials`, use the following commands to generate the appropriate string:

```bash
export AWS_ACCESS_KEY_ID="GK..."
export AWS_SECRET_ACCESS_KEY="..."
export HOST="s3.garage.localhost"
export PORT="4443"
export PROTOCOL="https"

cat > ~/.duck/credentials <<EOF
$PROTOCOL\://$AWS_ACCESS_KEY_ID@$HOST\:$PORT=$AWS_SECRET_ACCESS_KEY
EOF
```

And finally, I recommend appending a small wrapper to your `~/.bashrc` to avoid setting the username on each command (do not forget to replace `GK...` by your access key):

```bash
function duck { command duck --username GK... $@ ; }
```

Finally, you can then use `duck` as follow:

```bash
# List buckets
duck --list garage:/

# List objects in a bucket
duck --list garage:/my-files/

# Download an object
duck --download garage:/my-files/an-object.txt /tmp/object.txt

# Upload an object
duck --upload /tmp/object.txt garage:/my-files/another-object.txt

# Delete an object
duck --delete garage:/my-files/an-object.txt
```

## WinSCP (libs3) {#winscp}

*You can find instructions on how to use the GUI in french [in our wiki](https://wiki.deuxfleurs.fr/fr/Guide/Garage/WinSCP).*

How to use `winscp.com`, the CLI interface of WinSCP:

```
open s3://GKxxxxx:yyyyyyy@127.0.0.1:4443 -certificate=* -rawsettings S3DefaultRegion=garage S3UrlStyle=1
ls
ls my-files/
get my-files/an-object.txt Z:\tmp\object.txt
put Z:\tmp\object.txt my-files/another-object.txt
rm my-files/an-object
exit
```

Notes:
  - It seems WinSCP supports only TLS connections for S3
  - `-certificate=*` allows self-signed certificates, remove it if you have valid certificates


## sftpgo {#sftpgo}

sftpgo needs a database to work, by default it uses sqlite and does not require additional configuration.
You can then directly init it:

```
sftpgo initprovider
```

Then you can directly launch the daemon that will listen by default on `:8080 (http)` and `:2022 (ssh)`:

```
sftpgo serve
```

Go to the admin web interface (http://[::1]:8080/web/admin/), create the required admin account, then create a user account.
Choose a username (eg: `ada`) and a password.

In the filesystem section, choose:
  - Storage: AWS S3 (Compatible)
  - Bucket: *your bucket name*
  - Region: `garage` (or the one you defined in `config.toml`)
  - Access key: *your access key*
  - Access secret: *your secret key*
  - Endpoint: *your endpoint*, eg. `https://garage.example.tld`, note that the protocol (`https` here) must be specified. Non standard ports and `http` have not been tested yet.
  - Keep the default values for other fields
  - Tick "Use path-style addressing". It should work without ticking it if you have correctly configured your instance to use URL vhost-style.

Now you can access your bucket through SFTP:

```
sftp -P2022 ada@[::1]
ls
```

And through the web interface at http://[::1]:8080/web/client

