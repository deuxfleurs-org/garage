+++
title = "Apps (Nextcloud, Peertube...)"
weight = 5
+++

In this section, we cover the following web applications:

| Name | Status | Note |
|------|--------|------|
| [Nextcloud](#nextcloud)     | ✅       |  Both Primary Storage and External Storage are supported    |
| [Peertube](#peertube)     | ✅       | Supported with the website endpoint, proxifying private videos unsupported     |
| [Mastodon](#mastodon)     | ✅       | Natively supported    |
| [Matrix](#matrix)     | ✅       |  Tested with `synapse-s3-storage-provider`    |
| [ejabberd](#ejabberd)     | ✅       |  `mod_s3_upload`    |
| [Pixelfed](#pixelfed)     | ❓       |  Not yet tested    |
| [Pleroma](#pleroma)     | ❓       |  Not yet tested    |
| [Lemmy](#lemmy)     | ✅        |  Supported with pict-rs    |
| [Funkwhale](#funkwhale)     | ❓       | Not yet tested     |
| [Misskey](#misskey)     | ❓       | Not yet tested     |
| [Prismo](#prismo)     | ❓       | Not yet tested     |
| [Owncloud OCIS](#owncloud-infinite-scale-ocis) |  ❓| Not yet tested |

## Nextcloud

Nextcloud is a popular file synchronisation and backup service.
By default, Nextcloud stores its data on the local filesystem.
If you want to expand your storage to aggregate multiple servers, Garage is the way to go.

A S3 backend can be configured in two ways on Nextcloud, either as Primary Storage or as an External Storage.
Primary storage will store all your data on S3, in an opaque manner, and will provide the best performances.
External storage enable you to select which data will be stored on S3, your file hierarchy will be preserved in S3, but it might be slower.

In the following, we cover both methods but before reading our guide, we suppose you have done some preliminary steps.
First, we expect you have an already installed and configured Nextcloud instance.
Second, we suppose you have created a key and a bucket.

As a reminder, you can create a key for your nextcloud instance as follow:

```bash
garage key new --name nextcloud-key
```

Keep the Key ID and the Secret key in a pad, they will be needed later.  
Then you can create a bucket and give read/write rights to your key on this bucket with:

```bash
garage bucket create nextcloud
garage bucket allow nextcloud --read --write --key nextcloud-key
```


### Primary Storage

Now edit your Nextcloud configuration file to enable object storage.
On my installation, the config. file is located at the following path: `/var/www/nextcloud/config/config.php`.  
We will add a new root key to the `$CONFIG` dictionnary named `objectstore`:

```php
<?php
$CONFIG = array(
/* your existing configuration */
'objectstore' => [
    'class' => '\\OC\\Files\\ObjectStore\\S3',
    'arguments' => [
        'bucket' => 'nextcloud',   // Your bucket name, must be created before
        'autocreate' => false,     // Garage does not support autocreate
        'key'    => 'xxxxxxxxx',   // The Key ID generated previously
        'secret' => 'xxxxxxxxx',   // The Secret key generated previously
        'hostname' => '127.0.0.1', // Can also be a domain name, eg. garage.example.com
        'port' => 3900,            // Put your reverse proxy port or your S3 API port
        'use_ssl' => false,        // Set it to true if you have a TLS enabled reverse proxy
        'region' => 'garage',      // Garage has only one region named "garage"
        'use_path_style' => true   // Garage supports only path style, must be set to true
    ],
],
```

That's all, your Nextcloud will store all your data to S3.
To test your new configuration, just reload your Nextcloud webpage and start sending data.

*External link:* [Nextcloud Documentation > Primary Storage](https://docs.nextcloud.com/server/latest/admin_manual/configuration_files/primary_storage.html)

### External Storage

**From the GUI.** Activate the "External storage support" app from the "Applications" page (click on your account icon on the top right corner of your screen to display the menu). Go to your parameters page (also located below your account icon). Click on external storage (or the corresponding translation in your language).

[![Screenshot of the External Storage form](cli-nextcloud-gui.png)](cli-nextcloud-gui.png)
*Click on the picture to zoom*

Add a new external storage. Put what you want in "folder name" (eg. "shared"). Select "Amazon S3". Keep "Access Key" for the Authentication field.
In Configuration, put your bucket name (eg. nextcloud), the host (eg. 127.0.0.1), the port (eg. 3900 or 443), the region (garage). Tick the SSL box if you have put an HTTPS proxy in front of garage. You must tick the "Path access" box and you must leave the "Legacy authentication (v2)" box empty. Put your Key ID (eg. GK...) and your Secret Key in the last two input boxes. Finally click on the tick symbol on the right of your screen.

Now go to your "Files" app and a new "linked folder" has appeared with the name you chose earlier (eg. "shared").

*External link:* [Nextcloud Documentation > External Storage Configuration GUI](https://docs.nextcloud.com/server/latest/admin_manual/configuration_files/external_storage_configuration_gui.html)

**From the CLI.** First install the external storage application:

```bash
php occ app:install files_external
```

Then add a new mount point with:

```bash
 php occ files_external:create \
  -c bucket=nextcloud \
  -c hostname=127.0.0.1 \
  -c port=3900 \
  -c region=garage \
  -c use_ssl=false \
  -c use_path_style=true \
  -c legacy_auth=false \
  -c key=GKxxxx \
  -c secret=xxxx \
  shared amazons3 amazons3::accesskey
```

Adapt the `hostname`, `port`, `use_ssl`, `key`, and `secret` entries to your configuration.
Do not change the `use_path_style` and `legacy_auth` entries, other configurations are not supported.

*External link:* [Nextcloud Documentation > occ command > files external](https://docs.nextcloud.com/server/latest/admin_manual/configuration_server/occ_command.html#files-external-label)


## Peertube

Peertube proposes a clever integration of S3 by directly exposing its endpoint instead of proxifying requests through the application.
In other words, Peertube is only responsible of the "control plane" and offload the "data plane" to Garage.
In return, this system is a bit harder to configure.
We show how it is still possible to configure Garage with Peertube, allowing you to spread the load and the bandwidth usage on the Garage cluster.

Starting from version 5.0, Peertube also supports improving the security for private videos by not exposing them directly
but relying on a single control point in the Peertube instance. This is based on S3 per-object and prefix ACL, which are not currently supported
in Garage, so this feature is unsupported. While this technically impedes security for private videos, it is not a blocking issue and could be
a reasonable trade-off for some instances.

### Create resources in Garage

Create a key for Peertube:

```bash
garage key new --name peertube-key
```

Keep the Key ID and the Secret key in a pad, they will be needed later.  

We need two buckets, one for normal videos (named peertube-video) and one for webtorrent videos (named peertube-playlist).
```bash
garage bucket create peertube-video
garage bucket create peertube-playlist
```

Now we allow our key to read and write on these buckets:

```
garage bucket allow peertube-playlists --read --write --owner --key peertube-key
garage bucket allow peertube-videos --read --write --owner --key peertube-key
```

We also need to expose these buckets publicly to serve their content to users:

```bash
garage bucket website --allow peertube-playlists
garage bucket website --allow peertube-videos
```

Finally, we must allow Cross-Origin Resource Sharing (CORS). 
CORS are required by your browser to allow requests triggered from the peertube website (eg. peertube.tld) to your bucket's domain (eg. peertube-videos.web.garage.tld)

```bash
export CORS='{"CORSRules":[{"AllowedHeaders":["*"],"AllowedMethods":["GET"],"AllowedOrigins":["*"]}]}'
aws --endpoint http://s3.garage.localhost s3api put-bucket-cors --bucket peertube-playlists --cors-configuration $CORS
aws --endpoint http://s3.garage.localhost s3api put-bucket-cors --bucket peertube-videos --cors-configuration $CORS
```

These buckets are now accessible on the web port (by default 3902) with the following URL: `http://<bucket><root_domain>:<web_port>` where the root domain is defined in your configuration file (by default `.web.garage`). So we have currently the following URLs: 
  * http://peertube-playlists.web.garage:3902
  * http://peertube-videos.web.garage:3902

Make sure you (will) have a corresponding DNS entry for them.


### Configure Peertube

You must edit the file named `config/production.yaml`, we are only modifying the root key named `object_storage`:

```yaml
object_storage:
  enabled: true

  # Put localhost only if you have a garage instance running on that node
  endpoint: 'http://localhost:3900' # or "garage.example.com" if you have TLS on port 443

  # Garage supports only one region for now, named garage
  region: 'garage'

  credentials:
    access_key_id: 'GKxxxx'
    secret_access_key: 'xxxx'

  max_upload_part: 2GB

  proxy:
    # You may enable this feature, yet it will not provide any security benefit, so
    # you should rather benefit from Garage public endpoint for all videos
    proxify_private_files: false

  streaming_playlists:
    bucket_name: 'peertube-playlist'

    # Keep it empty for our example
    prefix: ''

    # You must fill this field to make Peertube use our reverse proxy/website logic
    base_url: 'http://peertube-playlists.web.garage.localhost' # Example: 'https://mirror.example.com'

  # Same settings but for webtorrent videos
  videos:
    bucket_name: 'peertube-video'
    prefix: ''
    # You must fill this field to make Peertube use our reverse proxy/website logic
    base_url: 'http://peertube-videos.web.garage.localhost'
```

### That's all

Everything must be configured now, simply restart Peertube and try to upload a video.

Peertube will start by serving the video from its own domain while it is encoding.
Once the encoding is done, the video is uploaded to Garage.
You can now reload the page and see in your browser console that data are fetched directly from your bucket.

*External link:* [Peertube Documentation > Remote Storage](https://docs.joinpeertube.org/admin-remote-storage)

## Mastodon

Mastodon natively supports the S3 protocol to store media files, and it works out-of-the-box with Garage.
You will need to expose your Garage bucket as a website: that way, media files will be served directly from Garage.

### Performance considerations

Mastodon tends to store many small objects over time: expect hundreds of thousands of objects,
with average object size ranging from 50 KB to 150 KB.

As such, your Garage cluster should be configured appropriately for good performance:

- use Garage v0.8.0 or higher with the [LMDB database engine](@documentation/reference-manual/configuration.md#db-engine-since-v0-8-0).
  With the default Sled database engine, your database could quickly end up taking tens of GB of disk space.
- the Garage database should be stored on a SSD

### Creating your bucket

This is the usual Garage setup:

```bash
garage key new --name mastodon-key
garage bucket create mastodon-data
garage bucket allow mastodon-data --read --write --key mastodon-key
```

Note the Key ID and Secret Key.

### Exposing your bucket as a website

Create a DNS name to serve your media files, such as `my-social-media.mydomain.tld`.
This name will be publicly exposed to the users of your Mastodon instance: they
will load images directly from this DNS name.

As [documented here](@/documentation/cookbook/exposing-websites.md),
add this DNS name as alias to your bucket, and expose it as a website:

```bash
garage bucket alias mastodon-data my-social-media.mydomain.tld
garage bucket website --allow mastodon-data
```

Then you will likely need to [setup a reverse proxy](@/documentation/cookbook/reverse-proxy.md)
in front of it to serve your media files over HTTPS.

### Cleaning up old media files before migration

Mastodon instance quickly accumulate a lot of media files from the federation.
Most of them are not strictly necessary because they can be fetched again from
other servers.  As such, it is highly recommended to clean them up before
migration, this will greatly reduce the migration time.

From the [official Mastodon documentation](https://docs.joinmastodon.org/admin/tootctl/#media):

```bash
$ RAILS_ENV=production bin/tootctl media remove --days 3
$ RAILS_ENV=production bin/tootctl media remove-orphans
$ RAILS_ENV=production bin/tootctl preview_cards remove --days 15
```

Here is a typical disk usage for a small but multi-year instance after cleanup:

```bash
$ RAILS_ENV=production bin/tootctl media usage
Attachments:	5.67 GB (1.14 GB local)
Custom emoji:	295 MB (0 Bytes local)
Preview cards:	154 MB
Avatars:	3.77 GB (127 KB local)
Headers:	8.72 GB (242 KB local)
Backups:	0 Bytes
Imports:	1.7 KB
Settings:	0 Bytes
```

Unfortunately, [old avatars and headers cannot currently be cleaned up](https://github.com/mastodon/mastodon/issues/9567).

### Migrating your data

Data migration should be done with an efficient S3 client.
The [minio client](@documentation/connect/cli.md#minio-client) is a good choice
thanks to its mirror mode:

```bash
mc mirror ./public/system/ garage/mastodon-data
```

Here is a typical bucket usage after all data has been migrated:

```bash
$ garage bucket info mastodon-data

Size: 20.3 GiB (21.8 GB)
Objects: 175968
```

### Configuring Mastodon

In your `.env.production` configuration file:

```bash
S3_ENABLED=true
# Internal access to Garage
S3_ENDPOINT=http://my-garage-instance.mydomain.tld:3900
S3_REGION=garage
S3_BUCKET=mastodon-data
# Change this (Key ID and Secret Key of your Garage key)
AWS_ACCESS_KEY_ID=GKe88df__CHANGETHIS__c5145
AWS_SECRET_ACCESS_KEY=a2f7__CHANGETHIS__77fcfcf7a58f47a4aa4431f2e675c56da37821a1070000
# What name gets exposed to users (HTTPS is implicit)
S3_ALIAS_HOST=my-social-media.mydomain.tld
```

For more details, see the [reference Mastodon documentation](https://docs.joinmastodon.org/admin/config/#cdn).

Restart all Mastodon services and everything should now be using Garage!
You can check the URLs of images in the Mastodon web client, they should start
with `https://my-social-media.mydomain.tld`.

### Last migration sync

After Mastodon is successfully using Garage, you can run a last sync from the local filesystem to Garage:

```bash
mc mirror --newer-than "3h" ./public/system/ garage/mastodon-data
```

### References

[cybrespace's guide to migrate to S3](https://github.com/cybrespace/cybrespace-meta/blob/master/s3.md)
(the guide is for Amazon S3, so the configuration is a bit different, but the rest is similar)


## Matrix

Matrix is a chat communication protocol. Its main stable server implementation, [Synapse](https://matrix-org.github.io/synapse/latest/), provides a module to store media on a S3 backend. Additionally, a server independent media store supporting S3 has been developped by the community, it has been made possible thanks to how the matrix API has been designed and will work with implementations like Conduit, Dendrite, etc.

### synapse-s3-storage-provider (synapse only)

Supposing you have a working synapse installation, you can add the module with pip:

```bash
 pip3 install --user git+https://github.com/matrix-org/synapse-s3-storage-provider.git
```

Now create a bucket and a key for your matrix instance (note your Key ID and Secret Key somewhere, they will be needed later):

```bash
garage key new --name matrix-key
garage bucket create matrix
garage bucket allow matrix --read --write --key matrix-key
```

Then you must edit your server configuration (eg. `/etc/matrix-synapse/homeserver.yaml`) and add the `media_storage_providers` root key:

```yaml
media_storage_providers:
- module: s3_storage_provider.S3StorageProviderBackend
  store_local: True    # do we want to store on S3 media created by our users?
  store_remote: True   # do we want to store on S3 media created
                       # by users of others servers federated to ours?
  store_synchronous: True  # do we want to wait that the file has been written before returning?
  config:
    bucket: matrix       # the name of our bucket, we chose matrix earlier
    region_name: garage  # only "garage" is supported for the region field
    endpoint_url: http://localhost:3900 # the path to the S3 endpoint
    access_key_id: "GKxxx" # your Key ID
    secret_access_key: "xxxx" # your Secret Key
```

Note that uploaded media will also be stored locally and this behavior can not be deactivated, it is even required for
some operations like resizing images.
In fact, your local filesysem is considered as a cache but without any automated way to garbage collect it.

We can build our garbage collector with `s3_media_upload`, a tool provided with the module.
If you installed the module with the command provided before, you should be able to bring it in your path:

```
PATH=$HOME/.local/bin/:$PATH
command -v s3_media_upload
```

Now we can write a simple script (eg `~/.local/bin/matrix-cache-gc`):

```bash
#!/bin/bash

## CONFIGURATION ##
AWS_ACCESS_KEY_ID=GKxxx
AWS_SECRET_ACCESS_KEY=xxxx
S3_ENDPOINT=http://localhost:3900
S3_BUCKET=matrix
MEDIA_STORE=/var/lib/matrix-synapse/media
PG_USER=matrix
PG_PASS=xxxx
PG_DB=synapse
PG_HOST=localhost
PG_PORT=5432

## CODE ##
PATH=$HOME/.local/bin/:$PATH
cat > database.yaml <<EOF 
user: $PG_USER
password: $PG_PASS
database: $PG_DB
host: $PG_HOST
port: $PG_PORT
EOF

s3_media_upload update-db 1d
s3_media_upload --no-progress check-deleted $MEDIA_STORE
s3_media_upload --no-progress upload $MEDIA_STORE $S3_BUCKET --delete --endpoint-url $S3_ENDPOINT
```

This script will list all the medias that were not accessed in the 24 hours according to your database.
It will check if, in this list, the file still exists in the local media store.
For files that are still in the cache, it will upload them to S3 if they are not already present (in case of a crash or an initial synchronisation).
Finally, the script will delete these files from the cache.

Make this script executable and check that it works:

```bash
chmod +x $HOME/.local/bin/matrix-cache-gc
matrix-cache-gc
```

Add it to your crontab. Open the editor with:

```bash
crontab -e
```

And add a new line. For example, to run it every 10 minutes:

```cron
*/10 * * * * $HOME/.local/bin/matrix-cache-gc
```

*External link:* [Github > matrix-org/synapse-s3-storage-provider](https://github.com/matrix-org/synapse-s3-storage-provider)

### matrix-media-repo (server independent)

*External link:* [matrix-media-repo Documentation > S3](https://docs.t2bot.io/matrix-media-repo/configuration/s3-datastore.html)

## ejabberd

ejabberd is an XMPP server implementation which, with the `mod_s3_upload`
module in the [ejabberd-contrib](https://github.com/processone/ejabberd-contrib)
repository, can be integrated to store chat media files in Garage.

For uploads, this module leverages presigned URLs - this allows XMPP clients to
directly send media to Garage. Receiving clients then retrieve this media
through the [static website](@/documentation/cookbook/exposing-websites.md)
functionality.

As the data itself is publicly accessible to someone with knowledge of the
object URL - users are recommended to use
[E2EE](@/documentation/cookbook/encryption.md) to protect this data-at-rest
from unauthorized access.

Install the module with:

```bash
ejabberdctl module_install mod_s3_upload
```

Create the required key and bucket with:

```bash
garage key new --name ejabberd
garage bucket create objects.xmpp-server.fr
garage bucket allow objects.xmpp-server.fr --read --write --key ejabberd
garage bucket website --allow objects.xmpp-server.fr
```

The module can then be configured with:

```
  mod_s3_upload:
    #bucket_url: https://objects.xmpp-server.fr.my-garage-instance.mydomain.tld
    bucket_url: https://my-garage-instance.mydomain.tld/objects.xmpp-server.fr
    access_key_id: GK...
    access_key_secret: ...
    region: garage
    download_url: https://objects.xmpp-server.fr
```

Other configuration options can be found in the
[configuration YAML file](https://github.com/processone/ejabberd-contrib/blob/master/mod_s3_upload/conf/mod_s3_upload.yml).

## Pixelfed

[Pixelfed Technical Documentation > Configuration](https://docs.pixelfed.org/technical-documentation/env.html#filesystem)

## Pleroma

[Pleroma Documentation > Pleroma.Uploaders.S3](https://docs-develop.pleroma.social/backend/configuration/cheatsheet/#pleromauploaderss3)

## Lemmy

Lemmy uses pict-rs that [supports S3 backends](https://git.asonix.dog/asonix/pict-rs/commit/f9f4fc63d670f357c93f24147c2ee3e1278e2d97).
This feature requires `pict-rs >= 4.0.0`.

### Creating your bucket

This is the usual Garage setup:

```bash
garage key new --name pictrs-key
garage bucket create pictrs-data
garage bucket allow pictrs-data --read --write --key pictrs-key
```

Note the Key ID and Secret Key.

### Migrating your data

If your pict-rs instance holds existing data, you first need to migrate to the S3 bucket.

Stop pict-rs, then run the migration utility from local filesystem to the bucket:

```
pict-rs \
  filesystem -p /path/to/existing/files \
  object-store \
   -e my-garage-instance.mydomain.tld:3900 \
   -b pictrs-data \
   -r garage \
   -a GK... \
   -s abcdef0123456789...
```

This is pretty slow, so hold on while migrating.

### Running pict-rs with an S3 backend

Pict-rs supports both a configuration file and environment variables.

Either set the following section in your `pict-rs.toml`:

```
[store]
type = 'object_storage'
endpoint = 'http://my-garage-instance.mydomain.tld:3900'
bucket_name = 'pictrs-data'
region = 'garage'
access_key = 'GK...'
secret_key = 'abcdef0123456789...'
```

... or set these environment variables:


```
PICTRS__STORE__TYPE=object_storage
PICTRS__STORE__ENDPOINT=http:/my-garage-instance.mydomain.tld:3900
PICTRS__STORE__BUCKET_NAME=pictrs-data
PICTRS__STORE__REGION=garage
PICTRS__STORE__ACCESS_KEY=GK...
PICTRS__STORE__SECRET_KEY=abcdef0123456789...
```


## Funkwhale

[Funkwhale Documentation > S3 Storage](https://docs.funkwhale.audio/admin/configuration.html#s3-storage)

## Misskey

[Misskey Github > commit 9d94424](https://github.com/misskey-dev/misskey/commit/9d944243a3a59e8880a360cbfe30fd5a3ec8d52d)

## Prismo

[Prismo Gitlab > .env.production.sample](https://gitlab.com/prismosuite/prismo/-/blob/dev/.env.production.sample#L26-33)

## Owncloud Infinite Scale (ocis)

OCIS could be compatible with S3:
  - [Deploying OCIS with S3](https://owncloud.dev/ocis/deployment/ocis_s3/)
  - [OCIS 1.7 release note](https://central.owncloud.org/t/owncloud-infinite-scale-tech-preview-1-7-enables-s3-storage/32514/3)

## Unsupported

  - Mobilizon: No S3 integration
  - WriteFreely: No S3 integration
  - Plume: No S3 integration
