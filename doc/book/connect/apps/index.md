+++
title = "Apps (Nextcloud, Peertube...)"
weight = 5
+++

In this section, we cover the following software: [Nextcloud](#nextcloud), [Peertube](#peertube), [Mastodon](#mastodon), [Matrix](#matrix)

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
In return, this system is a bit harder to configure, especially with Garage that supports less feature than other older S3 backends.
We show that it is still possible to configure Garage with Peertube, allowing you to spread the load and the bandwidth usage on the Garage cluster.

### Enable path-style access by patching Peertube

First, you will need to apply a small patch on Peertube ([#4510](https://github.com/Chocobozzz/PeerTube/pull/4510)):

```diff
From e3b4c641bdf67e07d406a1d49d6aa6b1fbce2ab4 Mon Sep 17 00:00:00 2001
From: Martin Honermeyer <maze@strahlungsfrei.de>
Date: Sun, 31 Oct 2021 12:34:04 +0100
Subject: [PATCH] Allow setting path-style access for object storage

---
 config/default.yaml                                           | 4 ++++
 config/production.yaml.example                                | 4 ++++
 server/initializers/config.ts                                 | 1 +
 server/lib/object-storage/shared/client.ts                    | 3 ++-
 .../production/config/custom-environment-variables.yaml       | 2 ++
 5 files changed, 13 insertions(+), 1 deletion(-)

diff --git a/config/default.yaml b/config/default.yaml
index cf9d69a6211..4efd56fb804 100644
--- a/config/default.yaml
+++ b/config/default.yaml
@@ -123,6 +123,10 @@ object_storage:
     # You can also use AWS_SECRET_ACCESS_KEY env variable
     secret_access_key: ''
 
+  # Reference buckets via path rather than subdomain
+  # (i.e. "my-endpoint.com/bucket" instead of "bucket.my-endpoint.com")
+  force_path_style: false
+
   # Maximum amount to upload in one request to object storage
   max_upload_part: 2GB
 
diff --git a/config/production.yaml.example b/config/production.yaml.example
index 70993bf57a3..9ca2de5f4c9 100644
--- a/config/production.yaml.example
+++ b/config/production.yaml.example
@@ -121,6 +121,10 @@ object_storage:
     # You can also use AWS_SECRET_ACCESS_KEY env variable
     secret_access_key: ''
 
+  # Reference buckets via path rather than subdomain
+  # (i.e. "my-endpoint.com/bucket" instead of "bucket.my-endpoint.com")
+  force_path_style: false
+
   # Maximum amount to upload in one request to object storage
   max_upload_part: 2GB
 
diff --git a/server/initializers/config.ts b/server/initializers/config.ts
index 8375bf4304c..d726c59a4b6 100644
--- a/server/initializers/config.ts
+++ b/server/initializers/config.ts
@@ -91,6 +91,7 @@ const CONFIG = {
       ACCESS_KEY_ID: config.get<string>('object_storage.credentials.access_key_id'),
       SECRET_ACCESS_KEY: config.get<string>('object_storage.credentials.secret_access_key')
     },
+    FORCE_PATH_STYLE: config.get<boolean>('object_storage.force_path_style'),
     VIDEOS: {
       BUCKET_NAME: config.get<string>('object_storage.videos.bucket_name'),
       PREFIX: config.get<string>('object_storage.videos.prefix'),
diff --git a/server/lib/object-storage/shared/client.ts b/server/lib/object-storage/shared/client.ts
index c9a61459336..eadad02f93f 100644
--- a/server/lib/object-storage/shared/client.ts
+++ b/server/lib/object-storage/shared/client.ts
@@ -26,7 +26,8 @@ function getClient () {
         accessKeyId: OBJECT_STORAGE.CREDENTIALS.ACCESS_KEY_ID,
         secretAccessKey: OBJECT_STORAGE.CREDENTIALS.SECRET_ACCESS_KEY
       }
-      : undefined
+      : undefined,
+    forcePathStyle: CONFIG.OBJECT_STORAGE.FORCE_PATH_STYLE
   })
 
   logger.info('Initialized S3 client %s with region %s.', getEndpoint(), OBJECT_STORAGE.REGION, lTags())
diff --git a/support/docker/production/config/custom-environment-variables.yaml b/support/docker/production/config/custom-environment-variables.yaml
index c7cd28e6521..a960bab0bc9 100644
--- a/support/docker/production/config/custom-environment-variables.yaml
+++ b/support/docker/production/config/custom-environment-variables.yaml
@@ -54,6 +54,8 @@ object_storage:
 
   region: "PEERTUBE_OBJECT_STORAGE_REGION"
 
+  force_path_style: "PEERTUBE_OBJECT_STORAGE_FORCE_PATH_STYLE"
+
   max_upload_part:
     __name: "PEERTUBE_OBJECT_STORAGE_MAX_UPLOAD_PART"
     __format: "json"
```

You can then recompile it with:

```
npm run build
```

And it can be started with:

```
NODE_ENV=production NODE_CONFIG_DIR=/srv/peertube/config node dist/server.js
```


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
garage bucket allow peertube-playlist --read --write --key peertube-key
garage bucket allow peertube-video --read --write --key peertube-key
```

Finally, we need to expose these buckets publicly to serve their content to users:

```bash
garage bucket website --allow peertube-playlist
garage bucket website --allow peertube-video
```

These buckets are now accessible on the web port (by default 3902) with the following URL: `http://<bucket><root_domain>:<web_port>` where the root domain is defined in your configuration file (by default `.web.garage`). So we have currently the following URLs: 
  * http://peertube-playlist.web.garage:3902
  * http://peertube-video.web.garage:3902

Make sure you (will) have a corresponding DNS entry for them.

### Configure a Reverse Proxy to serve CORS

Now we will configure a reverse proxy in front of Garage.
This is required as we have no other way to serve CORS headers yet.
Check the [Configuring a reverse proxy](@/documentation/cookbook/reverse-proxy.md) section to know how.

Now make sure that your 2 dns entries are pointing to your reverse proxy.

### Configure Peertube

You must edit the file named `config/production.yaml`, we are only modifying the root key named `object_storage`:

```yaml
object_storage:
  enabled: true

  # Put localhost only if you have a garage instance running on that node
  endpoint: 'http://localhost:3900' # or "garage.example.com" if you have TLS on port 443

  # This entry has been added by our patch, must be set to true
  force_path_style: true

  # Garage supports only one region for now, named garage
  region: 'garage'

  credentials:
    access_key_id: 'GKxxxx'
    secret_access_key: 'xxxx'

  max_upload_part: 2GB

  streaming_playlists:
    bucket_name: 'peertube-playlist'

    # Keep it empty for our example
    prefix: ''

    # You must fill this field to make Peertube use our reverse proxy/website logic
    base_url: 'http://peertube-playlist.web.garage' # Example: 'https://mirror.example.com'

  # Same settings but for webtorrent videos
  videos:
    bucket_name: 'peertube-video'
    prefix: ''
    # You must fill this field to make Peertube use our reverse proxy/website logic
    base_url: 'http://peertube-video.web.garage'
```

### That's all

Everything must be configured now, simply restart Peertube and try to upload a video.
You must see in your browser console that data are fetched directly from our bucket (through the reverse proxy).

### Miscellaneous

*Known bug:* The playback does not start and some 400 Bad Request Errors appear in your browser console and on Garage.
If the description of the error contains HTTP Invalid Range: InvalidRange, the error is due to a buggy ffmpeg version.
You must avoid the 4.4.0 and use either a newer or older version.

*Associated issues:* [#137](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/137), [#138](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/138), [#140](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/140). These issues are non blocking.

*External link:* [Peertube Documentation > Remote Storage](https://docs.joinpeertube.org/admin-remote-storage)

## Mastodon

https://docs.joinmastodon.org/admin/config/#cdn

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

## Pixelfed

https://docs.pixelfed.org/technical-documentation/env.html#filesystem

## Pleroma

https://docs-develop.pleroma.social/backend/configuration/cheatsheet/#pleromauploaderss3

## Lemmy

via pict-rs
https://git.asonix.dog/asonix/pict-rs/commit/f9f4fc63d670f357c93f24147c2ee3e1278e2d97

## Funkwhale

https://docs.funkwhale.audio/admin/configuration.html#s3-storage

## Misskey

https://github.com/misskey-dev/misskey/commit/9d944243a3a59e8880a360cbfe30fd5a3ec8d52d

## Prismo

https://gitlab.com/prismosuite/prismo/-/blob/dev/.env.production.sample#L26-33

## Owncloud Infinite Scale (ocis)

## Unsupported

  - Mobilizon: No S3 integration
  - WriteFreely: No S3 integration
  - Plume: No S3 integration
