+++
title = "Websites (Hugo, Jekyll, Publii...)"
weight = 10
+++

Garage is also suitable to host static websites.
While they can be deployed with traditional CLI tools, some static website generators have integrated options to ease your workflow.

## Hugo

Add to your `config.toml` the following section:

```toml
[[deployment.targets]]
 URL = "s3://<bucket>?endpoint=<endpoint>&disableSSL=<bool>&s3ForcePathStyle=true&region=garage"
```

For example:

```toml
[[deployment.targets]]
 URL = "s3://my-blog?endpoint=localhost:9000&disableSSL=true&s3ForcePathStyle=true&region=garage"
```

Then inform hugo of your credentials:

```bash
export AWS_ACCESS_KEY_ID=GKxxx
export AWS_SECRET_ACCESS_KEY=xxx
```

And finally build and deploy your website:

```bsh
hugo
hugo deploy
```

*External links:*
  - [gocloud.dev > aws > Supported URL parameters](https://pkg.go.dev/gocloud.dev/aws?utm_source=godoc#ConfigFromURLParams)
  - [Hugo Documentation > hugo deploy](https://gohugo.io/hosting-and-deployment/hugo-deploy/)

## Publii

It would require a patch either on Garage or on Publii to make both systems work.

Currently, the proposed workaround is to deploy your website manually:
  - On the left menu, click on Server, choose Manual Deployment (the logo looks like a compressed file)
  - Set your website URL, keep Output type as "Non-compressed catalog"
  - Click on Save changes
  - Click on Sync your website (bottom left of the app)
  - On the new page, click again on Sync your website
  - Click on Get website files
  - You need to synchronize the output folder you see in your file explorer, we will use minio client.

Be sure that you [configured minio client](@/documentation/connect/cli.md#minio-client-recommended).

Then copy this output folder

```bash
mc mirror --overwrite output garage/my-site
```

## Generic (eg. Jekyll)

Some tools do not support sending to a S3 backend but output a compiled folder on your system.
We can then use any CLI tool to upload this content to our S3 target.

First, start by [configuring minio client](@/documentation/connect/cli.md#minio-client-recommended).

Then build your website:

```bash
jekyll build
```

And copy jekyll's output folder on S3:

```bash
mc mirror --overwrite _site garage/my-site
```
