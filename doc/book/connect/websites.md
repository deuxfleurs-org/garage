+++
title = "Websites (Hugo, Jekyll, Publii...)"
weight = 10
+++

Garage is also suitable [to host static websites](@/documentation/cookbook/exposing-websites.md).
While they can be deployed with traditional CLI tools, some static website generators have integrated options to ease your workflow.

| Name | Status | Note |
|------|--------|------|
| [Hugo](#hugo)     | ✅       | Publishing logic is integrated in the tool  |
| [Publii](#publii)     | ✅       | Require a correctly configured s3 vhost endpoint     |
| [Generic Static Site Generator](#generic-static-site-generator)     | ✅        |  Works for Jekyll, Zola, Gatsby, Pelican, etc.    |

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

[![A screenshot of Publii's GUI](./publii.png)](./publii.png)

Deploying a website to Garage from Publii is natively supported.
First, make sure that your Garage administrator allowed and configured Garage to support vhost access style.
We also suppose that your bucket ("my-bucket") and key is already created and configured.

Then, from the left menu, click on server. Choose "S3" as the protocol.
In the configuration window, enter:
  - Your finale website URL (eg. "http://my-bucket.web.garage.localhost:3902")
  - Tick "Use a custom S3 provider"
  - Set the S3 endpoint, (eg. "http://s3.garage.localhost:3900")
  - Then put your access key (eg. "GK..."), your secret key, and your bucket (eg. "my-bucket")
  - And hit the button "Save settings"

Now, each time you want to publish your website from Publii, just hit the bottom left button "Sync your website"!



## Generic Static Site Generator

Some tools do not support sending to a S3 backend but output a compiled folder on your system.
We can then use any CLI tool to upload this content to our S3 target.

First, start by [configuring minio client](@/documentation/connect/cli.md#minio-client).

Then build your website (example for jekyll):

```bash
jekyll build
```

And copy its output folder (`_site` for Jekyll) on S3:

```bash
mc mirror --overwrite _site garage/my-site
```
