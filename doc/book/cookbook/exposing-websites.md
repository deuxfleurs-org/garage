+++
title = "Exposing buckets as websites"
weight = 25
+++

## Configuring a bucket for website access

There are three methods to expose buckets as website:

1. using the PutBucketWebsite S3 API call, which is allowed for access keys that have the owner permission bit set

2. from the Garage CLI, by an adminstrator of the cluster

3. using the Garage administration API

The `PutBucketWebsite` API endpoint [is documented](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketWebsite.html) in the official AWS docs.
This endpoint can also be called [using `aws s3api`](https://docs.aws.amazon.com/cli/latest/reference/s3api/put-bucket-website.html) on the command line.
The website configuration supported by Garage is only a subset of the possibilities on Amazon S3: redirections are not supported, only the index document and error document can be specified.

If you want to expose your bucket as a website from the CLI, use this simple command:

```bash
garage bucket website --allow my-website
```

Now it will be **publicly** exposed on the web endpoint (by default listening on port 3902).

## How exposed websites work

Our website serving logic is as follow:

  - Supports only static websites (no support for PHP or other languages)
  - Does not support directory listing
  - The index file is defined per-bucket and can be specified in the `PutBucketWebsite` call
     or on the CLI using the `--index-document` parameter (default: `index.html`)
  - A custom error document for 404 errors can be specified in the `PutBucketWebsite` call
    or on the CLI using the `--error-document` parameter

Now we need to infer the URL of your website through your bucket name.
Let assume:
  - we set `root_domain = ".web.example.com"` in `garage.toml` ([ref](@/documentation/reference-manual/configuration.md#web_root_domain))
  - our bucket name is `garagehq.deuxfleurs.fr`.

Our bucket will be served if the Host field matches one of these 2 values (the port is ignored):

  - `garagehq.deuxfleurs.fr.web.example.com`: you can dedicate a subdomain to your users (here `web.example.com`).

  - `garagehq.deuxfleurs.fr`: your users can bring their own domain name, they just need to point them to your Garage cluster.

You can try this logic locally, without configuring any DNS, thanks to `curl`:

```bash
# prepare your test
echo hello world > /tmp/index.html
mc cp /tmp/index.html garage/garagehq.deuxfleurs.fr

curl -H 'Host: garagehq.deuxfleurs.fr' http://localhost:3902
# should print "hello world"

curl -H 'Host: garagehq.deuxfleurs.fr.web.example.com' http://localhost:3902
# should also print "hello world"
```

Now that you understand how website logic works on Garage, you can:

 - make the website endpoint listens on port 80 (instead of 3902)
 - use iptables to redirect the port 80 to the port 3902:  
   `iptables -t nat -A PREROUTING -p tcp -dport 80 -j REDIRECT -to-port 3902`
 - or configure a [reverse proxy](@/documentation/cookbook/reverse-proxy.md) in front of Garage to add TLS (HTTPS), CORS support, etc.

You can also take a look at [Website Integration](@/documentation/connect/websites.md) to see how you can add Garage to your workflow.
