+++
title = "Integrations"
weight = 3
sort_by = "weight"
template = "documentation.html"
+++


Garage implements the Amazon S3 protocol, which makes it compatible with many existing software programs.

In particular, you will find here instructions to connect it with:

  - [web applications](@/documentation/connect/apps/index.md)
  - [website hosting](@/documentation/connect/websites.md)
  - [software repositories](@/documentation/connect/repositories.md)
  - [CLI tools](@/documentation/connect/cli.md)
  - [your own code](@/documentation/connect/code.md)

### Generic instructions

To configure S3-compatible software to interact with Garage,
you will need the following parameters:

- An **API endpoint**: this corresponds to the HTTP or HTTPS address
  used to contact the Garage server. When runing Garage locally this will usually
  be `http://127.0.0.1:3900`. In a real-world setting, you would usually have a reverse-proxy
  that adds TLS support and makes your Garage server available under a public hostname
  such as `https://garage.example.com`.

- An **API access key** and its associated **secret key**. These usually look something
  like this: `GK3515373e4c851ebaad366558` (access key),
  `7d37d093435a41f2aab8f13c19ba067d9776c90215f56614adad6ece597dbb34` (secret key).
  These keys are created and managed using the `garage` CLI, as explained in the
  [quick start](@/documentation/quick-start/_index.md) guide.

Most S3 clients can be configured easily with these parameters,
provided that you follow the following guidelines:

- **Force path style:** Garage does not support DNS-style buckets, which are now by default
  on Amazon S3. Instead, Garage uses the legacy path-style bucket addressing.
  Remember to configure your client to acknowledge this fact.

- **Configuring the S3 region:** Garage requires your client to talk to the correct "S3 region",
  which is set in the configuration file. This is often set just to `garage`.
  If this is not configured explicitly, clients usually try to talk to region `us-east-1`.
  Garage should normally redirect your client to the correct region,
  but in case your client does not support this you might have to configure it manually.
