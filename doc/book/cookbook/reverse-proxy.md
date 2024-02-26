+++
title = "Configuring a reverse proxy"
weight = 30
+++

The main reason to add a reverse proxy in front of Garage is to provide TLS to your users and serve multiple web services on port 443.

In production you will likely need your certificates signed by a certificate authority.
The most automated way is to use a provider supporting the [ACME protocol](https://datatracker.ietf.org/doc/html/rfc8555) 
such as [Let's Encrypt](https://letsencrypt.org/), [ZeroSSL](https://zerossl.com/) or [Buypass Go SSL](https://www.buypass.com/ssl/products/acme).

If you are only testing Garage, you can generate a self-signed certificate to follow the documentation:

```bash
openssl req \
    -new \
    -x509 \
    -keyout /tmp/garage.key \
    -out /tmp/garage.crt \
    -nodes  \
    -subj "/C=XX/ST=XX/L=XX/O=XX/OU=XX/CN=localhost/emailAddress=X@X.XX" \
    -addext "subjectAltName = DNS:localhost, IP:127.0.0.1"

cat /tmp/garage.key /tmp/garage.crt > /tmp/garage.pem
```

Be careful as you will need to allow self signed certificates in your client.
For example, with minio, you must add the `--insecure` flag.
An example:

```bash
mc ls --insecure garage/
```

## socat (only for testing purposes)

If you want to test Garage with a TLS frontend, socat can do it for you in a single command:

```bash
socat \
"openssl-listen:443,\
reuseaddr,\
fork,\
verify=0,\
cert=/tmp/garage.pem" \
tcp4-connect:localhost:3900
```

## Nginx

Nginx is a well-known reverse proxy suitable for production.
We do the configuration in 3 steps: first we define the upstream blocks ("the backends")
then we define the server blocks ("the frontends") for the S3 endpoint and finally for the web endpoint.

The following configuration blocks can be all put in the same `/etc/nginx/sites-available/garage.conf`.
To make your configuration active, run `ln -s /etc/nginx/sites-available/garage.conf /etc/nginx/sites-enabled/`.
If you directly put the instructions in the root `nginx.conf`, keep in mind that these configurations must be enclosed inside a `http { }` block.

And do not forget to reload nginx with `systemctl reload nginx` or `nginx -s reload`.

### Exposing the S3 endpoints

First, we need to tell to nginx how to access our Garage cluster.
Because we have multiple nodes, we want to leverage all of them by spreading the load.
In nginx, we can do that with the `upstream` directive.

Then in a `server` directive, we define the vhosts, the TLS certificates and the proxy rule.

A possible configuration:

```nginx
upstream s3_backend {
  # If you have a garage instance locally.
  server 127.0.0.1:3900;
  # You can also put your other instances.
  server 192.168.1.3:3900;
  # Domain names also work.
  server garage1.example.com:3900;
  # A "backup" server is only used if all others have failed.
  server garage-remote.example.com:3900 backup;
  # You can assign weights if you have some servers
  # that can serve more requests than others.
  server garage2.example.com:3900 weight=2;
}

server {
  listen [::]:443 http2 ssl;

  ssl_certificate     /tmp/garage.crt;
  ssl_certificate_key /tmp/garage.key;

  # You need multiple server names here:
  #  - s3.garage.tld is used for path-based s3 requests
  #  - *.s3.garage.tld is used for vhost-based s3 requests
  server_name s3.garage.tld *.s3.garage.tld;

  location / {
    proxy_pass http://s3_backend;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header Host $host;
    # Disable buffering to a temporary file.
    proxy_max_temp_file_size 0;
  }
}
```

### Exposing the web endpoint

To better understand the logic involved, you can refer to the [Exposing buckets as websites](/cookbook/exposing_websites.html) section.
Otherwise, the configuration is very similar to the S3 endpoint.
You must only adapt `upstream` with the web port instead of the s3 port and change the `server_name` and `proxy_pass` entry

A possible configuration:


```nginx
upstream web_backend {
  server 127.0.0.1:3902;
  server 192.168.1.3:3902;
  server garage1.example.com:3902;
  server garage2.example.com:3902 weight=2;
}

server {
  listen [::]:443 http2 ssl;

  ssl_certificate     /tmp/garage.crt;
  ssl_certificate_key /tmp/garage.key;

  # You need multiple server names here:
  #  - *.web.garage.tld is used for your users wanting a website without reserving a domain name
  #  - example.com, my-site.tld, etc. are reserved domain name by your users that chose to host their website as a garage's bucket
  server_name *.web.garage.tld example.com my-site.tld;

  location / {
    proxy_pass http://web_backend;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header Host $host;
  }
}
```

## Apache httpd

@TODO

## Traefik v2

We will see in this part how to set up a reverse proxy with [Traefik](https://docs.traefik.io/).

Here is [a basic configuration file](https://doc.traefik.io/traefik/https/acme/#configuration-examples):

```toml
[entryPoints]
  [entryPoints.web]
    address = ":80"

  [entryPoints.websecure]
    address = ":443"

[certificatesResolvers.myresolver.acme]
  email = "your-email@example.com"
  storage = "acme.json"
  [certificatesResolvers.myresolver.acme.httpChallenge]
    # used during the challenge
    entryPoint = "web"
```

### Add Garage service

To add Garage on Traefik you should declare two new services using its IP
address (or hostname) and port, these are used for the S3, and web components
of Garage:

```toml
[http.services]
  [http.services.garage-s3-service.loadBalancer]
    [[http.services.garage-s3-service.loadBalancer.servers]]
      url = "http://xxx.xxx.xxx.xxx"
      port = 3900

  [http.services.garage-web-service.loadBalancer]
    [[http.services.garage-web-service.loadBalancer.servers]]
      url = "http://xxx.xxx.xxx.xxx"
      port = 3902
```

It's possible to declare multiple Garage servers as back-ends:

```toml
[http.services]
    [[http.services.garage-s3-service.loadBalancer.servers]]
      url = "http://xxx.xxx.xxx.xxx"
      port = 3900
    [[http.services.garage-s3-service.loadBalancer.servers]]
      url = "http://yyy.yyy.yyy.yyy"
      port = 3900
    [[http.services.garage-s3-service.loadBalancer.servers]]
      url = "http://zzz.zzz.zzz.zzz"
      port = 3900

    [[http.services.garage-web-service.loadBalancer.servers]]
      url = "http://xxx.xxx.xxx.xxx"
      port = 3902
    [[http.services.garage-web-service.loadBalancer.servers]]
      url = "http://yyy.yyy.yyy.yyy"
      port = 3902
    [[http.services.garage-web-service.loadBalancer.servers]]
      url = "http://zzz.zzz.zzz.zzz"
      port = 3902
```

Traefik can remove unhealthy servers automatically with [a health check configuration](https://doc.traefik.io/traefik/routing/services/#health-check):

```
[http.services]
  [http.services.garage-s3-service.loadBalancer]
    [http.services.garage-s3-service.loadBalancer.healthCheck]
      path = "/health"
      port = "3903"
      #interval = "15s"
      #timeout = "2s"

  [http.services.garage-web-service.loadBalancer]
    [http.services.garage-web-service.loadBalancer.healthCheck]
      path = "/health"
      port = "3903"
      #interval = "15s"
      #timeout = "2s"
```

### Adding a website

To add a new website, add the following declaration to your Traefik configuration file:

```toml
[http.routers]
  [http.routers.garage-s3]
    rule = "Host(`s3.example.org`)"
    service = "garage-s3-service"
    entryPoints = ["websecure"]

  [http.routers.my_website]
    rule = "Host(`yoururl.example.org`)"
    service = "garage-web-service"
    entryPoints = ["websecure"]
```

Enable HTTPS access to your website with the following configuration section ([documentation](https://doc.traefik.io/traefik/https/overview/)):

```toml
...
    entryPoints = ["websecure"]
    [http.routers.my_website.tls]
      certResolver = "myresolver"
...
```

### Adding compression

Add the following configuration section [to compress response](https://doc.traefik.io/traefik/middlewares/http/compress/) using [gzip](https://developer.mozilla.org/en-US/docs/Glossary/GZip_compression) before sending them to the client:

```toml
[http.routers]
  [http.routers.my_website]
    ...
    middlewares = ["compression"]
    ...
[http.middlewares]
  [http.middlewares.compression.compress]
```

### Add caching response

Traefik's caching middleware is only available on [entreprise version](https://doc.traefik.io/traefik-enterprise/middlewares/http-cache/), however the freely-available [Souin plugin](https://github.com/darkweak/souin#tr%C3%A6fik-container) can also do the job. (section to be completed)

### Complete example

```toml
[entryPoints]
  [entryPoints.web]
    address = ":80"

  [entryPoints.websecure]
    address = ":443"

[certificatesResolvers.myresolver.acme]
  email = "your-email@example.com"
  storage = "acme.json"
  [certificatesResolvers.myresolver.acme.httpChallenge]
    # used during the challenge
    entryPoint = "web"

[http.routers]
  [http.routers.garage-s3]
    rule = "Host(`s3.example.org`)"
    service = "garage-s3-service"
    entryPoints = ["websecure"]

  [http.routers.my_website]
    rule = "Host(`yoururl.example.org`)"
    service = "garage-web-service"
    middlewares = ["compression"]
    entryPoints = ["websecure"]

[http.services]
  [http.services.garage-s3-service.loadBalancer]
    [http.services.garage-s3-service.loadBalancer.healthCheck]
      path = "/health"
      port = "3903"
      #interval = "15s"
      #timeout = "2s"

  [http.services.garage-web-service.loadBalancer]
    [http.services.garage-web-service.loadBalancer.healthCheck]
      path = "/health"
      port = "3903"
      #interval = "15s"
      #timeout = "2s"

    [[http.services.garage-s3-service.loadBalancer.servers]]
      url = "http://xxx.xxx.xxx.xxx"
      port = 3900
    [[http.services.garage-s3-service.loadBalancer.servers]]
      url = "http://yyy.yyy.yyy.yyy"
      port = 3900
    [[http.services.garage-s3-service.loadBalancer.servers]]
      url = "http://zzz.zzz.zzz.zzz"
      port = 3900

    [[http.services.garage-web-service.loadBalancer.servers]]
      url = "http://xxx.xxx.xxx.xxx"
      port = 3902
    [[http.services.garage-web-service.loadBalancer.servers]]
      url = "http://yyy.yyy.yyy.yyy"
      port = 3902
    [[http.services.garage-web-service.loadBalancer.servers]]
      url = "http://zzz.zzz.zzz.zzz"
      port = 3902

[http.middlewares]
  [http.middlewares.compression.compress]
```

## Caddy

Your Caddy configuration can be as simple as:

```caddy
s3.garage.tld, *.s3.garage.tld {
	reverse_proxy localhost:3900 192.168.1.2:3900 example.tld:3900 {
        health_uri       /health
        health_port      3903
        #health_interval 15s
        #health_timeout  5s
    }
}

*.web.garage.tld {
	reverse_proxy localhost:3902 192.168.1.2:3902 example.tld:3902 {
        health_uri       /health
        health_port      3903
        #health_interval 15s
        #health_timeout  5s
    }
}

admin.garage.tld {
	reverse_proxy localhost:3903 {
        health_uri       /health
        health_port      3903
        #health_interval 15s
        #health_timeout  5s
    }
}
```

But at the same time, the `reverse_proxy` is very flexible.
For a production deployment, you should [read its documentation](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy) as it supports features like DNS discovery of upstreams, load balancing with checks, streaming parameters, etc.

### Caching

Caddy can compiled with a
[cache plugin](https://github.com/caddyserver/cache-handler) which can be used
to provide a hot-cache at the webserver-level for static websites hosted by
Garage.

This can be configured as follows:

```caddy
# Caddy global configuration section
{
	# Bare minimum configuration to enable cache.
	order cache before rewrite

	cache

	#cache
	#	allowed_http_verbs GET
	#	default_cache_control public
	#	ttl 8h
	#}
}

# Site specific section
https:// {
	cache

	#cache {
	#	timeout {
	#		backend 30s
	#	}
	#}

	reverse_proxy ...
}
```

Caching is a complicated subject, and the reader is encouraged to study the
available options provided by the plugin.

### On-demand TLS

Caddy supports a technique called
[on-demand TLS](https://caddyserver.com/docs/automatic-https#on-demand-tls), by
which one can configure the webserver to provision TLS certificates when a
client first connects to it.

In order to prevent an attack vector whereby domains are simply pointed at your
webserver and certificates are requested for them - Caddy can be configured to
ask Garage if a domain is authorized for web hosting, before it then requests
a TLS certificate.

This 'check' endpoint, which is on the admin port (3903 by default), can be
configured in Caddy's global section as follows:

```caddy
{
	...
	on_demand_tls {
		ask http://localhost:3903/check
		interval 2m
		burst 5
	}
	...
}
```

The host section can then be configured with (note that this uses the web
endpoint instead):

```caddy
# For a specific set of subdomains
*.web.garage.tld {
	tls {
		on_demand
	}

	reverse_proxy localhost:3902 192.168.1.2:3902 example.tld:3902
}

# Accept all domains on HTTPS
# Never configure this without global section above
https:// {
	tls {
		on_demand
	}

	reverse_proxy localhost:3902 192.168.1.2:3902 example.tld:3902
}
```

More information on how this endpoint is implemented in Garage is available
in the [Admin API Reference](@/documentation/reference-manual/admin-api.md) page.

### Fileserver browser

Caddy's built-in
[file_server](https://caddyserver.com/docs/caddyfile/directives/file_server)
browser functionality can be extended with the
[caddy-fs-s3](https://github.com/sagikazarmark/caddy-fs-s3) module.

This can be configured to use Garage as a backend with the following
configuration:

```caddy
browse.garage.tld {
    file_server {
        fs s3 {
            bucket test-bucket
            region garage

            endpoint https://s3.garage.tld
            use_path_style
        }

        browse
    }
}
```

Caddy must also be configured with the required `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` environment variables to access the bucket.
