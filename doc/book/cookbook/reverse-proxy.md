+++
title = "Configuring a reverse proxy"
weight = 30
+++

The main reason to add a reverse proxy in front of Garage is to provide TLS to your users.

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

### Defining backends

First, we need to tell to nginx how to access our Garage cluster.
Because we have multiple nodes, we want to leverage all of them by spreading the load.

In nginx, we can do that with the upstream directive.
Because we have 2 endpoints: one for the S3 API and one to serve websites,
we create 2 backends named respectively `s3_backend` and `web_backend`.

A documented example for the `s3_backend` assuming you chose port 3900:

```nginx
upstream s3_backend {
  # if you have a garage instance locally
  server 127.0.0.1:3900;
  # you can also put your other instances
  server 192.168.1.3:3900;
  # domain names also work
  server garage1.example.com:3900;
  # you can assign weights if you have some servers 
  # that are more powerful than others
  server garage2.example.com:3900 weight=2;
}
```

A similar example for the `web_backend` assuming you chose port 3902:

```nginx
upstream web_backend {
  server 127.0.0.1:3902;
  server 192.168.1.3:3902;
  server garage1.example.com:3902;
  server garage2.example.com:3902 weight=2;
}
```

### Exposing the S3 API

The configuration section for the S3 API is simple as we only support path-access style yet.
We simply configure the TLS parameters and forward all the requests to the backend:

```nginx
server {
  listen [::]:443 http2 ssl;
  ssl_certificate     /tmp/garage.crt;
  ssl_certificate_key /tmp/garage.key;

  # should be the endpoint you want
  # aws uses s3.amazonaws.com for example
  server_name garage.example.com;

  location / {
    proxy_pass http://s3_backend;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header Host $host;
  }
}

```

### Exposing the web endpoint

The web endpoint is a bit more complicated to configure as it listens on many different `Host` fields.
To better understand the logic involved, you can refer to the [Exposing buckets as websites](@/documentation/cookbook/exposing-websites.md) section.
Also, for some applications, you may need to serve CORS headers: Garage can not serve them directly but we show how we can use nginx to serve them.
You can use the following example as your starting point:

```nginx
server {
  listen [::]:443 http2 ssl;
  ssl_certificate     /tmp/garage.crt;
  ssl_certificate_key /tmp/garage.key;

  # We list all the Hosts fields that can access our buckets
  server_name *.web.garage
              example.com
              my-site.tld
              ;

  location / {
    # Add these headers only if you want to allow CORS requests
    # For production use, more specific rules would be better for your security
    add_header Access-Control-Allow-Origin *;
    add_header Access-Control-Max-Age 3600;
    add_header Access-Control-Expose-Headers Content-Length;
    add_header Access-Control-Allow-Headers Range;

    # We do not forward OPTIONS requests to Garage
    # as it does not support them but they are needed for CORS.
    if ($request_method = OPTIONS) {
      return 200;
    }

    proxy_pass http://web_backend;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header Host $host;
  }
}
```


## Apache httpd

@TODO

## Traefik

@TODO
