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

## Traefik

@TODO
