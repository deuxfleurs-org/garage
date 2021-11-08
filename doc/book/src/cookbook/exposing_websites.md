# Exposing buckets as websites

You can expose your bucket as a website with this simple command:

```bash
garage bucket website --allow my-website
```

Now it will be **publicly** exposed on the web endpoint (by default listening on port 3902).

Our website serving logic is as follow:
  - Supports only static websites (no support for PHP or other languages)
  - Does not support directory listing
  - The index is defined in your `garage.toml`. ([ref](/reference_manual/configuration.html#index))

Now we need to infer the URL of your website through your bucket name.
Let assume:
  - we set `root_domain = ".web.example.com"` in `garage.toml` ([ref](/reference_manual/configuration.html#root_domain))
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
 - or configure a [reverse proxy](reverse_proxy.html) in front of Garage to add TLS (HTTPS), CORS support, etc.

You can also take a look at [Website Integration](/connect/websites.html) to see how you can add Garage to your workflow.
