# Configuring a reverse proxy

## Nginx

```nginx
server {
  # In production you should use TLS instead of plain HTTP
  listen [::]:80;

  # We 
  server_name *.web.garage
              example.com
              my-site.tld
              ;

  location / {
    add_header Access-Control-Allow-Origin *;
    add_header Access-Control-Max-Age 3600;
    add_header Access-Control-Expose-Headers Content-Length;
    add_header Access-Control-Allow-Headers Range;

    # We do not forward OPTIONS requests to Garage
    # as it does not support them but they are needed for CORS.
    if ($request_method = OPTIONS) {
      return 200;
    }

    # If your do not have a Garage instance on the reverse proxy, change the URL here.
    proxy_pass http://127.0.0.1:3902;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header Host $host;
  }
}
```


## Apache httpd

## Traefik
