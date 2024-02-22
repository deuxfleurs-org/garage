+++
title = "Administration API"
weight = 40
+++

The Garage administration API is accessible through a dedicated server whose
listen address is specified in the `[admin]` section of the configuration
file (see [configuration file
reference](@/documentation/reference-manual/configuration.md))

**WARNING.** At this point, there is no commitment to the stability of the APIs described in this document.
We will bump the version numbers prefixed to each API endpoint each time the syntax
or semantics change, meaning that code that relies on these endpoint will break
when changes are introduced.

Versions:
 - Before Garage 0.7.2 - no admin API
 - Garage 0.7.2 - admin APIv0
 - Garage 0.9.0 - admin APIv1, deprecate admin APIv0



## Access control

The admin API uses two different tokens for access control, that are specified in the config file's `[admin]` section:

- `metrics_token`: the token for accessing the Metrics endpoint (if this token
  is not set in the config file, the Metrics endpoint can be accessed without
  access control);

- `admin_token`: the token for accessing all of the other administration
  endpoints (if this token is not set in the config file, access to these
  endpoints is disabled entirely).

These tokens are used as simple HTTP bearer tokens. In other words, to
authenticate access to an admin API endpoint, add the following HTTP header
to your request:

```
Authorization: Bearer <token>
```

## Administration API endpoints

### Metrics `GET /metrics`

Returns internal Garage metrics in Prometheus format.
The metrics are directly documented when returned by the API.

**Example:**

```
$ curl -i http://localhost:3903/metrics
HTTP/1.1 200 OK
content-type: text/plain; version=0.0.4
content-length: 12145
date: Tue, 08 Aug 2023 07:25:05 GMT

# HELP api_admin_error_counter Number of API calls to the various Admin API endpoints that resulted in errors
# TYPE api_admin_error_counter counter
api_admin_error_counter{api_endpoint="CheckWebsiteEnabled",status_code="400"} 1
api_admin_error_counter{api_endpoint="CheckWebsiteEnabled",status_code="404"} 3
# HELP api_admin_request_counter Number of API calls to the various Admin API endpoints
# TYPE api_admin_request_counter counter
api_admin_request_counter{api_endpoint="CheckWebsiteEnabled"} 7
api_admin_request_counter{api_endpoint="Health"} 3
# HELP api_admin_request_duration Duration of API calls to the various Admin API endpoints
...
```

### Health `GET /health`

Returns `200 OK` if enough nodes are up to have a quorum (ie. serve requests),
otherwise returns `503 Service Unavailable`.

**Example:**

```
$ curl -i http://localhost:3903/health
HTTP/1.1 200 OK
content-type: text/plain
content-length: 102
date: Tue, 08 Aug 2023 07:22:38 GMT

Garage is fully operational
Consult the full health check API endpoint at /v0/health for more details
```

### On-demand TLS `GET /check`

To prevent abuse for on-demand TLS, Caddy developers have specified an endpoint that can be queried by the reverse proxy
to know if a given domain is allowed to get a certificate. Garage implements these endpoints to tell if a given domain is handled by Garage or is garbage.

Garage responds with the following logic:
 - If the domain matches the pattern `<bucket-name>.<s3_api.root_domain>`, returns 200 OK
 - If the domain matches the pattern `<bucket-name>.<s3_web.root_domain>` and website is configured for `<bucket>`, returns 200 OK
 - If the domain matches the pattern `<bucket-name>` and website is configured for `<bucket>`, returns 200 OK
 - Otherwise, returns 404 Not Found, 400 Bad Request or 5xx requests.

*Note 1: because in the path-style URL mode, there is only one domain that is not known by Garage, hence it is not supported by this API endpoint.
You must manually declare the domain in your reverse-proxy. Idem for K2V.*

*Note 2: buckets in a user's namespace are not supported yet by this endpoint. This is a limitation of this endpoint currently.*

**Example:** Suppose a Garage instance is configured with `s3_api.root_domain = .s3.garage.localhost` and `s3_web.root_domain = .web.garage.localhost`.

With a private `media` bucket (name in the global namespace, website is disabled), the endpoint will feature the following behavior:

```
$ curl -so /dev/null -w "%{http_code}" http://localhost:3903/check?domain=media.s3.garage.localhost
200
$ curl -so /dev/null -w "%{http_code}" http://localhost:3903/check?domain=media
400
$ curl -so /dev/null -w "%{http_code}" http://localhost:3903/check?domain=media.web.garage.localhost
400
```

With a public `example.com` bucket (name in the global namespace, website is activated), the endpoint will feature the following behavior:

```
$ curl -so /dev/null -w "%{http_code}" http://localhost:3903/check?domain=example.com.s3.garage.localhost
200
$ curl -so /dev/null -w "%{http_code}" http://localhost:3903/check?domain=example.com
200
$ curl -so /dev/null -w "%{http_code}" http://localhost:3903/check?domain=example.com.web.garage.localhost
200
```


**References:**
 - [Using On-Demand TLS](https://caddyserver.com/docs/automatic-https#using-on-demand-tls)
 - [Add option for a backend check to approve use of on-demand TLS](https://github.com/caddyserver/caddy/pull/1939)
 - [Serving tens of thousands of domains over HTTPS with Caddy](https://caddy.community/t/serving-tens-of-thousands-of-domains-over-https-with-caddy/11179)

### Cluster operations

These endpoints have a dedicated OpenAPI spec.
 - APIv1 - [HTML spec](https://garagehq.deuxfleurs.fr/api/garage-admin-v1.html) - [OpenAPI YAML](https://garagehq.deuxfleurs.fr/api/garage-admin-v1.yml)
 - APIv0 (deprecated) - [HTML spec](https://garagehq.deuxfleurs.fr/api/garage-admin-v0.html) - [OpenAPI YAML](https://garagehq.deuxfleurs.fr/api/garage-admin-v0.yml)

Requesting the API from the command line can be as simple as running:

```bash
curl -H 'Authorization: Bearer s3cr3t' http://localhost:3903/v0/status | jq
```

For more advanced use cases, we recommend using a SDK.  
[Go to the "Build your own app" section to know how to use our SDKs](@/documentation/build/_index.md)
