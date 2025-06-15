+++
title = "Administration API"
weight = 40
+++

The Garage administration API is accessible through a dedicated server whose
listen address is specified in the `[admin]` section of the configuration
file (see [configuration file
reference](@/documentation/reference-manual/configuration.md)).

The current version of the admin API is v2.  No breaking changes to the Garage
administration API will be published outside of a major release.

History of previous versions:

 - Before Garage v0.7.2 - no admin API
 - Garage v0.7.2 - admin API v0
 - Garage v0.9.0 - admin API v1, deprecate admin API v0
 - Garage v2.0.0 - admin API v2, deprecate admin API v1

## Access control

### Using an API token

Administration API tokens tokens are used as simple HTTP bearer tokens. In
other words, to authenticate access to an admin API endpoint, add the following
HTTP header to your request:

```
Authorization: Bearer <token>
```

### User-defined API tokens

Cluster administrators may dynamically define administration tokens using the CLI commands under `garage admin-token`.
Such tokens may be limited in scope, meaning that they may enable access to only a subset of API calls.
They may also have an expiration date to limit their use in time.

Here is an example to create an administration token that is valid for 30 days
and gives access to only a subset of API calls, allowing it to create buckets
and access keys and give keys permissions on buckets:

```bash
$ garage admin-token create --expires-in 30d \
    --scope ListBuckets,GetBucketInfo,ListKeys,GetKeyInfo,CreateBucket,CreateKey,AllowBucketKey,DenyBucketKey \
    my-token
This is your secret bearer token, it will not be shown again by Garage:

  8ed1830b10a276ff57061950.kOSIpxWK9zSGbTO9Xadpv3YndSFWma0_snXcYHaORXk

==== ADMINISTRATION TOKEN INFORMATION ====
Token ID:    8ed1830b10a276ff57061950
Token name:  my-token
Created:     2025-06-15 15:12:44.160 +02:00
Validity:    valid
Expiration:  2025-07-15 15:12:44.117 +02:00

Scope:       ListBuckets
             GetBucketInfo
             ListKeys
             GetKeyInfo
             CreateBucket
             CreateKey
             AllowBucketKey
             DenyBucketKey
```

When running this command, your token will be shown only once and **will never
be shown again by Garage**, so make sure to save it directly.  The token is
hashed internally, and is identified by its prefix (32 hex digits followed by a
dot) which is saved in clear.

When running `garage admin-token list`, you might see something like this:

```
ID                        Created     Name                                       Expiration                      Scope
-                         -           metrics_token (from daemon configuration)  never                           Metrics
8ed1830b10a276ff57061950  2025-06-15  my-token                                   2025-07-15 15:12:44.117 +02:00  ListBuckets, ... (8)
```

### Master API tokens

The admin API can also use two different master tokens for access control,
specified in the config file's `[admin]` section:

- `metrics_token`: the token for accessing the Metrics endpoint. If this token
  is not set in the config file, the Metrics endpoint can be accessed without
  access control.

- `admin_token`: the token for accessing all of the other administration
  endpoints. If this token is not set in the config file, access to these
  endpoints is only possible with a user-defined admin token.

With the introduction of multiple user-defined admin tokens, the use of master
API tokens is now discouraged.


## Using the admin API

All of the admin API endpoints are described in the OpenAPI specification:

 - APIv2 - [HTML spec](https://garagehq.deuxfleurs.fr/api/garage-admin-v2.html) - [OpenAPI JSON](https://garagehq.deuxfleurs.fr/api/garage-admin-v2.json)
 - APIv1 (deprecated) - [HTML spec](https://garagehq.deuxfleurs.fr/api/garage-admin-v1.html) - [OpenAPI YAML](https://garagehq.deuxfleurs.fr/api/garage-admin-v1.yml)
 - APIv0 (deprecated) - [HTML spec](https://garagehq.deuxfleurs.fr/api/garage-admin-v0.html) - [OpenAPI YAML](https://garagehq.deuxfleurs.fr/api/garage-admin-v0.yml)

Making a request to the API from the command line can be as simple as running:

```bash
curl -H 'Authorization: Bearer s3cr3t' http://localhost:3903/v2/GetClusterStatus | jq
```

For more advanced use cases, we recommend using an SDK.  
[Go to the "Build your own app" section to know how to use our SDKs](@/documentation/build/_index.md)

### Making API calls from the `garage` CLI

Since v2.0.0, the `garage` binary provides a subcommand `garage json-api` that
allows you to invoke the API without making an HTTP request.  This can be
useful for scripting Garage deployments.

`garage json-api` proxies API calls through Garage's internal RPC protocol,
therefore it does not require any form of authentication: RPC connection
parameters are discovered automatically to contact the locally-running Garage
instance (as when running any other `garage` CLI command).

For simple calls that take no parameters, usage is as follows:

```
$ garage json-api GetClusterHealth
{
  "connectedNodes": 3,
  "knownNodes": 3,
  "partitions": 256,
  "partitionsAllOk": 256,
  "partitionsQuorum": 256,
  "status": "healthy",
  "storageNodes": 3,
  "storageNodesOk": 3
}
```

If you need to specify a JSON body for your call, you can add it directly after
the name of the function you are calling:

```
$ garage json-api CreateAdminToken '{"name": "test"}'
```

Or you can feed it through stdin by adding a `-` as the last command parameter:

```
$ garage json-api CreateAdminToken -
{"name": "test"}
<EOF>
```

For admin API calls that would have taken query parameters in their HTTP version, these parameters can be passed in the JSON body object:

```
$ garage json-api GetAdminTokenInfo '{"id":"b0e6e0ace2c0b2aca4cdb2de"}'
```

For admin API calls that take both query parameters and a JSON body, combine them in the following fashion:

```
$ garage json-api UpdateAdminToken '{"id":"b0e6e0ace2c0b2aca4cdb2de", "body":{"name":"not a test"}}'
```

## Special administration API endpoints

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
Consult the full health check API endpoint at /v2/GetClusterHealth for more details
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
