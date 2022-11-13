+++
title = "Administration API"
weight = 60
+++

The Garage administration API is accessible through a dedicated server whose
listen address is specified in the `[admin]` section of the configuration
file (see [configuration file
reference](@/documentation/reference-manual/configuration.md))

**WARNING.** At this point, there is no comittement to stability of the APIs described in this document.
We will bump the version numbers prefixed to each API endpoint at each time the syntax
or semantics change, meaning that code that relies on these endpoint will break
when changes are introduced.

The Garage administration API was introduced in version 0.7.2, this document
does not apply to older versions of Garage.


## Access control

The admin API uses two different tokens for acces control, that are specified in the config file's `[admin]` section:

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

### Metrics-related endpoints

#### Metrics `GET /metrics`

Returns internal Garage metrics in Prometheus format.

### Cluster operations

These endpoints are defined on a dedicated [Redocly page](https://garagehq.deuxfleurs.fr/api/garage-admin-v0.html). You can also download its [OpenAPI specification](https://garagehq.deuxfleurs.fr/api/garage-admin-v0.yml).

Requesting the API from the command line can be as simple as running:

```bash
curl -H 'Authorization: Bearer s3cr3t' http://localhost:3903/v0/status | jq
```

For more advanced use cases, we recommend using a SDK.  
[Go to the "Build your own app" section to know how to use our SDKs](@/documentation/build/_index.md)
