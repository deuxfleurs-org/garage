+++
title = "Administration API"
weight = 16
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

#### GetClusterStatus `GET /v0/status`

Returns the cluster's current status in JSON, including:

- ID of the node being queried and its version of the Garage daemon
- Live nodes
- Currently configured cluster layout
- Staged changes to the cluster layout

Example response body:

```json
{
  "node": "ec79480e0ce52ae26fd00c9da684e4fa56658d9c64cdcecb094e936de0bfe71f",
  "garage_version": "git:v0.8.0",
  "knownNodes": {
    "ec79480e0ce52ae26fd00c9da684e4fa56658d9c64cdcecb094e936de0bfe71f": {
      "addr": "10.0.0.11:3901",
      "is_up": true,
      "last_seen_secs_ago": 9,
      "hostname": "node1"
    },
    "4a6ae5a1d0d33bf895f5bb4f0a418b7dc94c47c0dd2eb108d1158f3c8f60b0ff": {
      "addr": "10.0.0.12:3901",
      "is_up": true,
      "last_seen_secs_ago": 1,
      "hostname": "node2"
    },
    "23ffd0cdd375ebff573b20cc5cef38996b51c1a7d6dbcf2c6e619876e507cf27": {
      "addr": "10.0.0.21:3901",
      "is_up": true,
      "last_seen_secs_ago": 7,
      "hostname": "node3"
    },
    "e2ee7984ee65b260682086ec70026165903c86e601a4a5a501c1900afe28d84b": {
      "addr": "10.0.0.22:3901",
      "is_up": true,
      "last_seen_secs_ago": 1,
      "hostname": "node4"
    }
  },
  "layout": {
    "version": 12,
    "roles": {
      "ec79480e0ce52ae26fd00c9da684e4fa56658d9c64cdcecb094e936de0bfe71f": {
        "zone": "dc1",
        "capacity": 4,
        "tags": [
          "node1"
        ]
      },
      "4a6ae5a1d0d33bf895f5bb4f0a418b7dc94c47c0dd2eb108d1158f3c8f60b0ff": {
        "zone": "dc1",
        "capacity": 6,
        "tags": [
          "node2"
        ]
      },
      "23ffd0cdd375ebff573b20cc5cef38996b51c1a7d6dbcf2c6e619876e507cf27": {
        "zone": "dc2",
        "capacity": 10,
        "tags": [
          "node3"
        ]
      }
    },
    "stagedRoleChanges": {
      "e2ee7984ee65b260682086ec70026165903c86e601a4a5a501c1900afe28d84b": {
        "zone": "dc2",
        "capacity": 5,
        "tags": [
          "node4"
        ]
      }
    }
  }
}
```

#### ConnectClusterNodes `POST /v0/connect`

Instructs this Garage node to connect to other Garage nodes at specified addresses.

Example request body:

```json
[
    "ec79480e0ce52ae26fd00c9da684e4fa56658d9c64cdcecb094e936de0bfe71f@10.0.0.11:3901",
    "4a6ae5a1d0d33bf895f5bb4f0a418b7dc94c47c0dd2eb108d1158f3c8f60b0ff@10.0.0.12:3901"
]
```

The format of the string for a node to connect to is: `<node ID>@<ip address>:<port>`, same as in the `garage node connect` CLI call.

Example response:

```json
[
    {
        "success": true,
        "error": null
    },
    {
        "success": false,
        "error": "Handshake error"
    }
]
```

#### GetClusterLayout `GET /v0/layout`

Returns the cluster's current layout in JSON, including:

- Currently configured cluster layout
- Staged changes to the cluster layout

(the info returned by this endpoint is a subset of the info returned by GetClusterStatus)

Example response body:

```json
{
  "version": 12,
  "roles": {
    "ec79480e0ce52ae26fd00c9da684e4fa56658d9c64cdcecb094e936de0bfe71f": {
      "zone": "dc1",
      "capacity": 4,
      "tags": [
        "node1"
      ]
    },
    "4a6ae5a1d0d33bf895f5bb4f0a418b7dc94c47c0dd2eb108d1158f3c8f60b0ff": {
      "zone": "dc1",
      "capacity": 6,
      "tags": [
        "node2"
      ]
    },
    "23ffd0cdd375ebff573b20cc5cef38996b51c1a7d6dbcf2c6e619876e507cf27": {
      "zone": "dc2",
      "capacity": 10,
      "tags": [
        "node3"
      ]
    }
  },
  "stagedRoleChanges": {
    "e2ee7984ee65b260682086ec70026165903c86e601a4a5a501c1900afe28d84b": {
      "zone": "dc2",
      "capacity": 5,
      "tags": [
        "node4"
      ]
    }
  }
}
```

#### UpdateClusterLayout `POST /v0/layout`

Send modifications to the cluster layout. These modifications will
be included in the staged role changes, visible in subsequent calls
of `GetClusterLayout`. Once the set of staged changes is satisfactory,
the user may call `ApplyClusterLayout` to apply the changed changes,
or `Revert ClusterLayout` to clear all of the staged changes in
the layout.

Request body format:

```json
{
  <node_id>: {
    "capacity": <new_capacity>,
    "zone": <new_zone>,
    "tags": [
      <new_tag>,
      ...
    ]
  },
  <node_id_to_remove>: null,
  ...
}
```

Contrary to the CLI that may update only a subset of the fields
`capacity`, `zone` and `tags`, when calling this API all of these
values must be specified.


#### ApplyClusterLayout `POST /v0/layout/apply`

Applies to the cluster the layout changes currently registered as
staged layout changes.

Request body format:

```json
{
  "version": 13
}
```

Similarly to the CLI, the body must include the version of the new layout
that will be created, which MUST be 1 + the value of the currently
existing layout in the cluster.

#### RevertClusterLayout `POST /v0/layout/revert`

Clears all of the staged layout changes.

Request body format:

```json
{
  "version": 13
}
```

Reverting the staged changes is done by incrementing the version number
and clearing the contents of the staged change list.
Similarly to the CLI, the body must include the incremented
version number, which MUST be 1 + the value of the currently
existing layout in the cluster.


### Access key operations

#### ListKeys `GET /v0/key`

Returns all API access keys in the cluster.

Example response:

```json
[
  {
    "id": "GK31c2f218a2e44f485b94239e",
    "name": "test"
  },
  {
    "id": "GKe10061ac9c2921f09e4c5540",
    "name": "test2"
  }
]
```

#### CreateKey `POST /v0/key`

Creates a new API access key.

Request body format:

```json
{
    "name": "NameOfMyKey"
}
```

#### ImportKey `POST /v0/key/import`

Imports an existing API key.

Request body format:

```json
{
    "accessKeyId": "GK31c2f218a2e44f485b94239e",
    "secretAccessKey": "b892c0665f0ada8a4755dae98baa3b133590e11dae3bcc1f9d769d67f16c3835",
    "name": "NameOfMyKey"
}
```

#### GetKeyInfo `GET /v0/key?id=<acces key id>`
#### GetKeyInfo `GET /v0/key?search=<pattern>`

Returns information about the requested API access key.

If `id` is set, the key is looked up using its exact identifier (faster).
If `search` is set, the key is looked up using its name or prefix
of identifier (slower, all keys are enumerated to do this).

Example response:

```json
{
  "name": "test",
  "accessKeyId": "GK31c2f218a2e44f485b94239e",
  "secretAccessKey": "b892c0665f0ada8a4755dae98baa3b133590e11dae3bcc1f9d769d67f16c3835",
  "permissions": {
    "createBucket": false
  },
  "buckets": [
    {
      "id": "70dc3bed7fe83a75e46b66e7ddef7d56e65f3c02f9f80b6749fb97eccb5e1033",
      "globalAliases": [
        "test2"
      ],
      "localAliases": [],
      "permissions": {
        "read": true,
        "write": true,
        "owner": false
      }
    },
    {
      "id": "d7452a935e663fc1914f3a5515163a6d3724010ce8dfd9e4743ca8be5974f995",
      "globalAliases": [
        "test3"
      ],
      "localAliases": [],
      "permissions": {
        "read": true,
        "write": true,
        "owner": false
      }
    },
    {
      "id": "e6a14cd6a27f48684579ec6b381c078ab11697e6bc8513b72b2f5307e25fff9b",
      "globalAliases": [],
      "localAliases": [
        "test"
      ],
      "permissions": {
        "read": true,
        "write": true,
        "owner": true
      }
    },
    {
      "id": "96470e0df00ec28807138daf01915cfda2bee8eccc91dea9558c0b4855b5bf95",
      "globalAliases": [
        "alex"
      ],
      "localAliases": [],
      "permissions": {
        "read": true,
        "write": true,
        "owner": true
      }
    }
  ]
}
```

#### DeleteKey `DELETE /v0/key?id=<acces key id>`

Deletes an API access key.

#### UpdateKey `POST /v0/key?id=<acces key id>`

Updates information about the specified API access key.

Request body format:

```json
{
    "name": "NameOfMyKey",
    "allow": {
        "createBucket": true,
    },
    "deny": {}
}
```

All fields (`name`, `allow` and `deny`) are optionnal.
If they are present, the corresponding modifications are applied to the key, otherwise nothing is changed.
The possible flags in `allow` and `deny` are: `createBucket`.


### Bucket operations

#### ListBuckets `GET /v0/bucket`

Returns all storage buckets in the cluster.

Example response:

```json
[
  {
    "id": "70dc3bed7fe83a75e46b66e7ddef7d56e65f3c02f9f80b6749fb97eccb5e1033",
    "globalAliases": [
      "test2"
    ],
    "localAliases": []
  },
  {
    "id": "96470e0df00ec28807138daf01915cfda2bee8eccc91dea9558c0b4855b5bf95",
    "globalAliases": [
      "alex"
    ],
    "localAliases": []
  },
  {
    "id": "d7452a935e663fc1914f3a5515163a6d3724010ce8dfd9e4743ca8be5974f995",
    "globalAliases": [
      "test3"
    ],
    "localAliases": []
  },
  {
    "id": "e6a14cd6a27f48684579ec6b381c078ab11697e6bc8513b72b2f5307e25fff9b",
    "globalAliases": [],
    "localAliases": [
      {
        "accessKeyId": "GK31c2f218a2e44f485b94239e",
        "alias": "test"
      }
    ]
  }
]
```

#### GetBucketInfo `GET /v0/bucket?id=<bucket id>`
#### GetBucketInfo `GET /v0/bucket?globalAlias=<alias>`

Returns information about the requested storage bucket.

If `id` is set, the bucket is looked up using its exact identifier.
If `globalAlias` is set, the bucket is looked up using its global alias.
(both are fast)

Example response:

```json
{
    "id": "afa8f0a22b40b1247ccd0affb869b0af5cff980924a20e4b5e0720a44deb8d39",
        "globalAliases": [],
        "websiteAccess": false,
        "websiteConfig": null,
        "keys": [
        {
            "accessKeyId": "GK31c2f218a2e44f485b94239e",
            "name": "Imported key",
            "permissions": {
                "read": true,
                "write": true,
                "owner": true
            },
            "bucketLocalAliases": [
                "debug"
            ]
        }
        ],
        "objects": 14827,
        "bytes": 13189855625,
        "unfinshedUploads": 0,
        "quotas": {
            "maxSize": null,
            "maxObjects": null
        }
}
```

#### CreateBucket `POST /v0/bucket`

Creates a new storage bucket.

Request body format:

```json
{
    "globalAlias": "NameOfMyBucket"
}
```

OR

```json
{
    "localAlias": {
        "accessKeyId": "GK31c2f218a2e44f485b94239e",
        "alias": "NameOfMyBucket",
        "allow": {
            "read": true,
            "write": true,
            "owner": false
        }
    }
}
```

OR

```json
{}
```

Creates a new bucket, either with a global alias, a local one,
or no alias at all.

Technically, you can also specify both `globalAlias` and `localAlias` and that would create
two aliases, but I don't see why you would want to do that.

#### DeleteBucket `DELETE /v0/bucket?id=<bucket id>`

Deletes a storage bucket. A bucket cannot be deleted if it is not empty.

Warning: this will delete all aliases associated with the bucket!

#### UpdateBucket `PUT /v0/bucket?id=<bucket id>`

Updates configuration of the given bucket.

Request body format:

```json
{
    "websiteAccess": {
        "enabled": true,
        "indexDocument": "index.html",
        "errorDocument": "404.html"
    },
    "quotas": {
        "maxSize": 19029801,
        "maxObjects": null,
    }
}
```

All fields (`websiteAccess` and `quotas`) are optionnal.
If they are present, the corresponding modifications are applied to the bucket, otherwise nothing is changed.

In `websiteAccess`: if `enabled` is `true`, `indexDocument` must be specified.
The field `errorDocument` is optional, if no error document is set a generic
error message is displayed when errors happen. Conversely, if `enabled` is
`false`, neither `indexDocument` nor `errorDocument` must be specified.

In `quotas`: new values of `maxSize` and `maxObjects` must both be specified, or set to `null`
to remove the quotas. An absent value will be considered the same as a `null`. It is not possible
to change only one of the two quotas.

### Operations on permissions for keys on buckets

#### BucketAllowKey `POST /v0/bucket/allow`

Allows a key to do read/write/owner operations on a bucket.

Request body format:

```json
{
    "bucketId": "e6a14cd6a27f48684579ec6b381c078ab11697e6bc8513b72b2f5307e25fff9b",
    "accessKeyId": "GK31c2f218a2e44f485b94239e",
    "permissions": {
        "read": true,
        "write": true,
        "owner": true
    },
}
```

Flags in `permissions` which have the value `true` will be activated.
Other flags will remain unchanged.

#### BucketDenyKey `POST /v0/bucket/deny`

Denies a key from doing read/write/owner operations on a bucket.

Request body format:

```json
{
    "bucketId": "e6a14cd6a27f48684579ec6b381c078ab11697e6bc8513b72b2f5307e25fff9b",
    "accessKeyId": "GK31c2f218a2e44f485b94239e",
    "permissions": {
        "read": false,
        "write": false,
        "owner": true
    },
}
```

Flags in `permissions` which have the value `true` will be deactivated.
Other flags will remain unchanged.


### Operations on bucket aliases

#### GlobalAliasBucket `PUT /v0/bucket/alias/global?id=<bucket id>&alias=<global alias>`

Empty body. Creates a global alias for a bucket.

#### GlobalUnaliasBucket `DELETE /v0/bucket/alias/global?id=<bucket id>&alias=<global alias>`

Removes a global alias for a bucket.

#### LocalAliasBucket `PUT /v0/bucket/alias/local?id=<bucket id>&accessKeyId=<access key ID>&alias=<local alias>`

Empty body. Creates a local alias for a bucket in the namespace of a specific access key.

#### LocalUnaliasBucket `DELETE /v0/bucket/alias/local?id=<bucket id>&accessKeyId<access key ID>&alias=<local alias>`

Removes a local alias for a bucket in the namespace of a specific access key.

