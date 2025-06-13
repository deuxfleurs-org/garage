+++
title = "Administration API"
weight = 60
+++

The Garage administration API is accessible through a dedicated server whose
listen address is specified in the `[admin]` section of the configuration
file (see [configuration file
reference](@/documentation/reference-manual/configuration.md))

**WARNING.** At this point, there is no commitment to the stability of the APIs described in this document.
We will bump the version numbers prefixed to each API endpoint each time the syntax
or semantics change, meaning that code that relies on these endpoints will break
when changes are introduced.

The Garage administration API was introduced in version 0.7.2, this document
does not apply to older versions of Garage.


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

### Metrics-related endpoints

#### Metrics `GET /metrics`

Returns internal Garage metrics in Prometheus format.

#### Health `GET /health`

Used for simple health checks in a cluster setting with an orchestrator.
Returns an HTTP status 200 if the node is ready to answer user's requests,
and an HTTP status 503 (Service Unavailable) if there are some partitions
for which a quorum of nodes is not available.
A simple textual message is also returned in a body with content-type `text/plain`.
See `/v1/health` for an API that also returns JSON output.

### Cluster operations

#### GetClusterStatus `GET /v1/status`

Returns the cluster's current status in JSON, including:

- ID of the node being queried and its version of the Garage daemon
- Live nodes
- Currently configured cluster layout
- Staged changes to the cluster layout

Example response body:

```json
{
  "node": "b10c110e4e854e5aa3f4637681befac755154b20059ec163254ddbfae86b09df",
  "garageVersion": "v1.2.0",
  "garageFeatures": [
    "k2v",
    "lmdb",
    "sqlite",
    "metrics",
    "bundled-libs"
  ],
  "rustVersion": "1.68.0",
  "dbEngine": "LMDB (using Heed crate)",
  "layoutVersion": 5,
  "nodes": [
    {
      "id": "62b218d848e86a64f7fe1909735f29a4350547b54c4b204f91246a14eb0a1a8c",
      "role": {
        "id": "62b218d848e86a64f7fe1909735f29a4350547b54c4b204f91246a14eb0a1a8c",
        "zone": "dc1",
        "capacity": 100000000000,
        "tags": []
      },
      "addr": "10.0.0.3:3901",
      "hostname": "node3",
      "isUp": true,
      "lastSeenSecsAgo": 12,
      "draining": false,
      "dataPartition": {
        "available": 660270088192,
        "total": 873862266880
      },
      "metadataPartition": {
        "available": 660270088192,
        "total": 873862266880
      }
    },
    {
      "id": "a11c7cf18af297379eff8688360155fe68d9061654449ba0ce239252f5a7487f",
      "role": null,
      "addr": "10.0.0.2:3901",
      "hostname": "node2",
      "isUp": true,
      "lastSeenSecsAgo": 11,
      "draining": true,
      "dataPartition": {
        "available": 660270088192,
        "total": 873862266880
      },
      "metadataPartition": {
        "available": 660270088192,
        "total": 873862266880
      }
    },
    {
      "id": "a235ac7695e0c54d7b403943025f57504d500fdcc5c3e42c71c5212faca040a2",
      "role": {
        "id": "a235ac7695e0c54d7b403943025f57504d500fdcc5c3e42c71c5212faca040a2",
        "zone": "dc1",
        "capacity": 100000000000,
        "tags": []
      },
      "addr": "127.0.0.1:3904",
      "hostname": "lindy",
      "isUp": true,
      "lastSeenSecsAgo": 2,
      "draining": false,
      "dataPartition": {
        "available": 660270088192,
        "total": 873862266880
      },
      "metadataPartition": {
        "available": 660270088192,
        "total": 873862266880
      }
    },
    {
      "id": "b10c110e4e854e5aa3f4637681befac755154b20059ec163254ddbfae86b09df",
      "role": {
        "id": "b10c110e4e854e5aa3f4637681befac755154b20059ec163254ddbfae86b09df",
        "zone": "dc1",
        "capacity": 100000000000,
        "tags": []
      },
      "addr": "10.0.0.1:3901",
      "hostname": "node1",
      "isUp": true,
      "lastSeenSecsAgo": 3,
      "draining": false,
      "dataPartition": {
        "available": 660270088192,
        "total": 873862266880
      },
      "metadataPartition": {
        "available": 660270088192,
        "total": 873862266880
      }
    }
  ]
}
```

#### GetClusterHealth `GET /v1/health`

Returns the cluster's current health in JSON format, with the following variables:

- `status`: one of `healthy`, `degraded` or `unavailable`:
  - healthy: Garage node is connected to all storage nodes
  - degraded: Garage node is not connected to all storage nodes, but a quorum of write nodes is available for all partitions
  - unavailable: a quorum of write nodes is not available for some partitions
- `knownNodes`: the number of nodes this Garage node has had a TCP connection to since the daemon started
- `connectedNodes`: the nubmer of nodes this Garage node currently has an open connection to
- `storageNodes`: the number of storage nodes currently registered in the cluster layout
- `storageNodesOk`: the number of storage nodes to which a connection is currently open
- `partitions`: the total number of partitions of the data (currently always 256)
- `partitionsQuorum`: the number of partitions for which a quorum of write nodes is available
- `partitionsAllOk`: the number of partitions for which we are connected to all storage nodes responsible of storing it

Contrarily to `GET /health`, this endpoint always returns a 200 OK HTTP response code.

Example response body:

```json
{
  "status": "degraded",
  "knownNodes": 3,
  "connectedNodes": 3,
  "storageNodes": 4,
  "storageNodesOk": 3,
  "partitions": 256,
  "partitionsQuorum": 256,
  "partitionsAllOk": 64
}
```

#### ConnectClusterNodes `POST /v1/connect`

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

#### GetClusterLayout `GET /v1/layout`

Returns the cluster's current layout in JSON, including:

- Currently configured cluster layout
- Staged changes to the cluster layout

(the info returned by this endpoint is a subset of the info returned by GetClusterStatus)

Example response body:

```json
{
  "version": 12,
  "roles": [
    {
      "id": "ec79480e0ce52ae26fd00c9da684e4fa56658d9c64cdcecb094e936de0bfe71f",
      "zone": "dc1",
      "capacity": 10737418240,
      "tags": [
        "node1"
      ]
    },
    {
      "id": "4a6ae5a1d0d33bf895f5bb4f0a418b7dc94c47c0dd2eb108d1158f3c8f60b0ff",
      "zone": "dc1",
      "capacity": 10737418240,
      "tags": [
        "node2"
      ]
    },
    {
      "id": "23ffd0cdd375ebff573b20cc5cef38996b51c1a7d6dbcf2c6e619876e507cf27",
      "zone": "dc2",
      "capacity": 10737418240,
      "tags": [
        "node3"
      ]
    }
  ],
  "stagedRoleChanges": [
    {
      "id": "e2ee7984ee65b260682086ec70026165903c86e601a4a5a501c1900afe28d84b",
      "remove": false,
      "zone": "dc2",
      "capacity": 10737418240,
      "tags": [
        "node4"
      ]
    }
    {
      "id": "23ffd0cdd375ebff573b20cc5cef38996b51c1a7d6dbcf2c6e619876e507cf27",
      "remove": true,
      "zone": null,
      "capacity": null,
      "tags": null,
    }
  ]
}
```

#### UpdateClusterLayout `POST /v1/layout`

Send modifications to the cluster layout. These modifications will
be included in the staged role changes, visible in subsequent calls
of `GetClusterLayout`. Once the set of staged changes is satisfactory,
the user may call `ApplyClusterLayout` to apply the changed changes,
or `Revert ClusterLayout` to clear all of the staged changes in
the layout.

Request body format:

```json
[
  {
    "id": <node_id>,
    "capacity": <new_capacity>,
    "zone": <new_zone>,
    "tags": [
      <new_tag>,
      ...
    ]
  },
  {
    "id": <node_id_to_remove>,
    "remove": true
  }
]
```

Contrary to the CLI that may update only a subset of the fields
`capacity`, `zone` and `tags`, when calling this API all of these
values must be specified.

This returns the new cluster layout with the proposed staged changes,
as returned by GetClusterLayout.


#### ApplyClusterLayout `POST /v1/layout/apply`

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

This returns the message describing all the calculations done to compute the new
layout, as well as the description of the layout as returned by GetClusterLayout.

#### RevertClusterLayout `POST /v1/layout/revert`

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

This returns the new cluster layout with all changes reverted,
as returned by GetClusterLayout.


### Access key operations

#### ListKeys `GET /v1/key`

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

#### GetKeyInfo `GET /v1/key?id=<acces key id>`
#### GetKeyInfo `GET /v1/key?search=<pattern>`

Returns information about the requested API access key.

If `id` is set, the key is looked up using its exact identifier (faster).
If `search` is set, the key is looked up using its name or prefix
of identifier (slower, all keys are enumerated to do this).

Optionnally, the query parameter `showSecretKey=true` can be set to reveal the
associated secret access key.

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

#### CreateKey `POST /v1/key`

Creates a new API access key.

Request body format:

```json
{
    "name": "NameOfMyKey"
}
```

This returns the key info, including the created secret key,
in the same format as the result of GetKeyInfo.

#### ImportKey `POST /v1/key/import`

Imports an existing API key.
This will check that the imported key is in the valid format, i.e.
is a key that could have been generated by Garage.

Request body format:

```json
{
    "accessKeyId": "GK31c2f218a2e44f485b94239e",
    "secretAccessKey": "b892c0665f0ada8a4755dae98baa3b133590e11dae3bcc1f9d769d67f16c3835",
    "name": "NameOfMyKey"
}
```

This returns the key info in the same format as the result of GetKeyInfo.

#### UpdateKey `POST /v1/key?id=<acces key id>`

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

All fields (`name`, `allow` and `deny`) are optional.
If they are present, the corresponding modifications are applied to the key, otherwise nothing is changed.
The possible flags in `allow` and `deny` are: `createBucket`.

This returns the key info in the same format as the result of GetKeyInfo.

#### DeleteKey `DELETE /v1/key?id=<acces key id>`

Deletes an API access key.


### Bucket operations

#### ListBuckets `GET /v1/bucket`

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

#### GetBucketInfo `GET /v1/bucket?id=<bucket id>`
#### GetBucketInfo `GET /v1/bucket?globalAlias=<alias>`

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
        "unfinishedUploads": 1,
        "unfinishedMultipartUploads": 1,
        "unfinishedMultipartUploadParts": 11,
        "unfinishedMultipartUploadBytes": 41943040,
        "quotas": {
            "maxSize": null,
            "maxObjects": null
        }
}
```

#### CreateBucket `POST /v1/bucket`

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

#### UpdateBucket `PUT /v1/bucket?id=<bucket id>`

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

All fields (`websiteAccess` and `quotas`) are optional.
If they are present, the corresponding modifications are applied to the bucket, otherwise nothing is changed.

In `websiteAccess`: if `enabled` is `true`, `indexDocument` must be specified.
The field `errorDocument` is optional, if no error document is set a generic
error message is displayed when errors happen. Conversely, if `enabled` is
`false`, neither `indexDocument` nor `errorDocument` must be specified.

In `quotas`: new values of `maxSize` and `maxObjects` must both be specified, or set to `null`
to remove the quotas. An absent value will be considered the same as a `null`. It is not possible
to change only one of the two quotas.

#### DeleteBucket `DELETE /v1/bucket?id=<bucket id>`

Deletes a storage bucket. A bucket cannot be deleted if it is not empty.

Warning: this will delete all aliases associated with the bucket!


### Operations on permissions for keys on buckets

#### BucketAllowKey `POST /v1/bucket/allow`

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

#### BucketDenyKey `POST /v1/bucket/deny`

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

#### GlobalAliasBucket `PUT /v1/bucket/alias/global?id=<bucket id>&alias=<global alias>`

Empty body. Creates a global alias for a bucket.

#### GlobalUnaliasBucket `DELETE /v1/bucket/alias/global?id=<bucket id>&alias=<global alias>`

Removes a global alias for a bucket.

#### LocalAliasBucket `PUT /v1/bucket/alias/local?id=<bucket id>&accessKeyId=<access key ID>&alias=<local alias>`

Empty body. Creates a local alias for a bucket in the namespace of a specific access key.

#### LocalUnaliasBucket `DELETE /v1/bucket/alias/local?id=<bucket id>&accessKeyId<access key ID>&alias=<local alias>`

Removes a local alias for a bucket in the namespace of a specific access key.

