# Access control

The admin API uses two different tokens for acces control, that are specified in the config file's `[admin]` section:

- `metrics_token`: the token for accessing the Metrics endpoint (if this token is not set in the config file, the Metrics endpoint can be accessed without access control);
- `admin_token`: the token for accessing all of the other administration endpoints (if this token is not set in the config file, these endpoints can be accessed without access control).

# Administration API endpoints

## Metrics-related endpoints

### Metrics `GET /metrics`

Returns internal Garage metrics in Prometheus format.

## Cluster operations

### GetClusterStatus `GET /status`

Returns the cluster's current status in JSON, including:

- Live nodes
- Currently configured cluster layout
- Staged changes to the cluster layout

Example response body:

```json
{
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

### GetClusterLayout `GET /layout`

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

### UpdateClusterLayout `POST /layout`

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


### ApplyClusterLayout `POST /layout/apply`

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

### RevertClusterLayout `POST /layout/revert`

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

