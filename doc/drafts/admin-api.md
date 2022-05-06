# Access control

The admin API uses two different tokens for acces control, that are specified in the config file's `[admin]` section:

- `metrics_token`: the token for accessing the Metrics endpoint (if this token is not set in the config file, the Metrics endpoint can be accessed without access control);
- `admin_token`: the token for accessing all of the other administration endpoints (if this token is not set in the config file, these endpoints can be accessed without access control).

# Administration API endpoints

## Metrics `GET /metrics`

Returns internal Garage metrics in Prometheus format.

## GetClusterStatus `GET /status`

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

## GetClusterLayout `GET /layout`

Returns the cluster's current layout in JSON, including:

- Currently configured cluster layout
- Staged changes to the cluster layout

(the info returned by this endpoint is a subset of the info returned by GetClusterStatus)

Example response body:

```json
{
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
