+++
title = "Configuring a gateway node"
weight = 20
+++

Gateways allow you to expose Garage endpoints (S3 API and websites) without storing data on the node.

## Benefits

You can configure Garage as a gateway on all nodes that will consume your S3 API, it will provide you the following benefits:

  - **It removes 1 or 2 network RTT.** Instead of (querying your reverse proxy then) querying a random node of the cluster that will forward your request to the nodes effectively storing the data, your local gateway will directly knows which node to query.

  - **It eases server management.** Instead of tracking in your reverse proxy and DNS what are the current Garage nodes, your gateway being part of the cluster keeps this information for you. In your software, you will always specify `http://localhost:3900`.

  - **It simplifies security.** Instead of having to maintain and renew a TLS certificate, you leverage the Secret Handshake protocol we use for our cluster. The S3 API protocol will be in plain text but limited to your local machine.


## Spawn a Gateway

The instructions are similar to a regular node, the only option that is different is while configuring the node, you must set the `--gateway` parameter:

```bash
garage layout assign --gateway --tag gw1 -z dc1 <node_id>
garage layout show    # review the changes you are making
garage layout apply   # once satisfied, apply the changes
```

Then use `http://localhost:3900` when a S3 endpoint is required:

```bash
aws --endpoint-url http://127.0.0.1:3900 s3 ls
```

If a newly added gateway node seems to not be working, do a full table resync to ensure that bucket and key list are correctly propagated:

```bash
garage repair -a --yes tables
```
