# Gateways

Gateways allow you to expose Garage endpoints (S3 API and websites) without storing data on the node.

## Benefits

You can configure Garage as a gateway on all nodes that will consume your S3 API, it will provide you the following benefits:

  - **It removes 1 or 2 network RTT** Instead of (querying your reverse proxy then) querying a random node of the cluster that will forward your request to the nodes effectively storing the data, your local gateway will directly knows which node to query. 

  - **It ease server management** Instead of tracking in your reverse proxy and DNS what are the current Garage nodes, your gateway being part of the cluster keeps this information for you. In your software, you will always specify `http://localhost:3900`.

  - **It simplifies security** Instead of having to maintain and renew a TLS certificate, you leverage the Secret Handshake protocol we use for our cluster. The S3 API protocol will be in plain text but limited to your local machine.

## Limitations

Currently it will not work with minio client. Follow issue [#64](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/64) for more information.

## Spawn a Gateway

The instructions are similar to a regular node, the only option that is different is while configuring the node, you must set the `--gateway` parameter:

```bash
garage node configure --gateway --tag gw1 xxxx
```

Then use `http://localhost:3900` when a S3 endpoint is required:

```bash
aws --endpoint-url http://127.0.0.1:3900 s3 ls
```
