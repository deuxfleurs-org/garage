+++
title = "Javascript"
weight = 10
+++

## S3

*Coming soon*.

Some refs:
  - Minio SDK
    - [Reference](https://docs.min.io/docs/javascript-client-api-reference.html)

  - Amazon aws-sdk-js
    - [Installation](https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/getting-started.html)
    - [Reference](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html)
    - [Example](https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/s3-example-creating-buckets.html)

## K2V

*Coming soon*

## Administration

Install the SDK with:

```bash
npm install --save git+https://git.deuxfleurs.fr/garage-sdk/garage-admin-sdk-js.git
```

A short example:

```javascript
const garage = require('garage_administration_api_v1garage_v0_9_0');

const api = new garage.ApiClient("http://127.0.0.1:3903/v1");
api.authentications['bearerAuth'].accessToken = "s3cr3t";

const [node, layout, key, bucket] = [
  new garage.NodesApi(api),
  new garage.LayoutApi(api),
  new garage.KeyApi(api),
  new garage.BucketApi(api),
];

node.getNodes().then((data) => {
  console.log(`nodes: ${Object.values(data.knownNodes).map(n => n.hostname)}`)
}, (error) => {
  console.error(error);
});
```

See also:
 - [sdk repository](https://git.deuxfleurs.fr/garage-sdk/garage-admin-sdk-js)
 - [examples](https://git.deuxfleurs.fr/garage-sdk/garage-admin-sdk-generator/src/branch/main/example/javascript)
