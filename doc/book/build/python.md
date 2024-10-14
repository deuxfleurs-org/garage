+++
title = "Python"
weight = 20
+++

## S3

### Using Minio SDK

First install the SDK:

```bash
pip3 install minio
```

Then instantiate a client object using garage root domain, api key and secret:

```python
import minio

client = minio.Minio(
  "your.domain.tld",
  "GKyourapikey",
  "abcd[...]1234",
  # Force the region, this is specific to garage
  region="garage",
)
```

Then use all the standard S3 endpoints as implemented by the Minio SDK:

```
# List buckets
print(client.list_buckets())

# Put an object containing 'content' to /path in bucket named 'bucket':
content = b"content"
client.put_object(
  "bucket",
  "path",
  io.BytesIO(content),
  len(content),
)

# Read the object back and check contents
data = client.get_object("bucket", "path").read()
assert data == content
```

For further documentation, see the Minio SDK
[Reference](https://docs.min.io/docs/python-client-api-reference.html)

### Using Amazon boto3

*Coming soon*

See the official documentation:
  - [Installation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html)
  - [Reference](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html)
  - [Example](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html)

## K2V

*Coming soon*

## Admin API

You need at least Python 3.6, pip, and setuptools.
Because the python package is in a subfolder, the command is a bit more complicated than usual:

```bash
pip3 install --user 'git+https://git.deuxfleurs.fr/garage-sdk/garage-admin-sdk-python'
```

Now, let imagine you have a fresh Garage instance running on localhost, with the admin API configured on port 3903 with the bearer `s3cr3t`:

```python
import garage_admin_sdk
from garage_admin_sdk.apis import *
from garage_admin_sdk.models import *

configuration = garage_admin_sdk.Configuration(
  host = "http://localhost:3903/v1",
  access_token = "s3cr3t"
)

# Init APIs
api = garage_admin_sdk.ApiClient(configuration)
nodes, layout, keys, buckets = NodesApi(api), LayoutApi(api), KeyApi(api), BucketApi(api)

# Display some info on the node
status = nodes.get_nodes()
print(f"running garage {status.garage_version}, node_id {status.node}")

# Change layout of this node
current = layout.get_layout()
layout.add_layout([
  NodeRoleChange(
    id = status.node,
    zone = "dc1",
    capacity = 1000000000,
    tags = [ "dev" ],
  )
])
layout.apply_layout(LayoutVersion(
  version = current.version + 1
))

# Create key, allow it to create buckets
kinfo = keys.add_key(AddKeyRequest(name="openapi"))

allow_create = UpdateKeyRequestAllow(create_bucket=True)
keys.update_key(kinfo.access_key_id, UpdateKeyRequest(allow=allow_create))

# Create a bucket, allow key, set quotas
binfo = buckets.create_bucket(CreateBucketRequest(global_alias="documentation"))
binfo = buckets.allow_bucket_key(AllowBucketKeyRequest(
  bucket_id=binfo.id,
  access_key_id=kinfo.access_key_id,
  permissions=AllowBucketKeyRequestPermissions(read=True, write=True, owner=True),
))
binfo = buckets.update_bucket(binfo.id, UpdateBucketRequest(
  quotas=UpdateBucketRequestQuotas(max_size=19029801,max_objects=1500)))

# Display key
print(f"""
cluster ready
key id is {kinfo.access_key_id}
secret key is {kinfo.secret_access_key}
bucket {binfo.global_aliases[0]} contains {binfo.objects}/{binfo.quotas.max_objects} objects
""")
```

*This example is named `short.py` in the example folder. Other python examples are also available.*

See also:
 - [sdk repo](https://git.deuxfleurs.fr/garage-sdk/garage-admin-sdk-python)
 - [examples](https://git.deuxfleurs.fr/garage-sdk/garage-admin-sdk-generator/src/branch/main/example/python)

