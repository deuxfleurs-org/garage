## S3 Compatibility status

### Global S3 features

Implemented:

- path-style URLs (`garage.tld/bucket/key`)
- putting and getting objects in buckets
- multipart uploads
- listing objects
- access control on a per-key-per-bucket basis

Not implemented:

- vhost-style URLs (`bucket.garage.tld/key`)
- object-level ACL
- encryption
- most `x-amz-` headers


### Endpoint implementation

All APIs that are not mentionned are not implemented and will return a 400 bad request.

#### AbortMultipartUpload

Implemented.

#### CompleteMultipartUpload

Implemented badly. Garage will not check that all the parts stored correspond to the list given by the client in the request body. This means that the multipart upload might be completed with an invalid size. This is a bug and will be fixed.

#### CopyObject

Implemented.

#### CreateBucket

Garage does not accept creating buckets or giving access using API calls, it has to be done using the CLI tools. CreateBucket will return a 200 if the bucket exists and user has write access, and a 403 Forbidden in all other cases.

#### CreateMultipartUpload

Implemented.

#### DeleteBucket

Garage does not accept deleting buckets using API calls, it has to be done using the CLI tools. This request will return a 403 Forbidden.

#### DeleteObject

Implemented.

#### DeleteObjects

Implemented.

#### GetObject

Implemented.

#### HeadBucket

Implemented.

#### HeadObject

Implemented.

#### ListObjects

Implemented, but there isn't a very good specification of what `encoding-type=url` covers so there might be some encoding bugs. In our implementation the url-encoded fields are in the same in ListObjects as they are in ListObjectsV2.

#### ListObjectsV2

Implemented.

#### PutObject

Implemented.

#### UploadPart

Implemented.

