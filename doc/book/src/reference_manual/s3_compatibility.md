# S3 Compatibility status

## Global S3 features

Implemented:

- path-style URLs (`garage.tld/bucket/key`)
- vhost-style URLs (`bucket.garage.tld/key`)
- putting and getting objects in buckets
- multipart uploads
- listing objects
- access control on a per-key-per-bucket basis

Not implemented:

- object-level ACL
- [object versioning](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/166)
- encryption
- most `x-amz-` headers


## Endpoint implementation

All APIs that are not mentionned are not implemented and will return a 400 bad request.

| Endpoint                     | Status                           |
|------------------------------|----------------------------------|
| AbortMultipartUpload         | Implemented                      |
| CompleteMultipartUpload      | Implemented                      |
| CopyObject                   | Implemented                      |
| CreateBucket                 | Unsupported, stub (see below)    |
| CreateMultipartUpload        | Implemented                      |
| DeleteBucket                 | Unsupported (see below)          |
| DeleteBucketWebsite          | Implemented                      |
| DeleteObject                 | Implemented                      |
| DeleteObjects                | Implemented                      |
| GetBucketLocation            | Implemented                      |
| GetBucketVersioning          | Stub (see below)                 |
| GetBucketWebsite             | Unsupported                      |
| GetObject                    | Implemented                      |
| HeadBucket                   | Implemented                      |
| HeadObject                   | Implemented                      |
| ListBuckets                  | Implemented                      |
| ListObjects                  | Implemented, bugs? (see below)   |
| ListObjectsV2                | Implemented                      |
| PutObject                    | Implemented                      |
| PutBucketWebsite             | Partially implemented (see below)|
| UploadPart                   | Implemented                      |



- **CreateBucket:** Garage does not yet accept creating buckets or giving access using API calls, it has to be done using the CLI tools. CreateBucket will return a 200 if the bucket exists and user has write access, and a 403 Forbidden in all other cases.

- **DeleteBucket:** Garage does not yet accept deleting buckets using API calls, it has to be done using the CLI tools. This request will return a 403 Forbidden.

- **GetBucketVersioning:** Stub implementation (Garage does not yet support versionning so this always returns
"versionning not enabled").

- **ListObjects:** Implemented, but there isn't a very good specification of what `encoding-type=url` covers so there might be some encoding bugs. In our implementation the url-encoded fields are in the same in ListObjects as they are in ListObjectsV2.

- **PutBucketWebsite:** Implemented, but only store if website is enabled, not more complexe informations.

- **GetBucketWebsite:** Not implemented yet, will be when PubBucketWebsite store more informations.
