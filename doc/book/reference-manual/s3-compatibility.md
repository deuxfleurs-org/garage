+++
title = "S3 Compatibility status"
weight = 20
+++

## DISCLAIMER

**The compatibility list for other platforms is given only for informational
purposes and based on available documentation.** They are sometimes completed,
in a best effort approach, with the source code and inputs from maintainers
when documentation is lacking. We are not proactively monitoring new versions
of each software: check the modification history to know when the page has been
updated for the last time. Some entries will be inexact or outdated. For any
serious decision, you must make your own tests.
**The official documentation of each project can be accessed by clicking on the
project name in the column header.**

Feel free to open a PR to suggest fixes this table. Minio is missing because they do not provide a public S3 compatibility list.

## Update history

- 2022-02-07 - First version of this page
- 2022-05-25 - Many Ceph S3 endpoints are not documented but implemented. Following a notification from the Ceph community, we added them.



## High-level features

| Feature                      | Garage                           | [Openstack Swift](https://docs.openstack.org/swift/latest/s3_compat.html) | [Ceph Object Gateway](https://docs.ceph.com/en/latest/radosgw/s3/) | [Riak CS](https://docs.riak.com/riak/cs/2.1.1/references/apis/storage/s3/index.html) | [OpenIO](https://docs.openio.io/latest/source/arch-design/s3_compliancy.html) |
|------------------------------|----------------------------------|-----------------|---------------|---------|-----|
| [signature v2](https://docs.aws.amazon.com/general/latest/gr/signature-version-2.html) (deprecated) | ❌ Missing | ✅ |  ✅ | ✅ | ✅ |
| [signature v4](https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html) |  ✅ Implemented |  ✅ |  ✅ | ❌ | ✅ |
| [URL path-style](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html#path-style-access) (eg. `host.tld/bucket/key`) |  ✅ Implemented | ✅ |  ✅ | ❓| ✅ |
| [URL vhost-style](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html#virtual-hosted-style-access) URL (eg. `bucket.host.tld/key`) |  ✅ Implemented | ❌| ✅| ✅ | ✅ |
| [Presigned URLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ShareObjectPreSignedURL.html) |  ✅ Implemented | ❌|  ✅ | ✅ |  ✅(❓) |

*Note:* OpenIO does not says if it supports presigned URLs. Because it is part
of signature v4 and they claim they support it without additional precisions,
we suppose that OpenIO supports presigned URLs.


## Endpoint implementation

All endpoints that are missing on Garage will return a 501 Not Implemented.
Some `x-amz-` headers are not implemented.

### Core endoints

| Endpoint                     | Garage                           | [Openstack Swift](https://docs.openstack.org/swift/latest/s3_compat.html) | [Ceph Object Gateway](https://docs.ceph.com/en/latest/radosgw/s3/) | [Riak CS](https://docs.riak.com/riak/cs/2.1.1/references/apis/storage/s3/index.html) | [OpenIO](https://docs.openio.io/latest/source/arch-design/s3_compliancy.html) |
|------------------------------|----------------------------------|-----------------|---------------|---------|-----|
| [CreateBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html)                 | ✅ Implemented                      | ✅ | ✅ | ✅ | ✅ |
| [DeleteBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html)                 | ✅ Implemented                      | ✅ |  ✅ | ✅ | ✅ |
| [GetBucketLocation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html)            | ✅ Implemented                 | ✅ | ✅ | ❌ | ✅ |
| [HeadBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html)                   | ✅ Implemented                      | ✅ | ✅ |  ✅ | ✅ |
| [ListBuckets](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html)                  | ✅ Implemented                      | ❌| ✅ | ✅ | ✅ |
| [HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html)                   | ✅ Implemented                      | ✅ | ✅ | ✅ | ✅ |
| [CopyObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html)                   | ✅ Implemented                      |  ✅ | ✅ | ✅ | ✅ |
| [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html)                 | ✅ Implemented                      | ✅ | ✅ | ✅ | ✅ |
| [DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)                | ✅ Implemented                      |  ✅  | ✅ | ✅ | ✅ |
| [GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)                    | ✅ Implemented                      |  ✅ | ✅ | ✅ | ✅ |
| [ListObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html)                  | ✅ Implemented (see details below)   | ✅ | ✅ |  ✅ | ❌|
| [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)                | ✅ Implemented                      | ❌|  ✅  | ❌| ✅ |
| [PostObject](https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html)                  | ✅ Implemented                      | ❌| ✅ | ❌| ❌|
| [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html)                    | ✅ Implemented                      | ✅ | ✅ | ✅ | ✅ |

**ListObjects:** Implemented, but there isn't a very good specification of what
`encoding-type=url` covers so there might be some encoding bugs. In our
implementation the url-encoded fields are in the same in ListObjects as they
are in ListObjectsV2.

*Note: Ceph API documentation is incomplete and lacks at least HeadBucket and UploadPartCopy,
but these endpoints are documented in [Red Hat Ceph Storage - Chapter 2. Ceph Object Gateway and the S3 API](https://access.redhat.com/documentation/en-us/red_hat_ceph_storage/4/html/developer_guide/ceph-object-gateway-and-the-s3-api)*

### Multipart Upload endpoints

| Endpoint                     | Garage                           | [Openstack Swift](https://docs.openstack.org/swift/latest/s3_compat.html) | [Ceph Object Gateway](https://docs.ceph.com/en/latest/radosgw/s3/) | [Riak CS](https://docs.riak.com/riak/cs/2.1.1/references/apis/storage/s3/index.html) | [OpenIO](https://docs.openio.io/latest/source/arch-design/s3_compliancy.html) |
|------------------------------|----------------------------------|-----------------|---------------|---------|-----|
| [AbortMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html)         | ✅ Implemented                      |  ✅ | ✅ | ✅ | ✅ |
| [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)   | ✅ Implemented (see details below)                      | ✅ | ✅ | ✅ | ✅ |
| [CreateMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html)        | ✅ Implemented                      | ✅| ✅ | ✅ | ✅ |
| [ListMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUpload.html)          | ✅ Implemented                      | ✅ | ✅ | ✅ | ✅ |
| [ListParts](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html)                    | ✅ Implemented                      | ✅ | ✅ | ✅ | ✅ |
| [UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html)                  | ✅ Implemented (see details below)                      | ✅ | ✅| ✅ | ✅ |
| [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html)               | ✅ Implemented                      | ✅ | ✅ | ✅ | ✅ |

Our implementation of Multipart Upload is currently a bit more restrictive than Amazon's one in some edge cases.
For more information, please refer to our [issue tracker](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/204).

### Website endpoints

| Endpoint                     | Garage                           | [Openstack Swift](https://docs.openstack.org/swift/latest/s3_compat.html) | [Ceph Object Gateway](https://docs.ceph.com/en/latest/radosgw/s3/) | [Riak CS](https://docs.riak.com/riak/cs/2.1.1/references/apis/storage/s3/index.html) | [OpenIO](https://docs.openio.io/latest/source/arch-design/s3_compliancy.html) |
|------------------------------|----------------------------------|-----------------|---------------|---------|-----|
| [DeleteBucketWebsite](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketWebsite.html)          | ✅ Implemented                      | ❌| ❌| ❌| ❌|
| [GetBucketWebsite](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketWebsite.html)             | ✅ Implemented                      |  ❌ | ❌| ❌| ❌|
| [PutBucketWebsite](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketWebsite.html)             | ⚠ Partially implemented (see below)| ❌| ❌| ❌| ❌|
| [DeleteBucketCors](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketCors.html)             | ✅ Implemented                      |  ❌|  ✅ | ❌| ✅ |
| [GetBucketCors](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketCors.html)                | ✅ Implemented                      |  ❌ |  ✅ | ❌| ✅ |
| [PutBucketCors](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketCors.html)                | ✅ Implemented                      | ❌|  ✅ | ❌| ✅ |

**PutBucketWebsite:** Implemented, but only stores the index document suffix and the error document path. Redirects are not supported.

*Note: Ceph radosgw has some support for static websites but it is different from the Amazon one. It also does not implement its configuration endpoints.*

### ACL, Policies endpoints

Amazon has 2 access control mechanisms in S3: ACL (legacy) and policies (new one).
Garage implements none of them, and has its own system instead, built around a per-access-key-per-bucket logic.
See Garage CLI reference manual to learn how to use Garage's permission system.

| Endpoint                     | Garage                           | [Openstack Swift](https://docs.openstack.org/swift/latest/s3_compat.html) | [Ceph Object Gateway](https://docs.ceph.com/en/latest/radosgw/s3/) | [Riak CS](https://docs.riak.com/riak/cs/2.1.1/references/apis/storage/s3/index.html) | [OpenIO](https://docs.openio.io/latest/source/arch-design/s3_compliancy.html) |
|------------------------------|----------------------------------|-----------------|---------------|---------|-----|
| [DeleteBucketPolicy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketPolicy.html) | ❌ Missing | ❌|  ✅ | ✅ | ❌|
| [GetBucketPolicy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicy.html) | ❌ Missing | ❌|  ✅ | ⚠ | ❌|
| [GetBucketPolicyStatus](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketPolicyStatus.html) | ❌ Missing | ❌| ✅ | ❌| ❌|
| [PutBucketPolicy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketPolicy.html) | ❌ Missing | ❌|  ✅ | ⚠ | ❌|
| [GetBucketAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html) | ❌ Missing | ✅ | ✅ | ✅ | ✅ |
| [PutBucketAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html) | ❌ Missing | ✅ | ✅ | ✅ | ✅ |
| [GetObjectAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAcl.html) | ❌ Missing | ✅ | ✅ | ✅ | ✅ |
| [PutObjectAcl](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAcl.html) | ❌ Missing | ✅ | ✅ | ✅ | ✅ |

*Notes:* Riak CS only supports a subset of the policy configuration.

### Versioning, Lifecycle endpoints

Garage does not (yet) support object versioning.
If you need this feature, please [share your use case in our dedicated issue](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/166).

| Endpoint                     | Garage                           | [Openstack Swift](https://docs.openstack.org/swift/latest/s3_compat.html) | [Ceph Object Gateway](https://docs.ceph.com/en/latest/radosgw/s3/) | [Riak CS](https://docs.riak.com/riak/cs/2.1.1/references/apis/storage/s3/index.html) | [OpenIO](https://docs.openio.io/latest/source/arch-design/s3_compliancy.html) |
|------------------------------|----------------------------------|-----------------|---------------|---------|-----|
| [DeleteBucketLifecycle](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html) | ❌ Missing | ❌| ✅| ❌| ✅|
| [GetBucketLifecycleConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html) | ❌ Missing | ❌| ✅ | ❌| ✅|
| [PutBucketLifecycleConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html) | ❌ Missing | ❌| ✅ | ❌| ✅|
| [GetBucketVersioning](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html)          | ❌ Stub (see below)       | ✅| ✅ | ❌| ✅|
| [ListObjectVersions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectVersions.html) | ❌ Missing | ❌| ✅ | ❌| ✅|
| [PutBucketVersioning](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html) | ❌ Missing | ❌| ✅| ❌| ✅|


**GetBucketVersioning:** Stub implementation (Garage does not yet support versionning so this always returns "versionning not enabled").

### Replication endpoints

Please open an issue if you have a use case for replication.

| Endpoint                     | Garage                           | [Openstack Swift](https://docs.openstack.org/swift/latest/s3_compat.html) | [Ceph Object Gateway](https://docs.ceph.com/en/latest/radosgw/s3/) | [Riak CS](https://docs.riak.com/riak/cs/2.1.1/references/apis/storage/s3/index.html) | [OpenIO](https://docs.openio.io/latest/source/arch-design/s3_compliancy.html) |
|------------------------------|----------------------------------|-----------------|---------------|---------|-----|
| [DeleteBucketReplication](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketReplication.html) | ❌ Missing | ❌| ✅ | ❌| ❌|
| [GetBucketReplication](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketReplication.html) | ❌ Missing | ❌| ✅ | ❌| ❌|
| [PutBucketReplication](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketReplication.html) | ❌ Missing | ❌| ⚠ | ❌| ❌|

*Note: Ceph documentation briefly says that Ceph supports
[replication through the S3 API](https://docs.ceph.com/en/latest/radosgw/multisite-sync-policy/#s3-replication-api)
but with some limitations.
Additionaly, replication endpoints are not documented in the S3 compatibility page so I don't know what kind of support we can expect.*

### Locking objects

Amazon defines a concept of [object locking](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html) that can be achieved either through a Retention period or a Legal hold.

| Endpoint                     | Garage                           | [Openstack Swift](https://docs.openstack.org/swift/latest/s3_compat.html) | [Ceph Object Gateway](https://docs.ceph.com/en/latest/radosgw/s3/) | [Riak CS](https://docs.riak.com/riak/cs/2.1.1/references/apis/storage/s3/index.html) | [OpenIO](https://docs.openio.io/latest/source/arch-design/s3_compliancy.html) |
|------------------------------|----------------------------------|-----------------|---------------|---------|-----|
| [GetObjectLegalHold](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLegalHold.html) | ❌ Missing | ❌| ✅ | ❌| ❌|
| [PutObjectLegalHold](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html) | ❌ Missing | ❌| ✅ | ❌| ❌|
| [GetObjectRetention](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html) | ❌ Missing | ❌| ✅ | ❌| ❌|
| [PutObjectRetention](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html) | ❌ Missing | ❌| ✅ | ❌| ❌|
| [GetObjectLockConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLockConfiguration.html) | ❌ Missing | ❌| ✅ | ❌| ❌|
| [PutObjectLockConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html) | ❌ Missing | ❌| ✅ | ❌| ❌|

### (Server-side) encryption

We think that you can either encrypt your server partition or do client-side encryption, so we did not implement server-side encryption for Garage.
Please open an issue if you have a use case.

| Endpoint                     | Garage                           | [Openstack Swift](https://docs.openstack.org/swift/latest/s3_compat.html) | [Ceph Object Gateway](https://docs.ceph.com/en/latest/radosgw/s3/) | [Riak CS](https://docs.riak.com/riak/cs/2.1.1/references/apis/storage/s3/index.html) | [OpenIO](https://docs.openio.io/latest/source/arch-design/s3_compliancy.html) |
|------------------------------|----------------------------------|-----------------|---------------|---------|-----|
| [DeleteBucketEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketEncryption.html) | ❌ Missing | ❌| ✅ | ❌| ❌|
| [GetBucketEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html) | ❌ Missing | ❌| ✅ | ❌| ❌|
| [PutBucketEncryption](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html) | ❌ Missing | ❌| ✅ | ❌| ❌|

### Misc endpoints

| Endpoint                     | Garage                           | [Openstack Swift](https://docs.openstack.org/swift/latest/s3_compat.html) | [Ceph Object Gateway](https://docs.ceph.com/en/latest/radosgw/s3/) | [Riak CS](https://docs.riak.com/riak/cs/2.1.1/references/apis/storage/s3/index.html) | [OpenIO](https://docs.openio.io/latest/source/arch-design/s3_compliancy.html) |
|------------------------------|----------------------------------|-----------------|---------------|---------|-----|
| [GetBucketNotificationConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketNotificationConfiguration.html) | ❌ Missing | ❌| ✅ | ❌| ❌|
| [PutBucketNotificationConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketNotificationConfiguration.html) | ❌ Missing | ❌| ✅ | ❌| ❌|
| [DeleteBucketTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html) | ❌ Missing | ❌| ✅ | ❌| ✅ |
| [GetBucketTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html) | ❌ Missing | ❌| ✅ | ❌| ✅ |
| [PutBucketTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html) | ❌ Missing | ❌| ✅ | ❌| ✅ |
| [DeleteObjectTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html) | ❌ Missing | ❌| ✅ | ❌| ✅ |
| [GetObjectTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTagging.html) | ❌ Missing | ❌| ✅ | ❌| ✅ |
| [PutObjectTagging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectTagging.html) | ❌ Missing | ❌| ✅ | ❌| ✅ |
| [GetObjectTorrent](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectTorrent.html) | ❌ Missing | ❌| ✅ | ❌| ❌|

### Vendor specific endpoints

<details><summary>Display Amazon specifc endpoints</summary>


| Endpoint                     | Garage                           | [Openstack Swift](https://docs.openstack.org/swift/latest/s3_compat.html) | [Ceph Object Gateway](https://docs.ceph.com/en/latest/radosgw/s3/) | [Riak CS](https://docs.riak.com/riak/cs/2.1.1/references/apis/storage/s3/index.html) | [OpenIO](https://docs.openio.io/latest/source/arch-design/s3_compliancy.html) |
|------------------------------|----------------------------------|-----------------|---------------|---------|-----|
| [DeleteBucketAnalyticsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketAnalyticsConfiguration.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [DeleteBucketIntelligentTieringConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketIntelligentTieringConfiguration.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [DeleteBucketInventoryConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketInventoryConfiguration.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [DeleteBucketMetricsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketMetricsConfiguration.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [DeleteBucketOwnershipControls](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketOwnershipControls.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [DeletePublicAccessBlock](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeletePublicAccessBlock.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [GetBucketAccelerateConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAccelerateConfiguration.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [GetBucketAnalyticsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAnalyticsConfiguration.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [GetBucketIntelligentTieringConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketIntelligentTieringConfiguration.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [GetBucketInventoryConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketInventoryConfiguration.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [GetBucketLogging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLogging.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [GetBucketMetricsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketMetricsConfiguration.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [GetBucketOwnershipControls](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketOwnershipControls.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [GetBucketRequestPayment](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketRequestPayment.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [GetPublicAccessBlock](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetPublicAccessBlock.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [ListBucketAnalyticsConfigurations](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBucketAnalyticsConfigurations.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [ListBucketIntelligentTieringConfigurations](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBucketIntelligentTieringConfigurations.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [ListBucketInventoryConfigurations](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBucketInventoryConfigurations.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [ListBucketMetricsConfigurations](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBucketMetricsConfigurations.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [PutBucketAccelerateConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAccelerateConfiguration.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [PutBucketAnalyticsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAnalyticsConfiguration.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [PutBucketIntelligentTieringConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketIntelligentTieringConfiguration.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [PutBucketInventoryConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketInventoryConfiguration.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [PutBucketLogging](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLogging.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [PutBucketMetricsConfiguration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketMetricsConfiguration.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [PutBucketOwnershipControls](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketOwnershipControls.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [PutBucketRequestPayment](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketRequestPayment.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [PutPublicAccessBlock](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutPublicAccessBlock.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [RestoreObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_RestoreObject.html) | ❌ Missing | ❌| ❌| ❌| ❌|
| [SelectObjectContent](https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html) | ❌ Missing | ❌| ❌| ❌| ❌|

</details>

