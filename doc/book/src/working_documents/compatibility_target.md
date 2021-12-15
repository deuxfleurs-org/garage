# S3 compatibility target

If there is a specific S3 functionnality you have a need for, feel free to open
a PR to put the corresponding endpoints higher in the list.  Please explain
your motivations for doing so in the PR message.

| Priority                   | Endpoints  |
| -------------------------- | --------- |
| **S-tier** (high priority) | |
| 							 | HeadBucket |
| 							 | GetBucketLocation |
| 							 | [*CreateBucket*](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/97) |
| 							 | [*DeleteBucket*](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/97) |
| 							 | ListBuckets |
| 							 | ListObjects |
| 							 | ListObjectsV2 |
| 							 | HeadObject |
| 							 | GetObject |
| 							 | PutObject |
| 							 | CopyObject |
| 							 | DeleteObject |
| 							 | DeleteObjects |
| 							 | CreateMultipartUpload |
| 							 | CompleteMultipartUpload |
| 							 | AbortMultipartUpload |
| 							 | UploadPart |
| 							 | [*ListMultipartUploads*](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/103) |
| 							 | [*ListParts*](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/103) |
| **A-tier** (will implement)     | |
| 							 | [*GetBucketCors*](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/138) |
| 							 | [*PutBucketCors*](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/138) |
| 							 | [*DeleteBucketCors*](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/138) |
| 							 | [*UploadPartCopy*](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/160) |
| 							 | [*GetBucketWebsite*](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/77) |
| 							 | [*PutBucketWebsite*](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/77) |
| 							 | DeleteBucketWebsite |
| ~~~~~~~~~~~~~~~~~~~~~~~~~~ | ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ |
| **B-tier** | |
| 							 | GetBucketAcl |
| 							 | PutBucketAcl |
| 							 | GetObjectLockConfiguration |
| 							 | PutObjectLockConfiguration |
| 							 | GetObjectRetention |
| 							 | PutObjectRetention |
| 							 | GetObjectLegalHold |
| 							 | PutObjectLegalHold |
| **C-tier** | |
| 							 | GetBucketVersioning |
| 							 | PutBucketVersioning |
| 							 | ListObjectVersions |
| 							 | GetObjectAcl |
| 							 | PutObjectAcl |
| 							 | GetBucketLifecycleConfiguration |
| 							 | PutBucketLifecycleConfiguration |
| 							 | DeleteBucketLifecycle |
| **garbage-tier**   | |
| 							 | DeleteBucketEncryption |
| 							 | DeleteBucketAnalyticsConfiguration |
| 							 | DeleteBucketIntelligentTieringConfiguration |
| 							 | DeleteBucketInventoryConfiguration |
| 							 | DeleteBucketMetricsConfiguration |
| 							 | DeleteBucketOwnershipControls |
| 							 | DeleteBucketPolicy |
| 							 | DeleteBucketReplication |
| 							 | DeleteBucketTagging |
| 							 | DeleteObjectTagging |
| 							 | DeletePublicAccessBlock |
| 							 | GetBucketAccelerateConfiguration |
| 							 | GetBucketAnalyticsConfiguration |
| 							 | GetBucketEncryption |
| 							 | GetBucketIntelligentTieringConfiguration |
| 							 | GetBucketInventoryConfiguration |
| 							 | GetBucketLogging |
| 							 | GetBucketMetricsConfiguration |
| 							 | GetBucketNotificationConfiguration |
| 							 | GetBucketOwnershipControls |
| 							 | GetBucketPolicy |
| 							 | GetBucketPolicyStatus |
| 							 | GetBucketReplication |
| 							 | GetBucketRequestPayment |
| 							 | GetBucketTagging |
| 							 | GetObjectTagging |
| 							 | GetObjectTorrent |
| 							 | GetPublicAccessBlock |
| 							 | ListBucketAnalyticsConfigurations |
| 							 | ListBucketIntelligentTieringConfigurations |
| 							 | ListBucketInventoryConfigurations |
| 							 | ListBucketMetricsConfigurations |
| 							 | PutBucketAccelerateConfiguration |
| 							 | PutBucketAnalyticsConfiguration |
| 							 | PutBucketEncryption |
| 							 | PutBucketIntelligentTieringConfiguration |
| 							 | PutBucketInventoryConfiguration |
| 							 | PutBucketLogging |
| 							 | PutBucketMetricsConfiguration |
| 							 | PutBucketNotificationConfiguration |
| 							 | PutBucketOwnershipControls |
| 							 | PutBucketPolicy |
| 							 | PutBucketReplication |
| 							 | PutBucketRequestPayment |
| 							 | PutBucketTagging |
| 							 | PutObjectTagging |
| 							 | PutPublicAccessBlock |
| 							 | RestoreObject |
| 							 | SelectObjectContent |
