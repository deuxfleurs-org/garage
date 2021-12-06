# S3 compatibility target

If there is a specific S3 functionnality you have a need for, feel free to open
a PR to put the corresponding endpoints higher in the list.  Please explain
your motivations for doing so in the PR message.

| Priority                   | Endpoints  |
| -------------------------- | --------- |
| **S-tier** (high priority) | |
| 							 | HeadBucket |
| 							 | GetBucketLocation |
| 							 | *CreateBucket* |
| 							 | *DeleteBucket* |
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
| 							 | *ListMultipartUploads* |
| 							 | *ListParts* |
| **A-tier** (will implement)     | |
| 							 | *GetBucketCors* |
| 							 | *PutBucketCors* |
| 							 | *DeleteBucketCors* |
| 							 | *UploadPartCopy* |
| 							 | *GetBucketWebsite* |
| 							 | *PutBucketWebsite* |
| 							 | *DeleteBucketWebsite* |
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
| **garbage-tier**   | |
| 							 | DeleteBucketEncryption |
| 							 | DeleteBucketAnalyticsConfiguration |
| 							 | DeleteBucketIntelligentTieringConfiguration |
| 							 | DeleteBucketInventoryConfiguration |
| 							 | DeleteBucketLifecycle |
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
| 							 | GetBucketLifecycleConfiguration |
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
| 							 | PutBucketLifecycleConfiguration |
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
