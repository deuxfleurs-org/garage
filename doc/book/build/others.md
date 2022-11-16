+++
title = "Others"
weight = 99
+++

## S3

If you are developping a new application, you may want to use Garage to store your user's media.

The S3 API that Garage uses is a standard REST API, so as long as you can make HTTP requests,
you can query it. You can check the [S3 REST API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html) from Amazon to learn more.

Developping your own wrapper around the REST API is time consuming and complicated.
Instead, there are some libraries already avalaible.

Some of them are maintained by Amazon, some by Minio, others by the community.

### PHP

  - Amazon aws-sdk-php
    - [Installation](https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/getting-started_installation.html)
    - [Reference](https://docs.aws.amazon.com/aws-sdk-php/v3/api/api-s3-2006-03-01.html)
    - [Example](https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/s3-examples-creating-buckets.html)

### Java

  - Minio SDK
    - [Reference](https://docs.min.io/docs/java-client-api-reference.html)

  - Amazon aws-sdk-java
    - [Installation](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html)
    - [Reference](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3Client.html)
    - [Example](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/examples-s3-objects.html)

### .NET

  - Minio SDK
    - [Reference](https://docs.min.io/docs/dotnet-client-api-reference.html)

  - Amazon aws-dotnet-sdk

### C++

  - Amazon aws-cpp-sdk

### Haskell

  - Minio SDK
    - [Reference](https://docs.min.io/docs/haskell-client-api-reference.html)
