+++
title = "Your code (PHP, JS, Go...)"
weight = 30
+++

If you are developping a new application, you may want to use Garage to store your user's media.

The S3 API that Garage uses is a standard REST API, so as long as you can make HTTP requests,
you can query it. You can check the [S3 REST API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html) from Amazon to learn more.

Developping your own wrapper around the REST API is time consuming and complicated.
Instead, there are some libraries already avalaible.

Some of them are maintained by Amazon, some by Minio, others by the community.

## PHP

  - Amazon aws-sdk-php
    - [Installation](https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/getting-started_installation.html)
    - [Reference](https://docs.aws.amazon.com/aws-sdk-php/v3/api/api-s3-2006-03-01.html)
    - [Example](https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/s3-examples-creating-buckets.html)

## Javascript

  - Minio SDK
    - [Reference](https://docs.min.io/docs/javascript-client-api-reference.html)

  - Amazon aws-sdk-js
    - [Installation](https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/getting-started.html)
    - [Reference](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html)
    - [Example](https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/s3-example-creating-buckets.html)

## Golang

  - Minio minio-go-sdk
    - [Reference](https://docs.min.io/docs/golang-client-api-reference.html)

  - Amazon aws-sdk-go-v2
    - [Installation](https://aws.github.io/aws-sdk-go-v2/docs/getting-started/)
    - [Reference](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/s3)
    - [Example](https://aws.github.io/aws-sdk-go-v2/docs/code-examples/s3/putobject/)

## Python

  - Minio SDK
    - [Reference](https://docs.min.io/docs/python-client-api-reference.html)

  - Amazon boto3
    - [Installation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html)
    - [Reference](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html)
    - [Example](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html)

## Java

  - Minio SDK
    - [Reference](https://docs.min.io/docs/java-client-api-reference.html)

  - Amazon aws-sdk-java
    - [Installation](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html)
    - [Reference](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3Client.html)
    - [Example](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/examples-s3-objects.html)

## Rust

  - Amazon aws-rust-sdk
    - [Github](https://github.com/awslabs/aws-sdk-rust)

## .NET

  - Minio SDK
    - [Reference](https://docs.min.io/docs/dotnet-client-api-reference.html)

  - Amazon aws-dotnet-sdk

## C++

  - Amazon aws-cpp-sdk

## Haskell

  - Minio SDK
    - [Reference](https://docs.min.io/docs/haskell-client-api-reference.html)
