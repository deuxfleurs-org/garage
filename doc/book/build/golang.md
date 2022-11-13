+++
title = "Golang"
weight = 30
+++

## S3

*Coming soon*

Some refs:
  - Minio minio-go-sdk
    - [Reference](https://docs.min.io/docs/golang-client-api-reference.html)

  - Amazon aws-sdk-go-v2
    - [Installation](https://aws.github.io/aws-sdk-go-v2/docs/getting-started/)
    - [Reference](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/s3)
    - [Example](https://aws.github.io/aws-sdk-go-v2/docs/code-examples/s3/putobject/)

## K2V

*Coming soon*

## Administration

Install the SDK with:

```bash
go get git.deuxfleurs.fr/garage-sdk/garage-admin-sdk-golang
```

A short example:

```go
package main

import (
    "context"
    "fmt"
    "os"
    garage "git.deuxfleurs.fr/garage-sdk/garage-admin-sdk-golang"
)

func main() {
    // Set Host and other parameters
    configuration := garage.NewConfiguration()
    configuration.Host = "127.0.0.1:3903"


    // We can now generate a client
    client := garage.NewAPIClient(configuration)

    // Authentication is handled through the context pattern
    ctx := context.WithValue(context.Background(), garage.ContextAccessToken, "s3cr3t")

    // Send a request
    resp, r, err := client.NodesApi.GetNodes(ctx).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `NodesApi.GetNodes``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }

    // Process the response
    fmt.Fprintf(os.Stdout, "Target hostname: %v\n", resp.KnownNodes[resp.Node].Hostname)
}
```

See also:
 - [generated doc](https://git.deuxfleurs.fr/garage-sdk/garage-admin-sdk-golang)
 - [examples](https://git.deuxfleurs.fr/garage-sdk/garage-admin-sdk-generator/src/branch/main/example/golang)
