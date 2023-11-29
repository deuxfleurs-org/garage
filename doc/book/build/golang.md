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
    "strings"
    garage "git.deuxfleurs.fr/garage-sdk/garage-admin-sdk-golang"
)

func main() {
    // Initialization
    configuration := garage.NewConfiguration()
    configuration.Host = "127.0.0.1:3903"
    client := garage.NewAPIClient(configuration)
    ctx := context.WithValue(context.Background(), garage.ContextAccessToken, "s3cr3t")

    // Nodes
    fmt.Println("--- nodes ---")
    nodes, _, _ := client.NodesApi.GetNodes(ctx).Execute()
    fmt.Fprintf(os.Stdout, "First hostname: %v\n", nodes.KnownNodes[0].Hostname)
    capa := int64(1000000000)
    change := []garage.NodeRoleChange{
	    garage.NodeRoleChange{NodeRoleUpdate: &garage.NodeRoleUpdate {
	        Id: *nodes.KnownNodes[0].Id,
	        Zone: "dc1",
	        Capacity: *garage.NewNullableInt64(&capa),
	        Tags: []string{ "fast", "amd64" },
	    }},
    }
    staged, _, _ := client.LayoutApi.AddLayout(ctx).NodeRoleChange(change).Execute()
    msg, _, _ := client.LayoutApi.ApplyLayout(ctx).LayoutVersion(*garage.NewLayoutVersion(staged.Version + 1)).Execute()
    fmt.Printf(strings.Join(msg.Message, "\n")) // Layout configured

    health, _, _ := client.NodesApi.GetHealth(ctx).Execute()
    fmt.Printf("Status: %s, nodes: %v/%v, storage: %v/%v, partitions: %v/%v\n", health.Status, health.ConnectedNodes, health.KnownNodes, health.StorageNodesOk, health.StorageNodes, health.PartitionsAllOk, health.Partitions)

    // Key
    fmt.Println("\n--- key ---")
    key := "openapi-key"
    keyInfo, _, _ := client.KeyApi.AddKey(ctx).AddKeyRequest(garage.AddKeyRequest{Name: *garage.NewNullableString(&key) }).Execute()
    defer client.KeyApi.DeleteKey(ctx).Id(*keyInfo.AccessKeyId).Execute()
    fmt.Printf("AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n", *keyInfo.AccessKeyId, *keyInfo.SecretAccessKey.Get())

    id := *keyInfo.AccessKeyId
    canCreateBucket := true
    updateKeyRequest := *garage.NewUpdateKeyRequest()
    updateKeyRequest.SetName("openapi-key-updated")
    updateKeyRequest.SetAllow(garage.UpdateKeyRequestAllow { CreateBucket: &canCreateBucket })
    update, _, _ := client.KeyApi.UpdateKey(ctx).Id(id).UpdateKeyRequest(updateKeyRequest).Execute()
    fmt.Printf("Updated %v with key name %v\n", *update.AccessKeyId, *update.Name)

    keyList, _, _ := client.KeyApi.ListKeys(ctx).Execute()
    fmt.Printf("Keys count: %v\n", len(keyList))

    // Bucket
    fmt.Println("\n--- bucket ---")
    global_name := "global-ns-openapi-bucket"
    local_name := "local-ns-openapi-bucket"
    bucketInfo, _, _ := client.BucketApi.CreateBucket(ctx).CreateBucketRequest(garage.CreateBucketRequest{
        GlobalAlias: &global_name,
        LocalAlias: &garage.CreateBucketRequestLocalAlias {
            AccessKeyId: keyInfo.AccessKeyId,
	    Alias: &local_name,
        },
    }).Execute()
    defer client.BucketApi.DeleteBucket(ctx).Id(*bucketInfo.Id).Execute()
    fmt.Printf("Bucket id: %s\n", *bucketInfo.Id)

    updateBucketRequest := *garage.NewUpdateBucketRequest()
    website := garage.NewUpdateBucketRequestWebsiteAccess()
    website.SetEnabled(true)
    website.SetIndexDocument("index.html")
    website.SetErrorDocument("errors/4xx.html")
    updateBucketRequest.SetWebsiteAccess(*website)
    quotas := garage.NewUpdateBucketRequestQuotas()
    quotas.SetMaxSize(1000000000)
    quotas.SetMaxObjects(999999999)
    updateBucketRequest.SetQuotas(*quotas)
    updatedBucket, _, _ := client.BucketApi.UpdateBucket(ctx).Id(*bucketInfo.Id).UpdateBucketRequest(updateBucketRequest).Execute()
    fmt.Printf("Bucket %v website activation: %v\n", *updatedBucket.Id, *updatedBucket.WebsiteAccess)

    bucketList, _, _ := client.BucketApi.ListBuckets(ctx).Execute()
    fmt.Printf("Bucket count: %v\n", len(bucketList))
}
```

See also:
 - [generated doc](https://git.deuxfleurs.fr/garage-sdk/garage-admin-sdk-golang)
 - [examples](https://git.deuxfleurs.fr/garage-sdk/garage-admin-sdk-generator/src/branch/main/example/golang)
