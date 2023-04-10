$version: "2"
namespace org.deuxfleurs.garage.k2v

use aws.api#service
use aws.auth#sigv4
use aws.protocols#restJson1

@service(sdkId: "k2v")
@sigv4(name: "k2v")
@restJson1
service K2V {
    version: "2023-04-10"
    resources: [ Item ]
    //operations: [ PollItem, ReadIndex, InsertBatch, DeleteBatch, PollRange ]
}

resource Item {
  read: ReadItem
  put: InsertItem
  //delete: DeleteItem
  //list: ReadBatch
}

@mixin
structure KeySelector {
  @required
  @httpLabel
  bucketName: String

  @required
  @httpLabel
  partitionKey: String

  @required
  @httpQuery("sort_key")
  sortKey: String
}

@readonly
@http(method: "GET", uri: "/{bucketName}/{partitionKey}", code: 200)
operation ReadItem {
  input: ReadItemInput
  output: ReadItemOutput
}

@input
structure ReadItemInput with [KeySelector] {}

@output
structure ReadItemOutput {
  value: ItemList
}

@sparse
list ItemList {
  member: Blob
}

@idempotent
@http(method: "PUT", uri: "/{bucketName}/{partitionKey}", code: 204)
operation InsertItem {
  input: InsertItemInput
}

@input
structure InsertItemInput with [KeySelector] {}

//operation DeleteItem {
//  input: String
//  output: String
//}

//operation PollItem {
//  input: String
//  output: String
//}

//operation ReadIndex {
//  input: String
//  output: String
//}

//operation InsertBatch {
//  input: String
//  output: String
//}

//operation DeleteBatch {
//  input: String
//  output: String
//}

//operation PollRange {
//  input: String
//  output: String
//}
