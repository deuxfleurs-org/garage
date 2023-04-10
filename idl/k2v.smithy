$version: "2"
namespace org.deuxfleurs.garage.k2v

service Item {
    version: "2023-04-10"
    resources: [ Item ]
    operations: [ PollItem ]
}

resource Item {
  read: ReadItem
  put: InsertItem
  delete: DeleteItem
  list: ReadIndex
}

operation ReadItem {
  input: ReadItemInput
  output: ReadItemOutput
}

@input
structure ReadItemInput {
  bucket: String
  partitionKey: String
  sortKey: String
}

@output
union ReadItemOutput {
  list: ReadItemOutputList
  raw: blob
}

@sparse
list ReadItemOutputList {
  member: String
}

operation PollItem {
  input:
  output:
}

operation InsertItem {
  input:
  output:
}

operation DeleteItem {
  input:
  output:
}
