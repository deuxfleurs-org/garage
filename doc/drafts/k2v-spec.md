# Specification of the Garage K2V API (K2V = Key/Key/Value)

- We are storing triplets of the form `(partition key, sort key, value)` -> no
  user-defined fields, the client is responsible of writing whatever he wants
  in the value (typically an encrypted blob). Values are binary blobs, which
  are always represented as their base64 encoding in the JSON API. Partition
  keys and sort keys are utf8 strings.

- Triplets are stored in buckets; each bucket stores a separate set of triplets

- Bucket names and access keys are the same as for accessing the S3 API

- K2V triplets exist separately from S3 objects. K2V triplets don't exist for
  the S3 API, and S3 objects don't exist for the K2V API.

- Values stored for triplets have associated causality information, that enables
  Garage to detect concurrent writes. In case of concurrent writes, Garage
  keeps the concurrent values until a further write supersedes the concurrent
  values. This is the same method as Riak KV implements. The method used is
  based on DVVS (dotted version vector sets), described in the paper "Scalable
  and Accurate Causality Tracking for Eventually Consistent Data Stores", as
  well as [here](https://github.com/ricardobcl/Dotted-Version-Vectors)


## Data format

### Triple format

Triples in K2V are constituted of three fields:

- a partition key (`pk`), an utf8 string that defines in what partition the
  triplet is stored; triplets in different partitions cannot be listed together
  in a ReadBatch command, or deleted together in a DeleteBatch command: a
  separate command must be included in the ReadBatch/DeleteBatch call for each
  partition key in which the client wants to read/delete lists of items

- a sort key (`sk`), an utf8 string that defines the index of the triplet inside its
  partition; triplets are uniquely idendified by their partition key + sort key

- a value (`v`), an opaque binary blob associated to the partition key + sort key;
  they are transmitted as binary when possible but in most case in the JSON API
  they will be represented as strings using base64 encoding; a value can also
  be `null` to indicate a deleted triplet (a `null` value is called a tombstone)

### Causality information

K2V supports storing several concurrent values associated to a pk+sk, in the
case where insertion or deletion operations are detected to be concurrent (i.e.
there is not one that was aware of the other, they are not causally dependant
one on the other).  In practice, it even looks more like the opposite: to
overwrite a previously existing value, the client must give a "causality token"
that "proves" (not in a cryptographic sense) that it had seen a previous value.
Otherwise, the value written will not overwrite an existing value, it will just
create a new concurrent value.

The causality token is a binary/b64-encoded representation of a context,
specified below.

A set of concurrent values looks like this:

```
(node1, tdiscard1, (v1, t1), (v2, t2)) ; tdiscard1 < t1 < t2
(node2, tdiscard2, (v3, t3)            ; tdiscard2 < t3
```

`tdiscard` for a node `i` means that all values inserted by node `i` with times
`<= tdiscard` are obsoleted, i.e. have been read by a client that overwrote it
afterwards.

The associated context would be the following: `[(node1, t2), (node2, t3)]`,
i.e. if a node reads this set of values and inserts a new values, we will now
have `tdiscard1 = t2` and `tdiscard2 = t3`, to indicate that values v1, v2 and v3
are obsoleted by the new write.

**Basic insertion.** To insert a new value `v4` with context `[(node1, t2), (node2, t3)]`, in a
simple case where there was no insertion in-between reading the value
mentionned above and writing `v4`, and supposing that node2 receives the
InsertItem query:

- `node2` generates a timestamp `t4` such that `t4 > t3`.
- the new state is as follows:

```
(node1, tdiscard1', ())       ; tdiscard1' = t2
(node2, tdiscard2', (v4, t4)) ; tdiscard2' = t3
```

**A more complex insertion example.** In the general case, other intermediate values could have
been written before `v4` with context `[(node1, t2), (node2, t3)]` is sent to the system.
For instance, here is a possible sequence of events:

1. First we have the set of values v1, v2 and v3 described above.
   A node reads it, it obtains values v1, v2 and v3 with context `[(node1, t2), (node2, t3)]`.

2. A node writes a value `v5` with context `[(node1, t1)]`, i.e. `v5` is only a
   successor of v1 but not of v2 or v3. Suppose node1 receives the write, it
   will generate a new timestamp `t5` larger than all of the timestamps it
   knows of, i.e. `t5 > t2`. We will now have:

```
(node1, tdiscard1'', (v2, t2), (v5, t5)) ; tdiscard1'' = t1 < t2 < t5
(node2, tdiscard2, (v3, t3)              ; tdiscard2 < t3
```

3. Now `v4` is written with context `[(node1, t2), (node2, t3)]`, and node2
   processes the query. It will generate `t4 > t3` and the state will become:

```
(node1, tdiscard1', (v5, t5))       ; tdiscard1' = t2 < t5
(node2, tdiscard2', (v4, t4))       ; tdiscard2' = t3
```

**Generic algorithm for handling insertions:** A certain node n handles the
InsertItem and is responsible for the correctness of this procedure.

1. Lock the key (or the whole table?) at this node to prevent concurrent updates of the value that would mess things up
2. Read current set of values
3. Generate a new timestamp that is larger than the largest timestamp for node n
4. Add the inserted value in the list of values of node n
5. Update the discard times to be the times set in the context, and accordingly discard overwritten values
6. Release lock
7. Propagate updated value to other nodes
8. Return to user when propagation achieved the write quorum (propagation to other nodes continues asynchronously)

**Encoding of contexts:**

Contexts consist in a list of (node id, timestamp) pairs.
They are encoded in binary as follows:

```
checksum: u64, [ node: u64, timestamp: u64 ]*
```

The checksum is just the XOR of all of the node IDs and timestamps.

Once encoded in binary, contexts are written and transmitted in base64.


### Indexing

K2V keeps an index, a secondary data structure that is updated asynchronously,
that keeps tracks of the number of triplets stored for each partition key.
This allows easy listing of all of the partition keys for which triplets exist
in a bucket, as the partition key becomes the sort key in the index.

How indexing works:

- Each node keeps a local count of how many items it stores for each partition,
  in a local database tree that is updated atomically when an item is modified.
- These local counters are asynchronously stored in the index table which is
  a regular Garage table spread in the network. Counters are stored as LWW values,
  so basically the final table will have the following structure:

```
- pk: bucket
- sk: partition key for which we are counting
- v:  lwwmap (node id -> number of items)
```

The final number of items present in the partition can be estimated by taking
the maximum of the values (i.e. the value for the node that announces having
the most items for that partition). In most cases the values for different node
IDs should all be the same; more precisely, three node IDs should map to the
same non-zero value, and all other node IDs that are present are tombstones
that map to zeroes.  Note that we need to filter out values from nodes that are
no longer part of the cluster layout, as when nodes are removed they won't
necessarily have had the time to set their counters to zero.

## Important details

**THIS SECTION CONTAINS A FEW WARNINGS ON THE K2V API WHICH ARE IMPORTANT
TO UNDERSTAND IN ORDER TO USE IT CORRECTLY.**

- **Internal server errors on updates do not mean that the update isn't stored.**
  K2V will return an internal server error when it cannot reach a quorum of nodes on
  which to save an updated value. However the value may still be stored on just one
  node, which will then propagate it to other nodes asynchronously via anti-entropy.

- **Batch operations are not transactions.** When calling InsertBatch or DeleteBatch,
  items may appear partially inserted/deleted while the operation is being processed.
  More importantly, if InsertBatch or DeleteBatch returns an internal server error,
  some of the items to be inserted/deleted might end up inserted/deleted on the server,
  while others may still have their old value.

- **Concurrent values are deduplicated.** When inserting a value for a key,
  Garage might internally end up
  storing the value several times if there are network errors. These values will end up as
  concurrent values for a key, with the same byte string (or `null` for a deletion).
  Garage fixes this by deduplicating concurrent values when they are returned to the
  user on read operations. Importantly, *Garage does not differentiate between duplicate
  concurrent values due to the user making the same call twice, or Garage having to
  do an internal retry*. This means that all duplicate concurrent values are deduplicated
  when an item is read: if the user inserts twice concurrently the same value, they will
  only read it once.

## API Endpoints

**Remark.** Example queries and responses here are given in JSON5 format
for clarity. However the actual K2V API uses basic JSON so all examples
and responses need to be translated.

### Operations on single items

**ReadItem: `GET /<bucket>/<partition key>?sort_key=<sort key>`**


Query parameters:

| name       | default value | meaning                          |
|------------|---------------|----------------------------------|
| `sort_key` | **mandatory** | The sort key of the item to read |

Returns the item with specified partition key and sort key. Values can be
returned in either of two ways:

1. a JSON array of base64-encoded values, or `null`'s for tombstones, with
   header `Content-Type: application/json`

2. in the case where there are no concurrent values, the single present value
   can be returned directly as the response body (or an HTTP 204 NO CONTENT for
   a tombstone), with header `Content-Type: application/octet-stream`

The choice between return formats 1 and 2 is directed by the `Accept` HTTP header:

- if the `Accept` header is not present, format 1 is always used

- if `Accept` contains `application/json` but not `application/octet-stream`,
  format 1 is always used

- if `Accept` contains `application/octet-stream` but not `application/json`,
  format 2 is used when there is a single value, and an HTTP error 409 (HTTP
  409 CONFLICT) is returned in the case of multiple concurrent values
  (including concurrent tombstones)

- if `Accept` contains both, format 2 is used when there is a single value, and
  format 1 is used as a fallback in case of concurrent values

- if `Accept` contains none, HTTP 406 NOT ACCEPTABLE is raised

Example query:

```
GET /my_bucket/mailboxes?sort_key=INBOX HTTP/1.1
```

Example response:

```json
HTTP/1.1 200 OK
X-Garage-Causality-Token: opaquetoken123
Content-Type: application/json

[
  "b64cryptoblob123",
  "b64cryptoblob'123"
]
```

Example response in case the item is a tombstone:

```
HTTP/1.1 200 OK
X-Garage-Causality-Token: opaquetoken999
Content-Type: application/json

[
  null
]
```

Example query 2:

```
GET /my_bucket/mailboxes?sort_key=INBOX HTTP/1.1
Accept: application/octet-stream
```

Example response if multiple concurrent versions exist:

```
HTTP/1.1 409 CONFLICT
X-Garage-Causality-Token: opaquetoken123
Content-Type: application/octet-stream
```

Example response in case of single value:

```
HTTP/1.1 200 OK
X-Garage-Causality-Token: opaquetoken123
Content-Type: application/octet-stream

cryptoblob123
```

Example response in case of a single value that is a tombstone:

```
HTTP/1.1 204 NO CONTENT
X-Garage-Causality-Token: opaquetoken123
Content-Type: application/octet-stream
```


**PollItem: `GET /<bucket>/<partition key>?sort_key=<sort key>&causality_token=<causality token>`**

This endpoint will block until a new value is written to a key.

The GET parameter `causality_token` should be set to the causality
token returned with the last read of the key, so that K2V knows
what values are concurrent or newer than the ones that the
client previously knew.

This endpoint returns the new value in the same format as ReadItem.
If no new value is written and the timeout elapses,
an HTTP 304 NOT MODIFIED is returned.

Query parameters:

| name              | default value | meaning                                                                    |
|-------------------|---------------|----------------------------------------------------------------------------|
| `sort_key`        | **mandatory** | The sort key of the item to read                                           |
| `causality_token` | **mandatory** | The causality token of the last known value or set of values               |
| `timeout`         | 300           | The timeout before 304 NOT MODIFIED is returned if the value isn't updated |

The timeout can be set to any number of seconds, with a maximum of 600 seconds (10 minutes).


**InsertItem: `PUT /<bucket>/<partition key>?sort_key=<sort_key>`**

Inserts a single item. This request does not use JSON, the body is sent directly as a binary blob.

To supersede previous values, the HTTP header `X-Garage-Causality-Token` should
be set to the causality token returned by a previous read on this key. This
header can be ommitted for the first writes to the key.

Example query:

```
PUT /my_bucket/mailboxes?sort_key=INBOX HTTP/1.1
X-Garage-Causality-Token: opaquetoken123

myblobblahblahblah
```

Example response:

```
HTTP/1.1 204 No Content
```

**DeleteItem: `DELETE /<bucket>/<partition key>?sort_key=<sort_key>`**

Deletes a single item. The HTTP header `X-Garage-Causality-Token` must be set
to the causality token returned by a previous read on this key, to indicate
which versions of the value should be deleted. The request will not process if
`X-Garage-Causality-Token` is not set.

Example query:

```
DELETE /my_bucket/mailboxes?sort_key=INBOX HTTP/1.1
X-Garage-Causality-Token: opaquetoken123
```

Example response:

```
HTTP/1.1 204 NO CONTENT
```

### Operations on index

**ReadIndex: `GET /<bucket>?start=<start>&end=<end>&limit=<limit>`**

Lists all partition keys in the bucket for which some triplets exist, and gives
for each the number of triplets, total number of values (which might be bigger
than the number of triplets in case of conflicts), total number of bytes of
these values, and number of triplets that are in a state of conflict.
The values returned are an approximation of the true counts in the bucket,
as these values are asynchronously updated, and thus eventually consistent.

Query parameters:

| name      | default value | meaning                                                        |
|-----------|---------------|----------------------------------------------------------------|
| `prefix`  | `null`        | Restrict listing to partition keys that start with this prefix |
| `start`   | `null`        | First partition key to list, in lexicographical order          |
| `end`     | `null`        | Last partition key to list (excluded)                          |
| `limit`   | `null`        | Maximum number of partition keys to list                       |
| `reverse` | `false`       | Iterate in reverse lexicographical order                       |

The response consists in a JSON object that repeats the parameters of the query and gives the result (see below).

The listing starts at partition key `start`, or if not specified at the
smallest partition key that exists.  It returns partition keys in increasing
order, or decreasing order if `reverse` is set to `true`,
and stops when either of the following conditions is met:

1. if `end` is specfied, the partition key `end` is reached or surpassed (if it
   is reached exactly, it is not included in the result)

2. if `limit` is specified, `limit` partition keys have been listed

3. no more partition keys are available to list

In case 2, and if there are more partition keys to list before condition 1
triggers, then in the result `more` is set to `true` and `nextStart` is set to
the first partition key that couldn't be listed due to the limit. In the first
case (if the listing stopped because of the `end` parameter), `more` is not set
and the `nextStart` key is not specified.

Note that if `reverse` is set to `true`, `start` is the highest key
(in lexicographical order) for which values are returned.
This means that if an `end` is specified, it must be smaller than `start`,
otherwise no values will be returned.

Example query:

```
GET /my_bucket HTTP/1.1
```

Example response:

```json
HTTP/1.1 200 OK

{
  prefix: null,
  start: null,
  end: null,
  limit: null,
  reverse: false,
  partitionKeys: [
    {
      pk: "keys",
      entries: 3043,
      conflicts: 0,
      values: 3043,
      bytes: 121720,
    },
    {
      pk: "mailbox:INBOX",
      entries: 42,
      conflicts: 1,
      values: 43,
      bytes: 142029,
    },
    {
      pk: "mailbox:Junk",
      entries: 2991
      conflicts: 0,
      values: 2991,
      bytes: 12019322,
    },
    {
      pk: "mailbox:Trash",
      entries: 10,
      conflicts: 0,
      values: 10,
      bytes: 32401,
    },
    {
      pk: "mailboxes",
      entries: 3,
      conflicts: 0,
      values: 3,
      bytes: 3019,
    },
  ],
  more: false,
  nextStart: null,
}
```


### Operations on batches of items

**InsertBatch: `POST /<bucket>`**

Simple insertion and deletion of triplets. The body is just a list of items to
insert in the following format:
`{ pk: "<partition key>", sk: "<sort key>", ct: "<causality token>"|null, v: "<value>"|null }`.

The causality token should be the one returned in a previous read request (e.g.
by ReadItem or ReadBatch), to indicate that this write takes into account the
values that were returned from these reads, and supersedes them causally. If
the triplet is inserted for the first time, the causality token should be set to
`null`.

The value is expected to be a base64-encoded binary blob. The value `null` can
also be used to delete the triplet while preserving causality information: this
allows to know if a delete has happenned concurrently with an insert, in which
case both are preserved and returned on reads (see below).

Partition keys and sort keys are utf8 strings which are stored sorted by
lexicographical ordering of their binary representation.

Example query:

```json
POST /my_bucket HTTP/1.1

[
  { pk: "mailbox:INBOX", sk: "001892831", ct: "opaquetoken321", v: "b64cryptoblob321updated" },
  { pk: "mailbox:INBOX", sk: "001892912", ct: null, v: "b64cryptoblob444" },
  { pk: "mailbox:INBOX", sk: "001892932", ct: "opaquetoken654", v: null },
]
```

Example response:

```
HTTP/1.1 204 NO CONTENT
```


**ReadBatch: `POST /<bucket>?search`**, or alternatively<br/>
**ReadBatch: `SEARCH /<bucket>`**

Batch read of triplets in a bucket.

The request body is a JSON list of searches, that each specify a range of
items to get (to get single items, set `singleItem` to `true`). A search is a
JSON struct with the following fields:

| name            | default value | meaning                                                                                |
|-----------------|---------------|----------------------------------------------------------------------------------------|
| `partitionKey`  | **mandatory** | The partition key in which to search                                                   |
| `prefix`        | `null`        | Restrict items to list to those whose sort keys start with this prefix                 |
| `start`         | `null`        | The sort key of the first item to read                                                 |
| `end`           | `null`        | The sort key of the last item to read (excluded)                                       |
| `limit`         | `null`        | The maximum number of items to return                                                  |
| `reverse`       | `false`       | Iterate in reverse lexicographical order on sort keys                                  |
| `singleItem`    | `false`       | Whether to return only the item with sort key `start`                                  |
| `conflictsOnly` | `false`       | Whether to return only items that have several concurrent values                       |
| `tombstones`    | `false`       | Whether or not to return tombstone lines to indicate the presence of old deleted items |


For each of the searches, triplets are listed and returned separately. The
semantics of `prefix`, `start`, `end`, `limit` and `reverse` are the same as for ReadIndex. The
additionnal parameter `singleItem` allows to get a single item, whose sort key
is the one given in `start`. Parameters `conflictsOnly` and `tombstones`
control additional filters on the items that are returned.

The result is a list of length the number of searches, that consists in for
each search a JSON object specified similarly to the result of ReadIndex, but
that lists triplets within a partition key.

The format of returned tuples is as follows: `{ sk: "<sort key>", ct: "<causality
token>", v: ["<value1>", ...] }`, with the following fields:

- `sk` (sort key): any unicode string used as a sort key

- `ct` (causality token): an opaque token served by the server (generally
  base64-encoded) to be used in subsequent writes to this key

- `v` (list of values): each value is a binary blob, always base64-encoded;
  contains multiple items when concurrent values exists

- in case of concurrent update and deletion, a `null` is added to the list of concurrent values

- if the `tombstones` query parameter is set to `true`, tombstones are returned
  for items that have been deleted (this can be useful for inserting after an
  item that has been deleted, so that the insert is not considered
  concurrent with the delete). Tombstones are returned as tuples in the
  same format with only `null` values

Example query:

```json
POST /my_bucket?search HTTP/1.1

[
  {
    partitionKey: "mailboxes",
  },
  {
    partitionKey: "mailbox:INBOX",
    start: "001892831",
    limit: 3,
  },
  {
    partitionKey: "keys",
    start: "0",
    singleItem: true,
  },
]
```

Example associated response body:

```json
HTTP/1.1 200 OK

[
  {
    partitionKey: "mailboxes",
    prefix: null,
    start: null,
    end: null,
    limit: null,
    reverse: false,
    conflictsOnly: false,
    tombstones: false,
    singleItem: false,
    items: [
      { sk: "INBOX", ct: "opaquetoken123", v: ["b64cryptoblob123", "b64cryptoblob'123"] },
      { sk: "Trash", ct: "opaquetoken456", v: ["b64cryptoblob456"] },
      { sk: "Junk", ct: "opaquetoken789", v: ["b64cryptoblob789"] },
    ],
    more: false,
    nextStart: null,
  },
  {
    partitionKey: "mailbox::INBOX",
    prefix: null,
    start: "001892831",
    end: null,
    limit: 3,
    reverse: false,
    conflictsOnly: false,
    tombstones: false,
    singleItem: false,
    items: [
      { sk: "001892831", ct: "opaquetoken321", v: ["b64cryptoblob321"] },
      { sk: "001892832", ct: "opaquetoken654", v: ["b64cryptoblob654"] },
      { sk: "001892874", ct: "opaquetoken987", v: ["b64cryptoblob987"] },
    ],
    more: true,
    nextStart: "001892898",
  },
  {
    partitionKey: "keys",
    prefix: null,
    start: "0",
    end: null,
    conflictsOnly: false,
    tombstones: false,
    limit: null,
    reverse: false,
    singleItem: true,
    items: [
      { sk: "0", ct: "opaquetoken999", v: ["b64binarystuff999"] },
    ],
    more: false,
    nextStart: null,
  },
]
```



**DeleteBatch: `POST /<bucket>?delete`**

Batch deletion of triplets. The request format is the same for `POST
/<bucket>?search` to indicate items or range of items, except that here they
are deleted instead of returned, but only the fields `partitionKey`, `prefix`, `start`,
`end`, and `singleItem` are supported. Causality information is not given by
the user: this request will internally list all triplets and write deletion
markers that supersede all of the versions that have been read.

This request returns for each series of items to be deleted, the number of
matching items that have been found and deleted.

Example query:

```json
POST /my_bucket?delete HTTP/1.1

[
  {
    partitionKey: "mailbox:OldMailbox",
  },
  {
    partitionKey: "mailbox:INBOX",
    start: "0018928321",
    singleItem: true,
  },
]
```

Example response:

```json
HTTP/1.1 200 OK

[
  {
    partitionKey: "mailbox:OldMailbox",
    prefix: null,
    start: null,
    end: null,
    singleItem: false,
    deletedItems: 35,
  },
  {
    partitionKey: "mailbox:INBOX",
    prefix: null,
    start: "0018928321",
    end: null,
    singleItem: true,
    deletedItems: 1,
  },
]
```

**PollRange: `POST /<bucket>/<partition key>?poll_range`**, or alternatively<br/>
**PollRange: `SEARCH /<bucket>/<partition key>?poll_range`**

Polls a range of items for changes.

The query body is a JSON object consisting of the following fields:

| name            | default value | meaning                                                                                |
|-----------------|---------------|----------------------------------------------------------------------------------------|
| `prefix`        | `null`        | Restrict items to poll to those whose sort keys start with this prefix                 |
| `start`         | `null`        | The sort key of the first item to poll                                                 |
| `end`           | `null`        | The sort key of the last item to poll (excluded)                                       |
| `timeout`       | 300           | The timeout before 304 NOT MODIFIED is returned if no value in the range is updated    |
| `seenMarker`    | `null`        | An opaque string returned by a previous PollRange call, that represents items already seen |

The timeout can be set to any number of seconds, with a maximum of 600 seconds (10 minutes).

The response is either:

- A HTTP 304 NOT MODIFIED response with an empty body, if the timeout expired and no changes occurred

- A HTTP 200 response, indicating that some changes have occurred since the last PollRange call, in which case a JSON object is returned in the body with the following fields:

| name            | meaning                                                                                |
|-----------------|----------------------------------------------------------------------------------------|
| `seenMarker`    | An opaque string that represents items already seen for future PollRange calls         |
| `items`         | The list of items that have changed since last PollRange call, in the same format as ReadBatch |

If no seen marker is known by the caller, it can do a PollRange call
without specifying `seenMarker`. In this case, the PollRange call will
complete immediately, and return the current content of the range (which
can be empty) and a seen marker to be used in further PollRange calls. This
is the only case in which PollRange might return an HTTP 200 with an empty
set of items.

A seen marker returned as a response to a PollRange query can be used for further PollRange
queries on the same range, or for PollRange queries in a subrange of the initial range.
It may not be used for PollRange queries on ranges larger or outside of the initial range.

Example query:

```json
SEARCH /my_bucket?poll_range HTTP/1.1

{
  "prefix": "0391.",
  "start": "0391.000001973107",
  "seenMarker": "opaquestring123",
}
```


Example response:

```json
HTTP/1.1 200 OK
Content-Type: application/json

{
  "seenMarker": "opaquestring456",
  "items": [
    { sk: "0391.000001973221", ct: "opaquetoken123", v: ["b64cryptoblob123", "b64cryptoblob'123"] },
    { sk: "0391.000001974191", ct: "opaquetoken456", v: ["b64cryptoblob456", "b64cryptoblob'456"] },
  ]
}
```


## Internals: causality tokens

The method used is based on DVVS (dotted version vector sets). See:

- the paper "Scalable and Accurate Causality Tracking for Eventually Consistent Data Stores"
- <https://github.com/ricardobcl/Dotted-Version-Vectors>

For DVVS to work, write operations (at each node) must take a lock on the data table.
