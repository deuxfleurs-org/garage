+++
title = "Request routing logic"
weight = 10
+++

Data retrieval requests to Garage endpoints (S3 API and websites) are resolved 
to an individual object in a bucket. Since objects are replicated to multiple nodes 
Garage must ensure consistency before answering the request.

## Using quorum to ensure consistency

Garage ensures consistency by attempting to establish a quorum with the
data nodes responsible for the object. When a majority of the data nodes
have provided metadata on a object Garage can then answer the request.

When a request arrives Garage will, assuming the recommended 3 replicas, perform the following actions:

- Make a request to the two preferred nodes for object metadata
- Try the third node if one of the two initial requests fail
- Check that the metadata from at least 2 nodes match
- Check that the object hasn't been marked deleted
- Answer the request with inline data from metadata if object is small enough
- Or get data blocks from the preferred nodes and answer using the assembled object

Garage dynamically determines which nodes to query based on health, preference, and 
which nodes actually host a given data. Garage has no concept of "primary" so any 
healthy node with the data can be used as long as a quorum is reached for the metadata.

## Node health

Garage keeps a TCP session open to each node in the cluster and periodically pings them. If a connection
cannot be established, or a node fails to answer a number of pings, the target node is marked as failed.
Failed nodes are not used for quorum or other internal requests.

## Node preference

Garage prioritizes which nodes to query according to a few criteria:

- A node always prefers itself if it can answer the request
- Then the node prioritizes nodes in the same zone
- Finally the nodes with the lowest latency are prioritized 


For further reading on the cluster structure look at the [gateway](@/documentation/cookbook/gateways.md) 
and [cluster layout management](@/documentation/reference-manual/layout.md) pages.