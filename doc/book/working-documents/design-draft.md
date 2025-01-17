+++
title = "Design draft (obsolete)"
weight = 900
+++

**WARNING: this documentation is a design draft which was written before Garage's actual implementation.
The general principle are similar, but details have not been updated.**


#### Modules

- `membership/`: configuration, membership management (gossip of node's presence and status), ring generation --> what about Serf (used by Consul/Nomad) : https://www.serf.io/? Seems a huge library with many features so maybe overkill/hard to integrate
- `metadata/`: metadata management
- `blocks/`: block management, writing, GC and rebalancing
- `internal/`: server to server communication (HTTP server and client that reuses connections, TLS if we want, etc)
- `api/`: S3 API
- `web/`: web management interface

#### Metadata tables

**Objects:**

- *Hash key:* Bucket name (string)
- *Sort key:* Object key (string)
- *Sort key:* Version timestamp (int)
- *Sort key:* Version UUID (string)
- Complete: bool
- Inline: bool, true for objects < threshold (say 1024)
- Object size (int)
- Mime type (string)
- Data for inlined objects (blob)
- Hash of first block otherwise (string)

*Having only a hash key on the bucket name will lead to storing all file entries of this table for a specific bucket on a single node. At the same time, it is the only way I see to rapidly being able to list all bucket entries...*

**Blocks:**

- *Hash key:* Version UUID (string)
- *Sort key:* Offset of block in total file (int)
- Hash of data block (string)

A version is defined by the existence of at least one entry in the blocks table for a certain version UUID.
We must keep the following invariant: if a version exists in the blocks table, it has to be referenced in the objects table.
We explicitly manage concurrent versions of an object: the version timestamp and version UUID columns are index columns, thus we may have several concurrent versions of an object.
Important: before deleting an older version from the objects table, we must make sure that we did a successful delete of the blocks of that version from the blocks table.

Thus, the workflow for reading an object is as follows:

1. Check permissions (LDAP)
2. Read entry in object table. If data is inline, we have its data, stop here.
   -> if several versions, take newest one and launch deletion of old ones in background
3. Read first block from cluster. If size <= 1 block, stop here.
4. Simultaneously with previous step, if size > 1 block: query the Blocks table for the IDs of the next blocks
5. Read subsequent blocks from cluster

Workflow for PUT:

1. Check write permission (LDAP)
2. Select a new version UUID
3. Write a preliminary entry for the new version in the objects table with complete = false
4. Send blocks to cluster and write entries in the blocks table
5. Update the version with complete = true and all of the accurate information (size, etc)
6. Return success to the user
7. Launch a background job to check and delete older versions

Workflow for DELETE:

1. Check write permission (LDAP)
2. Get current version (or versions) in object table
3. Do the deletion of those versions NOT IN A BACKGROUND JOB THIS TIME
4. Return succes to the user if we were able to delete blocks from the blocks table and entries from the object table

To delete a version:

1. List the blocks from Cassandra
2. For each block, delete it from cluster. Don't care if some deletions fail, we can do GC.
3. Delete all of the blocks from the blocks table
4. Finally, delete the version from the objects table

Known issue: if someone is reading from a version that we want to delete and the object is big, the read might be interrupted. I think it is ok to leave it like this, we just cut the connection if data disappears during a read.

("Soit P un problème, on s'en fout est une solution à ce problème")

#### Block storage on disk

**Blocks themselves:**

- file path = /blobs/(first 3 hex digits of hash)/(rest of hash)

**Reverse index for GC & other block-level metadata:**

- file path = /meta/(first 3 hex digits of hash)/(rest of hash)
- map block hash -> set of version UUIDs where it is referenced

Usefull metadata:

- list of versions that reference this block in the Casandra table, so that we can do GC by checking in Cassandra that the lines still exist
- list of other nodes that we know have acknowledged a write of this block, useful in the rebalancing algorithm

Write strategy: have a single thread that does all write IO so that it is serialized (or have several threads that manage independent parts of the hash space). When writing a blob, write it to a temporary file, close, then rename so that a concurrent read gets a consistent result (either not found or found with whole content).

Read strategy: the only read operation is get(hash) that returns either the data or not found (can do a corruption check as well and return corrupted state if it is the case). Can be done concurrently with writes.

**Internal API:**

- get(block hash) -> ok+data/not found/corrupted
- put(block hash & data, version uuid + offset) -> ok/error
- put with no data(block hash, version uuid + offset) -> ok/not found plz send data/error
- delete(block hash, version uuid + offset) -> ok/error

GC: when last ref is deleted, delete block.
Long GC procedure: check in Cassandra that version UUIDs still exist and references this block.

Rebalancing: takes as argument the list of newly added nodes.

- List all blocks that we have. For each block:
- If it hits a newly introduced node, send it to them.
  Use put with no data first to check if it has to be sent to them already or not.
  Use a random listing order to avoid race conditions (they do no harm but we might have two nodes sending the same thing at the same time thus wasting time).
- If it doesn't hit us anymore, delete it and its reference list.

Only one balancing can be running at a same time. It can be restarted at the beginning with new parameters.

#### Membership management

Two sets of nodes:

- set of nodes from which a ping was recently received, with status: number of stored blocks, request counters, error counters, GC%, rebalancing%
  (eviction from this set after say 30 seconds without ping)
- set of nodes that are part of the system, explicitly modified by the operator using the web UI (persisted to disk),
  is a CRDT using a version number for the value of the whole set

Thus, three states for nodes:

- healthy: in both sets
- missing: not pingable but part of desired cluster
- unused/draining: currently present but not part of the desired cluster, empty = if contains nothing, draining = if still contains some blocks

Membership messages between nodes:

- ping with current state + hash of current membership info -> reply with same info
- send&get back membership info (the ids of nodes that are in the two sets): used when no local membership change in a long time and membership info hash discrepancy detected with first message (passive membership fixing with full CRDT gossip)
- inform of newly pingable node(s) -> no result, when receive new info repeat to all (reliable broadcast)
- inform of operator membership change -> no result, when receive new info repeat to all (reliable broadcast)

Ring: generated from the desired set of nodes, however when doing read/writes on the ring, skip nodes that are known to be not pingable.
The tokens are generated in a deterministic fashion from node IDs (hash of node id + token number from 1 to K).
Number K of tokens per node: decided by the operator & stored in the operator's list of nodes CRDT. Default value proposal: with node status information also broadcast disk total size and free space, and propose a default number of tokens equal to 80%Free space / 10Gb. (this is all user interface)


#### Constants

- Block size: around 1MB ? --> Exoscale use 16MB chunks
- Number of tokens in the hash ring: one every 10Gb of allocated storage
- Threshold for storing data directly in Cassandra objects table: 1kb bytes (maybe up to 4kb?)
- Ping timeout (time after which a node is registered as unresponsive/missing): 30 seconds
- Ping interval: 10 seconds
- ??

#### Links

- CDC: <https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf>
- Erasure coding: <http://web.eecs.utk.edu/~jplank/plank/papers/CS-08-627.html>
- [Openstack Storage Concepts](https://docs.openstack.org/arch-design/design-storage/design-storage-concepts.html)
- [RADOS](https://doi.org/10.1145/1374596.1374606) [[pdf](https://ceph.com/assets/pdfs/weil-rados-pdsw07.pdf)]
