# [RFC] Garbage Collector Elimination

## Statement of problem and prior art

Currently, Garage's garbage collector has a few identified issues.
Namely, it can only be run if all nodes in a partition are currently online,
it may not be correct in front of a rebalancing (this is partially mitigated by a 24h delay added to tombstone deletion),
and it isn't resilient to a subset of nodes being restored from snapshots.
It's not clear if it is possible to implement a garbage collection process that can eliminate tombstones, but also support
a node rollback to a point in time where a key existed.

This problematic, perhaps unsurprisingly, maps very well to the general abstraction of CRDTs for sets, where a whole partition
would be a single CRDT. The semantic required in Garage demands reinsertion of deleted keys, which excludes the most simple
forms of sets such as G-Sets and 2P-Sets. As the goal is to not handle garbage collection of tombstones, a standard ORSet
is also unfitting. In fact it could be argued Garage already uses something akin to an ORSet with a garbage collector today.

There exists CRDTs supporting this feature-set, one of which is an OptORSet[^1].
It only needs a set-wide metadata proportional in the number of writers, and a per alive-key metadata proportional in the number
of writers to that key (but no metadata for dead keys).
These metadata are akin to DVVs[^2]. An element is considered new if its DVV comes causaly after the current DVV of the set.
Correctness of this algorithm depends however on having causal delivery, which isn't given in Garage, neither in general nor in
presence of snapshot restoration.

## Proposal

We devise a new kind of CRDT based on the core ideas of an OptORSet, but replacing each version inside its DVVs with a list of
range of observed updates, which we name a Seen Vector (SV). Such metadata can in the worst case grow linearly with the number of
insertion. In practice, assuming all elements are eventually known to every replica, the storage requirement of a SV is equivalent
to that of a standard DVV. Under causal delivery, a SV degenerates into a standard DVV.

To add an element to the set, a node increments its own version counter, and sends an update containing (element, replica, version).
On receiving such an update, a node checks if it has already seen this particular (replica, version), and if so ignore it.
If it hasn't seen that update, it saves the new element, and update its SV to include that new (replica, version).

TODO: describe formaly the algorithm

## Storage evaluation

As stated, if all updates are received, the SV is similar in size to that of a standard DVV. It may be however that a node create and
immediately delete an element, creating holes in its sequence from the point of view of other replicas. These holes could be filled
through an interactive process where the replica observing holes asks the node to scan over the whole set, and for each version in these
hole, reply if no element has that exact version number. Holes caused by existing elements should be eventually fixed by an anti-entropy
process, so replying with these elements appears unnecessary.

The per-element storage requirement is proportional to the number of replicas having modified that element, even under non steady-state.
This happens because as we always exchange whole elements, we have causal delivery for individual keys.

## Replica version rollback

This scheme assumes the same node won't issue the same version twice, which isn't a given when a node might be rollback to a previous state.
The author proposes that on initialization, a replica asks all other replicas for the highest version number they know for it.
If all replicas reply with a number less than or equal to the current version, it is safe to reuse the currently known number.
If some replicas reply with a number higher, the node increase its version to that number.
If at least one replica doesn't reply, it can't make any assumption about its actual version number.
The node then increment a generation number, which is made part of its replica id, starts a new sequence from zero.

## Appendix: providing SV to the underlying elements

Some more complexe elements may want to have access to a version id and the Seen Vector to perform their own internal merge operations.
The author reckon this may help implementing S3 versioning, by giving a simple way for Objects to know if an ObjectVersion was yet
unknown or is known and already deleted.

## References
 	
[^1]: An optimized conflict-free replicated set, https://doi.org/10.48550/arXiv.1210.3368
[^2]: Dotted Version Vectors: Logical Clocks for Optimistic Replication, https://doi.org/10.48550/arXiv.1011.5808
