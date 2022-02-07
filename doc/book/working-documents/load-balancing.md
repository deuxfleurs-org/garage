+++
title = "Load balancing data"
weight = 10
+++

**This is being yet improved in release 0.5. The working document has not been updated yet, it still only applies to Garage 0.2 through 0.4.**

I have conducted a quick study of different methods to load-balance data over different Garage nodes using consistent hashing.

## Requirements

- *good balancing*: two nodes that have the same announced capacity should receive close to the same number of items

- *multi-datacenter*: the replicas of a partition should be distributed over as many datacenters as possible

- *minimal disruption*: when adding or removing a node, as few partitions as possible should have to move around

- *order-agnostic*: the same set of nodes (each associated with a datacenter name
  and a capacity) should always return the same distribution of partition
  replicas, independently of the order in which nodes were added/removed (this
  is to keep the implementation simple)

## Methods

### Naive multi-DC ring walking strategy

This strategy can be used with any ring-like algorithm to make it aware of the *multi-datacenter* requirement:

In this method, the ring is a list of positions, each associated with a single node in the cluster.
Partitions contain all the keys between two consecutive items of the ring.
To find the nodes that store replicas of a given partition:

- select the node for the position of the partition's lower bound
- go clockwise on the ring, skipping nodes that:
  - we halve already selected
  - are in a datacenter of a node we have selected, except if we already have nodes from all possible datacenters

In this way the selected nodes will always be distributed over
`min(n_datacenters, n_replicas)` different datacenters, which is the best we
can do.

This method was implemented in the first version of Garage, with the basic
ring construction from Dynamo DB that consists in associating `n_token` random positions to
each node (I know it's not optimal, the Dynamo paper already studies this).

### Better rings

The ring construction that selects `n_token` random positions for each nodes gives a ring of positions that
is not well-balanced: the space between the tokens varies a lot, and some partitions are thus bigger than others.
This problem was demonstrated in the original Dynamo DB paper.

To solve this, we want to apply a better second method for partitionning our dataset:

1. fix an initially large number of partitions (say 1024) with evenly-spaced delimiters, 

2. attribute each partition randomly to a node, with a probability
   proportionnal to its capacity (which `n_tokens` represented in the first
   method)

For now we continue using the multi-DC ring walking described above.

I have studied two ways to do the attribution of partitions to nodes, in a way that is deterministic:

- Min-hash: for each partition, select node that minimizes `hash(node, partition_number)`
- MagLev: see [here](https://blog.acolyer.org/2016/03/21/maglev-a-fast-and-reliable-software-network-load-balancer/)

MagLev provided significantly better balancing, as it guarantees that the exact
same number of partitions is attributed to all nodes that have the same
capacity (and that this number is proportionnal to the node's capacity, except
for large values), however in both cases:

- the distribution is still bad, because we use the naive multi-DC ring walking
  that behaves strangely due to interactions between consecutive positions on
  the ring

- the disruption in case of adding/removing a node is not as low as it can be,
  as we show with the following method.

A quick description of MagLev (backend = node, lookup table = ring):

> The basic idea of Maglev hashing is to assign a preference list of all the
> lookup table positions to each backend. Then all the backends take turns
> filling their most-preferred table positions that are still empty, until the
> lookup table is completely filled in. Hence, Maglev hashing gives an almost
> equal share of the lookup table to each of the backends. Heterogeneous
> backend weights can be achieved by altering the relative frequency of the
> backends’ turns…

Here are some stats (run `scripts/simulate_ring.py` to reproduce):

```
##### Custom-ring (min-hash) #####

#partitions per node (capacity in parenthesis):
- datura (8) :  227
- digitale (8) :  351
- drosera (8) :  259
- geant (16) :  476
- gipsie (16) :  410
- io (16) :  495
- isou (8) :  231
- mini (4) :  149
- mixi (4) :  188
- modi (4) :  127
- moxi (4) :  159

Variance of load distribution for load normalized to intra-class mean
(a class being the set of nodes with the same announced capacity): 2.18%     <-- REALLY BAD

Disruption when removing nodes (partitions moved on 0/1/2/3 nodes):
removing atuin digitale : 63.09% 30.18% 6.64% 0.10%
removing atuin drosera : 72.36% 23.44% 4.10% 0.10%
removing atuin datura : 73.24% 21.48% 5.18% 0.10%
removing jupiter io : 48.34% 38.48% 12.30% 0.88%
removing jupiter isou : 74.12% 19.73% 6.05% 0.10%
removing grog mini : 84.47% 12.40% 2.93% 0.20%
removing grog mixi : 80.76% 16.60% 2.64% 0.00%
removing grog moxi : 83.59% 14.06% 2.34% 0.00%
removing grog modi : 87.01% 11.43% 1.46% 0.10%
removing grisou geant : 48.24% 37.40% 13.67% 0.68%
removing grisou gipsie : 53.03% 33.59% 13.09% 0.29%
on average:  69.84% 23.53% 6.40% 0.23%                  <-- COULD BE BETTER

--------

##### MagLev #####

#partitions per node:
- datura (8) :  273
- digitale (8) :  256
- drosera (8) :  267
- geant (16) :  452
- gipsie (16) :  427
- io (16) :  483
- isou (8) :  272
- mini (4) :  184
- mixi (4) :  160
- modi (4) :  144
- moxi (4) :  154

Variance of load distribution: 0.37%                <-- Already much better, but not optimal

Disruption when removing nodes (partitions moved on 0/1/2/3 nodes):
removing atuin digitale : 62.60% 29.20% 7.91% 0.29%
removing atuin drosera : 65.92% 26.56% 7.23% 0.29%
removing atuin datura : 63.96% 27.83% 7.71% 0.49%
removing jupiter io : 44.63% 40.33% 14.06% 0.98%
removing jupiter isou : 63.38% 27.25% 8.98% 0.39%
removing grog mini : 72.46% 21.00% 6.35% 0.20%
removing grog mixi : 72.95% 22.46% 4.39% 0.20%
removing grog moxi : 74.22% 20.61% 4.98% 0.20%
removing grog modi : 75.98% 18.36% 5.27% 0.39%
removing grisou geant : 46.97% 36.62% 15.04% 1.37%
removing grisou gipsie : 49.22% 36.52% 12.79% 1.46%
on average:  62.94% 27.89% 8.61% 0.57%                  <-- WORSE THAN PREVIOUSLY
```

### The magical solution: multi-DC aware MagLev

Suppose we want to select three replicas for each partition (this is what we do in our simulation and in most Garage deployments).
We apply MagLev three times consecutively, one for each replica selection.
The first time is pretty much the same as normal MagLev, but for the following times, when a node runs through its preference
list to select a partition to replicate, we skip partitions for which adding this node would not bring datacenter-diversity.
More precisely, we skip a partition in the preference list if:

- the node already replicates the partition (from one of the previous rounds of MagLev)
- the node is in a datacenter where a node already replicates the partition and there are other datacenters available

Refer to `method4` in the simulation script for a formal definition.

```
##### Multi-DC aware MagLev #####

#partitions per node:
- datura (8) :  268                 <-- NODES WITH THE SAME CAPACITY
- digitale (8) :  267                   HAVE THE SAME NUM OF PARTITIONS
- drosera (8) :  267                    (+- 1)
- geant (16) :  470
- gipsie (16) :  472
- io (16) :  516
- isou (8) :  268
- mini (4) :  136
- mixi (4) :  136
- modi (4) :  136
- moxi (4) :  136

Variance of load distribution: 0.06%                <-- CAN'T DO BETTER THAN THIS

Disruption when removing nodes (partitions moved on 0/1/2/3 nodes):
removing atuin digitale : 65.72% 33.01% 1.27% 0.00%
removing atuin drosera : 64.65% 33.89% 1.37% 0.10%
removing atuin datura : 66.11% 32.62% 1.27% 0.00%
removing jupiter io : 42.97% 53.42% 3.61% 0.00%
removing jupiter isou : 66.11% 32.32% 1.56% 0.00%
removing grog mini : 80.47% 18.85% 0.68% 0.00%
removing grog mixi : 80.27% 18.85% 0.88% 0.00%
removing grog moxi : 80.18% 19.04% 0.78% 0.00%
removing grog modi : 79.69% 19.92% 0.39% 0.00%
removing grisou geant : 44.63% 52.15% 3.22% 0.00%
removing grisou gipsie : 43.55% 52.54% 3.91% 0.00%
on average:  64.94% 33.33% 1.72% 0.01%                  <-- VERY GOOD (VERY LOW VALUES FOR 2 AND 3 NODES)
```
