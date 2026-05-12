#import "@preview/slydst:0.1.5": *

#show: slides

#title-slide[
  #image("../../sticker/Garage.png", width: 30%)
  #v(1em)
  #text(1.2em, weight: "bold")[Garage]
  #v(1em)
  Alex Auvolat, Deuxfleurs Association

  #v(1em)
  #link("https://garagehq.deuxfleurs.fr/")
  #v(0.5em)
  Matrix channel: `#garage:deuxfleurs.fr`
]

== Who I am

#grid(
  columns: (2fr, 6fr, 2fr),
  [
    #image("../assets/alex.jpg", width: 40%)
  ],
  [
    #text(weight: "bold")[Alex Auvolat]\
    PhD; co-founder of Deuxfleurs
  ],
  []
)

#v(2em)

#grid(
  columns: (2fr, 6fr, 2fr),
  [
    #image("../assets/logos/deuxfleurs.svg", width: 50%)
  ],
  [
    #text(weight: "bold")[Deuxfleurs]\
    A non-profit self-hosting collective,\
    member of the CHATONS network
  ],
  [
    #image("../assets/logos/logo_chatons.png", width: 70%)
  ]
)

== Our objective at Deuxfleurs

#align(center)[
  #text(weight: "bold")[
    Promote self-hosting and small-scale hosting\
    as an alternative to large cloud providers
  ]
]

#v(2em)

Why is it hard?

#v(2em)

#align(center)[
  #underline[Resilience]\
  #text(size: 0.8em)[we want good uptime/availability with low supervision]
]

== Our very low-tech infrastructure

- Commodity hardware (e.g. old desktop PCs)\
  #text(size: 0.8em)[(can die at any time)]
- Regular Internet (e.g. FTTB, FTTH) and power grid connections\
  #text(size: 0.8em)[(can be unavailable randomly)]
- *Geographical redundancy* (multi-site replication)

#v(1em)

#image("../assets/neptune.jpg", width: 80%)

#pagebreak()

#image("../assets/inframap_jdll2023.pdf", width: 80%)

== How to make this happen

#image("../assets/intro/slide1.png", width: 80%)
#image("../assets/intro/slide2.png", width: 80%)
#image("../assets/intro/slide3.png", width: 80%)

== Distributed file systems are slow

File systems are complex, for example:

- Concurrent modification by several processes
- Folder hierarchies
- Other requirements of the POSIX spec (e.g. locks)

Coordination in a distributed system is costly.

Costs explode with commodity hardware / Internet connections\
#text(size: 0.8em)[(we experienced this!)]

== A simpler solution: object storage

Only two operations:

- Put an object at a key
- Retrieve an object from its key

#text(size: 0.8em)[(and a few others)]

Sufficient for many applications!

== A simpler solution: object storage

#grid(
  columns: (3fr, 3fr, 3fr),
  [#image("../assets/logos/Amazon-S3.jpg", height: 6em)],
  [#image("../assets/logos/minio.png", height: 5em)],
  [#image("../../logo/garage_hires_crop.png", height: 6em)]
)

S3: a de-facto standard, many compatible applications

MinIO is self-hostable but not suited for geo-distributed deployments

*Garage is a self-hosted drop-in replacement for the Amazon S3 object store*

== Principle 1: based on CRDTs

//#section[Principle 1: based on CRDTs]

== CRDTs / weak consistency instead of consensus

#underline[Internally, Garage uses only CRDTs] (conflict-free replicated data types)

Why not Raft, Paxos, ...? Issues of consensus algorithms:

- *Software complexity*
- *Performance issues:*
  - The leader is a *bottleneck* for all requests
  - *Sensitive to higher latency* between nodes
  - *Takes time to reconverge* when disrupted (e.g. node going down)

== The data model of object storage

Object storage is basically a *key-value store*:

#table(
  columns: (2fr, 5fr),
  align: left,
  [*Key: file path + name*], [*Value: file data + metadata*],
  [`index.html`], [
    Content-Type: text/html; charset=utf-8\
    Content-Length: 24929\
    \<binary blob\>
  ],
  [`img/logo.svg`], [
    Content-Type: text/svg+xml\
    Content-Length: 13429\
    \<binary blob\>
  ],
  [`download/index.html`], [
    Content-Type: text/html; charset=utf-8\
    Content-Length: 26563\
    \<binary blob\>
  ]
)

- Maps well to CRDT data types
- Read-after-write consistency with quorums

== Performance gains in practice

#image("../assets/perf/endpoint_latency_0.7_0.8_minio.png", width: 80%)

== Principle 2: geo-distributed data model

//#section[Principle 2: geo-distributed data model]

== Key-value stores, upgraded: the Dynamo model

*Two keys:*

- Partition key: used to divide data into partitions (a.k.a. shards)
- Sort key: used to identify items inside a partition

#table(
  columns: (2fr, 2fr, 3fr),
  align: left,
  [*Partition key: bucket*], [*Sort key: filename*], [*Value*],
  [`website`], [`index.html`], [(file data)],
  [`website`], [`img/logo.svg`], [(file data)],
  [`website`], [`download/index.html`], [(file data)],
  [`backup`], [`borg/index.2822`], [(file data)],
  [`backup`], [`borg/data/2/2329`], [(file data)],
  [`backup`], [`borg/data/2/2680`], [(file data)],
  [`private`], [`qq3a2nbe1qjq0ebbvo6ocsp6co`], [(file data)]
)

== Layout computation

#image("../assets/screenshots/garage_status_0.9_prod_zonehl.png", width: 100%)
#image("../assets/map.png", width: 70%)

Garage stores replicas on different zones when possible

== What a "layout" is

*A layout is a precomputed index table:*

#table(
  columns: (2fr, 2fr, 2fr, 2fr),
  align: left,
  [*Partition*], [*Node 1*], [*Node 2*], [*Node 3*],
  [Partition 0], [df-ymk (bespin)], [Abricot (scorpio)], [Courgette (neptune)],
  [Partition 1], [Ananas (scorpio)], [Courgette (neptune)], [df-ykl (bespin)],
  [Partition 2], [df-ymf (bespin)], [Celeri (neptune)], [Abricot (scorpio)],
  [⋮], [⋮], [⋮], [⋮],
  [Partition 255], [Concombre (neptune)], [df-ykl (bespin)], [Abricot (scorpio)]
)

The index table is built centrally using an optimal algorithm, then propagated to all nodes

#text(size: 0.8em)[
  Oulamara, M., & Auvolat, A. (2023). _An algorithm for geo-distributed and redundant storage in Garage_. arXiv preprint arXiv:2302.13798.
]

== The relationship between partition and partition key

#table(
  columns: (2fr, 2fr, 2fr, 3fr),
  align: left,
  [*Partition key*], [*Partition*], [*Sort key*], [*Value*],
  [`website`], [Partition 12], [`index.html`], [(file data)],
  [`website`], [Partition 12], [`img/logo.svg`], [(file data)],
  [`website`], [Partition 12], [`download/index.html`], [(file data)],
  [`backup`], [Partition 42], [`borg/index.2822`], [(file data)],
  [`backup`], [Partition 42], [`borg/data/2/2329`], [(file data)],
  [`backup`], [Partition 42], [`borg/data/2/2680`], [(file data)],
  [`private`], [Partition 42], [`qq3a2nbe1qjq0ebbvo6ocsp6co`], [(file data)]
)

To read or write an item: hash partition key → determine partition number (first 8 bits) → find associated nodes

== Garage's internal data structures

#image("../assets/garage_tables.pdf", width: 75%)

== Operating Garage clusters

//#section[Operating Garage clusters]

== Operating Garage

#image("../assets/screenshots/garage_status_0.10.png", width: 90%)
#image("../assets/screenshots/garage_status_unhealthy_0.10.png", width: 90%)

== Background synchronization

#image("../assets/garage_sync.drawio.pdf", width: 60%)

== Digging deeper

#image("../assets/screenshots/garage_stats_0.10.png", width: 90%)
#image("../assets/screenshots/garage_worker_list_0.10.png", width: 50%)
#image("../assets/screenshots/garage_worker_param_0.10.png", width: 60%)

== Monitoring with Prometheus + Grafana

#image("../assets/screenshots/grafana_dashboard.png", width: 90%)

== Debugging with traces

#image("../assets/screenshots/jaeger_listobjects.png", width: 80%)

== Scaling Garage clusters

//#section[Scaling Garage clusters]

== Potential limitations and bottlenecks

- Global:
  - Max. ~100 nodes per cluster (excluding gateways)
- Metadata:
  - One big bucket = bottleneck, object list on 3 nodes only
- Block manager:
  - Lots of small files on disk
  - Processing the resync queue can be slow

== Deployment advice for very large clusters

- Metadata storage:
  - ZFS mirror (x2) on fast NVMe
  - Use LMDB storage engine
- Data block storage:
  - Use Garage's native multi-HDD support
  - XFS on individual drives
  - Increase block size (1MB → 10MB, requires more RAM and good networking)
  - Tune `resync-tranquility` and `resync-worker-count` dynamically
- Other:
  - Split data over several buckets
  - Use less than 100 storage nodes
  - Use gateway nodes

Our deployments: < 10 TB. Some people have done more!

== Where to find us

#image("../../logo/garage_hires.png", width: 25%)
#link("https://garagehq.deuxfleurs.fr/")
#link("mailto:garagehq@deuxfleurs.fr")
`#garage:deuxfleurs.fr` on Matrix

#v(1.5em)
#image("../assets/logos/rust_logo.png", width: 6%)
#image("../assets/logos/AGPLv3_Logo.png", width: 13%)
