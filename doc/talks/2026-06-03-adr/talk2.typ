#import "@preview/slydst:0.1.5": *

#show: slides

#title-slide[
  #image("../../sticker/Garage.png", width: 30%)
  #v(1em)
  #text(1.2em, weight: "bold")[Garage, an S3 backend as reliable as possible]
  #v(1em)
  Garage Authors

  #v(1em)
  #link("https://garagehq.deuxfleurs.fr/")
  #v(0.5em)
  #link("mailto:garagehq@deuxfleurs.fr")
  #v(0.5em)
  Matrix channel: `#garage:deuxfleurs.fr`
]

== Meet Garage

== A non-profit initiative

#grid(
  columns: (2fr, 8fr),
  [
    #image("../assets/logos/deuxfleurs.svg", width: 50%)
  ],
  [
    *Part of a degrowth initiative*\
    Garage has been created at Deuxfleurs where we experiment running Internet services without datacenter on commodity and refurbished hardware.
  ]
)

#v(2em)

#grid(
  columns: (2fr, 8fr),
  [
    #image("../assets/community.png", width: 50%)
  ],
  [
    *Developed by a community*\
    #text(size: 0.8em)[Some recent contributors: Arthur C, Charles H, dongdigua, Etienne L, Jonah A, Julien K, Lapineige, MagicRR, Milas B, Niklas M, RockWolf, Schwitzd, trinity-1686a, Xavier S, babykart, Baptiste J, eddster2309, James O'C, Joker9944, Maximilien R, Renjaya RZ, Yureka...]
  ]
)

#v(2em)

#grid(
  columns: (2fr, 8fr),
  [
    #image("../assets/logos/AGPLv3_Logo.png", width: 50%)
  ],
  [
    *Owned by nobody, open-core is impossible, zero VC money*\
    AGPL + no Contributor License Agreement = Garage ownership spreads among hundredth of contributors.
  ]
)

== Getting support for Garage

#grid(
  columns: (2fr, 4fr, 3fr, 1fr),
  [
    #image("../assets/alex.jpg", width: 40%)
  ],
  [
    *Alex Auvolat*\
    PhD; co-founder of Deuxfleurs\
    Garage maintainer, Freelance
  ],
  [
    #image("../assets/support.png", width: 40%)
  ],
  [
    ""
  ]
)

#v(2em)

#grid(
  columns: (2fr, 4fr, 4fr),
  [
    #image("../assets/quentin.jpg", width: 40%)
  ],
  [
    *Quentin Dufour*\
    PhD; co-founder of Deuxfleurs\
    Garage contributor, Freelance
  ],
  [
    For support requests, write at:\
    #link("mailto:garagehq@deuxfleurs.fr")
  ]
)

#v(2em)

#grid(
  columns: (2fr, 4fr, 4fr),
  [
    #image("../assets/armael.jpg", width: 40%)
  ],
  [
    *Armaël Guéneau*\
    PhD; member of Deuxfleurs\
    Garage contributor, Freelance
  ],
  [
    Eligible: email support, architecture design, specific feature development, etc.
  ]
)

== Our initial goal

#align(center)[
  #text(size: 1.2em)[Being a self-sovereign community to be free of our degrowth choice]
  #v(1em)
  $arrow.b.double$
  #v(1em)
  As web citizens, datacenters are big black boxes.\
  We want to leave them to autonomously manage our servers.
  #v(1em)
  $arrow.b.double$
  #v(1em)
  We want reliable services without relying on dedicated hardware or places.
]

== Building a resilient system with cheap stuff

- Commodity hardware (e.g. old desktop PCs)\
  #text(size: 0.8em)[(can die at any time)]
- Regular Internet (e.g. FTTB, FTTH) and power grid connections\
  #text(size: 0.8em)[(can be unavailable randomly)]
- *Geographical redundancy* (multi-site replication)

#v(1em)
#image("../assets/neptune.jpg", width: 80%)
#pagebreak()
#image("../assets/atuin.jpg", width: 80%)
#pagebreak()
#image("../assets/inframap_jdll2023.pdf", width: 80%)

== Object storage: a crucial component

#grid(
  columns: (3fr, 3fr, 3fr),
  [#image("../assets/logos/Amazon-S3.jpg", height: 6em)],
  [#image("../assets/logos/minio.png", height: 5em)],
  [#image("../../logo/garage_hires_crop.png", height: 6em)]
)

S3: a de-facto standard, many compatible applications

MinIO is self-hostable but not suited for geo-distributed deployments

*Garage is a self-hosted drop-in replacement for the Amazon S3 object store*

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

== Performance gains in practice

#image("../assets/perf/endpoint_latency_0.7_0.8_minio.png", width: 80%)

== Production clusters

== Deployment kinds

#image("../assets/cluster_kind.png", width: 90%)

== How big they are?

#image("../assets/cluster_size.png", width: 90%)

_"Petabyte storage setup for a video site. Nginx as CDN in-front using garage-s3-website feature. Each storage node has ~64TB storage with raid10, no replication within garage. 25gbit nic. haproxy to loadbalance across 5 nodes. mostly reads with very few writes."_

#v(1em)

_"We currently manage 7 Garage nodes, 28TB total storage, 6M blocks for 3M objects and 4TB of object data. We have been running Garage in production for 2.5 years."_

== Operating Garage

#image("../assets/screenshots/garage_status_0.10.png", width: 90%)
#image("../assets/screenshots/garage_status_unhealthy_0.10.png", width: 90%)

== Garage's architecture

#image("../assets/garage.drawio.pdf", width: 45%)
#image("../assets/garage_sync.drawio.pdf", width: 60%)

== Digging deeper

#image("../assets/screenshots/garage_stats_0.10.png", width: 90%)
#image("../assets/screenshots/garage_worker_list_0.10.png", width: 50%)
#image("../assets/screenshots/garage_worker_param_0.10.png", width: 60%)

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

== Focus on Deuxfleurs

Host institutional websites, partnership with a web agency.\
Matrix media backend.

Plan to use it as an email backend for an internally developed email server.

== Recent developments

#image("../assets/tl.drawio.png", width: 80%)

== April 2022 - Garage v0.7.0

Focus on #underline[observability and ecosystem integration]

- *Monitoring:* metrics and traces, using OpenTelemetry
- Replication modes with 1 or 2 copies / weaker consistency
- Kubernetes integration for node discovery
- Admin API (v0.7.2)

== Metrics (Prometheus + Grafana)

#image("../assets/screenshots/grafana_dashboard.png", width: 90%)

== Traces (Jaeger)

#image("../assets/screenshots/jaeger_listobjects.png", width: 80%)

== November 2022 - Garage v0.8.0

Focus on #underline[performance]

- *Alternative metadata DB engines* (LMDB, Sqlite)
- *Performance improvements:* block streaming, various optimizations...
- Bucket quotas (max size, max \#objects)
- Quality of life improvements, observability, etc.

== About metadata DB engines

*Issues with Sled:*

- Huge files on disk
- Unpredictable performance, especially on HDD
- API limitations
- Not actively maintained

#v(2em)
*LMDB:* very stable, good performance, file size is reasonable\
*Sqlite* also available as a second choice

#v(1em)
Sled will be removed in Garage v1.0

== DB engine performance comparison

#image("../assets/perf/db_engine.png", width: 60%)
#text(size: 0.8em)[NB: Sqlite was slow due to synchronous mode, now configurable]

== Block streaming

#image("../assets/schema-streaming-1.png", width: 80%)
#image("../assets/schema-streaming-2.png", width: 80%)

== TTFB benchmark

#image("../assets/perf/ttfb.png", width: 80%)

== Throughput benchmark

#image("../assets/perf/io-0.7-0.8-minio.png", width: 70%)

== October 2023 - Garage v0.9.0

Focus on #underline[streamlining & usability]

- Support multiple HDDs per node
- S3 compatibility:
  - support basic lifecycle configurations
  - allow for multipart upload part retries
- LMDB by default, deprecation of Sled
- New layout computation algorithm

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

== April 2024 - Garage v1.0.0

Focus on #underline[consistency, security & stability]

- Fix consistency issues when reshuffling data (Jepsen testing)
- *Security audit* by Radically Open Security
- Misc. S3 features (SSE-C, checksums, ...) and compatibility fixes

== Garage v2.0.0

Focus on #underline[]
- TODO

== Currently funding...

_..._

== We run community surveys

#image("../assets/survey_requested_features.png", width: 60%)

== Where to find us

#image("../../logo/garage_hires.png", width: 25%)
#link("https://garagehq.deuxfleurs.fr/")
#link("mailto:garagehq@deuxfleurs.fr")
`#garage:deuxfleurs.fr` on Matrix

#v(1.5em)
#image("../assets/logos/rust_logo.png", width: 6%)
#image("../assets/logos/AGPLv3_Logo.png", width: 13%)
