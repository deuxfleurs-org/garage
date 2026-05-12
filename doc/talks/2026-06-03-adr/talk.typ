#import "@preview/slydst:0.1.5": *

// some display rules

#set par(spacing: 2em)
#set list(spacing: 1em)

#show link: set text(font: "DejaVu Sans Mono", size: 9pt)

// some functions to customize styles

#let vhcenter(content) = [
  #v(1fr)
  #align(center)[#content]
  #v(1fr)
]

#let imgcenter(..args) = vhcenter(image(..args))

#let mytable(..args) = {
  show table.cell: set text(size: 9pt)
  set table(stroke: 0.5pt + black)
  grid(
    columns: (1cm, 1fr, 1cm),
    [], table(..args), []
  )
}

// actual slides

#show: slides.with(
  //title: "Garage",
  authors: ("Alex Auvolat",),
  date: "2026-06-03",
  layout: "large",
  //ratio: 16/9,
  ratio: 4/3,
  title-color: rgb("#ff9329"),
)

#title-slide[
  #align(center)[
    #image("../../sticker/Garage.png", width: 20%)
    #v(1em)
    *An introduction to Garage*\
    Alex Auvolat, Deuxfleurs

    #v(1em)
    #link("https://garagehq.deuxfleurs.fr/")\
    Matrix channel: `#garage:deuxfleurs.fr`
  ]
]

== A non-profit initiative

#grid(
  columns: (2fr, 8fr),
  [#v(2em)],[],
  [
    #image("../assets/logos/deuxfleurs.svg", width: 50%)
  ],
  [
    *Part of a degrowth initiative*\
    Garage has been created at Deuxfleurs, where we experiment running Internet services without datacenter on commodity and refurbished hardware.
  ],
  [#v(2em)],[],
  [
    #image("../assets/community.png", width: 50%)
  ],
  [
    *Developed by a community*\
    #text(size: 0.8em)[Some recent contributors: Arthur C, Charles H, dongdigua, Etienne L, Jonah A, Julien K, Lapineige, MagicRR, Milas B, Niklas M, RockWolf, Schwitzd, trinity-1686a, Xavier S, babykart, Baptiste J, eddster2309, James O'C, Joker9944, Maximilien R, Renjaya RZ, Yureka...]
  ],
  [#v(3em)],[],
  [
    #image("../assets/logos/AGPLv3_Logo.png", width: 50%)
  ],
  [
    *Owned by nobody*\
    AGPL + no Contributor License Agreement = Garage ownership spreads among dozens of contributors.
  ]
)

== Our initial objective at Deuxfleurs

#v(4em)

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
//== Building a resilient system with cheap stuff
//
#v(4em)

#[
  #set list(spacing: 2em)
  - Commodity hardware (e.g. old desktop PCs)\
    #text(size: 0.8em)[(can die at any time)]
  - Regular Internet (e.g. FTTB, FTTH) and power grid connections\
    #text(size: 0.8em)[(can be unavailable randomly)]
  - *Geographical redundancy* (multi-site replication)
]

#pagebreak()
#imgcenter("../assets/neptune.jpg", width: 100%)

#pagebreak()
#imgcenter("../assets/atuin.jpg", width: 100%)

#pagebreak()
#imgcenter("../assets/inframap_jdll2023.pdf", width: 100%)

== Object storage: a crucial component

#vhcenter[
  #grid(
    columns: (3fr, 3fr, 3fr),
    [#image("../assets/logos/Amazon-S3.jpg", height: 6em)],
    [#image("../assets/logos/minio.png", height: 5em)],
    [#image("../../logo/garage_hires_crop.png", height: 6em)]
  )
]

S3: a de-facto standard, many compatible applications

MinIO: not suited for geo-distributed deployments, becoming closed source

*Garage is a self-hosted drop-in replacement for the Amazon S3 object store*

#v(2em)

== Principle 1: geo-distributed data model

#imgcenter("../assets/map.png", width: 90%)

Garage stores replicas on different zones when possible

== Zone-aware cluster configuration

#imgcenter("../assets/screenshots/garage_status_0.9_prod_zonehl.png", width: 100%)

Trust model: full trust between zones

#v(5em)

== Principle 2: based on CRDTs

#v(1cm)

#underline[Internally, Garage uses only CRDTs] (conflict-free replicated data types)

Why not Raft, Paxos, ...? Issues of consensus algorithms:

- *Software complexity*
- *Performance issues:*
  - The leader is a *bottleneck* for all requests
  - *Sensitive to higher latency* between nodes
  - *Takes time to reconverge* when disrupted (e.g. node going down)


== The data model of object storage

#[
  #set list(spacing: 1em)

  Object storage is basically a *key-value store*:
  
  #mytable(
    columns: (2fr, 5fr),
    align: left,
    [*Key: file path + name*], [*Value: file data + metadata*],
    [`index.html`], text(size: 8pt)[
      `Content-Type: text/html; charset=utf-8`\
      `Content-Length: 24929`\
      `<binary blob>`
    ],
    [`img/logo.svg`], text(size: 8pt)[
      `Content-Type: text/svg+xml`\
      `Content-Length: 13429`\
      `<binary blob>`
    ],
    [`download/index.html`], text(size: 8pt)[
      `Content-Type: text/html; charset=utf-8`\
      `Content-Length: 26563`\
      `<binary blob>`
    ]
  )
  
*Consistency model:*

- Not ACID (not required by S3 spec) / not linearizable
- *Read-after-write consistency*\
  #text(size: 0.8em)[(stronger than eventual consistency)]
]


== Performance evaluation

#imgcenter("../assets/perf/endpoint_latency_0.7_0.8_minio.png", width: 100%)
#pagebreak()

#imgcenter("../assets/perf/ttfb.png", width: 100%)
#pagebreak()

#imgcenter("../assets/perf/io-0.7-0.8-minio.png", width: 100%)


== Garage in the wild

#imgcenter("../assets/cluster_kind.png", width: 100%)

== Size of known deployments

#imgcenter("../assets/cluster_size.png", width: 100%)

_"Petabyte storage setup for a video site. Nginx as CDN in-front using garage-s3-website feature. Each storage node has ~64TB storage with raid10, no replication within garage. 25gbit nic. haproxy to loadbalance across 5 nodes. mostly reads with very few writes."_

_"We currently manage 7 Garage nodes, 28TB total storage, 6M blocks for 3M objects and 4TB of object data. We have been running Garage in production for 2.5 years."_

= Deploying Garage

== Chosing a replication factor

#vhcenter[
  #mytable(
    columns: (0.7fr, 1fr, 1.3fr),
    inset: 0.8em,
    align: center + horizon,
    table.header[*Replication factor*][*Pro*][*Cons*],
    [*1*], [easy single-node setup\ full space efficiency], [no metadata redundancy\ *vunlerable to hardware crash or data corruption*\ no high-availability],
    [*2*], [redundancy\ limited storage overhead], [limited high-availability\ (read-only when one node is unavailable)],
    [*3*], [high-availability setup\ best data resilience], [big storage overhead],
    [*4, 5, ...*], [possible if needed], [...],

  )

  #v(0.5cm)
  *Important note:* metadata replication == data replication\
  Choose well, this cannot be changed easily!
]

== Setting up data and metadata storage

#vhcenter[
  #mytable(
    columns: (0.7fr, 1fr, 1fr),
    inset: 0.8em,
    align: center + horizon,
    table.header[][*Metadata storage*][*Data storage*],
    [*Content*],[access keys, buckets\ index of objects],[raw data blocks],
    [*Size*],[\< 10\% of data\ rarely over 100GB],[replication × dataset size\ *no erasure-coding*],
    [*Constraints*],[latency sensitive\ write-intensive under load],[big\ many files],
    [*Ideal hardware*],[entreprise-grade SSD],[HDD],
    [*Recommended redundancy*],[RAID1],[none, use disks directly\ *avoid RAID if possible*],
    [*Recommended filesystem*],[ZFS, Btrfs],[XFS on invidual disks],
    [*Tunables in Garage*],[database engine\
                            automatic snapshots],[block size\ compression],
  )
]

== Picking a metadata engine


#vhcenter[
  All files-to-block mappings are stored in the metadata engine, including bucket and object metadata. Files below 3KB are stored directly in the metadata engine.
  #v(0.5cm)

  #mytable(
    columns: (0.7fr, 1fr, 1.3fr),
    inset: 0.8em,
    align: center + horizon,
    table.header[*Metadata engine*][*Characteristics*][*Use case*],
    [*SQlite*],[safer],[single node deployment\ small clusters\ clusters with infrequent access],
    [*LMDB*],[faster\ sometimes has inexplicable corruptions],[larger clusters with metadata redundancy],
    [*Fjall*],[experimental\ best of both worlds?],[help us test it!],
  )

  #v(0.5cm)
  Metadata engine can be set node per-node, and changed later with a migration tool
]

== Avoiding common issues as soon as possible

#vhcenter[
  #mytable(
    columns: (1fr, 1.4fr),
    inset: 0.8em,
    align: center + horizon,
    table.header[*Risk*][*How to avoid*],
    [*Metadata corruption*\ (esp. with LMDB)],[Configure automatic snapshots with\ `metadata_auto_snapshot_interval`\ Use replication factor 2 or 3],
    [*Data not well balanced between nodes*],[Avoid clusters with too many nodes\ Target: \#nodes ≤ 10 × replication_factor],
    [*Performance issues with many objects in one single bucket*],[Spread your data over multiple buckets],
    [*Performance issues with big objects*],[Increase `block_size` configuration parameter\ Target: object size ≤ 1000 × `block_size`,\ `block_size` ≤ 100MB],
    [*Performance issues with many small objects*],[Have enough RAM to fit the entire metadata DB],
  )
]

== Other things to consider during set-up

#vhcenter[
  #mytable(
    columns: (1fr, 1.2fr),
    inset: 0.8em,
    align: center + horizon,
    [*Tools for cluster deployment*],[Ansible + systemd\ NixOS\ Kubernetes or Nomad with Docker],
    [*Initial cluster setup*],[Manual layout configuration\ Read the documentation!],
    [*TLS support on public endpoints*],[Add an external reverse-proxy (Nginx, ...)],
    [*S3 anonymous access*],[Not implemented, use website endpoint],
    [*Monitoring*],[Prometheus + Grafana for Garage metrics\ External tool to monitor HDD health],
  )
]

== Monitoring with Prometheus + Grafana

#imgcenter("../2026-01-31-fosdem/assets/garage-stats.png", width: 83%)

== Common issues and their solutions

#vhcenter[
  #mytable(
    columns: (1fr, 1.5fr),
    inset: 0.8em,
    align: center + horizon,
    table.header[*Problem*][*Solution*],
    table.cell(rowspan: 2)[*S3 access authorization issues*],[Correctly set the `region` parameter in your S3 client\ default = `garage`, not `us-east-1`],[Check your reverse proxy configuration],
    [*Debugging other API issues*],[Set `RUST_LOG=garage=debug` to investigate],
    [*Resync queue fills up*],[`garage worker set -a resync-worker-count 8`\ `garage worker set -a resync-tranquility 0`],
    [*LMDB database too big*],[Stop garage and compact with `mdb_copy -c`],
    [*Data recovery with dead/unavailable nodes*],[Consistency mode `degraded` allows to read data from an unhealthy cluster. *Do not use it for regular operation.*],
    [*Other issues*],[Ask us on matrix `#garage:deuxfleurs.fr` or open an issue on `git.deuxfleurs.fr`\
      Provide the output of `garage status`, `garage stats` and relevant metrics and logs],

  )
]

== Future developments

#imgcenter("../assets/survey_requested_features.png", width: 80%)

#pagebreak()
#imgcenter("../2026-01-31-fosdem/assets/Garage Web Admin - Dashboard@2x.png", width: 100%)

#pagebreak()
#imgcenter("../2026-01-31-fosdem/assets/Garage Web Admin - Bucket details page@2x.png", width: 100%)

== Where to find us

#align(center)[
  #v(1fr)

  #image("../../logo/garage_hires.png", width: 25%)
  #link("https://garagehq.deuxfleurs.fr/")\
  #link("mailto:garagehq@deuxfleurs.fr")\
  `#garage:deuxfleurs.fr` on Matrix

  #v(1fr)
  #grid(columns: (6%,3%,13%),
    image("../assets/logos/rust_logo.png"),
    [],
    image("../assets/logos/AGPLv3_Logo.png"),
  )
]
