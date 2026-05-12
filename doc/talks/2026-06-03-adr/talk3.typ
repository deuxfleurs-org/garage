#import "@preview/slydst:0.1.5": *

#show: slides

#title-slide[
  #image("../../sticker/Garage.pdf", width: 30%)
  #v(1em)
  #text(1.2em, weight: "bold")[Garage Object Storage: 2.0 update and best practices]
  #v(0.5em)
  _a new storage platform for self-hosted geo-distributed clusters_
  #v(1em)
  Maximilien Richer, Deuxfleurs

  #v(1em)
  #link("https://garagehq.deuxfleurs.fr/")
  #v(0.5em)
  Matrix channel: `\#garage:deuxfleurs.fr`
]

== Our objective at Deuxfleurs

#align(center)[
  French association promoting digital sovereignty and privacy\
  through self-hosting hosting *as an alternative to large cloud providers*
]
#v(2em)
#align(center)[
  *This requires #underline[resilience]*\
  #text(size: 0.8em)[(we want good uptime/availability with low supervision)]
]

== But what is Garage, exactly?

*Garage is a self-hosted drop-in replacement for the Amazon S3 object store*\
that implements resilience through geographical redundancy on commodity hardware

#v(1em)
#image("../2026-01-31-fosdem/assets/garageuses.png", width: 80%)

== What makes Garage different?

*Coordination-free:*
#v(2em)
- No Raft or Paxos
- Internal data types are CRDTs
- All nodes are equivalent (no master/leader/index node)

#v(2em)
$\to$ less sensitive to higher latencies between nodes

== What makes Garage different?

#align(center)[
  _TODO update with latest garage and minio versions_
  #image("../2026-01-31-fosdem/assets/endpoint-latency-dc.png", width: 90%)
]

== What makes Garage different?

*Consistency model:*
#v(2em)
- Not ACID (not required by S3 spec) / not linearizable
- *Read-after-write consistency*\
  #text(size: 0.8em)[(stronger than eventual consistency)]

== What makes Garage different?

*Location-aware:*
#v(2em)
#image("../2026-01-31-fosdem/assets/location-aware.png", width: 100%)
#v(2em)
Garage replicates data on different zones when possible

== What makes Garage different?

#image("../2026-01-31-fosdem/assets/map.png", width: 80%)

== An ever-increasing compatibility list

#image("../2026-01-31-fosdem/assets/compatibility.png", width: 70%)

== Version history and roadmap

- v0.3: initial beta release (2021)
- v0.7: first released version (2022)
- v1.0: stable release (2024), will be deprecated in summer 2026 1y after v2.0 was released
- v2.0: stable release (2025)
  - new HTTP admin API
  - reworded replication configuration: `replication_mode` changed to `replication_factor` \& `consistency_policy`
-

#align(center)[
  v3.0: TBA may include versionning support, tag on buckets and objects, retention policies...
]

== Best practices for Garage deployments

#align(center)[
  #text(1.2em, weight: "bold")[Best practices for Garage deployments]
]

== Things you should know

- no TLS support, use your own proxy
- no anonymous access (use website endpoint)
- you need to assign roles to nodes manually
- the replication factor cannot be changed easily
- the default region is `garage` and not `us-east-1`
- only use the `degraded` consistency policy for data recovery!

== What hardware should I use?

- do NOT use network file storage (NFS, SMB, etc.) for `metadata`
- get a *write-intensive flash disk* for the `metadata` folder
- set `metadata` on a RAID1 if possible, with a COW filesystem (e.g. Btrfs or ZFS)
- get large HDDs for the `data` folder
- use XFS and garage multi-hdd mode for best performance
- you can use a RAID for data but you'll leave a lot of performance on the table

#align(center)[
  _Garage doesn't require a powerful CPUs nor much RAM, but your performance will depend on your disks!_
]

== Picking a metadata engine

All files-to-block mappings are stored in the metadata engine, including bucket and object metadata. Files below 3KB are stored directly in the metadata engine.

#v(1em)
- Sled: removed in 1.x, move to SQLite or LMDB
- *SQLite*: safer, _recommended for small clusters and single-node_
- LMDB: faster, recommended for large clusters with metadata redundancy
  - Warning: limited to 480 bytes per key with LMDB (not an issue in practice)
- Fjall: experimental but promising rust-native engine, test it and let us know!

#align(center)[
  Metadata engine can be set node per node, and changed later with a migration tool
]

== Single-node deployment

- garage was initially designed for multi-node deployments
- single-node deployments are possible, but you will lose resilience
- *If you do please ensure you have backups* (especially for metadata)
  - set up `metadata_auto_snapshot_interval`
- use sqlite to minimize data loss risks on powercuts
- or use a UPS!

#v(1em)
Use `github.com/bikeshedder/garage-single-node` for an easy single-node setup!

== Multi-node deployment

- try to have geo-distributed zones
- multiple nodes per zone to add more capacity
- at least 3 zones for best resilience
- keep in mind your available network and IO bandwidth
- *Rebalancing a cluster can take multiple weeks with large HDDs and slow network links*
- monitor your nodes with Prometheus + Grafana

#align(center)[
  Deuxfleurs has been running a 9TB (3TB usable) 8-nodes cluster (3+3+2) over retail fiber (10ms site-to-site latency) for close to 5 years now. We heard there are petabyte clusters out there!
]

== Deploying and administering garage at scale

- deploy with your favorite tool (eg. Ansible) and system manager (eg. systemd)
- or use Docker, docker-compose, Kubernetes or Nomad
- Kubernetes and Consul are supported for node-to-node discovery
  - you'll still have to manage the layout manually!
- use gateway nodes to optimize network usage
- ajust `resync-tranquility` and `scrub-tranquility` to your ressources

#align(center)[
  Kubernetes storage controller: `github.com/bmarinov/garage-storage-controller`
]

== Community UI available!

#align(center)[
  #image("../2026-01-31-fosdem/assets/community-ui.png", width: 90%)
  #v(-1em)
  #link("https://github.com/khairul169/garage-webui")
]

== Official Embedded UI coming later this year!

#align(center)[
  #image("../2026-01-31-fosdem/assets/Garage Web Admin - Dashboard@2x.png", width: 90%)
  #v(-1em)
]

== Official Embedded UI coming this year!

#align(center)[
  #image("../2026-01-31-fosdem/assets/Garage Web Admin - Bucket details page@2x.png", width: 90%)
  #v(-1em)
]

== How to make sense of garage metrics?

#align(center)[
  #image("../2026-01-31-fosdem/assets/garage-stats.png", width: 70%)
  #v(-1em)
]

== What if things go wrong?

- set logs to debug with `RUST_LOG=garage_api_common=debug,garage_api_s3=debug,garage=debug`
- auth issues: check your reverse proxy configuration
- slow resync: check your network and disk IO usage, and `resync-tranquility` worker configuration
- big LMDB database: stop garage and compact with `mdb_copy -c`
- ask us on matrix `\#garage:deuxfleurs.fr` or open an issue on git.deuxfleurs.fr!
  - provide the output of `garage status`, `garage stats` and relevant metrics and logs

== Moving from Minio

- list your buckets and your keys
- create buckets and keys on the garage cluster
  - you cannot import non-garage keys yet, patch to come soon!
- loop over buckets, copy with rclone
  - see doc #link("https://garagehq.deuxfleurs.fr/documentation/connect/cli/")
- blog post coming soon!

== Demo time!

== Get Garage now!

#align(center)[
  #image("../../logo/garage_hires.png", width: 30%)
  #v(-1em)
  #link("https://garagehq.deuxfleurs.fr/")
  Matrix channel: `\#garage:deuxfleurs.fr`
  #v(2em)
  #image("../2026-01-31-fosdem/assets/rust_logo.png", width: 9%)
  #image("../2026-01-31-fosdem/assets/AGPLv3_Logo.png", width: 20%)
]
