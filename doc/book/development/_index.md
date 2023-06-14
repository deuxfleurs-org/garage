+++
title = "Development"
weight = 80
sort_by = "weight"
template = "documentation.html"
+++

Now that you are a Garage expert, you want to enhance it, you are in the right place!
We discuss here how to hack on Garage, how we manage its development, etc.

## Rust API (docs.rs)
If you encounter a specific bug in Garage or plan to patch it, you may jump directly to the source code's documentation!

  - [garage\_api](https://docs.rs/garage_api/latest/garage_api/) - contains the S3 standard API endpoint
  - [garage\_model](https://docs.rs/garage_model/latest/garage_model/) - contains Garage's model built on the table abstraction
  - [garage\_rpc](https://docs.rs/garage_rpc/latest/garage_rpc/) - contains Garage's federation protocol
  - [garage\_table](https://docs.rs/garage_table/latest/garage_table/) - contains core Garage's CRDT datatypes
  - [garage\_util](https://docs.rs/garage_util/latest/garage_util/) - contains garage helpers
  - [garage\_web](https://docs.rs/garage_web/latest/garage_web/) - contains the S3 website endpoint
