# Garage

THIS IS ALL WORK IN PROGRESS. NOTHING TO SEE YET BUT THANKS FOR YOUR INTEREST.

Garage implements an S3-compatible object store with high resiliency to network failures, machine failure, and sysadmin failure.

## To log:

```
RUST_LOG=garage=debug cargo run --release -- server -c config_file.toml
```

## What to repair

- `tables`: to do a full sync of metadata, should not be necessary because it is done every hour by the system
- `versions` and `block_refs`: very time consuming, usefull if deletions have not been propagated, improves garbage collection
- `blocks`: very usefull to resync/rebalance blocks betweeen nodes
