+++
title = "Multi-HDD support"
weight = 15
+++


Since v0.9, Garage natively supports nodes that have several storage drives
for storing data blocks (not for metadata storage).

## Initial setup

To set up a new Garage storage node with multiple HDDs,
format and mount all your drives in different directories,
and use a Garage configuration as follows:

```toml
data_dir = [
    { path = "/path/to/hdd1", capacity = "2T" },
    { path = "/path/to/hdd2", capacity = "4T" },
]
```

Garage will automatically balance all blocks stored by the node
among the different specified directories, proportionally to the
specified capacities.

## Updating the list of storage locations

If you add new storage locations to your `data_dir`,
Garage will not rebalance existing data between storage locations.
Newly written blocks will be balanced proportionally to the specified capacities,
and existing data may be moved between drives to improve balancing,
but only opportunistically when a data block is re-written (e.g. an object
is re-uploaded, or an object with a duplicate block is uploaded).

To understand precisely what is happening, we need to dive in to how Garage
splits data among the different storage locations.

First of all, Garage divides the set of all possible block hashes
in a fixed number of slices (currently 1024), and assigns
to each slice a primary storage location among the specified data directories.
The number of slices having their primary location in each data directory
is proportionnal to the capacity specified in the config file.

When Garage receives a block to write, it will always write it in the primary
directory of the slice that contains its hash.

Now, to be able to not lose existing data blocks when storage locations
are added, Garage also keeps a list of secondary data directories
for all of the hash slices. Secondary data directories for a slice indicates
storage locations that once were primary directories for that slice, i.e. where
Garage knows that data blocks of that slice might be stored.
When Garage is requested to read a certain data block,
it will first look in the primary storage directory of its slice,
and if it doesn't find it there it goes through all of the secondary storage
locations until it finds it. This allows Garage to continue operating
normally when storage locations are added, without having to shuffle
files between drives to place them in the correct location.

This relatively simple strategy works well but does not ensure that data
is correctly balanced among drives according to their capacity.
To rebalance data, two strategies can be used:

- Lazy rebalancing: when a block is re-written (e.g. the object is re-uploaded),
  Garage checks whether the existing copy is in the primary directory of the slice
  or in a secondary directory. If the current copy is in a secondary directory,
  Garage re-writes a copy in the primary directory and deletes the one from the
  secondary directory. This might never end up rebalancing everything if there
  are data blocks that are only read and never written.

- Active rebalancing: an operator of a Garage node can explicitly launch a repair
  procedure that rebalances the data directories, moving all blocks to their
  primary location. Once done, all secondary locations for all hash slices are
  removed so that they won't be checked anymore when looking for a data block.

## Read-only storage locations

If you would like to move all data blocks from an existing data directory to one
or several new data directories, mark the old directory as read-only:

```toml
data_dir = [
    { path = "/path/to/old_data", read_only = true },
    { path = "/path/to/new_hdd1", capacity = "2T" },
    { path = "/path/to/new_hdd2", capacity = "4T" },
]
```

Garage will be able to read requested blocks from the read-only directory.
Garage will also move data out of the read-only directory either progressively
(lazy rebalancing) or if requested explicitly (active rebalancing).

Once an active rebalancing has finished, your read-only directory should be empty:
it might still contain subdirectories, but no data files. You can check that
it contains no files using:

```bash
find -type f /path/to/old_data      # should not print anything
```

at which point it can be removed from the `data_dir` list in your config file.
