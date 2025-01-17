+++
title = "Testing strategy"
weight = 30
+++


## Testing Garage

Currently, we have the following tests:

- some unit tests spread around the codebase
- integration tests written in Rust (`src/garage/test`) to check that Garage operations perform correctly
- integration test for compatibility with external tools (`script/test-smoke.sh`)

We have also tried `minio/mint` but it fails a lot and for now we haven't gotten a lot from it.

In the future:

1. We'd like to have a systematic way of testing with `minio/mint`,
   it would add value to Garage by providing a compatibility score and reference that can be trusted.
2. We'd also like to do testing with Jepsen in some way.

## How to instrument Garagae

We should try to test in least invasive ways, i.e. minimize the impact of the testing framework on Garage's source code. This means for example:

- Not abstracting IO/nondeterminism in the source code
- Not making `garage` a shared library (launch using `execve`, it's perfectly fine)

Instead, we should focus on building a clean outer interface for the `garage` binary,
for example loading configuration using environnement variables instead of the configuration file if that's helpfull for writing the tests.

There are two reasons for this:

- Keep the soure code clean and focused
- Test something that is as close as possible as the true garage that will actually be running

Reminder: rules of simplicity, concerning changes to Garage's source code.
Always question what we are doing.
Never do anything just because it looks nice or because we "think" it might be useful at some later point but without knowing precisely why/when.
Only do things that make perfect sense in the context of what we currently know.

## References

Testing is a research field on its own.
About testing distributed systems:

 - [Jepsen](https://jepsen.io/) is a testing framework designed to test distributed systems. It can mock some part of the system like the time and the network.
 - [FoundationDB Testing Approach](https://www.micahlerner.com/2021/06/12/foundationdb-a-distributed-unbundled-transactional-key-value-store.html#what-is-unique-about-foundationdbs-testing-framework). They chose to abstract "all sources of nondeterminism and communication are abstracted, including network, disk, time, and pseudo random number generator" to be able to run tests by simulating faults.
 - [Testing Distributed Systems](https://asatarin.github.io/testing-distributed-systems/) - Curated list of resources on testing distributed systems
 
About S3 compatibility:
  - [ceph/s3-tests](https://github.com/ceph/s3-tests)
  - (deprecated) [minio/s3verify](https://blog.min.io/s3verify-a-simple-tool-to-verify-aws-s3-api-compatibility/)
  - [minio/mint](https://github.com/minio/mint)

About benchmarking S3 (I think it is not necessarily very relevant for this iteration):
  - [minio/warp](https://github.com/minio/warp)
  - [wasabi-tech/s3-benchmark](https://github.com/wasabi-tech/s3-benchmark)
  - [dvassallo/s3-benchmark](https://github.com/dvassallo/s3-benchmark)
  - [intel-cloud/cosbench](https://github.com/intel-cloud/cosbench) - used by Ceph
  
Engineering blog posts:
 - [Quincy @ Scale: A Tale of Three Large-Scale Clusters](https://ceph.io/en/news/blog/2022/three-large-scale-clusters/)

Interesting blog posts on the blog of the Sled database: 

- <https://sled.rs/simulation.html>
- <https://sled.rs/perf.html>
  
Misc:
  - [mutagen](https://github.com/llogiq/mutagen) - mutation testing is a way to assert our test quality by mutating the code and see if the mutation makes the tests fail
  - [fuzzing](https://rust-fuzz.github.io/book/) - cargo supports fuzzing, it could be a way to test our software reliability in presence of garbage data.
  

