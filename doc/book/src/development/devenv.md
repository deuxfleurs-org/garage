# Setup your development environment

We propose the following quickstart to setup a full dev. environment as quickly as possible:

  1. Setup a rust/cargo environment. eg. `dnf install rust cargo`
  2. Install awscli v2 by following the guide [here](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html).
  3. Run `cargo build` to build the project
  4. Run `./script/dev-cluster.sh` to launch a test cluster (feel free to read the script)
  5. Run `./script/dev-configure.sh` to configure your test cluster with default values (same datacenter, 100 tokens)
  6. Run `./script/dev-bucket.sh` to create a bucket named `eprouvette` and an API key that will be stored in `/tmp/garage.s3`
  7. Run `source ./script/dev-env-aws.sh` to configure your CLI environment
  8. You can use `garage` to manage the cluster. Try `garage --help`.
  9. You can use the `awsgrg` alias to add, remove, and delete files. Try `awsgrg help`, `awsgrg cp /proc/cpuinfo s3://eprouvette/cpuinfo.txt`, or `awsgrg ls s3://eprouvette`. `awsgrg` is a wrapper on the `aws s3` command pre-configured with the previously generated API key (the one in `/tmp/garage.s3`) and localhost as the endpoint.

Now you should be ready to start hacking on garage!


