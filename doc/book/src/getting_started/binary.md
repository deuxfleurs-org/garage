# Get a binary

Currently, only two installations procedures are supported for Garage: from Docker (x86\_64 for Linux) and from source.
In the future, we plan to add a third one, by publishing a compiled binary (x86\_64 for Linux).
We did not test other architecture/operating system but, as long as your architecture/operating system is supported by Rust, you should be able to run Garage (feel free to report your tests!).

## From Docker

Our docker image is currently named `lxpz/garage_amd64` and is stored on the [Docker Hub](https://hub.docker.com/r/lxpz/garage_amd64/tags?page=1&ordering=last_updated).
We encourage you to use a fixed tag (eg. `v0.1.1d`) and not the `latest` tag.
For this example, we will use the latest published version at the time of the writing which is `v0.1.1d` but it's up to you
to check [the most recent versions on the Docker Hub](https://hub.docker.com/r/lxpz/garage_amd64/tags?page=1&ordering=last_updated).

For example:

```
sudo docker pull lxpz/garage_amd64:v0.1.1d
```

## From source


