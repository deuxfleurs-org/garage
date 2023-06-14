+++
title = "Build your own app"
weight = 40
sort_by = "weight"
template = "documentation.html"
+++

Garage has many API that you can rely on to build complex applications.
In this section, we reference the existing SDKs and give some code examples.


## ⚠️ DISCLAIMER

**K2V AND ADMIN SDK ARE TECHNICAL PREVIEWS**. The following limitations apply:
  - The API is not complete, some actions are possible only through the `garage` binary
  - The underlying admin API is not yet stable nor complete, it can breaks at any time
  - The generator configuration is currently tweaked, the library might break at any time due to a generator change
  - Because the API and the library are not stable, none of them are published in a package manager (npm, pypi, etc.)
  - This code has not been extensively tested, some things might not work (please report!)

To have the best experience possible, please consider:
  - Make sure that the version of the library you are using is pinned (`go.sum`, `package-lock.json`, `requirements.txt`).
  - Before upgrading your Garage cluster, make sure that you can find a version of this SDK that works with your targeted version and that you are able to update your own code to work with this new version of the library.
  - Join our Matrix channel at `#garage:deuxfleurs.fr`, say that you are interested by this SDK, and report any friction.
  - If stability is critical, mirror this repository on your own infrastructure, regenerate the SDKs and upgrade them at your own pace.


## About the APIs

Code can interact with Garage through 3 different APIs: S3, K2V, and Admin.
Each of them has a specific scope.

### S3

De-facto standard, introduced by Amazon, designed to store blobs of data.

### K2V

A simple database API similar to RiakKV or DynamoDB.
Think a key value store with some additional operations.
Its design is inspired by Distributed Hash Tables (DHT).

More information:
  - [In the reference manual](@/documentation/reference-manual/k2v.md)


### Administration

Garage operations can also be automated through a REST API.
We are currently building this SDK for [Python](@/documentation/build/python.md#admin-api), [Javascript](@/documentation/build/javascript.md#administration) and [Golang](@/documentation/build/golang.md#administration).

More information:
  - [In the reference manual](@/documentation/reference-manual/admin-api.md)
  - [Full specifiction](https://garagehq.deuxfleurs.fr/api/garage-admin-v0.html)
