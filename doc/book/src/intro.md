<p align="center" style="text-align:center;">
	<a href="https://garagehq.deuxfleurs.fr">
	<img alt="Garage's Logo" src="img/logo.svg" height="200" />
	</a>
</p>

<p align="center" style="text-align:center;">
	[ <a href="https://garagehq.deuxfleurs.fr/_releases.html">Download</a>
	| <a href="https://git.deuxfleurs.fr/Deuxfleurs/garage">Git repository</a>
	| <a href="https://matrix.to/#/%23garage:deuxfleurs.fr">Matrix channel</a>
	| <a href="https://drone.deuxfleurs.fr/Deuxfleurs/garage">Drone CI</a>
	]
</p>


# Data resiliency for everyone

Garage is an **open-source** distributed **storage service** you can **self-host** to fullfill many needs:

<p align="center" style="text-align:center; margin-bottom: 5rem;">
<img alt="Summary of the possible usages with a related icon: host a website, store media and backup target" src="img/usage.svg" />
</p>

<p align="center" style="text-align:center; margin-bottom: 5rem;">
<a href="/design/goals.html#use-cases">⮞ learn more about use cases ⮜</a>
</p>

Garage implements the **[Amazon S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)** and thus is already **compatible** with many applications:

<p align="center" style="text-align:center; margin-bottom: 8rem;">
<img alt="Garage is already compatible with Nextcloud, Mastodon, Matrix Synapse, Cyberduck, RClone and Peertube" src="img/software.svg" />
</p>

<p align="center" style="text-align:center; margin-bottom: 5rem;">
<a href="/connect/index.html">⮞ learn more about integrations ⮜</a>
</p>


Garage provides **data resiliency** by **replicating** data 3x over **distant** servers:

<p align="center" style="text-align:center; margin-bottom: 5rem;">
<img alt="An example deployment on a map with servers in 5 zones: UK, France, Belgium, Germany and Switzerland. Each chunk of data is replicated in 3 of these 5 zones." src="img/map.svg" />
</p>

<p align="center" style="text-align:center; margin-bottom: 5rem;">
<a href="/design/index.html">⮞ learn more about our design ⮜</a>
</p>

Did you notice that *this website* is hosted and served by Garage?

## Keeping requirements low

We worked hard to keep requirements as low as possible as we target the largest possible public.

  * **CPU:** any x86\_64 CPU from the last 10 years, ARMv7 or ARMv8.
  * **RAM:** 1GB
  * **Disk Space:** at least 16GB
  * **Network:** 200ms or less, 50 Mbps or more
  * **Heterogeneous hardware:** build a cluster with whatever second-hand machines are available

*For the network, as we do not use consensus algorithms like Paxos or Raft, Garage is not as latency sensitive.*
*Thanks to Rust and its zero-cost abstractions, we keep CPU and memory low.*

## Built on the shoulder of giants

  - [Dynamo: Amazon’s Highly Available Key-value Store ](https://dl.acm.org/doi/abs/10.1145/1323293.1294281) by DeCandia et al.
  - [Conflict-Free Replicated Data Types](https://link.springer.com/chapter/10.1007/978-3-642-24550-3_29) by Shapiro et al.
  - [Maglev: A Fast and Reliable Software Network Load Balancer](https://www.usenix.org/conference/nsdi16/technical-sessions/presentation/eisenbud) by Eisenbud et al.

## Talks

  - [(fr, 2021-11-13, video) Garage : Mille et une façons de stocker vos données](https://video.tedomum.net/w/moYKcv198dyMrT8hCS5jz9) and [slides (html)](https://rfid.deuxfleurs.fr/presentations/2021-11-13/garage/) - during [RFID#1](https://rfid.deuxfleurs.fr/programme/2021-11-13/) event

  - [(en, 2021-04-28, pdf) Distributed object storage is centralised](https://git.deuxfleurs.fr/Deuxfleurs/garage/raw/commit/b1f60579a13d3c5eba7f74b1775c84639ea9b51a/doc/talks/2021-04-28_spirals-team/talk.pdf)

  - [(fr, 2020-12-02, pdf) Garage : jouer dans la cour des grands quand on est un hébergeur associatif](https://git.deuxfleurs.fr/Deuxfleurs/garage/raw/commit/b1f60579a13d3c5eba7f74b1775c84639ea9b51a/doc/talks/2020-12-02_wide-team/talk.pdf)

## Community

If you want to discuss with us, you can join our Matrix channel at [#garage:deuxfleurs.fr](https://matrix.to/#/#garage:deuxfleurs.fr).
Our code repository and issue tracker, which is the place where you should report bugs, is managed on [Deuxfleurs' Gitea](https://git.deuxfleurs.fr/Deuxfleurs/garage).

## License

Garage's source code, is released under the [AGPL v3 License](https://www.gnu.org/licenses/agpl-3.0.en.html).
Please note that if you patch Garage and then use it to provide any service over a network, you must share your code!

# Sponsors and funding

The Deuxfleurs association has received a grant from [NGI POINTER](https://pointer.ngi.eu/), to fund 3 people working on Garage full-time for a year: from October 2021 to September 2022.

<div style="display: flex; justify-content: space-around">
  <a href="https://pointer.ngi.eu/">
    <img style="height:100px" src="img/ngi-logo.png" alt="NGI Pointer logo">
  </a>
  <a href="https://ec.europa.eu/programmes/horizon2020/what-horizon-2020">
    <img style="height:100px" src="img/eu-flag-logo.png" alt="EU flag logo">
  </a>
</div>

_This project has received funding from the European Union’s Horizon 2020 research and innovation programme within the framework of the NGI-POINTER Project funded under grant agreement N° 871528._
