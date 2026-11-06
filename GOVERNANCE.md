# Governance of Garage

This documents how the Garage project operates. It reflects the state of the project as of July 2026 and is not optimal. The team is interested to improve it in the future.

## Team organization

* **Contributors**: anyone can contribute by proposing changes in issues and pull requests.

* **Maintainers**: they are responsible for reviewing, merging pull requests, publishing releases and triaging issues.
  The current maintainers are:
   * Alex (handle `lx`)
   * Trinity (handle `trinity-1686a`)
   * Maximilien (handle `halfa`), who is in particular responsible for coordinating effort on the Kubernetes integration / Helm chart.
 
  They are added to a white-list of the branch protection rule of the repository to enable them to merge pull requests.
  To become a maintainer, you need to be a long-term contributor and earn the personal trust of Alex.
  There is no set process for leaving the maintainer role.

* **Lead developer**: Alex (handle `lx`) is the lead developer and is responsible of ensuring the
correctness of Garage and stability between version upgrades. He may transfer this role to someone else as he sees fit.

## Communication channels

The team coordinates in the following channels:
* The [issue tracker](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues) and [pull requests](https://git.deuxfleurs.fr/Deuxfleurs/garage/pulls) of the official repository.
* The [`#Garage :: Community` channel on Deuxfleur's Zulip chat](https://zulip.deuxfleurs.fr/#narrow/channel/42-Garage-.3A.3A-Community)
* The [`#garage:deuxfleurs.fr` matrix channel](https://matrix.to/#/#garage:deuxfleurs.fr) (in English), open to anyone.
  On this channel, users may ask for support and discussions about development also happen.

The moderators for those discussion channels are the Garage maintainers.

## Decision procedures

Decisions are taken by lazy consensus, with the lead developer settling discussions when a consensus cannot be reached.

## Governance changes

There is no set process for changing the governance of garage.

## See also

* [Project goals](https://garagehq.deuxfleurs.fr/documentation/design/goals/)
* [Contributing instructions](https://git.deuxfleurs.fr/Deuxfleurs/garage/src/branch/main-v2/CONTRIBUTING.md)
