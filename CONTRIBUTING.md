# Contributing to Garage

## Policy on AI

To ensure the quality of the codebase and documentation, the use of AI,
including LLMs and coding agents, is strictly restricted in the following way:

- AI **must not** be used to write documentation

- **Do not** use AI to write bug reports, commit descriptions and pull request
  messages

- **Do not** use AI agents to make contributions to Garage, all contributions
  must be led by a human that know what they are doing at all times

- AI **may** be used for some tedious code generation tasks, limited to very
  mechanical translations from API docs or boilerplate writing. The code
  generated must be so simple as to make it clear that it cannot be covered by
  copyright.

You are free to make use of AI privately to explore the codebase and solve
conceptual problems, but please restrain from copying the output from an LLM
anywhere in your code or on the issue tracker, or from letting an agent edit
the codebase directly.


## Asking questions

Read the documentation before asking questions.
Do not use the issue tracker to ask questions about Garage.
Questions asked on the issue tracker will be closed.

Ask questions on the Matrix channel `#garage:deuxfleurs.fr` so that any
community member can see your question and help you out.

If you need in-depth support from the Garage developers specifically, write to
`garagehq@deuxfleurs.fr`. Even if you do so, we do not commit to giving you an
answer.


## Reporting bugs

When writing a bug report, use this checklist:

- For bugs that can be reproduced:
    - confirm that you are using the latest version of Garage and that the bug still exists in this version
    - set the log level to debug using the `RUST_LOG=garage=debug` environment variable and reproduce the bug to get more verbose logs

- Check whether there is already an open issue in the bug tracker. If so, your bug report is still valuable but please add it as a comment to the existing issue instead of opening a new one.

- Collect as much information as possible:
  - logs of the Garage daemon at the time the issue happened, including logs that show what was happening before the issue occurred
  - the output of `garage status`
  - the output of `garage stats -a`
  - the output of `garage layout history`

- Write a detailed bug report, including:
  - a description of your cluster (number of nodes, hardware, operating system, networking, etc)
  - a detailed description of what you did that led to the issue, including any code or command line that invoked a Garage API
  - what you were expecting
  - what actually happened, and how that's different from what you expected
  - the information collected previously
  - if possible, simple steps to help the developers reproduce the issue locally

Bug reports that are imprecise or otherwise unactionable will be closed.


## Suggesting new features

Garage can be improved in many ways, but just suggesting a new feature does not mean we will implement it.
Feature requests that may lead to an actual implementation are feature requests that:

- are precise and actionable, i.e. include a precise description of the expected behavior and any necessary architectural details required for the implementation
- are motivated by actual need from a variety of users

Moreover, a certain number of features are defined as out-of-scope for Garage, including but not limited to:

- extensions to the S3 API that are not present on AWS
- features that require the implementation of a consensus algorithm
- more generally, features that are incompatible with the architecture of Garage and its goal of staying simple

Only feature requests in one of the following category may stay open in the issue tracker:

- features that the Garage team wants to work on
- features that are being actively worked on by an external contributor which is clearly identified
- features that are easy to implement and could be an easy task for a new contributor that wants to get to know the codebase

All other feature requests will be closed after a few months of inactivity, so as to keep the number of open issues to a manageable level.
Feature requests that are clearly out of scope will be closed directly.


## Improving the documentation

An easy way to contribute to Garage which also adds a lot of value is to
improve the documentation.  Make sure to write in clear technical English, and
write unambiguously.  Documentation contributions are very appreciated if they
are well-written.


## For developers

We welcome code contributions to Garage that adhere to our standards for quality:

- Changes should be reviewed from a functional perspective to ensure that they work well with the existing codebase and do not introduce bugs or subtle issues.

- You must have tested your contribution to make sure that it does what it says. The amount of testing required is proportional to the complexity of the change introduced.

- Any new feature must be properly documented following existing practices (see below).

- Unit tests should be included when relevant.

- Contributions should pass basic lints for syntactic quality (`cargo fmt`, `cargo clippy`, `typos`).

- Contributions should pass our CI test suite.

- No user-facing breaking changes may be introduced between major releases.

- No internal data model change may be introduced between major releases, to
  ensure that Garage daemons with different minor/patch versions numbers can
  work together in a cluster. For major releases, a proper migration path
  should be implemented and tested thoroughly.

Please follow up on your work when changes are requested, to avoid stale PRs.
Do not take it personally if a Garage developer pushes directly to your branch
to modify your contribution, as this might be necessary to get it merged
faster.

### Properly documenting your contribution

#### Configuration options

New configuration options should be documented in
`doc/book/reference-manual/configuration.md`.  The documentation for a
configuration option should be exhaustive. For instance, for choice options all
choices should be listed explicitly with a precise description of their
meaning.

In terms of syntax, all configuration options should appear in three places:

- in the example at the top, with an example value
- in the index of all configuration options which is sorted by alphabetical order
- in its dedicated subsection with full reference text

#### CLI commands and command flags

CLI commands are self-documented using the doc commends in the codebase.
Make sure to write clear and precise comments for all options you are adding.

#### S3 features

If you implement new S3 features, make sure to update the compatibility matrix in `doc/book/reference-manual/s3-compatibility.md`.

#### Admin API

The admin API has an OpenAPI specification that is automatically generated
using Utoipa, from a description of each endpoint that is given in
`src/api/admin/openapi.rs` and a description of data structure schemas in
`src/api/admin/api.rs`. The code in `openapi.rs` is only used to generate the
OpenAPI specification document and not for the actual implementation in Garage,
whereas structures defined in `api.rs` are also used for the implementation of
API calls. Make sure to write good doc comments for all of these items so that
the OpenAPI specification will be precise and accurate.

An up-to-date version of the OpenAPI specification document should be kept in
the repository in `doc/api/garage-admin-v2.json`. When you are making changes
to the admin API, update this document with the following command:

```
cargo run -- admin-api-schema > doc/api/garage-admin-v2.json
```


## Garage team organization

Alex (handle `lx`) is the lead developer and is responsible of ensuring the
correctness of Garage and stability between version upgrades.

The other maintainers are Trinity (handle `trinity-1686a`) and Maximilien (handle `halfa`).

Maximilien is responsible for coordinating effort on the Kubernetes integration / Helm chart.

## Pull request merging criteria

The following PRs should only be merged after review and approval from Alex:

- PRs that introduce architectural changes, such as changes in the data model
  or change in the coordination protocols between nodes

- PRs that introduce changes on the format of data structures used for
  persistent disk storage and internal cluster communication (RPC)

- PRs that are suspected of introducing some kind of breakage or unexpected
  behavior due to their complexity

PRs that introduce breaking change for users but don't fall in one of the
previous category should be discussed between maintainers to evaluate the
impact on users when upgrading.  Alex's approval is not required to merge them
as long as they are clearly identified as breaking in the PR title, and are
properly merged in the branch for the next major version and not in the current
main branch.

All other PRs can be merged by any maintainer on their own, once they are
confident that the quality standards defined in this document are respected
before merging.

## Merging strategy

When merging PRs, maintainers should ensure that a Git commit is created by
Forgejo that records the PR number, its title and its text in the commit
message.  If a PR is fixing an issue, make sure that the issue number is
included in the PR title as well.  This is to ensure that when releasing a new
version of Garage, the changelog in the release notes can be properly
constructed by reading the Git log since the last release.

We also want to keep the history "almost linear" to facilitate the use of `git
bisect` if it ever were necessary. This leaves the following two merging
strategies:

- For PRs that consist of many commits that should stay independent, the
  "rebase and create merge commit" strategy should be used. The merge commit is
  created automatically by Forgejo and saves the PR's number, title and text in
  the commit message.

- For PRs that consist of only one commit, or a few number of commits that can
  be merged, the "create squash commit" strategy should be used. This way a
  single commit will be created by Forgejo which also saves the PR's number,
  title and text in the commit message.

When cherry-picking commits from one branch to the other, a simple fast-forward
merging strategy can be used if the commit message already references a PR
number.
