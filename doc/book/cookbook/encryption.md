+++
title = "Encryption"
weight = 50
+++

Encryption is a recurring subject when discussing Garage. 
Garage does not handle data encryption by itself, but many things can
already be done with Garage's current feature set and the existing ecosystem.

This page takes a high level approach to security in general and data encryption
in particular.


# Examining your need for encryption

- Why do you want encryption in Garage?

- What is your threat model? What are you fearing?
  - A stolen HDD?
  - A curious administrator?
  - A malicious administrator?
  - A remote attacker?
  - etc.

- What services do you want to protect with encryption?
  - An existing application? Which one? (eg. Nextcloud)
  - An application that you are writing

- Any expertise you may have on the subject

This page explains what Garage provides, and how you can improve the situation by yourself
by adding encryption at different levels.

We would be very curious to know your needs and thougs about ideas such as
encryption practices and things like key management, as we want Garage to be a
serious base platform for the developpment of secure, encrypted applications.
Do not hesitate to come talk to us if you have any thoughts or questions on the
subject.


# Capabilities provided by Garage

## Traffic is encrypted between Garage nodes

RPCs between Garage nodes are encrypted. More specifically, contrary to many
distributed software, it is impossible in Garage to have clear-text RPC.  We
use the [kuska handshake](https://github.com/Kuska-ssb/handshake) library which
implements a protocol that has been clearly reviewed, Secure ScuttleButt's
Secret Handshake protocol.  This is why setting a `rpc_secret` is mandatory,
and that's also why your nodes have super long identifiers.

## HTTP API endpoints provided by Garage are in clear text

Adding TLS support built into Garage is not currently planned.

## Garage stores data in plain text on the filesystem or encrypted using customer keys (SSE-C)

For standard S3 API requests, Garage does not encrypt data at rest by itself.
For the most generic at rest encryption of data, we recommend setting up your
storage partitions on encrypted LUKS devices.

If you are developping your own client software that makes use of S3 storage,
we recommend implementing data encryption directly on the client side and never
transmitting plaintext data to Garage. This makes it easy to use an external
untrusted storage provider if necessary.

Garage does support [SSE-C
encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html),
an encryption mode of Amazon S3 where data is encrypted at rest using
encryption keys given by the client.  The encryption keys are passed to the
server in a header in each request, to encrypt or decrypt data at the moment of
reading or writing. The server discards the key as soon as it has finished
using it for the request.  This mode allows the data to be encrypted at rest by
Garage itself, but it requires support in the client software. It is also not
adapted to a model where the server is not trusted or assumed to be
compromised, as the server can easily know the encryption keys.  Note however
that when using SSE-C encryption, the only Garage node that knows the
encryption key passed in a given request is the node to which the request is
directed (which can be a gateway node), so it is easy to have untrusted nodes
in the cluster as long as S3 API requests containing SSE-C encryption keys are
not directed to them.

Implementing automatic data encryption directly in Garage without client-side
management of keys (something like
[SSE-S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingServerSideEncryption.html))
could make things simpler for end users that don't want to setup LUKS, but also
raises many more questions, especially around key management: for encryption of
data, where could Garage get the encryption keys from? If we encrypt data but
keep the keys in a plaintext file next to them, it's useless. We probably don't
want to have to manage secrets in Garage as it would be very hard to do in a
secure way. At the time of speaking, there are no plans to implement this in
Garage.


# Adding data encryption using external tools

## Encrypting traffic between a Garage node and your client

You have multiple options to have encryption between your client and a node:

  - Setup a reverse proxy with TLS / ACME / Let's encrypt
  - Setup a Garage gateway locally, and only contact the garage daemon on `localhost`
  - Only contact your Garage daemon over a secure, encrypted overlay network such as Wireguard

## Encrypting data at rest

Protects against the following threats:

- Stolen HDD

Crucially, does not protect againt malicious sysadmins or remote attackers that
might gain access to your servers.

Methods include full-disk encryption with tools such as LUKS.

## Encrypting data on the client side

Protects againt the following threats:

- A honest-but-curious administrator
- A malicious administrator that tries to corrupt your data
- A remote attacker that can read your server's data

Implementations are very specific to the various applications. Examples:

- Matrix: uses the OLM protocol for E2EE of user messages. Media files stored
  in Matrix are probably encrypted using symmetric encryption, with a key that is
  distributed in the end-to-end encrypted message that contains the link to the object.

- XMPP: clients normally support either OMEMO / OpenPGP for the E2EE of user
  messages. Media files are encrypted per
  [XEP-0454](https://xmpp.org/extensions/xep-0454.html).

- Aerogramme: use the user's password as a key to decrypt data in the user's bucket

- Cyberduck: comes with support for
  [Cryptomator](https://docs.cyberduck.io/cryptomator/) which allows users to
  create client-side vaults to encrypt files in before they are uploaded to a
  cloud storage endpoint.
