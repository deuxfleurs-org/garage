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

## Garage stores data in plain text on the filesystem

Garage does not handle data encryption at rest by itself, and instead delegates
to the user to add encryption, either at the storage layer (LUKS, etc) or on
the client side (or both). There are no current plans to add data encryption
directly in Garage.

Implementing data encryption directly in Garage might make things simpler for
end users, but also raises many more questions, especially around key
management: for encryption of data, where could Garage get the encryption keys
from ? If we encrypt data but keep the keys in a plaintext file next to them,
it's useless. We probably don't want to have to manage secrets in garage as it
would be very hard to do in a secure way. Maybe integrate with an external
system such as Hashicorp Vault?


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

- Aerogramme: use the user's password as a key to decrypt data in the user's bucket

