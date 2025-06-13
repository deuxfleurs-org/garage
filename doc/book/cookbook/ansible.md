+++
title = "Deploying with Ansible"
weight = 35
+++

While Ansible is not officially supported to deploy Garage, several community members
have published Ansible roles.  We list them and compare them below.

## Comparison of Ansible roles

| Feature                            | [ansible-role-garage](#zorun-ansible-role-garage) | [garage-docker-ansible-deploy](#moan0s-garage-docker-ansible-deploy) | [eddster ansible-role-garage](#eddster-ansible-role-garage) |
|------------------------------------|---------------------------------------------|---------------------------------------------------------------|---------------------------------|
| **Runtime**                        | Systemd                                     | Docker                                                        | Systemd                         |
| **Target OS**                      | Any Linux                                   | Any Linux                                                     | Any Linux                       |
| **Architecture**                   | amd64, arm64, i686                          | amd64, arm64                                                  | arm64, arm, 386, amd64                            |
| **Additional software**            | None                                        | Traefik                                                       | Ngnix and Keepalived (optional) |
| **Automatic node connection**      | ❌                                          | ✅                                                            | ✅                              |
| **Layout management**              | ❌                                          | ✅                                                            | ✅                              |
| **Manage buckets & keys**          | ❌                                          | ✅ (basic)                                                    | ✅                              |
| **Allow custom Garage config**     | ✅                                          | ❌                                                            | ❌                              |
| **Facilitate Garage upgrades**     | ✅                                          | ❌                                                            | ✅                              |
| **Multiple instances on one host** | ✅                                          | ✅                                                            | ❌                              |


## zorun/ansible-role-garage

[Source code](https://github.com/zorun/ansible-role-garage), [Ansible galaxy](https://galaxy.ansible.com/zorun/garage)

This role is voluntarily simple: it relies on the official Garage static
binaries and only requires Systemd.  As such, it should work on any
Linux-based OS.

To make things more flexible, the user has to provide a Garage
configuration template.  This allows to customize Garage configuration in
any way.

Some more features might be added, such as a way to automatically connect
nodes to each other or to define a layout.

## moan0s/garage-docker-ansible-deploy

[Source code](https://github.com/moan0s/garage-docker-ansible-deploy), [Blog post](https://hyteck.de/post/garage/)

This role is based on the Docker image for Garage, and comes with
"batteries included": it will additionally install Docker and Traefik. In
addition, it is "opinionated" in the sense that it expects a particular
deployment structure (one instance per disk, one gateway per host,
structured DNS names, etc).

As a result, this role makes it easier to start with Garage on Ansible,
but is less flexible.

## eddster2309/ansible-role-garage

[Source code](https://github.com/eddster2309/ansible-role-garage), [Ansible galaxy](https://galaxy.ansible.com/ui/standalone/roles/eddster2309/garage/)

This role is a opinionated but customisable role using the official Garage
static binaries and only requires Systemd. As such it should work on any
Linux based host. It includes all the nesscary configuration to
automatically setup a clustered Garage deployment. Most Garage
configuration options are exposed through Ansible variables so while you
can't provide a custom config you can get very close. It can optionally
installed a HA nginx deployment with Keepalived.
