# jepsen.garage

Jepsen checking of Garage consistency properties.

## Usage

Requirements:

- vagrant
- VirtualBox, configured so that nodes can take an IP in a private network `192.168.56.0/24`
- a user that can create VirtualBox VMs
- leiningen
- gnuplot

Set up VMs:

```
vagrant up
```

Run tests (this one should fail):

```
lein run test --nodes-file nodes.vagrant --time-limit 64 --concurrency 50 --rate 50 --workload reg
```

## License

Copyright © 2023 Alex Auvolat

This program and the accompanying materials are made available under the
terms of the GNU Affero General Public License v3.0.
