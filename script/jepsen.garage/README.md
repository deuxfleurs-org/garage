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

These ones are working:

```
lein run test --nodes-file nodes.vagrant --time-limit 64 --rate 50  --concurrency 50 --workload set1
lein run test --nodes-file nodes.vagrant --time-limit 64 --rate 50  --concurrency 50 --workload set2
```

## Results

**Register linear, without timestamp patch**

Command: `lein run test --nodes-file nodes.vagrant --time-limit 60 --rate 20  --concurrency 20 --workload reg --ops-per-key 100`

Results: fails with a simple clock-scramble nemesis.

Explanation: without the timestamp patch, nodes will create objects using their
local clock only as a timestamp, so the ordering will be all over the place if
clocks are scrambled.

**Register linear, with timestamp patch**

Command: `lein run test --nodes-file nodes.vagrant --time-limit 60 --rate 20  --concurrency 20 --workload reg --ops-per-key 100 -I`

Results:

- No failure with clock-scramble nemesis
- Fails with clock-scramble nemesis + partition nemesis

Explanation: S3 objects are not meant to behave like linearizable registers. TODO explain using a counter-example

**Read-after-write CRDT register model**: TODO: determine the expected semantics of such a register, code a checker and show that results are correct

**Set, basic test**

Command: `lein run test --nodes-file nodes.vagrant --time-limit 60 --rate 20  --concurrency 20 --workload set1 --ops-per-key 100`

Results:

- ListObjects returns objects not within prefix???? -> BAD, definitely a bug, but maybe it's in the instrumentation code?

In `store/garage set1/20231019T163358.615+0200`:

```
INFO [2023-10-19 16:35:20,977] clojure-agent-send-off-pool-207 - jepsen.garage.set list results for prefix set20/ : (set13/0 set13/1 set13/10 set13/11 set13/12 set13/13 set13/14 set13/15 set13/16 set13/17 set13/18 set13/19 set13/2 set13/20 set13/21 set13/22 set13/23 set13/24 set13/25 set13/26 set13/27 set13/28 set13/29 set13/3 set13/30 set13/31 set13/32 set13/33 set13/34 set13/35 set13/36 set13/37 set13/38 set13/39 set13/4 set13/40 set13/41 set13/42 set13/43 set13/44 set13/45 set13/46 set13/47 set13/48 set13/49 set13/5 set13/50 set13/51 set13/52 set13/53 set13/54 set13/55 set13/56 set13/57 set13/58 set13/59 set13/6 set13/60 set13/61 set13/62 set13/63 set13/64 set13/65 set13/66 set13/67 set13/68 set13/69 set13/7 set13/70 set13/71 set13/72 set13/73 set13/74 set13/75 set13/76 set13/77 set13/78 set13/79 set13/8 set13/80 set13/81 set13/82 set13/83 set13/84 set13/85 set13/86 set13/87 set13/88 set13/89 set13/9 set13/90 set13/91 set13/92 set13/93 set13/94 set13/95 set13/96 set13/97 set13/98 set13/99)  (node: http://192.168.56.25:3900 )

```

- Sometimes ListObjects returns an empty list???? -> BAD, quorums should ensure this doesn't happen

## License

Copyright © 2023 Alex Auvolat

This program and the accompanying materials are made available under the
terms of the GNU Affero General Public License v3.0.
