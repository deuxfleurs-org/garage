# jepsen.garage

Jepsen checking of Garage consistency properties.

## Usage

Requirements:

- vagrant
- VirtualBox, configured so that nodes can take an IP in a private network `192.168.56.0/24` (it's the default)
- a user that can create VirtualBox VMs
- leiningen
- gnuplot

Set up VMs before running tests:

```
vagrant up
```

Run tests: see commands below.


## Results

### Register linear, without timestamp patch

Command: `lein run test --nodes-file nodes.vagrant --time-limit 60 --rate 100  --concurrency 20 --workload reg1 --ops-per-key 100`

Results without timestamp patch:

- Fails with a simple clock-scramble nemesis (`--scenario c`).
  Explanation: without the timestamp patch, nodes will create objects using their
  local clock only as a timestamp, so the ordering will be all over the place if
  clocks are scrambled.

Results with timestamp patch (`--patch tsfix2`):

- No failure with clock-scramble nemesis

- Fails with clock-scramble nemesis + partition nemesis (`--scenario cp`).

**This test is expected to fail.**
Indeed, S3 objects are not meant to behave like linearizable registers.
TODO explain using a counter-example


### Read-after-write CRDT register model

Command: `lein run test --nodes-file nodes.vagrant --time-limit 60 --rate 100  --concurrency 100 --workload reg2 --ops-per-key 100`

Results without timestamp patch:

- Fails with a simple clock-scramble nemesis (`--scenario c`).
  Explanation: old values are not overwritten correctly when their timestamps are in the future.

Results with timestamp patch (`--patch tsfix2`):

- No failures with clock-scramble nemesis + partition nemesis (`--scenario cp`).
  This proves that `tsfix2` (PR#543) does improve consistency.

- **Fails with layout reconfiguration nemesis** (`--scenario r`).
  Example of a failed run: `garage reg2/20231024T120806.899+0200`.
  This is the failure mode we are looking for and trying to fix for NLnet task 3.

Results with NLnet task 3 code (commit 707442f5de, `--patch task3a`):

- No failures with `--scenario r` (0 of 10 runs), `--scenario pr` (0 of 10 runs),
  `--scenario cpr` (0 of 10 runs) and `--scenario dpr` (0 of 10 runs).

- Same with `--patch task3c` (commit `0041b013`, the final version).


### Set, basic test (write some items, then read)

Command: `lein run test --nodes-file nodes.vagrant --time-limit 60 --rate 200  --concurrency 200 --workload set1 --ops-per-key 100`

Results without NLnet task3 code (`--patch tsfix2`):

- For now, no failures with clock-scramble nemesis + partition nemesis -> TODO long test run

- Does not seem to fail with only the layout reconfiguation nemesis (<10 runs), although theoretically it could

- **Fails with the partition + layout reconfiguration nemesis** (`--scenario pr`).
  Example of a failed run: `garage set1/20231024T172214.488+0200` (1 failure in 4 runs).
  This is the failure mode we are looking for and trying to fix for NLnet task 3.

Results with NLnet task 3 code (commit 707442f5de, `--patch task3a`):

- The tests are buggy and often result in an "unknown" validity status, which
  is caused by some requests not returning results during network partitions or
  other nemesis-induced broken cluster states.  However, when the tests were
  able to finish, there were no failures with scenarios `r`, `pr`, `cpr`,
  `dpr`.


### Set, continuous test (interspersed reads and writes)

Command: `lein run test --nodes-file nodes.vagrant --time-limit 60 --rate 100  --concurrency 100 --workload set2 --ops-per-key 100`

Results without NLnet task3 code (`--patch tsfix2`):

- No failures with clock-scramble nemesis + db nemesis + partition nemesis (`--scenario cdp`) (0 failures in 10 runs).

- **Fails with just layout reconfiguration nemesis** (`--scenario r`).
  Example of a failed run: `garage set2/20231025T141940.198+0200` (10 failures in 10 runs).
  This is the failure mode we are looking for and trying to fix for NLnet task 3.

Results with NLnet task3 code (commit 707442f5de, `--patch task3a`):

- No failures with `--scenario r` (0 of 10 runs), `--scenario pr` (0 of 10 runs),
  `--scenario cpr` (0 of 10 runs) and `--scenario dpr` (0 of 10 runs).

- Same with `--patch task3c` (commit `0041b013`, the final version).


## NLnet task 3 final results

- With code from task3 (`--patch task3c`): [reg2 and set2](results/Results-2023-12-13-task3c.png), [set1](results/Results-2023-12-14-task3-set1.png).
- Without (`--patch tsfix2`): [reg2 and set2](results/Results-2023-12-13-tsfix2.png), set1 TBD.

## Investigating (and fixing) errors

### Segfaults

They are due to the download being interrupted in the middle (^C during first launch on clean VMs), the `garage` binary is truncated.
Add `:force?` to the `cached-wget!` call in `daemon.clj` to re-download the binary,
or restar the VMs to clear temporary files.

### In `jepsen.garage`: prefix wierdness

In `store/garage set1/20231019T163358.615+0200`:

```
INFO [2023-10-19 16:35:20,977] clojure-agent-send-off-pool-207 - jepsen.garage.set list results for prefix set20/ : (set13/0 set13/1 set13/10 set13/11 set13/12 set13/13 set13/14 set13/15 set13/16 set13/17 set13/18 set13/19 set13/2 set13/20 set13/21 set13/22 set13/23 set13/24 set13/25 set13/26 set13/27 set13/28 set13/29 set13/3 set13/30 set13/31 set13/32 set13/33 set13/34 set13/35 set13/36 set13/37 set13/38 set13/39 set13/4 set13/40 set13/41 set13/42 set13/43 set13/44 set13/45 set13/46 set13/47 set13/48 set13/49 set13/5 set13/50 set13/51 set13/52 set13/53 set13/54 set13/55 set13/56 set13/57 set13/58 set13/59 set13/6 set13/60 set13/61 set13/62 set13/63 set13/64 set13/65 set13/66 set13/67 set13/68 set13/69 set13/7 set13/70 set13/71 set13/72 set13/73 set13/74 set13/75 set13/76 set13/77 set13/78 set13/79 set13/8 set13/80 set13/81 set13/82 set13/83 set13/84 set13/85 set13/86 set13/87 set13/88 set13/89 set13/9 set13/90 set13/91 set13/92 set13/93 set13/94 set13/95 set13/96 set13/97 set13/98 set13/99)  (node: http://192.168.56.25:3900 )
```

After inspecting, the actual S3 call made was with prefix "set13/", so at least this is not an error in Garage itself but in the jepsen code.

Finally found out that this was due to closures not correctly capturing their context in the list function in s3api.clj (wtf clojure?)
Not sure exactly where it came from but it seems to have been fixed by making list-inner a separate function and not a sub-function,
and passing all values that were previously in the context (creds and prefix) as additional arguments.

### `reg2` test inconsistency, even with timestamp fix

The reg2 test is our custom checker for CRDT read-after-write on individual object keys, acting as registers which can be updated.
The test fails without the timestamp fix, which is expected as the clock scrambler will prevent nodes from having a correct ordering of objects.

With the timestamp fix (`--patch tsfix1`), the happenned-before relationship should at least be respected, meaning that when a PutObject call starts
after another PutObject call has ended, the second call should overwrite the value of the first call, and that value should not be
readable by future GetObject calls.
However, we observed inconsistencies even with the timestamp fix.

The inconsistencies seemed to always happenned after writing a nil value, which translates to a DeleteObject call
instead of a PutObject. By removing the possibility of writing nil values, therefore only doing
PutObject calls, the issue disappears. There is therefore an issue to fix in DeleteObject.

The issue in DeleteObject seems to have been fixed by commit `c82d91c6bccf307186332b6c5c6fc0b128b1b2b1`, which can be used using `--patch tsfix2`.


## License

Copyright © 2023 Alex Auvolat

This program and the accompanying materials are made available under the
terms of the GNU Affero General Public License v3.0.
