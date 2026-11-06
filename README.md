# plit

A single-leader distributed key-value store in Rust, using the [Raft consensus
algorithm](https://raft.github.io/) for leader election and log replication.
Three nodes with a crash-fault model. The name is
Ukrainian: *пліт* [plit] — "raft".

The testing is the point: plit is built **simulation-first**. The
protocol core does no I/O and gets driven by a deterministic, seeded event
scheduler, with a hand-written linearizability checker as an important
deliverable.

## What must plit guarantee

### Linearizability

Operations must return values in "real time":

- **If a request is fully processed before a new one begins, the new request
  must see the latest data.** Example: if these two operations run one after
  another:

      SET x 1  ->  returns OK
      GET x

  then the `GET` must return `1`.

- **Two consecutive requests on the same data, with no change in between,
  must both return the same value.**

### Fault tolerance

- **Network partitions:** nodes can stay healthy yet get disconnected from
  each other (somebody accidentally steps on a LAN cable connecting two
  servers). The cluster must still prevent false commits and data loss, and
  serve all data correctly.
- **Effective node recovery:** when a node crashes and reboots, it must
  recover to its durable state without losing any committed entries, and
  safely catch up with the cluster before it can once again become a fully
  participating replica.

## System model

plit is built on a **partially synchronous** network model with
**crash-recovery** faults. Message delays are "usually" predictable
(exceptions: process pauses, crashes, ...). When a node crashes, it is
expected to recover.

### Honest nodes, crash faults only

plit does not defend against malicious nodes — we trust the hardware because
we own it. This is a crash-fault, honest-nodes assumption, and it is exactly
why **3 nodes are sufficient**. A Byzantine fault-tolerant system would need
at least 7.

### Asynchronous networks, no delivery deadlines

plit's nodes talk over a shared, efficient communication channel: when data
needs to move, it uses all currently available bandwidth, like TCP. There is
no fixed bandwidth reservation and **no guaranteed delivery deadline** —
timeouts are hints, not contracts.

### Clocks are not trusted for ordering

- A **time-of-day clock** is synced against external time servers and can
  jump backwards — fine for user-facing timestamps, unsafe for ordering
  events in a distributed system.
- A **monotonic clock** never jumps back, but every machine has its own with
  a different origin — two machines cannot compare their monotonic clocks
  directly.

So plit orders events by the replicated log, not by timestamps.

## Simulation-first testing

Everything a real cluster does to you — message delivery, reordering, drops,
crashes, reboots, timer firings — happens as a scheduled event on a single
global agenda, dispatched by one deterministic loop.

- **The protocol core does no I/O.** No sockets, no threads, no wall-clock
  reads. A node is a state machine advanced one step at a time, and the
  harness decides what it sees next.
- **One seeded PRNG feeds all randomness.** A run is fully determined by its
  seed and config: same seed in, same run out, exactly.
- **Failures reproduce by seed.** When a sweep finds a bug, the report
  carries the seed, the config, the simulated time, and the step count — and
  re-running that seed replays the exact same path down to the failing
  transition (e.g. `seed 43, time: 500 ms, node 2: 5 vs 0`). The run also keeps a full
  event trace for when you want the whole story, not just the ending.
- **Runs are bounded twice.** Simulated time alone doesn't bound the work —
  messages branch into messages — so every run is capped by a step budget
  as well as a time horizon.
- **Checkers run inside the loop, not at the end.** Committed values must
  stay monotonic, and once the network goes quiet, all nodes must agree on
  the committed state (agreement at quiescence). A violation names the node
  and the exact transition that broke the rule.
- **Fault sweeps turn correctness into a number.** Thousands of seeds per
  configuration: a correct configuration stays clean across the sweep, and
  injected faults (message loss and friends) surface as reproducible seeds
  instead of flaky failures.

## Status

- [x] Deterministic simulation harness: seeded event agenda, tick/dispatch
  loop, step budgets, full trace capture, reproducible from seed alone
- [x] Safety checkers (monotonicity, agreement at quiescence) wired into the
  run loop; planted bugs caught in sweep runs with an exact reproduction
- [ ] Raft leader election in the harness
- [ ] Log replication + persistence
- [ ] ReadIndex + snapshots

## References

- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
  — Ongaro & Ousterhout (the Raft paper)
