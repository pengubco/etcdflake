# Design

i3d runs the [snowflake](https://en.wikipedia.org/wiki/Snowflake_ID) algorithm on many
stateless nodes and coordinates them through [etcd](https://etcd.io/). This document
explains the ID layout, how nodes share worker IDs, and why IDs stay unique when clocks,
networks or processes misbehave.

## Guarantees

1. **Unique.** No two IDs are equal, across all nodes and restarts, as long as etcd keeps
   its data.
2. **Strictly increasing per node.** IDs returned by one node, including within a
   `NextIDs` batch, increase.
3. **Roughly time-ordered across nodes.** IDs from different nodes are ordered by their
   timestamps, so ordering between nodes is only as good as clock agreement. Don't
   depend on it for correctness.
4. **Consistency over availability.** A node returns `UNAVAILABLE` rather than risk a
   duplicate.

## ID layout

```
 63  62                                   22 21          12 11            0
+---+---------------------------------------+--------------+---------------+
| 0 |  milliseconds since 2025-01-01 (41)   | worker (10)  | sequence (12) |
+---+---------------------------------------+--------------+---------------+
```

- The custom epoch (2025-01-01T00:00:00Z) makes 41 bits last until 2094. Counting from
  the Unix epoch would have overflowed in 2039.
- 10 bits give 1024 worker IDs. 12 bits give 4096 IDs per millisecond per node.
- `snowflake.Decompose` splits an ID back into timestamp, worker and sequence.

## etcd keys

All keys live under the configured prefix (e.g. `/i3d/`). One prefix is one cluster.

| Key                              | Value       | Lifetime                       |
|----------------------------------|-------------|--------------------------------|
| `workers/live/<id>`              | claim time  | attached to the owner's lease  |
| `workers/reserved_until/<id>`    | Unix ms     | permanent                      |

- **Liveness key.** Exists while a node owns the worker ID. It is tied to a lease (TTL
  10s) that the node keeps alive. If the node dies, the lease expires and the key
  disappears.
- **Reservation key.** The owner never issues an ID whose timestamp is above this value.
  It outlives the owner, so the next owner knows where to start.

## Time reservations

The central rule: **a node may only issue timestamps it has already reserved in etcd,
and a new owner only issues timestamps strictly above the stored reservation.**

1. **Extend before use.** Every second the owner writes
   `reserved_until = max(now, last issued) + 5s` in a transaction that succeeds only if the
   liveness key still carries the owner's lease. Only after the write succeeds does the
   generator's in-memory limit rise to that value. The stored reservation never decreases
   while the node is serving.
2. **Never exceed the limit.** The generator refuses to issue a timestamp above its limit
   and returns `UNAVAILABLE`.
3. **Start above the previous owner.** A node that claims a worker ID reads the stored
   reservation `R` and only issues timestamps `> R`.

Suppose the old owner issued timestamp `t_old` and the new owner issued `t_new`. Then
`t_old <= R < t_new`, so the two never collide. This holds whatever the two clocks say,
because the comparison is between the stored numbers, not between clocks. It also holds
when the old owner is partitioned, or paused (GC, `SIGSTOP`, VM migration) and resumes
after losing its lease: the old owner can't extend a reservation it no longer owns, so it
runs out of reserved time and stops issuing.

The claim transaction and the reservation transaction exclude each other through etcd's
serializable transactions. A reservation update either commits before the claim, so the
new owner sees it, or fails because the liveness key now belongs to another lease.

## Node lifecycle

**Join**

1. Grant a lease and keep it alive.
2. In two range reads, list the liveness keys and the reservation keys.
3. Candidates are the IDs in `[0, max_workers)` without a liveness key. Sort them by
   stored reservation, oldest first. Never-used IDs come first.
4. Skip candidates whose reservation is ahead of the local clock by `MaxClockAhead` (1s)
   or more. Using one would mean issuing timestamps far in the future.
5. Claim with one transaction: `if live/<id> doesn't exist and reserved_until/<id> is
   unchanged, then put live/<id> with the lease`. If another node won the race, try the
   next candidate.
6. Write the first reservation, then report ready.

**Serve.** Issue IDs under the generator's mutex. Extend the reservation every second.

**Leave (SIGTERM).**

1. Mark not-ready and set gRPC health to `NOT_SERVING`.
2. Drain in-flight RPCs.
3. Freeze the generator.
4. Shrink the reservation to the last issued timestamp. This is safe because nothing
   more will be issued.
5. Revoke the lease. The worker ID is free immediately, and a successor rarely has to
   wait for its clock.

**Crash.** The lease expires after the TTL and the liveness key disappears. The reservation
stays at most 5s ahead of the crashed node's clock, while a successor can only claim
after the 10s TTL. With reasonably synchronized clocks, the successor starts without waiting.

## Clock handling

Let `last` be the last issued timestamp and `now` the wall clock.

- `now > last`: issue at `now`, sequence 0.
- `now == last`: increment the sequence. If all 4096 are used, continue at `last + 1`.
- `now < last` (the clock stepped back): keep issuing from `last` with the next sequence.

The logical clock may run at most `MaxClockAhead` (1s) ahead of the wall clock. Within
that window, small backwards steps (NTP corrections, leap-second handling) and short
bursts above 4096 IDs/ms are absorbed with no error and no sleep. Beyond it the node
returns `UNAVAILABLE`, so clients retry elsewhere. If the gap reaches `StopOnClockSkew`
(5s), the node stops and exits non-zero. Its orchestrator restarts it, and the restarted
process claims a worker ID whose reservation is behind its clock.

## Failure modes

| Failure                          | Behaviour                                                                                  |
|----------------------------------|--------------------------------------------------------------------------------------------|
| etcd unreachable                 | Keeps serving from the current reservation (up to ~5s), then `UNAVAILABLE` and `/readyz` 503. Recovers when etcd is back, if the lease survived. |
| Lease lost / ID taken over       | Reservation update fails or keepalive ends. The node stops and exits non-zero.            |
| Process paused past the TTL      | On resume it can only issue up to its old reservation, which the new owner is above. Then it stops. |
| Clock steps back < 1s            | Absorbed transparently.                                                                     |
| Clock steps back 1–5s            | `UNAVAILABLE` until the clock catches up.                                                  |
| Clock steps back ≥ 5s            | Node stops. The restart claims a different worker ID.                                     |
| All worker IDs taken             | A new node fails to start (`no usable worker id`).                                         |
| etcd data lost                   | **Unsafe.** Reservations are gone, and a successor could reuse old timestamps. Run etcd durably. |

## Defaults

| Setting              | Default | Constraint                                                   |
|----------------------|---------|--------------------------------------------------------------|
| Lease TTL            | 10s     | How long a crashed node's ID stays taken.                    |
| Reservation window   | 5s      | How long a node survives without etcd. Keep it below the lease TTL so a successor after a crash rarely skips the ID. |
| Renew interval       | 1s      | Must be shorter than the reservation window.                 |
| Max clock ahead      | 1s      | Tolerance for backwards clock steps and bursts.              |
| Stop on clock skew   | 5s      | When to give up and restart.                                 |
| `max_workers`        | 1024    | Must be the same on every node in a cluster.                 |

## Why not a distributed lock?

A lock tells you who owned it when you asked, not who owns it now. A node that pauses
after acquiring it can't tell that the lock expired. Time reservations act as a fencing
token that is part of the ID itself: even a node with a stale view of ownership can only
produce IDs in a range that no one else will use.
