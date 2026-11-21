# Known limitations

Known limitations of the library. Each entry says what happens, what to do
about it, and where the evidence is.

- Write access to the bucket prefix is full control of its cells: checksums
  detect corruption, not forgery. Give each deployment its own prefix and
  readers a read-only credential
  ([design.md](design.md), section 3.1). No prefix may be a path prefix of
  another instance's in the same store (`app` and `app/cells` share keys).
- A file system whose writes can stall stalls every write of every cell. See
  [how-to-operate-a-cell.md](how-to-operate-a-cell.md), step 10.
- On a saturated CPU, the compaction at the release of a cell can be cut by
  `compaction: [release_deadline_ms:]`, and the next node reads a longer tail
  ([design.md](design.md), section 9). `move/4` and `hibernate/3` take a
  larger `release_deadline_ms:` for one call.
- Litestream 0.5.18 cannot restore the latest state of a cell whose chain has
  a gap (a cell that crashed with uploads in flight, until a later owner
  writes over it): restore it with `mix alambique.restore`, or with Litestream
  up to a TXID before the gap. Alambique does not apply files written by
  Litestream. See
  [segment-format.md](segment-format.md#litestream-compatibility).

## Load measurements

Measured on 2026-11-21 with the load test (round `o022vm-015`, commit
`932fd93`, library code of 0.15.0) on the reference VM (4 vCPUs, 5.9 GB),
the service with `+SDio` equal to its CPUs (see
[how-to-size-a-node.md](how-to-size-a-node.md)). Scenario A is 1 CPU and
512 MB, B is 2 CPUs and 2 GB; the store is SeaweedFS on a 2 GB tmpfs. A level
is good when memory, errors and `:busy` stay in bounds and, except for cells
awake, p99 is at most 250 ms (wakes: p95 at most 500 ms)
([how-to-load-test.md](how-to-load-test.md)). No step lost data or
returned an error.

| Case | Last good level | At that level | Limit |
| --- | --- | --- | --- |
| A, commits of 75 KB | 10/s | p99 48 ms, 21.9 ms CPU per commit | not found: 12/s filled the store's tmpfs |
| B, commits of 75 KB | 10/s | p99 62 ms, 25.2 ms CPU per commit | not found: 12/s filled the store's tmpfs |
| A, small commits | 400/s, not confirmed | p99 147 ms, CPU throttled by the cgroup | the repeat of 400/s and every level from 500/s were bad on memory |
| A, cells awake | 1000 | p99 286 ms (`cache256`: 414 ms) | 2000 was bad on memory |
| A, cold wakes | 20/s, not repeated | p99 184 ms, 16.3 ms CPU per wake | not found: 40/s would fill the harness's data tmpfs |

What these numbers do not show:

- No level is confirmed by a repeat: the large commits and the wakes stopped
  on a harness limit before it, and the repeat of 400 small commits per second
  failed.
- A node with one CPU runs out of memory (512 MB) before CPU on small commits
  and awake cells; its good levels near the limit had the CPU throttled.
- With the store on the VM's disk instead of tmpfs (an earlier round), the
  large commits reached 5/s (A) and 7/s (B), limited by the p99 of the disk.
- `lazy_restore: [verify_reads: true]` has not been measured under load.
