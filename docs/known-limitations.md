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
- The protection of a cell's connection holds against code that uses the
  connection of a write, not against code that reads or replaces the state of
  the cell process (`:sys.get_state/1`, `:sys.replace_state/2`, a debugger)
  or writes the database file by other means. A kept handle can read and
  nothing else ([design.md](design.md), section 15).
- The rows that `CREATE TABLE ... AS SELECT` writes are in the cell and in its
  store but not in the change feed: the triggers of the new table exist only
  once the statement has ended. Create the table, then fill it with
  `INSERT ... SELECT` in a write; those rows are in the feed
  ([how-to-consume-the-change-feed.md](how-to-consume-the-change-feed.md)).

## Load measurements

The cost of one cell, as measured under load, is in
[cost-of-a-cell.md](cost-of-a-cell.md). Not measured under load:
`lazy_restore: [verify_reads: true]`, and the memory of a busy awake cell.
