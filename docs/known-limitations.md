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
- A second read-write connection on the file of an awake cell is refused only
  when the library opens it. Another OS process, a raw write of the file,
  other native code in the VM, a connection that was opened before the cell
  woke and a connection through another VFS in another process are not
  prevented. The cell finds their commits and checkpoints by the header of
  the wal-index at its next write, cut, hibernation or feed cleanup. It never
  acknowledges those rows: with `sql: [orphan_frames: :stop]` it stops and the
  next wake restores the store's state, so the rows are gone; with `:replicate`
  it uploads them when they are still in the WAL, and stops when a checkpoint
  or a restart of the WAL took them out of it. Until the next of those events
  the rows are visible to the cell's reads.
- The rows that `CREATE TABLE ... AS SELECT` writes are in the cell and in its
  store but not in the change feed: the triggers of the new table exist only
  once the statement has ended. Create the table, then fill it with
  `INSERT ... SELECT` in a write; those rows are in the feed
  ([how-to-consume-the-change-feed.md](how-to-consume-the-change-feed.md)).
- A lazy restore serves pages of files above `lazy_restore: [verify_bytes:]`
  that have no page-hash sidecar (`lazy_restore: [page_sidecar:]`, off by
  default) before their content is checked. On arrival a ranged read is checked for
  the object (ETag, size, epoch, and with S3 the `Content-Range`) and the
  frame (page number, flags, length); the content of the page is checked at
  the end of the hydration, by the comparison with the whole objects and the
  database checksum. Reads before the end can see wrong content; with
  `lazy_restore: [verify_reads: true]` they wait for the end, and writes
  always do ([design.md](design.md), section 16). The end of the hydration
  reads whole every object of the chain that no sidecar covers. A file has
  a sidecar only if a writer with `page_sidecar: true` wrote it and no other
  sidecar write of that cell was under way; a crash or a failed write leaves
  it without one. A store whose ranged reads give no
  ETag is not pinned to the object that was staged.
- `awake: [idle_close_after_ms:]` returns to the system what the page arena
  holds for the connection (about 320 KiB for a cell of 2,000 notes), but
  little of the rest: the schema and the statements (about 38 KiB per cell)
  go back to the allocators of the BEAM, which keep their carriers, so a cell
  with no rows gives back 6 to 24 KiB of resident memory. A close syncs the
  database: on a disk, cells that close together queue their syncs (about 1 s
  for the median of 250 cells closing at once, 12 to 22 s for the slowest),
  and the queue is the tail of the calls that find the disk busy. A cell
  called about every `idle_close_after_ms` pays a reopen and a close on each
  call and gains nothing ([cost-of-a-cell.md](cost-of-a-cell.md)).
- Options chosen with `cell_options` that an awake cell cannot take live:
  `sql: [locking_mode:]` applies at the next wake (a refresh that changes it
  is refused), and `lazy_restore: [mode:]` is read at the next restore
  ([how-to-operate-a-cell.md](how-to-operate-a-cell.md), step 13).
- The page arena (`config :alambique, :page_arena`) works on Linux only. It
  reserves an address range with `mmap` at load (4 GiB by default, address
  space and not memory). When the system refuses the range (a low `ulimit -v`,
  a kernel that counts address space), or on another system, the library loads
  without the arena and the freed page-cache memory stays in the process:
  `page_arena_state` is 2 or 3 and the cost page's figures for it do not
  apply. When the range is full the BEAM's allocators serve the blocks
  (`page_arena_fallbacks`). Blocks allocated by a helper thread of
  `PRAGMA threads`, or through a shared cache (`cache=shared`), go to one
  region all connections share and release less
  ([cost-of-a-cell.md](cost-of-a-cell.md), section 5). The settings are read
  when the library loads and cannot change while the node runs.
- Each connection has its own page cache, so a cell cannot borrow the cache
  that idle cells leave unused: a cell whose working set is above `cache_kib`
  reads pages again. Raise `cache_kib` for it with `cell_options`.
- A node of a version without page-hash sidecars does not know the `.pages`
  objects: its retention and the cleanup of a moved cell leave them in place
  until a version with sidecars sweeps them
  ([segment-format.md](segment-format.md)).

## Load measurements

The cost of one cell, as measured under load, is in
[cost-of-a-cell.md](cost-of-a-cell.md). Not measured under load:
`lazy_restore: [verify_reads: true]`, and the memory of a busy awake cell.
