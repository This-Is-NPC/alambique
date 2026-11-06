# What does not work

Known defects that are still open. Limits that follow from the design are in
[design.md](design.md) under the section of the feature they belong to. Each
entry says what happens,
what to do about it, and where the evidence is.

## A node refuses new cells instead of hibernating idle ones

**What happens.** At `awake.max_cells` awake cells a call that would wake
another cell gets `{:error, :overloaded}`, even if some awake cells have been
idle for a long time. The node does not hibernate the least recently used cell
to make room. Calls to a cell with `awake.max_pending` calls waiting get
`{:error, :busy}`; the check is made by the sender, so a call whose caller
already gave up is still run if the cell accepted it, and the limit can be
passed by the number of callers that check at the same moment.

**Workaround.** Lower `idle_timeout_ms` for hosts that cycle many cell ids, or
call `Alambique.hibernate/3` for cells that are known to be done, and set
`awake.max_cells` below what the memory and the file descriptors of the node
allow. To bound memory instead of the count, set `awake.max_bytes`: it does
hibernate the least recently used cells to make room (design §7).

**Evidence.** `test/admission_test.exs`; design §7, "Admission".

## Cross-cell queries and cell reads can still fill the dirty IO pool

**What happens.** Every SQLite call runs on a dirty IO scheduler (10 by
default). The default `query: [concurrency:]` leaves two free, but several
`Alambique.Query` runs at once, or slow reads on many cells, can still hold all
of them, and the commits of other cells then wait for a free scheduler. A
statement is bounded by `sql: [statement_timeout_ms:]`, so the wait ends when
the statements do. There is no node-wide limit on concurrent user statements:
it needs a semaphore with a monitor on every read path, and it would make a
read wait for a slot on the cell process.

**Workaround.** Raise the dirty IO pool (`+SDio`), lower
`query: [concurrency:]` and `sql: [statement_timeout_ms:]` on nodes that run
both, or keep cross-cell queries on a node that owns few cells.

**Evidence.** Design §14 and `Alambique.Config` (`default_query_concurrency`).

## A query replica refresh costs the size of the database

**What happens.** `Alambique.Query` refreshes a cached replica by copying the
file, reading it once to compute its checksum, applying the new segments and
flushing the copy. The cost is proportional to the database, not to the
change: a 1 GB replica refreshed by one 4 KiB segment copies, reads and flushes
1 GB. The copy is needed because other queries may have the old file open.
Recording the checksum with the cache entry and using reflinks where the
filesystem has them would remove two of the passes, but change the cache
naming and need a platform check.

**Workaround.** Use `max_age_ms:` so that a run reuses a replica instead of
refreshing it, and keep the cache on a filesystem with fast copies.

**Evidence.** `Alambique.Query.Replica` (`apply_chain`); design §14.

## Replicas poll the store while idle, and store calls have no hard bound

**What happens.** A read replica lists two prefixes and reads the lease every
`replicas: [poll_ms:]` (250 ms) until it has been idle for `idle_ms`: at most
32 replicas by default, about 500 requests per second per node. Checked reads
on an idle cell each read the lease inside the cell process, and every
statement is prepared again at each call. A store call is retried until the
operation deadline, but the deadline is checked between attempts, so one call
to a custom store that never returns is not interrupted; for `Alambique.Store.S3`
the idle `:receive_timeout` (15 000) is longer than `op_timeout_ms` (10 000).

**Workaround.** Raise `replicas: [poll_ms:]` where reads tolerate the lag, and
set the S3 `:receive_timeout` below `op_timeout_ms`.

**Evidence.** `lib/alambique/replica.ex`, `lib/alambique/replication/retry.ex`.

## Some statements have no deadline

**What happens.** The `:sql` option `:statement_timeout_ms` bounds the
statements of `query/5`, `execute/5`, `Alambique.Tx.query/3` and `execute/3`,
and the reads of replicas and cross-cell queries. It does not bound migrations,
`Alambique.Tx.script/2` and the library's own statements. The deadline is
checked every 1 000 SQLite VM instructions, so a statement that spends a long
time inside one instruction (a very large `zeroblob`, say) ends when that
instruction does. A statement that scans a large table without returning rows
is bounded by the deadline but not by `:max_result_rows`.

**Workaround.** Keep migrations small, and lower `:statement_timeout_ms` for
instances that run host-supplied SQL. There is no per-call override of the
`:sql` limits yet.

**Evidence.** `test/sql_safety_test.exs` and the tests of
`native/alambique_nif/src/sqlite.rs` cover the bounded paths; design §7,
"Statements of callers".

## Node ids and data directories are not checked

**What happens.** Three requirements of the instance are not enforced:

- `node_id` must be unique in the cluster. Without distribution `node()` is
  `nonode@nohost`, so two VMs that use the default over one store and prefix
  take each other's leases for their own leftovers: the epoch rises on every
  call and writes fail with `:fenced` or `:lease_lost`. Data stays safe, because
  fencing still holds.
- `:data_dir` must belong to one instance. A second instance (or VM) on the
  same directory shares the files of cells with equal ids, and its start removes
  the first one's `read-replicas` and `work` directories.
- `:data_dir` must be on a case-sensitive file system. On a case-insensitive
  one, ids that differ only in case (`Org`, `org`) have separate leases but one
  local file.

**Workaround.** Set `node_id` explicitly when the VMs are not distributed, give
every instance its own `data_dir`, and use a case-sensitive file system (Linux
ext4, xfs; not a default macOS or Windows volume or a bind mount of one).

**Evidence.** None needed: the checks do not exist. They were left out because
the first is a race against the asynchronous `:pg` view of other nodes, and the
other two need a lock file or a case-insensitive mount to test.

## Litestream has not been run on our replicas

**What happens.** The key layout, the level-9 snapshots and the
`litestream-timestamp` metadata follow Litestream's S3 replica client, and the
files are LTX version 3, but no Litestream binary has read a replica of
Alambique. Nothing here shows that `litestream restore` works on one.
Alambique also cannot apply files written by Litestream itself, because they
carry no database checksums (the NIF and the lazy restore need them).

Since the tail rule (design §6), a restore of ours ends at its head when
level-0 files of one TXID are left past a gap. Litestream's restore of the
latest state fails on such a tail ("non-contiguous ltx files"). This was read
in Litestream's sources and not run.

**Workaround.** Restore with `mix alambique.restore`, which is tested.

**Evidence.** [segment-format.md](segment-format.md), "How the format is
checked", lists what the tests cover.

## Cleanups after a move are not rediscovered after a restart

**What happens.** The deletion of a moved cell's old objects waits for a
grace period in the compactor's memory. If that node restarts first, the
old objects stay until someone runs the cleanup, and moving the cell back
there answers `{:error, :destination_not_cleaned}` until it runs.

**Workaround.** `mix alambique.cleanup CELL LOCATION_ID --instance NAME --connect NODE`
or `Alambique.cleanup_moved/4`.

**Evidence.** [design.md §18](design.md#18-placement).
