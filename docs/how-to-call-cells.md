# How to call cells from your application

**The question:** how do I start an instance and call the functions of the
`Alambique` module?

This guide stands alone. The options, migrations and errors are in the
documentation of `Alambique`; the terms are in [concepts.md](concepts.md).

## Prerequisites

- An S3-compatible store with conditional writes, and its credentials
  ([storage-compatibility.md](storage-compatibility.md)). The S3 store's options
  are in [how-to-configure-the-s3-store.md](how-to-configure-the-s3-store.md).
- A local directory for `:data_dir`. One instance uses one directory. A second
  instance on the same directory fails to start with
  `{:error, {:data_dir_in_use, path}}`.

## 1. Start one instance per node

Use the same name, store and prefix on every node of the cluster. Step 5 shows
how to spread cells over several locations.

A prefix is a path such as `myapp` or `myapp/prod`: no empty, `.` or `..`
segments, no leading or trailing `/`, and no control characters. Do not give one
instance a prefix that is a path prefix of another instance's prefix in the same
store.

```elixir
children = [
  {Alambique,
   name: MyApp.Cells,
   store: {Alambique.Store.S3, s3_config},
   prefix: "myapp",
   data_dir: "/var/lib/myapp/cells",
   migrations: [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]}
]
```

`:migrations` can also be `{module, function}` or `{module, function,
extra_args}`. The function is called with the cell id (and the extra arguments)
at each wake, and returns the migrations of that cell.

## 2. Read and write

Call any cell from any node. By default a cell id is a string of
`[A-Za-z0-9_.-]`, at most 128 bytes, and not `.` or `..`. A placement can define
its own rule.

```elixir
{:ok, _} = Alambique.execute(MyApp.Cells, "org-1", "INSERT INTO notes (body) VALUES (?)", ["hi"])
{:ok, %{columns: ["body"], rows: [["hi"]]}} = Alambique.query(MyApp.Cells, "org-1", "SELECT body FROM notes")

{:ok, :done} =
  Alambique.transaction(MyApp.Cells, "org-1", fn tx ->
    {:ok, _} = Alambique.Tx.execute(tx, "DELETE FROM notes")
    :done
  end)
```

A write returns after its transaction is stored durably.

- Disk full on the node: a call returns `{:error, :disk_full}`. The write did
  not run, and the call can be repeated once space is free. See
  [how-to-operate-a-cell.md](how-to-operate-a-cell.md), section 12.
- Store unavailable: a call that needs the store returns
  `{:error, :store_unavailable}` once the circuit breaker is open. A write waits
  for the retry of its upload until its own timeout, then returns
  `{:error, :timeout}`. The write may still have landed. See the
  Troubleshooting section of [how-to-operate-a-cell.md](how-to-operate-a-cell.md).

Each statement is interrupted at the instance's `sql: [statement_timeout_ms:]`
(default: `op_timeout_ms`, 10 seconds) and returns `{:error, :statement_timeout}`.
A call can lower that deadline with `statement_timeout_ms:` in its options, but
not raise it.

### Statement rules and write errors

These rules hold for the statements a function runs and for the answers a
write can give. The reasons are in [design.md](design.md), section 15.

**Statements.** Each text given to `Alambique.execute/5`, `Alambique.query/5`
and the `Alambique.Tx` functions is one statement.

- `BEGIN`, `COMMIT`, `END` and `ROLLBACK` in user text, and `EXPLAIN` of them,
  give `{:error, :not_authorized}`, and the transaction goes on. This holds for
  `Tx.execute/3`, `Tx.query/3`, `Tx.script/2` and for any statement run on the
  connection of the handle with `Alambique.Sqlite` while the transaction is
  open. `SAVEPOINT`, `RELEASE` and `ROLLBACK TO` work; `Tx.rollback/2` aborts.
- The connection of the handle (`tx.conn`) is the cell's own, and it is for the
  function while it runs, in the process that runs it. While the function
  runs it can also run its own statements on it, but not close it
  (`Alambique.Sqlite.close/1`, `checkpoint/1` and a second
  `begin_transaction/1` give `{:error, :not_authorized}`). Once the function
  has returned, the handle can only read, and only in the cell's process: a
  write, a pragma that sets something, `ATTACH`, `ANALYZE`, `REINDEX`,
  `VACUUM` and `PRAGMA optimize` give `{:error, :not_authorized}`, and `close/1`
  and `checkpoint/1` do too. Any call on it from another process gives
  `{:error, :not_owner}`. A connection you open with `Alambique.Sqlite.open/2`
  has the same rule: the process that opened it uses it, closes it,
  interrupts it and reads its memory, and `Alambique.Sqlite.assign/2` names
  the one other process that may run statements on it. Keep rows, not the
  handle.
- Text after the first statement that holds another statement gives
  `{:error, {:multiple_statements, tail}}` (the start of the tail) and nothing
  runs. Spaces, comments and `;` may follow.
- Text with a NUL byte gives `{:error, :nul_in_statement}` and nothing runs.
- `Tx.script/2` runs the statements as SQLite delimits them. A statement that
  does not start with `CREATE`, `DROP` or `ALTER` (after blanks, comments and a
  UTF-8 byte order mark) gives `{:error, {:not_ddl, statement}}` and nothing
  after it runs. The statements before it have already run; `Tx.script/2` undoes
  them with a savepoint, while `Alambique.Sqlite.script/2` on a connection of
  your own does not. `Alambique.Sqlite.ddl?/1` is the one check of what counts
  as schema, also for `Tx.execute/3`.
- The rows of `CREATE TABLE ... AS SELECT` are not in the change feed; see
  [known-limitations.md](known-limitations.md).

**Answers of a write.**

- `{:error, :transaction_ended}`: SQLite ended the transaction itself (a
  conflict with `ROLLBACK`, `RAISE(ROLLBACK, ...)`, an interrupt, an I/O
  error). Every later statement of the function, `Tx.script/2` included,
  returns it without running, and so does the write when the function returns.
  Nothing was committed.
- `{:error, :orphan_frames}`: another writer changed the database file, so
  frames were committed that no write of the cell carries (they cannot come
  from a transaction function). With the default `sql: [orphan_frames: :stop]`
  the cell stops and the next wake restores what the store holds.
  `position/3` and `hibernate/3` answer it, and so does a write that found the
  other writer before it began: that write did not run. A write that failed
  answers its own error. With `:replicate` the cell uploads the frames and the
  write runs after them, unless a checkpoint or a restart of the WAL took the
  frames out of it: then no batch can carry them and the cell stops under
  both values.
- `{:error, :held_by_cell}`: `Alambique.Sqlite.open/2` was asked for a
  read-write connection on the file of an awake cell, by its name, by a hard
  link, with another VFS or by `ATTACH`. Open it read-only, or when the cell
  is asleep.
- `{:error, {:committed, reason}}`: only with `sql: [orphan_frames: :replicate]`.
  The write failed with `reason` but left committed frames; they were uploaded
  as a batch of their own, so its rows are durable.
- `{:error, :outcome_unknown}`: the write MAY be durable. It is the answer of a
  write that was in a batch when the cell stopped or the upload failed; do not
  repeat it unless it is idempotent.
- `{:error, {:query_only, reason}}`: the commit ran, the pragma that makes the
  connection read-only afterwards failed. The cell stops. With `:stop` the row
  is not durable; with `:replicate` the answer is `{:error, {:committed,
  {:query_only, reason}}}` and the rows are.
- `{:error, {:rollback_failed, reason}}`: the rollback failed and the
  transaction may still be open. The cell stops. The same two results stop
  the cell in a feed cleanup and in a migration of a serving cell, whose
  caller gets `{:error, {:migrate_failed, {:migration,
  version, reason}}}`.
- `{:error, {:stamp_failed, reason}}`: the feed rows of a batch could not be
  stamped, and the cell stops. The call that woke the cell gets it when the
  stamp of the wake's own migration failed: the cell does not serve and its
  lease is released. A migration of a serving cell gets `{:error,
  {:migrate_failed, {:stamp_failed, reason}}}`.

**Checkpoint.** `Alambique.checkpoint/3` answers `{:error, :busy}` when the
uploads were not acknowledged within its `:deadline_ms`, also when the
batch of `orphan_frames: :replicate` is among them.
`Alambique.Sqlite.checkpoint/1` gives `{:error, :not_authorized}` while the
transaction of the library is open on that connection.

**`sql: [orphan_frames: :stop | :replicate]`** (default `:stop`) says what the
cell does with committed frames no write accounts for. `:stop` stops the cell;
`:replicate` uploads them as a batch of their own, with their feed rows
stamped, and goes on. A cell checks for them at every cut, before a
hibernation, before every feed cleanup and before each write, by the header
of the shared-memory wal-index of its connection.

**`Alambique.Sqlite.begin_transaction/1` and `finish_transaction/3`.** The
first opens the transaction and arms the connection; the second ends it with
the token the first returned. They are the only way to begin and end a
transaction on a connection, and the transaction function has no token.
`Alambique.Sqlite.wal_advanced?/1` says whether a commit reached the WAL since
the begin.

## 3. Find and list cells

```elixir
{:ok, node} = Alambique.owner(MyApp.Cells, "org-1")
{:ok, node} = Alambique.owner(MyApp.Cells, "org-1", hint: true)  # cached owner, no store read
:none = Alambique.owner(MyApp.Cells, "org-2", peers: true)       # asks the nodes, no store, no wake
{:ok, %{epoch: epoch}} = Alambique.lease(MyApp.Cells, "org-1")   # without waking
{:ok, node} = Alambique.touch(MyApp.Cells, "org-1")              # wake it and say where
%{cells: ids, next: next, count: n} = Alambique.cells(MyApp.Cells, limit: 100)
{:ok, %{cells: ids, next: next}} = Alambique.list_cells(MyApp.Cells, prefix: "org-", limit: 100)
```

`owner/3` never wakes a cell. By default it reads the lease. With `peers: true`
it asks the processes instead: the local registry first, then the other members
of the instance in parallel. It makes no store request. `{:ok, node}` names the
node that holds the cell awake; `:none` means no member does. The answer can be
one message stale: a cell that is waking may still be reported as `:none`, and a
member that does not answer within `call_timeout_ms` counts as not holding the
cell. Use the default when `:none` must mean that the lease is free.

`Alambique.members/1` lists the nodes that run the instance, this one included,
from the view this node keeps (no store request). `Alambique.preferred/2` names
the node that routing prefers for a cell among them. A cell is woken there when no
node holds it:

```elixir
nodes = Alambique.members(MyApp.Cells)
node = Alambique.preferred(MyApp.Cells, "org-1")
```

`Alambique.cells/2` lists the cells awake on the local node, in id order. With
`details: true`, each entry is a map. A monitor can use it to pick a cell to
hibernate, to free memory or disk, without reading files:

```elixir
%{cells: entries} = Alambique.cells(MyApp.Cells, details: true, limit: 1_000)
# [%{id: "org-1", last_used: 1_234_567, wal_bytes: 24_752}, ...]

lru = Enum.min_by(entries, & &1.last_used)     # the least recently used cell
largest = Enum.max_by(entries, & &1.wal_bytes) # the cell with the largest WAL
:ok = Alambique.hibernate(MyApp.Cells, largest.id)
```

`last_used` is in `System.monotonic_time(:millisecond)` of this node, so
`System.monotonic_time(:millisecond) - last_used` is the idle time. It is `nil`
while the cell is waking: skip those entries when you compare them.
`wal_bytes` is the size of the cell's WAL after its last acknowledged batch.
Hibernating the cell checkpoints it and removes the WAL.

Each entry also has the cell's pid, whether it still serves, the calls waiting
on it, its TXIDs, commit queue, lease, change feed, page cache state, the
bytes SQLite and the process hold, and when and how it woke. The fields and
their cost are in the documentation of `Alambique.cells/2`. Every one of them
is read without calling the cell, so it never wakes or touches a busy one.
`Alambique.facts/2` gives the same fields for one cell, and says whether it has
a process on this node, how big its local file is and whether its wake is in
backoff, without waking it:

```elixir
{:ok, %{local_process: false, local_file_bytes: bytes, wake_backoff: nil}} =
  Alambique.facts(MyApp.Cells, "org-1")
```

`Alambique.list_cells/2` lists every cell stored in a location, asleep or awake,
from the schema index. Pass `next` as `after:` to get the next page.

## 4. Move, rewind and stop cells

```elixir
:ok = Alambique.hibernate(MyApp.Cells, "org-1")
{:ok, _node} = Alambique.move(MyApp.Cells, "org-1", :"other@host")
{:ok, node} = Alambique.take_over(MyApp.Cells, "org-1", :owner_down)  # before the lease expires
{:ok, %{txid: before}} = Alambique.position(MyApp.Cells, "org-1")    # a cut: earlier writes at or below it
{:ok, _} = Alambique.rewind(MyApp.Cells, "org-1", {:txid, before})   # back to that state, atomically
{:ok, _} = Alambique.query(MyApp.Cells, "org-1", "SELECT 1", [], verify_lease: true)
{:ok, version} = Alambique.migrate(MyApp.Cells, "org-1")              # an awake cell, in place
{:ok, %{hibernated: _, errors: _}} = Alambique.drain(MyApp.Cells)     # before a node stops
:ok = Alambique.join(MyApp.Cells)                                      # after a node started with join: false, or after a drain
```

`drain/2` hands the cells of a node over to another node that runs the instance:
see [how-to-swap-a-node.md](how-to-swap-a-node.md).

`take_over/4` fences the current owner at once. Use it when a failure detector
suspects the owner is gone. Every acknowledged write is kept, even when the
suspicion is wrong. `Alambique.freeze/3` keeps a cell from waking. See
[how-to-rewind-a-cell.md](how-to-rewind-a-cell.md) for rewinds and
[how-to-operate-a-cell.md](how-to-operate-a-cell.md) for the `mix` tasks.

## 5. Spread cells over locations

The library knows no regions, organizations or tenants. A placement module that
you supply says where a cell's data lives and what its ids look like. The module
implements `Alambique.Placement`:

```elixir
{Alambique,
 name: MyApp.Cells,
 placement: {MyApp.Placement, regions: ...},
 data_dir: "/var/lib/myapp/cells"}

{:ok, %Alambique.Location{id: "us"}} = Alambique.relocate(MyApp.Cells, "org-1", "us")
```

`relocate/4` moves the cell with a fence, so only one location accepts its
writes. The old location's objects are deleted after a grace period, and the cell
can move back to a location it left. See [design.md](design.md), section 18.

## 6. Read other things

- Row changes of all cells: [how-to-consume-the-change-feed.md](how-to-consume-the-change-feed.md).
- Read-only SQL over many cells: [how-to-query-across-cells.md](how-to-query-across-cells.md).
- Reads of a hot cell on other nodes: [how-to-read-replicas.md](how-to-read-replicas.md).
- Large cells restore lazily: a cell answers its first query before its database
  is downloaded. See [how-to-speed-up-a-wake.md](how-to-speed-up-a-wake.md).
