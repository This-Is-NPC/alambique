# How to read a hot cell from other nodes

**The question:** how do I serve the reads of a hot cell from the nodes that
do not own it, accepting a bounded lag, so the owner does not answer every read
and callers do not pay a hop to it?

This guide stands alone. The design and the proof of the bound are in
[design.md](design.md), section 21. The output below comes from running these
steps against `AlambiqueTestkit.MemoryStore` with two instances in one VM, an
owner and a reader. The two instances cannot forward to each other in that
setup, so a read that the reader sends to the owner answers `{:error,
{:unavailable, "owner_node", _}}` there; in a cluster the owner answers it.

## Before you start

- An instance of Alambique on each node that reads (see the
  [README](../README.md)), and a cell that is awake or asleep on some other
  node. A node that owns the cell never keeps a replica: it answers from the
  cell.
- Disk under the instance's `:data_dir` for the replicas
  (`<data_dir>/read-replicas`, at most 1 GiB per node by default), and a few
  store requests per second per replica (section "What it costs").
- A bound you can live with. A read with `{:bounded, 500}` contains every write
  whose caller was told it was done more than 500 ms before the read started. It
  may miss newer ones, and two reads of one caller, one on the owner and one on
  a replica, can see time go back by up to the bound.

## 1. Turn replicas on in the instance

Replicas are off unless the instance has the option. Start every node that
reads with it:

```elixir
{Alambique,
 name: MyApp.Cells,
 store: {Alambique.Store.S3, s3_config},
 prefix: "myapp",
 data_dir: "/var/lib/myapp/cells",
 replicas: [max_cells: 32, poll_ms: 250]}
```

`replicas: []` takes every default. The options are in the `Alambique`
module documentation under `:replicas`. Without the option, a bounded read goes
to the owner and nothing looks for the switch of step 2.

Expected result: the instance starts; `<data_dir>/read-replicas` exists once a
replica does.

## 2. Turn the switch on for the hot cells

A replica costs requests and disk on every node that reads the cell, so you
choose the cells. The switch is an object in the store:

```elixir
Alambique.replicas(MyApp.Cells, "org-1")
# {:ok, false}
Alambique.replicas(MyApp.Cells, "org-1", true)
# :ok
Alambique.replicas(MyApp.Cells, "org-1")
# {:ok, true}
```

None of these wakes the cell. Nodes notice the switch within `marker_ttl_ms`
(5 seconds by default).

## 3. Read with a bound

```elixir
Alambique.query(MyApp.Cells, "org-1", "SELECT body FROM notes", [],
  consistency: {:bounded, 500})
```

The first read on a node finds no replica. The owner answers it, and the node
starts building a replica in the background:

```
telemetry: [:alambique, :replica, :read] %{} %{reason: :no_replica, source: :owner, cell: "org-1"}
telemetry: [:alambique, :replica, :build] %{} %{result: :ok, cell: "org-1", txid: 3}
```

The next reads are served from the replica:

```
{:ok, %{columns: ["body"], rows: [["hi"]]}}
telemetry: [:alambique, :replica, :read] %{lag_ms: 82} %{source: :replica, cell: "org-1", txid: 3}
```

`lag_ms` is how long ago the replica last confirmed that it had everything in
the store. It is never above the bound. After a write on the owner, the replica
shows it by the next poll (`poll_ms`, 250 ms by default):

```
{:ok, %{columns: ["body"], rows: [["hi"], ["there"]]}}
```

A node that owns the cell answers `consistency: {:bounded, _}` from the cell
itself, as a strong read. `consistency: :strong` is the default and does not
look at replicas. `execute/5` and `transaction/4` refuse the option with
`{:error, :bad_option}`, and so does `query/5` when you combine `{:bounded, _}`
with `verify_lease: true`.

## 4. Choose what happens to a read that is too old

When the replica last confirmed more than the bound ago (a slow store, a bound
shorter than `poll_ms`), the instance option `on_stale` decides:

- `:refresh` (default): the read waits, the replica refreshes at once, and the
  read is answered by the replica as soon as a refresh that started recently
  enough finishes. If none does within `refresh_timeout_ms` (2 seconds), the
  owner answers.
- `:owner`: the owner answers at once. Choose it when a wait is worse than a hop.

The telemetry event says which side answered (`source`) and why the owner did
(`reason`: `:stale`, `:timeout`, `:building`, `:no_replica`, ...).

## 5. Turn it off

```elixir
Alambique.replicas(MyApp.Cells, "org-1", false)
# :ok
```

Each replica reads the switch again every `marker_ttl_ms`. When it is gone, the
replica stops and removes its file, so within `marker_ttl_ms` plus a poll the
disk is back to what it was:

```
telemetry: [:alambique, :replica, :stop] %{} %{reason: :marker_off, cell: "org-1"}
File.ls(".../read-replicas")
# {:ok, []}
```

A replica nobody reads for `idle_ms` (10 minutes) stops by itself. Moving a
cell to another location (`Alambique.relocate/4`) stops its replicas and does
not carry the switch: turn it on again for the new location.

## What it costs

With the defaults, about 12 store requests per second for each replica when
nothing changes: a listing of the level-0 files, a listing of the snapshots and
a read of the lease per poll, four times a second. Add one read per new
segment. Each node that reads the cell pays this. The replica's file is a
full copy of the database; `max_bytes` bounds the sum of the files on a node and
a replica that would not fit is not built. The first replica of a cell costs a
full restore.

## Troubleshooting

- **Every read goes to the owner.** Check the telemetry `reason`. `:off`: the
  instance has no `:replicas` option. `:no_replica` for long: the switch is off
  (`Alambique.replicas/2`), the node is at `max_cells` with every replica read
  recently, the replica would pass `max_bytes`, or its build failed (look for
  `[:alambique, :replica, :build]` with `result: :error` and for
  `[:alambique, :replica, :stop]`). The node does not retry a refused cell for
  `marker_ttl_ms`. `:owner_here`: the cell is awake on this node.
- **`:stale` or `:timeout` often.** The store takes longer than the bound to
  answer a listing. Raise the bound or lower `poll_ms`; look at
  `[:alambique, :replica, :refresh]` durations.
- **A replica rebuilds repeatedly.** `[:alambique, :replica, :refresh]` with
  `result: :rebuild` says why: a snapshot newer than the replica or a deleted
  file means retention or compaction removed files the replica had not reached
  (the replica was behind for longer than `deletion_grace_ms`).
- **Writes through a replica.** There is no way: statements that would write
  give `{:error, :read_only}`, and a replica never writes to the store.

## Next steps

- [design.md](design.md), section 21: the guarantee, what it does not promise,
  and the limits.
- [telemetry.md](telemetry.md): the replica events.
- [how-to-query-across-cells.md](how-to-query-across-cells.md): reports over
  many cells, which use a separate cache and always ask the store.
