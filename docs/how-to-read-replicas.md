# How to read a hot cell from other nodes

**The question:** how do I serve the reads of a cell from the nodes that do not
own it, accepting a bounded lag, so the owner does not answer every read?

This guide stands alone. The guarantee and its limits are in
[design.md](design.md), section 21. Option defaults are in the documentation of
the `Alambique` module, under `:replicas`.

## Prerequisites

- An instance of Alambique on each node that reads (see the
  [README](../README.md)). A node that owns the cell never keeps a replica for
  it: it answers from the cell.
- Disk under the instance's `:data_dir` for the replicas
  (`<data_dir>/read-replicas`, at most 1 GiB per node by default).
- A bound you can accept. A read with `consistency: {:bounded, 500}` contains
  every write whose caller was told it was done more than 500 ms before the
  read started. It may miss newer writes. Two reads by one caller, one answered
  by the owner and one by a replica, can see time go back by up to the bound.

## 1. Turn replicas on in the instance

Replicas are off unless the instance has the `:replicas` option:

```elixir
{Alambique,
 name: MyApp.Cells,
 store: {Alambique.Store.S3, s3_config},
 prefix: "myapp",
 data_dir: "/var/lib/myapp/cells",
 replicas: [max_cells: 32, poll_ms: 250, poll_max_ms: 5_000]}
```

`replicas: []` takes every default. Without the option, a bounded read goes to
the owner and never looks at the switch of step 2.

Expected result: the instance starts. `<data_dir>/read-replicas` exists once a
replica has been built.

## 2. Turn the switch on for a cell

A replica costs store requests and disk on every node that reads the cell, so
choose the cells. The switch is an object in the store:

```elixir
Alambique.replicas(MyApp.Cells, "org-1")
# {:ok, false}
Alambique.replicas(MyApp.Cells, "org-1", true)
# :ok
Alambique.replicas(MyApp.Cells, "org-1")
# {:ok, true}
```

Setting the switch does not wake the cell. Nodes see the switch within
`marker_ttl_ms` (5 seconds by default).

## 3. Read with a bound

```elixir
Alambique.query(MyApp.Cells, "org-1", "SELECT body FROM notes", [],
  consistency: {:bounded, 500})
```

The first read on a node has no replica. The owner answers it, and the node
starts building a replica in the background:

```
telemetry: [:alambique, :replica, :read] %{} %{source: :owner, reason: :no_replica, cell: "org-1"}
telemetry: [:alambique, :replica, :build] %{} %{result: :ok, cell: "org-1", txid: 3}
```

Later reads are served by the replica:

```
{:ok, %{columns: ["body"], rows: [["hi"]]}}
telemetry: [:alambique, :replica, :read] %{lag_ms: 82} %{source: :replica, cell: "org-1", txid: 3}
```

`lag_ms` is how long ago the replica last confirmed that it held everything in
the store. It is never above the bound. A write on the owner shows up on the
replica at its next poll (`poll_ms`, 250 ms by default, for a replica that is
being read).

A node that owns the cell answers `consistency: {:bounded, _}` from the cell
itself. `consistency: :strong` is the default and never uses replicas.
`execute/5` and `transaction/4` refuse the option with `{:error, :bad_option}`,
and so does `query/5` when `{:bounded, _}` is combined with `verify_lease: true`.

## 4. Choose what happens to a read that is too old

When the replica last confirmed more than the bound ago (a slow store, or a bound
shorter than `poll_ms`), the `:replicas` option `on_stale` decides:

- `:refresh` (default): the read waits while the replica refreshes. The replica
  answers once a refresh that started recently enough finishes. If none finishes
  within `refresh_timeout_ms` (2 seconds by default), the owner answers.
- `:owner`: the owner answers at once. Use it when waiting is worse than the
  extra hop.

The telemetry event `[:alambique, :replica, :read]` says who answered (`source`)
and, for the owner, why (`reason`): `:stale`, `:timeout`, `:building`,
`:no_replica`, `:owner_here`, `:off` or `:placement`.

## 5. Turn it off

```elixir
Alambique.replicas(MyApp.Cells, "org-1", false)
# :ok
```

Each node reads the switch again every `marker_ttl_ms`. When the switch is off,
the replica stops and removes its file within `marker_ttl_ms` plus one poll
(`poll_max_ms` when nobody reads it):

```
telemetry: [:alambique, :replica, :stop] %{} %{reason: :marker_off, cell: "org-1"}
File.ls(".../read-replicas")
# {:ok, []}
```

A replica that nobody reads for `idle_ms` (10 minutes by default) stops by
itself. Moving a cell to another location with `Alambique.relocate/4` stops its
replicas and does not carry the switch: turn it on again for the new location.

## What it costs

A replica polls the store every `poll_ms` while it is read, and less often when
nobody reads it: the interval grows to a quarter of the idle time, up to
`poll_max_ms`. A poll is a LIST of the level-0 files after the head, a LIST of
the snapshots and a GET of the lease. New segments are read as they appear.
Every node that reads the cell pays this. A replica file is a full copy of the
database, and `max_bytes` bounds the sum of the files on one node: a replica that
would not fit is not built. The first replica of a cell costs a full restore.

## Troubleshooting

- **Every read goes to the owner.** Read the `reason` of the telemetry event:
  - `:off`: the instance has no `:replicas` option.
  - `:no_replica` for a long time: the switch is off (`Alambique.replicas/2`),
    the node is at `max_cells` with every replica read recently, the replica
    would exceed `max_bytes`, or its build failed. Look for
    `[:alambique, :replica, :build]` with `result: :error` and for
    `[:alambique, :replica, :stop]`. After a refusal the node does not try
    again for `marker_ttl_ms`.
  - `:owner_here`: the cell is awake on this node.
- **`:stale` or `:timeout` often.** The store takes longer than the bound to
  answer. Raise the bound or lower `poll_ms`, and look at the durations of
  `[:alambique, :replica, :refresh]`.
- **A replica rebuilds again and again.** `[:alambique, :replica, :refresh]` with
  `result: :rebuild` gives a `reason`. `:snapshot` means a snapshot newer than the
  replica exists. `:vanished` means a file the replica still needed was deleted;
  this can happen when the replica falls behind for longer than
  `deletion_grace_ms`. `:gap` and `:segment` mean the chain of files does not
  connect.
- **Writes through a replica.** Not possible. Writes always go to the owner, so use
  `Alambique.execute/5` or `Alambique.transaction/4`, and a replica never writes
  to the store.

## Next steps

- [design.md](design.md), section 21: the guarantee, what it does not promise
  and its limits.
- [telemetry.md](telemetry.md): the replica events.
- [how-to-query-across-cells.md](how-to-query-across-cells.md): reads over many
  cells. Those use a separate cache and always check the store.
