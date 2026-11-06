# How to query across cells

**The question:** how do I run read-only SQL over many cells (every cell with
its row counts, one organization's data) without waking them or disturbing
their owners?

This guide stands alone. The design is in [design.md](design.md), section 14.
For analytics over all organizations use the change feed instead, see
[how-to-consume-the-change-feed.md](how-to-consume-the-change-feed.md). The
output below comes from running these commands against
`AlambiqueTestkit.MemoryStore`; against S3 the shapes are the same.

## Before you start

- An instance of Alambique running on the node that queries (see the
  [README](../README.md)). The node needs no cell of its own: it only reads the
  store. The `:migrations` of the instance are not used by queries.
- Disk under the instance's `:data_dir` for the replica cache
  (`<data_dir>/replicas`, 1 GiB by default).

In the examples cells `org-1` to `org-3` and `team-1` hold a `notes` table, and
`org-1` is asleep.

## 1. List the cells

```elixir
Enum.to_list(Alambique.Query.cells(MyApp.Cells, prefix: "org-"))
```

```
["org-1", "org-2", "org-3"]
```

Omit `prefix:` to scan every cell. This is an explicit scan of the store, so
with many cells prefer passing the ids you need. The listing is lazy and reads the store in pages
of 1 000 keys (`page_size:`, instance option `query: [page_size:]`), skipping
  the rest of each cell it has seen.

## 2. Run a statement on them

```elixir
Alambique.Query.all(MyApp.Cells, {:prefix, "org-"}, "SELECT count(*) AS notes FROM notes",
  [], ordered: true)
```

```
%{
  rows: [
    %Alambique.Query.Row{cell: "org-1", txid: 5, columns: ["notes"], values: [3]},
    %Alambique.Query.Row{cell: "org-2", txid: 3, columns: ["notes"], values: [1]},
    %Alambique.Query.Row{cell: "org-3", txid: 7, columns: ["notes"], values: [5]}
  ],
  errors: []
}
```

The first argument after the instance is `:all`, `{:prefix, p}` or your own
list or stream of cell ids. Each row says which cell it came from and at which
TXID (the state of the cell as of its last batch in the store). No cell was
woken and no lease was taken.

For a large result, use `Alambique.Query.stream/5`, which returns the rows as
cells finish, and `{:error, cell, reason}` for each cell that failed:

```elixir
MyApp.Cells
|> Alambique.Query.stream(:all, "SELECT count(*) FROM notes")
|> Enum.each(&IO.inspect/1)
```

## 3. Read the errors

A cell that fails does not stop the others. A cell that has no such table, one
that times out, a store error or a statement that writes show up in `errors`:

```elixir
Alambique.Query.all(MyApp.Cells, ["org-1", "nope", "org-2"], "DELETE FROM notes")
```

```
%{rows: [],
  errors: [
    {"nope", "no such table: notes"},
    {"org-1", :read_only},
    {"org-2", :read_only}
  ]}
```

Queries cannot write: `INSERT`, `DELETE`, DDL, `ATTACH`, `PRAGMA x = y` and the
pragmas that checkpoint or leave WAL mode give `:read_only`. Make changes through `Alambique.execute/5`.

## 4. Choose how fresh the data must be

By default every query asks the store for each cell's newest segments and
applies only the new ones. A write that was acknowledged before the query
started is always in the result. Two options change that:

- `max_age_ms: 60_000` uses a cached replica last used less than a minute ago
  without asking the store. Good for dashboards that are called often.
- `at: {:txid, n}` or `at: {:timestamp, unix_ms}` reads a past state. A cached
  replica at exactly that TXID is reused; a timestamp always restores.

After a write to `org-2`, a refresh of two cells cost one `get` and four
`list` calls (two listings per cell):

```
requests: [get: 1, list: 4]
```

## 5. Tune the limits

Per instance, in `Alambique` options:

```elixir
query: [cache_bytes: 2_147_483_648, concurrency: 16, timeout_ms: 30_000,
        page_size: 1_000, stale_tmp_seconds: 600, restore_attempts: 3]
```

Per call: `location:` (an `Alambique.Location`, its id, or `{store, prefix}`, or
a list of them for scans, instead of the locations of the placement), `concurrency:`, `timeout_ms:`, `ordered:` (rows in input order, default
completion order).

`timeout_ms` covers building the replica and running the statement; a statement
that runs past it is interrupted and the cell reports `:timeout`.

## Troubleshooting

- **`{:gap, txid}`, `:history_truncated` or `:epoch_regression` for a cell**:
  its files in the store are damaged or were partly deleted. The same error stops
  the cell from waking. See [how-to-operate-a-cell.md](how-to-operate-a-cell.md).
- **`{:error, :evicted}`**: the cache bound is smaller than what one run needs,
  and a replica was removed twice before it could be opened. Raise `cache_bytes`
  or lower `concurrency`.
- **Slow cold runs**: the first query of a cell restores it from the store.
  Later queries apply only new segments. Telemetry `[:alambique, :query, :replica]`
  shows the `source` of each ([telemetry.md](telemetry.md)).
- **Data looks old**: check `max_age_ms`, and that the row's `txid` is the
  newest in the store (`mix alambique.inspect`).
- **Very many organizations**: a query reads each cell. For cross-organization
  analytics follow the change feed into an analytics store.
