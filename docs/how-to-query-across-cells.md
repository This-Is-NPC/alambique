# How to query across cells

**The question:** how do I run read-only SQL over many cells (every cell with its
row counts, or one organization's data) without waking them or disturbing their
owners?

This guide stands alone. The design is in [design.md](design.md), section 14.
For analytics over all organizations, follow the change feed instead:
[how-to-consume-the-change-feed.md](how-to-consume-the-change-feed.md).

## Prerequisites

- An instance of Alambique running on the node that queries (see the
  [README](../README.md)). The node needs no cell of its own: it only reads the
  store. The `:migrations` of the instance are not used by queries.
- Disk under the instance's `:data_dir` for the replica cache
  (`<data_dir>/replicas`, 1 GiB by default).

The examples use cells `org-1` to `org-3` and `team-1`, which hold a `notes`
table. `org-1` is asleep. The output shown is an example of the result shape;
the values depend on your cells.

## 1. List the cells

```elixir
Enum.to_list(Alambique.Query.cells(MyApp.Cells, prefix: "org-"))
```

```
["org-1", "org-2", "org-3"]
```

Omit `prefix:` to scan every cell. A scan reads the store, so with many cells,
pass the ids you need instead. The listing is lazy. It reads the store in pages of
1 000 keys (`page_size:`, or the instance option `query: [page_size:]`) and skips
the rest of each cell it has already seen.

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

The first argument after the instance is `:all`, `{:prefix, p}`, or your own list
or stream of cell ids. Each row says which cell it came from, and the TXID it
reflects: the state of the cell as of its last batch in the store. The query
wakes no cell and takes no lease.

For a large result, use `Alambique.Query.stream/5`. It returns the rows as cells
finish, and `{:error, cell, reason}` for each cell that failed:

```elixir
MyApp.Cells
|> Alambique.Query.stream(:all, "SELECT count(*) FROM notes")
|> Enum.each(&IO.inspect/1)
```

## 3. Read the errors

A failing cell does not stop the others. A cell that has no such table, a cell
that times out, a store error or a statement that writes is reported in `errors`:

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

Queries cannot write. `INSERT`, `DELETE`, DDL, `ATTACH`, `PRAGMA x = y`, and the
pragmas that checkpoint or leave WAL mode return `:read_only`. Make changes with
`Alambique.execute/5`.

## 4. Choose how fresh the data must be

By default every query asks the store for each cell's newest segments, and
applies only the new ones. A write that was acknowledged before the query started
is always in the result. Two options trade freshness for fewer store requests:

- `max_age_ms: 60_000` uses a cached replica that was last used less than a minute
  ago, without asking the store. Use it for dashboards that are called often.
- `at: {:txid, n}` or `at: {:timestamp, unix_ms}` reads a past state. A cached
  replica is reused only when it is at exactly that TXID. A timestamp always
  restores the cell from the store.

## 5. Tune the limits

Per instance, in the `Alambique` options:

```elixir
query: [cache_bytes: 2_147_483_648, concurrency: 16, max_statement_share: 0.5,
        timeout_ms: 30_000, page_size: 1_000, stale_tmp_seconds: 600,
        restore_attempts: 3, reflink: true]
```

The defaults are: `cache_bytes` 1 GiB, `concurrency` 16, `max_statement_share` 0.5,
`timeout_ms` 30 000, `page_size` 1 000, `stale_tmp_seconds` 600,
`restore_attempts` 3 and `reflink` true.

Per call: `location:` (an `Alambique.Location`, its id, `{store, prefix}`, or a list
of them for scans, instead of the locations of the placement), `concurrency:`,
`max_statement_share:`, `timeout_ms:`, and `ordered:` (rows in input order; the
default is completion order).

`max_statement_share:` is the share of the statement slots
(`sql: [max_concurrent_statements:]`) that one query may hold at once. It is above
0 and at most 1. With the default 0.5, a query runs at most half of the slots at
once, so two queries fit together and slots remain for cells. With 1, a query may
take every slot, and a second query that arrives meanwhile gets `:busy`. This
limit covers statements only. It does not limit the cells waiting for the store;
that is `concurrency:`. See [cost-of-a-cell.md](cost-of-a-cell.md).

`timeout_ms` covers building the replica and running the statement. A statement
that runs past it is interrupted, and the cell reports `:timeout`.

## Troubleshooting

- **`{:gap, txid}`, `:history_truncated` or `:epoch_regression` for a cell**: its
  files in the store are damaged or were partly deleted. The same error stops the
  cell from waking. See [how-to-operate-a-cell.md](how-to-operate-a-cell.md).
- **`{:error, :evicted}`**: the cache bound is smaller than what one run needs, so
  a replica was removed twice before it could be opened. Raise `cache_bytes` or
  lower `concurrency`.
- **Slow cold runs**: the first query of a cell restores it from the store. Later
  queries apply only the new segments to a clone of the cached file. On a
  filesystem with reflinks (for example btrfs or xfs) the clone is cheap. On one
  without (for example ext4 or tmpfs) the file is copied and flushed each time. Keep
  `<data_dir>/replicas` on a reflink filesystem, or use `max_age_ms`. Set
  `query: [reflink: false]` to always copy. The telemetry event
  `[:alambique, :query, :replica]` gives the `source` of each read
  ([telemetry.md](telemetry.md)).
- **Data looks old**: check `max_age_ms`, and check that the row's `txid` is the
  newest in the store (`mix alambique.inspect`).
- **Very many organizations**: a query reads each cell. For analytics across
  organizations, follow the change feed into an analytics store.
