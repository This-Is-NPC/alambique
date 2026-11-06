# How to call cells from your application

**The question:** how do I start an instance and use the functions of the
`Alambique` module?

This guide stands alone. The options, the migrations and the errors are in the
documentation of `Alambique`; the words are in [concepts.md](concepts.md).

## Prerequisites

- An S3-compatible store with conditional writes and its credentials
  ([storage-compatibility.md](storage-compatibility.md)).
- A local directory for `:data_dir`, used by one instance only.

## 1. Start one instance per node

Use the same name, store and prefix on every node of the cluster. The store and
prefix are the shorthand for a single location; step 5 shows a placement.

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
extra_args}`, called with the cell id (and the extra arguments) at every wake
to choose that cell's migrations.

## 2. Read and write

Call any cell from any node. By default a cell id is a string of
`[A-Za-z0-9_.-]`, at most 128 bytes, but not `.` or `..`; a placement can
define its own rule.

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

## 3. Find and list cells

```elixir
{:ok, node} = Alambique.owner(MyApp.Cells, "org-1")
{:ok, node} = Alambique.owner(MyApp.Cells, "org-1", hint: true)  # cached owner, no store read
{:ok, %{epoch: epoch}} = Alambique.lease(MyApp.Cells, "org-1")   # without waking
{:ok, node} = Alambique.touch(MyApp.Cells, "org-1")              # wake it and say where
%{cells: ids, next: next, count: n} = Alambique.cells(MyApp.Cells, limit: 100)
{:ok, %{cells: ids, next: next}} = Alambique.list_cells(MyApp.Cells, prefix: "org-", limit: 100)
```

`Alambique.cells/2` lists the cells awake on the local node in id order.
`Alambique.list_cells/2` lists every cell stored in a location, asleep ones
too, from the schema index; pass `next` as `after:` for the next page.

## 4. Move, rewind and stop cells

```elixir
:ok = Alambique.hibernate(MyApp.Cells, "org-1")
{:ok, _node} = Alambique.move(MyApp.Cells, "org-1", :"other@host")
{:ok, node} = Alambique.take_over(MyApp.Cells, "org-1", :owner_down)  # before the lease expires
{:ok, %{txid: before}} = Alambique.position(MyApp.Cells, "org-1")    # a cut: earlier writes at or below it
{:ok, _} = Alambique.rewind(MyApp.Cells, "org-1", {:txid, before})   # later: back to that state, atomically
{:ok, _} = Alambique.query(MyApp.Cells, "org-1", "SELECT 1", [], verify_lease: true)
{:ok, version} = Alambique.migrate(MyApp.Cells, "org-1")              # an awake cell, in place
{:ok, %{hibernated: _, errors: _}} = Alambique.drain(MyApp.Cells)     # before a node stops
:ok = Alambique.join(MyApp.Cells)                                      # after join: false, or a drain
```

`drain/2` hands the cells of a node over to another node that started beside
it: [how-to-swap-a-node.md](how-to-swap-a-node.md).

`take_over/4` fences the current owner at once, when a failure detector
suspects it is gone, and keeps every acknowledged write even when the
suspicion is wrong. `Alambique.freeze/3` keeps a cell from waking. See
[how-to-rewind-a-cell.md](how-to-rewind-a-cell.md) for rewinds and
[how-to-operate-a-cell.md](how-to-operate-a-cell.md) for the `mix` tasks.

## 5. Spread cells over locations

The library knows no regions, organizations or tenants. A placement module you
supply says where a cell's data lives and how cell ids look:

```elixir
{Alambique,
 name: MyApp.Cells,
 placement: {MyApp.Placement, regions: ...},   # implements Alambique.Placement
 data_dir: "/var/lib/myapp/cells"}

{:ok, %Alambique.Location{id: "us"}} = Alambique.relocate(MyApp.Cells, "org-1", "us")
```

`relocate/4` moves the cell with a fence, so only one location accepts its
writes; the old location's objects are deleted after a grace period and the
cell can move back. See [design.md](design.md), section 18.

## 6. Read other things

- Row changes of all cells: [how-to-consume-the-change-feed.md](how-to-consume-the-change-feed.md).
- Read-only SQL over many cells: [how-to-query-across-cells.md](how-to-query-across-cells.md).
- Reads of a hot cell on other nodes: [how-to-read-replicas.md](how-to-read-replicas.md).
- Large cells restore lazily: a cell answers its first query before its
  database is downloaded ([how-to-speed-up-a-wake.md](how-to-speed-up-a-wake.md)).
