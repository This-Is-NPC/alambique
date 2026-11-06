# How to consume the change feed

**The question:** how do I read the row changes of all cells, in order and
without loss, and continue where I stopped after a restart?

This guide stands alone. The design is in [design.md](design.md), section 13.
The output below was produced by running the commands against
`AlambiqueTestkit.MemoryStore` with the default settings, so the timings are
those of a real run; against S3 the object keys and records have the same
shape.

## Before you start

- An instance of Alambique running (see the [README](../README.md)). Every
  write you make through `Alambique.execute/5` or `Alambique.transaction/4`
  is published, and so are the rows that migrations write.
- To use Broadway (step 5): `{:broadway, "~> 1.1"}` in your own `mix.exs`.
  Alambique lists it as an optional dependency, so you must add it yourself.

## 1. Build a feed context

```elixir
ctx = Alambique.Feed.ctx(MyApp.Cells)
```

The context carries the locations of the instance's placement, the number of
feed shards and the clock, and `settle_ms`: a read ignores objects younger than this, so that an object still
being written can never appear behind your position. With the default
settings it is 24 000 ms (10 s publication window, 10 s store operation, 2 s
skew on each side):

```
settle_ms: 24000
```

From a process that does not run the instance, build it from the store
(`Alambique.Feed.ctx(store: {Alambique.Store.S3, s3_config}, prefix: "myapp")`)
or from a list of `Alambique.Location` (`locations: [...]`), pass the same
`shards:` as the instance (default 16), and keep `settle_ms` at least that high.
The locations are those of the moment: build the context again when the
placement gains one.

## 2. Read from a position

Write some rows, then read:

```elixir
{:ok, _} = Alambique.execute(MyApp.Cells, "org-1", "INSERT INTO notes (body) VALUES (?)", ["hi"])
{:ok, _} = Alambique.execute(MyApp.Cells, "org-2", "INSERT INTO notes (body) VALUES (?)", ["hello"])
{:ok, _} = Alambique.execute(MyApp.Cells, "org-1", "UPDATE notes SET body = ? WHERE id = 1", ["hi there"])

Alambique.Feed.read(ctx, nil, 100)
```

Right after the writes nothing has settled yet:

```
{:ok, [], nil}
```

After `settle_ms` the same call returns the records and the position to
continue from:

```elixir
{:ok, records, position} = Alambique.Feed.read(ctx, nil, 100)
```

```
%Alambique.Feed.Record{
  cell: "org-1", table: "notes", op: :insert, key: %{"id" => 1},
  values: %{"body" => "hi", "id" => 1}, position: {3, 0}, id: "org-1:3:0", ...
}
%Alambique.Feed.Record{
  cell: "org-2", table: "notes", op: :insert, key: %{"id" => 1},
  values: %{"body" => "hello", "id" => 1}, position: {3, 0}, id: "org-2:3:0", ...
}
%Alambique.Feed.Record{
  cell: "org-1", table: "notes", op: :update, key: %{"id" => 1},
  values: %{"body" => "hi there", "id" => 1}, position: {4, 0}, id: "org-1:4:0", ...
}
position: {"default/0":"myapp/feed/000/1792997323658-0000000000000004-org-1","default/4":"myapp/feed/004/1792997323658-0000000000000003-org-2"}
```

`100` is the most feed objects one call reads; it returns all records of
them. An object holds one batch, or several when the instance sets
`feed: [object_batches:]` above 1 (step 10). Each location spreads its feed over 16 shards by a hash of the
cell id; a read lists every shard of every location once and merges them by
time, so it costs 16 listings per location however many cells exist. The
position is opaque (today a JSON text): it holds the key of the last object read
in each shard. Store it as it is. Reading again from it returns nothing new:

```
{:ok, [], position}   # the same position
```

## 3. Store the position after you processed the records

Process the records first, then save `position` where your process can find it
after a restart (a database row, a file). Use it as the second argument of the
next call. If you crash between processing and saving, the next run reads the
same records again: delivery is at least once.

## 4. Deduplicate by idempotency key

`record.id` (`"<cell>:<txid>:<n>"`) identifies a change. The same change can
arrive twice after an owner change or a lost acknowledgement; duplicates have
identical content. Make the write to your destination idempotent on `id`, for
example an upsert keyed by it. Per cell, the first time you see each `id` is in
commit order.

## 5. Use Broadway

`Alambique.Feed.Producer` polls the feed, hands each record to the pipeline as
a message, and calls `store_position` with the new position once every message
up to it was acknowledged:

```elixir
Broadway.start_link(MyApp.FeedPipeline,
  name: MyApp.FeedPipeline,
  producer: [
    module:
      {Alambique.Feed.Producer,
       ctx: Alambique.Feed.ctx(MyApp.Cells),
       position: MyApp.Positions.load(),
       store_position: &MyApp.Positions.save/1,
       poll_interval_ms: 1_000},
    concurrency: 1
  ],
  processors: [default: [concurrency: 4]]
)
```

Run one producer per feed (`concurrency: 1`). With a `handle_message/3` that
prints each record, a run against the two writes above prints:

```
org-1:3:0 insert notes %{"body" => "hi", "id" => 1}
org-2:3:0 insert notes %{"body" => "hello", "id" => 1}
saved position {"default/0":"myapp/feed/000/1792997323658-0000000000000004-org-1","default/4":"myapp/feed/004/1792997323658-0000000000000003-org-2"}
```

Restart the pipeline with the saved position and it continues after it.

## 6. Limit how long objects are kept

Every node deletes feed objects older than `retention_ms`, once an hour by
default. The default is seven days. Set another time, or `nil` to keep them for
ever, in the instance options:

```elixir
{Alambique, name: MyApp.Cells, ..., feed: [retention_ms: 30 * 24 * 3_600_000]}
```

A consumer that was away longer than the retention continues from the oldest
object left and has lost the changes in between. Compaction and segment
retention do not touch the feed. A service that reads no feed can turn it off
with `feed: false` and store much less
([how-to-size-storage.md](how-to-size-storage.md)).

## 7. Keep your own tables out of the feed

Tables that hold your application's bookkeeping, such as a job queue or
alarms, are not data to analyze. Name them with a common prefix and list it in
the instance options:

```elixir
{Alambique, name: MyApp.Cells, ..., feed: [internal_prefixes: ["_myapp_"]]}
```

A table whose name starts with one of the prefixes gets no triggers, so its
changes never reach the feed. The tables of the library (`_alambique_`) and of
SQLite (`sqlite_`) are always internal. The option is a list of non-empty
strings (default `[]`); the match is a case-sensitive prefix of the table
name. It only concerns the feed: `Alambique.query/5`, `Alambique.Query`,
migrations and the deploy check treat these tables like any other.

- **Changing the value on a cell that exists.** The triggers are generated
  again the next time the cell wakes (or changes its schema in a transaction):
  tables that match a new prefix lose their triggers, and tables that no longer
  match get them. Changes made before that are not published retroactively and
  nothing already published is removed.
- **Rolling deploy.** Give every node the same value before the cells move
  between them. A node with another value regenerates the triggers of each
  cell it wakes, so the cell is captured by the rules of the node that woke it
  last. Between wakes a cell keeps the triggers of its last waker, whichever
  node serves it. Roll the new value out like a schema change: all nodes, then
  trust the feed.

### Leave out columns and whole tables

The feed copies every column of a captured row, twice: into the cell's file
and into the feed object. A column you never read downstream, such as a mail
body or a file, can stay out of both:

```elixir
{Alambique,
 name: MyApp.Cells,
 ...,
 feed: [
   tables: %{
     "messages" => [omit: ["body"]],   # every column but these
     "audit" => [only: ["kind", "at"]], # the key and these columns
     "attachments" => false             # no record at all
   }
 ]}
```

- `true` (the default for every table) captures the table whole, `false`
  not at all, `[omit: columns]` every column but those and `[only: columns]`
  its key and those columns. `other_tables: :skip` (default `:capture`) turns
  the list into an allow list: only the tables with `true`, `omit` or `only`
  are captured.
- **What the consumer sees.** A column that is left out is **absent** from
  `values`; there is no marker and no `nil`. `key` is unchanged and its
  columns are always in `values`, whatever the selection says. A consumer that
  keeps a copy must not read a missing column as `NULL`, and must merge an
  `update` into the row it holds by `key`. The records of a table in `only`
  have just the key and the listed columns.
- **Nothing is written for them.** The triggers never read an omitted column,
  so it is not in the `_alambique_feed` rows, in the segments of the cell or in
  the feed objects. Rows captured before the change keep their columns until
  they are published and removed.
- **Updates.** An `UPDATE` that changes only omitted columns leaves no
  record, because it would repeat the last one. (A table captured whole still
  records every `UPDATE`, also one that changes nothing.)
- A name that the table does not have is ignored, so a column added later is
  left out from its creation.
- **Changing the selection** on a cell that exists regenerates its triggers at
  the next wake, as for `internal_prefixes`; records already published stay.
  Give every node the same value. A node of the release before this one does
  not know the option: it captures every column of every table of a cell it
  wakes, so a cell that moves back to such a node publishes whole rows.

## 8. Handle a rewind

A cell can be rewound to an earlier state (`Alambique.rewind/4`,
[how-to-rewind-a-cell.md](how-to-rewind-a-cell.md)). The feed then holds an
`Alambique.Feed.Rewind` among the records, after the records it undoes:

```
%Alambique.Feed.Rewind{cell: "org-1", from_txid: 6, to_txid: 3, txid: 7,
  target: {:txid, 3}, kept: ["_myapp_jobs"], id: "org-1:7:rewind", ...}
```

After it the cell is in the state it had when you had applied the records up
to `to_txid`. Put your copy of that cell back: revert the records with a
`position` TXID above `to_txid` (they stay on the feed, before the marker), or
rebuild the cell from another source. Then carry on; records after the marker
describe changes on top of the rewound state. Records at or below `to_txid`
can arrive again after a rewind with the ids they had: you deduplicate by `id`
anyway. A pipeline that matches only `%Alambique.Feed.Record{}` fails on the
marker, on purpose.

## 9. Handle a rejected object

An object that cannot be decoded (not JSON, an unknown `op`, a wrong shape,
which only a faulty or hostile writer of the store produces) does not stop the
feed. `read` returns one `Alambique.Feed.Rejected` in its place and the
position moves past it:

```
%Alambique.Feed.Rejected{key: "myapp/feed/004/1792997323658-0000000000000003-org-2",
  reason: :invalid_json, id: "rejected:myapp/feed/004/...", at: 1792997323658}
```

Log it and carry on; a `[:alambique, :feed, :read, :rejected]` event is
emitted too. A pipeline that matches only `%Alambique.Feed.Record{}` fails on
it, as it does on the rewind marker. Text values that are not valid UTF-8 are
not rejected: they arrive with each bad byte replaced by U+FFFD.

## 10. Read objects with several batches

By default an object holds one batch. A publisher with `feed: [object_batches:
16, linger_ms: 50]` (default 1 and 0) puts up to 16 queued batches in one
object, which takes fewer store requests per job. `Alambique.Feed.read/3`
returns the same records, with the same `position` and `id`, so a consumer
that only uses `read/3` or the Broadway producer needs no change. After a crash, a reader may meet the same record
in two objects: deduplicate by `record.id` (step 4). A `limit` still counts
objects, so a call can return more records than `limit`. A record becomes visible
up to `linger_ms` later; keep it well under `:window_ms`. A cell that
hibernates or shuts down does not make its records wait: it puts what it holds
first, for up to `feed: [stop_publish_ms:]` (default 5 000, 0 turns it off).
When it cannot (a lost lease, a store that fails, a crash), the records
appear when the next owner wakes the cell.

## 11. Decode the objects yourself

`Alambique.Feed.read/3` and the Broadway producer decode the objects. A reader
that does it itself reads this format: `"ALF"`, the byte `1`, then a zlib
stream of the JSON text of the object. It rejects any other body, as the
library does. Cells keep their rows in the cell's file until the node publishes
them, so a node that takes a cell over publishes what the other node left,
once.

Design section 13, "Object format", has the details.

## Troubleshooting

- **`read` returns `{:ok, [], position}` for a long time.** Objects younger
  than `settle_ms` are hidden on purpose. Lower `:window_ms` and
  `:op_timeout_ms` in the instance, or check that your `ctx` has the instance's
  `settle_ms`. Check `[:alambique, :feed, :publish]` events for
  `result: :error`: a publication that cannot reach the store is retried in the
  background, after `publish_retry_ms` and then at growing intervals up to
  `publish_retry_max_ms`. Commits are not affected, and the rows wait in the
  cell's file.
- **A record appears twice.** Expected after an owner change or a lost
  acknowledgement. Deduplicate by `record.id`.
- **A cell shows gaps in its TXIDs.** Fences, migrations and transactions
  that change no row use TXIDs without producing an object. Nothing is
  missing; see design section 13.
- **My job table appears on the feed.** List its prefix in
  `feed: [internal_prefixes: ...]` (step 7) and wait for the cell's next wake.
- **A migration floods the feed.** Rows that a migration writes, for example a
  backfill that updates every row, are published like any other change. Schema
  changes themselves are not.
- **Broadway stops saving the position.** A message that failed keeps the
  position behind its object until the producer restarts; fix the cause and
  restart the pipeline, which delivers the object again.

## Next steps

- [telemetry.md](telemetry.md) lists the `[:alambique, :feed, ...]` events.
- [design.md](design.md), section 13, defines the key layout and the ordering
  rules.
