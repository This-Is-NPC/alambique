# How to consume the change feed

**The question:** how do I read the row changes of all cells, in order and
without loss, and continue where I stopped after a restart?

This guide stands alone. The design is in [design.md](design.md), section 13.
The examples use the default settings. Object keys, timestamps and positions
in the output are shortened or replaced by placeholders.

## Prerequisites

- An instance of Alambique running (see the [README](../README.md)). Every
  write made through `Alambique.execute/5` or `Alambique.transaction/4` is
  published, and so are the rows that migrations write.
- For step 5 only: `{:broadway, "~> 1.1"}` in your own `mix.exs`. Alambique
  lists Broadway as an optional dependency, so your application must add it.

## 1. Build a feed context

```elixir
ctx = Alambique.Feed.ctx(MyApp.Cells)
```

The context holds the locations of the instance, the number of feed shards
(default 16), the clock and `settle_ms`. A read ignores objects younger than
`settle_ms`, so an object that is still being written never appears behind
your position. With the default settings `settle_ms` is 24 000 ms: a 10 s
publication window, a 10 s store operation and 2 s of clock skew on each side.

```
settle_ms: 24000
```

From a process that does not run the instance, build the context from the
store, `Alambique.Feed.ctx(store: {Alambique.Store.S3, s3_config}, prefix: "myapp")`,
or from a list of `Alambique.Location` structs (`locations: [...]`). Then pass
the same `shards:` as the instance and keep `settle_ms` at least as high as the
instance's. A context built from a store uses 30 000 ms by default. Build the
context again when the placement gains a location. Each store request of the
consumer is stopped after `timeout_ms` (10 000 ms by default) and returns
`{:error, :timeout}`.

## 2. Read from a position

Write some rows, then read:

```elixir
{:ok, _} = Alambique.execute(MyApp.Cells, "org-1", "INSERT INTO notes (body) VALUES (?)", ["hi"])
{:ok, _} = Alambique.execute(MyApp.Cells, "org-2", "INSERT INTO notes (body) VALUES (?)", ["hello"])
{:ok, _} = Alambique.execute(MyApp.Cells, "org-1", "UPDATE notes SET body = ? WHERE id = 1", ["hi there"])

Alambique.Feed.read(ctx, nil, 100)
```

Right after the writes nothing has settled yet, so the result is empty and the
position is `nil`:

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
position: <opaque position>
```

The second argument of `read/3` is the position of the previous call; `100` is
the most feed objects one call reads, and the call returns all records of
those objects. An object holds one batch, or several when the instance sets
`feed: [object_batches:]` above 1 (step 10).

Each location spreads its feed over the shards by a hash of the cell id. A read
lists every shard of every location once, so it costs one listing per shard per
location however many cells exist.

The position is opaque. Store it as it is. It holds the key of the last object
read in each shard. Reading again from it returns nothing new:

```
{:ok, [], position}   # the same position as before
```

## 3. Store the position after you process the records

Process the records first, then save `position` where your process finds it
after a restart: a database row or a file. Pass it as the second argument of
the next call. If you crash between processing and saving, the next run reads
the same records again. Delivery is therefore at least once.

## 4. Deduplicate by idempotency key

`record.id` (`"<cell>:<txid>:<n>"`) identifies a change. The same change can
arrive twice after an owner change or a lost acknowledgement, and both copies
have the same content. Make the write to your destination idempotent on `id`,
for example with an upsert keyed by it. The records of one cell arrive in
commit order, also after the cell moves to another location. Across cells they
arrive in publication time order.

## 5. Use Broadway

`Alambique.Feed.Producer` polls the feed, gives each record to the pipeline as
a message, and calls `store_position` with the new position once every message
up to it is acknowledged:

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
prints each record, the output for the three writes above is:

```
org-1:3:0 insert notes %{"body" => "hi", "id" => 1}
org-2:3:0 insert notes %{"body" => "hello", "id" => 1}
org-1:4:0 update notes %{"body" => "hi there", "id" => 1}
saved position <opaque position>
```

Restart the pipeline with the saved position, and it continues after it.

## 6. Limit how long objects are kept

Every node deletes feed objects older than `retention_ms` once an hour. The
default retention is seven days. Set another time in the instance options, or
`nil` to keep the objects for ever:

```elixir
{Alambique, name: MyApp.Cells, ..., feed: [retention_ms: 30 * 24 * 3_600_000]}
```

A consumer that was away for longer than the retention continues from the
oldest object left, and the changes in between are lost. Compaction and
segment retention do not touch the feed. A service that reads no feed can set
`feed: false`, which stores much less ([cost-of-a-cell.md](cost-of-a-cell.md)).

## 7. Keep your own tables and columns out of the feed

Tables that hold your application's bookkeeping, such as a job queue, are not
data to analyze. Give them a common name prefix and list it in the instance
options:

```elixir
{Alambique, name: MyApp.Cells, ..., feed: [internal_prefixes: ["_myapp_"]]}
```

A table whose name starts with one of the prefixes gets no triggers, so its
changes never reach the feed. The tables of the library (`_alambique_`) and of
SQLite (`sqlite_`) are always left out. The prefixes are case-sensitive, and
each must be a non-empty string. The option affects only the feed:
`Alambique.query/5`, migrations and the deploy check treat these tables like
any other.

A feed copies every column of a captured row into the cell's file and into the
feed object. A column that your consumer never reads, such as a large body,
can be left out of both:

```elixir
{Alambique,
 name: MyApp.Cells,
 ...,
 feed: [
   tables: %{
     "messages" => [omit: ["body"]],    # every column but these
     "audit" => [only: ["kind", "at"]], # the key and these columns
     "events" => [ops: [:insert]],      # inserts only, whole row
     "attachments" => false             # no record at all
   }
 ]}
```

- `true` (the default for every table) captures the whole table. `false`
  captures nothing. `[omit: columns]` captures every column except those.
  `[only: columns]` captures the key and those columns.
- `other_tables: :skip` (default `:capture`) turns the list into an allow list:
  only the tables with `true`, `omit` or `only` are captured.
- A left-out column is **absent** from `values`. There is no marker and no
  `nil`. `key` is never changed, and its columns are always in `values`. A
  consumer that keeps a copy must not read a missing column as `NULL`, and must
  merge an `update` into the row it holds, looked up by `key`.
- The triggers never read an omitted column, so it is not written to the
  `_alambique_feed` rows, the cell's segments or the feed objects.
- An `UPDATE` that changes only omitted columns leaves no record.
- `ops:` limits a table to some operations: a non-empty list of `:insert`,
  `:update` and `:delete`. It stands alone (`[ops: [:insert]]`, whole row) or
  goes with one of `omit:` or `only:`
  (`[ops: [:insert], omit: ["body"]]`). Without it all three are captured.
  Use it to keep a mass delete, such as a retention prune, out of the feed:
  with `[ops: [:insert, :update]]` a `DELETE` on the table leaves no record
  and runs no trigger. An empty list is an error; use `false` instead.
- `ops:` filters the operation of the **record**. A key-changing `UPDATE` is
  a `delete` of the old key and an `insert` of the new one, so each is kept
  only if listed: `[ops: [:insert]]` keeps the `insert` of the new key and
  drops the `delete`; `[ops: [:update]]` leaves no record for it. A table
  that drops deletes does not tell a consumer that a row is gone, so use it
  only when the consumer does not need that.
- Changing a value on a cell that exists regenerates its triggers the next time
  the cell wakes. Records already published stay. Give every node the same
  value. Nodes do not compare their values: the node that wakes a cell
  regenerates the triggers to its own value, so the records follow the node
  that owns the cell at that moment.

## 8. Handle a rewind

A cell can be rewound to an earlier state (`Alambique.rewind/4`,
[how-to-rewind-a-cell.md](how-to-rewind-a-cell.md)). The feed then holds an
`Alambique.Feed.Rewind` among the records, after the records it undoes:

```
%Alambique.Feed.Rewind{cell: "org-1", from_txid: 6, to_txid: 3, txid: 7,
  target: {:txid, 3}, kept: ["_myapp_jobs"], id: "org-1:7:rewind", ...}
```

After it, the cell is in the state it had when the records up to `to_txid`
were applied. Put your copy of that cell back: revert the records with a
`position` TXID above `to_txid`, which stay on the feed before the marker, or
rebuild the cell from another source. Then carry on. Records after the marker
describe changes on top of the rewound state. Records at or below `to_txid` can
arrive again with the ids they had, so deduplicating by `id` (step 4) covers
them. A pipeline that matches only `%Alambique.Feed.Record{}` fails on the
marker, on purpose.

## 9. Handle a rejected object

An object that cannot be decoded does not stop the feed. It is not JSON, has an
unknown `op` or the wrong shape. Only a faulty or hostile writer produces such
an object. `read` returns one `Alambique.Feed.Rejected` in its place, and the
position moves past it:

```
%Alambique.Feed.Rejected{key: "myapp/feed/004/<time>-<txid>-org-2",
  reason: :invalid_json, id: "rejected:myapp/feed/004/...", at: <ms>}
```

Log it and carry on. The `[:alambique, :feed, :read, :rejected]` event is
emitted too. A pipeline that matches only `%Alambique.Feed.Record{}` fails on a
rejected object, as it does on the rewind marker. Text that is not valid UTF-8
is not rejected. Each bad byte arrives replaced by U+FFFD.

## 10. Read objects with several batches

By default an object holds one batch. A publisher with
`feed: [object_batches: 16, linger_ms: 50]` (defaults 1 and 0) puts up to 16
queued batches in one object. `Alambique.Feed.read/3` returns the same records,
with the same `position` and `id`, so a consumer that uses `read/3` or the
Broadway producer needs no change.

A reader can meet the same record in two objects after a crash, so deduplicate
by `record.id` (step 4). The `limit` still counts objects, so one call can
return more records than `limit`. A record becomes visible up to `linger_ms`
later, so keep `linger_ms` well below `window_ms`. A cell that hibernates or
shuts down publishes what it holds first, for up to `feed: [stop_publish_ms:]`
(default 5 000; 0 turns it off). If it cannot, the records appear when the next
owner wakes the cell.

## 11. Choose when published rows leave the cell's file

A cell keeps the rows of a change in its file until the batch is published.
Published rows are removed later, in a transaction that is uploaded like any
other. `feed: [cleanup:]` says when:

| `cleanup:` | When the rows of published batches are removed | Cost |
| --- | --- | --- |
| `:in_write` | In the next write of a caller. | That write pays the delete, the pages it dirties and a larger upload, so a one-row write after a large transaction is several times slower. |
| `:idle` | In a transaction of the cell's own, once the cell has had no call for `cleanup_after_ms`. A write never removes them. | An idle cell uploads one more batch. Under constant load the rows pile up in the file until there is a quiet time. |
| `:after_publish` | In a transaction of the cell's own as soon as the publication is acknowledged, if the pipeline has room. A write never removes them. | One more batch per published batch, also under load. |
| `:idle_or_write` (default) | As `:idle`, and a write that finds rows waiting removes them as `:in_write` does. | The table stays bounded under constant load. A write after a burst can pay as with `:in_write`. |

`cleanup_after_ms` (default 1 000, below `idle_timeout_ms`; when
`idle_timeout_ms` is 1 000 or less the default is one less than it) is the time
without a call that makes the cell idle for `:idle` and `:idle_or_write`. Reads
count as calls, so a cell that is read often does not clean while idle. A cell
whose idle cache shrink is on (`awake: [idle_cache_after_ms:]`) removes the
published rows first and shrinks after.

`cleanup_rows` (default `nil`, all rows) bounds the rows one cleanup removes.
It cuts only between batches, never inside one: a batch that has more rows than
the bound goes whole. The cleanup then repeats until nothing is left. Use it
when a cell holds so many rows that deleting them stalls its next call.

A cleanup of the cell's own uses a TXID, like a migration, so a cell shows more
gaps in its TXIDs and `after_ack` runs for that batch too. A cleanup that fails
leaves the rows, is tried again at growing intervals and never stops the cell.
After a crash the next owner publishes the rows that are still in the file
again, with the same record ids (step 4).

## 12. Decode the objects yourself

`Alambique.Feed.read/3` and the Broadway producer decode the objects. A reader
that decodes them itself reads this format: the bytes `"ALF"`, the version byte
`1`, then a zlib stream of the JSON text of the object. It rejects any other
body, as the library does. Cells keep their rows in the cell's file until the
node publishes them. A node that takes a cell over publishes what the previous
node left, once.

The module documentation of `Alambique.Feed.Record` describes the body.

## 13. Keep a large transaction from stalling its cell

When a batch is acknowledged, the cell reads the captured rows of the batches
that were acknowledged and hands them to its publisher. For a batch of
thousands of rows this read runs in the cell's process, and a call that arrives
right after a large transaction waits for it. In a measurement of one batch of
4 500 rows the read took 2.7 to 3.4 ms, 91 to 92 % of the hand-over.

`feed: [hand_over_rows: n]` (default `nil`, no bound) hands the rows over in
pieces of at most `n`. Between two pieces the cell serves the calls in its
mailbox. A batch larger than `n` is cut inside, in the order of its changes,
and the publisher joins the pieces before it queues the batch, so record ids
(`cell:txid:n`) and objects are the same as without the bound. If the
publisher ends in the middle of a batch, the cell hands over that batch again
from its start. A cell that stops hands over everything without serving calls,
for at most `stop_publish_ms`; if that time passes, it gives up and the rows
stay in the file for the next owner, as for any batch that was not published.

The bound has a cost: each piece is a query on the cell's file. How small to
set it for a given workload is not measured yet.

## Troubleshooting

- **`read` returns `{:ok, [], position}` for a long time.** Objects younger
  than `settle_ms` are hidden on purpose. Check that your context has the
  instance's `settle_ms`. Lower `window_ms` and `op_timeout_ms` in the instance
  if the delay is too long. Check the `[:alambique, :feed, :publish]` events for
  `result: :error`. A publication that cannot reach the store is retried in the
  background, first after `publish_retry_ms` and then at growing intervals up to
  `publish_retry_max_ms`. Commits are not affected, and the rows wait in the
  cell's file.
- **A record appears twice.** This is expected after an owner change or a lost
  acknowledgement. Deduplicate by `record.id`.
- **A cell shows gaps in its TXIDs.** Fences, migrations, cleanups of the feed
  and transactions that change no row use TXIDs without producing an object. Nothing is missing. See
  [design.md](design.md), section 13.
- **A table of your application appears on the feed.** List its prefix in
  `feed: [internal_prefixes: ...]` (step 7). The change applies at the cell's
  next wake.
- **A migration produces many records.** Rows that a migration writes, for
  example a backfill that updates every row, are published like any other
  change. Schema changes themselves are not.
- **Broadway stops saving the position.** A message that failed holds the
  position behind its object until the producer restarts. Fix the cause and
  restart the pipeline. The object is delivered again.

## Next steps

- [telemetry.md](telemetry.md), "Feed events", lists the `[:alambique, :feed, ...]` events.
- [design.md](design.md), section 13, defines the key layout and the ordering rules.
