# How to rewind a cell

**The question:** how do I put a cell back to an earlier state, for example to
undo an operation and everything after it, without readers seeing a mixed
state and without losing the writes that come after the undo?

This guide stands alone. The design is in [design.md](design.md), section 20.
The output below was produced by running the commands against
`AlambiqueTestkit.MemoryStore` with the default settings: an instance named
`:demo` with `feed: [internal_prefixes: ["_myapp_"]]`, a table `notes` and a
table `_myapp_jobs` that holds the application's own jobs.

## Before you start

- The `:data_dir` of the node that calls it has room for two copies of the
  cell, and the node has memory for about five times the cell (step 5).
- Retention still holds the point you want (default: seven days of points,
  design section 9).

## 1. Find the point before the operation

A TXID names a replicated batch, and every transaction that commits before the
next upload shares it. `Alambique.Tx.txid/1` is the batch a transaction will
join, so `txid - 1` also drops the transactions that committed earlier in that
batch. Two calls give a cut without that loss.

From outside, `position/3` uploads what is waiting and answers where the chain
ends:

```elixir
Alambique.position(:demo, "org-1")
```

```
{:ok, %{txid: 3, epoch: 1, data: 3, awake: true}}
```

Every write committed before the call is at or below `txid`, every write
committed after is above it. An asleep cell is read from the store and not
woken (`awake: false`).

From inside the operation, run it with `isolated: true`. The transaction then
owns its batch, and `Alambique.Tx.before/1` is the TXID of the state just before
it:

```elixir
{:ok, :done} =
  Alambique.transaction(:demo, "org-1", fn tx ->
    {:ok, before} = Alambique.Tx.before(tx)
    # keep `before` where your undo can find it
    {:ok, _} = Alambique.Tx.execute(tx, "INSERT INTO notes (body) VALUES ('oops')")
    :done
  end, isolated: true)
```

An isolated transaction costs one more upload.
`before/1` of a transaction that is not isolated is `{:error, :not_isolated}`.

## 2. Rewind

After a few more writes the cell holds three notes and a job:

```
{:ok, %{columns: ["body"], rows: [["keep me"], ["oops"], ["later"]]}}
```

Rewind to the TXID before the operation:

```elixir
Alambique.rewind(:demo, "org-1", {:txid, 3})
```

```
{:ok, %{to: 3, from: 6, txid: 7, epoch: 2, schema_version: 1, kept: ["_myapp_jobs"]}}
```

`to` is the TXID that was restored, `from` the head that was replaced and
`txid` the TXID of the rewind, from which the cell goes on. `{:timestamp, ms}`
works the same way, with the TXID that time resolves to. The cell now holds the
target, and the job stayed:

```
{:ok, %{columns: ["body"], rows: [["keep me"]]}}
{:ok, %{columns: ["name"], rows: [["send mail"]]}}
```

The cell is fenced first: its owner hibernates, and until the rewind ends calls
for it answer `{:error, {:unavailable, "rewind", _}}`. The new state appears in
one conditional upload, so a crash leaves the old state or the new one, and
readers on any node see one or the other. The cell is not woken; its next call
wakes it. The TXIDs of the undone history stay in the store while retention
keeps them, so you can undo a rewind with a target from before it.

## 3. Choose what happens to your own tables

By default the tables named by `feed: [internal_prefixes: ...]` (here
`_myapp_jobs`) are carried over from the state being replaced, with their rows,
indexes, triggers and views: the host's bookkeeping does not go back, as the
output above shows. Pass `keep: :none` to rewind every table. Captured tables
are never carried over. Carried tables must not use virtual tables, and foreign
keys and triggers that tie them to rewound tables are not reconciled.

A target with another `PRAGMA user_version` is refused
(`{:error, {:schema_differs, target_version, current_version}}`), because the
next wake would run migrations again over carried tables. `schema: :migrate`
accepts it and lets the next wake migrate forward; it is refused with carried
tables, so combine it with `keep: :none`.

## 4. Tell your feed consumers

The rewind publishes an `Alambique.Feed.Rewind` record after the records it
undoes:

```
%Alambique.Feed.Rewind{cell: "org-1", from_txid: 6, to_txid: 3, txid: 7,
  target: {:txid, 3}, kept: ["_myapp_jobs"], id: "org-1:7:rewind", ...}
```

Consumers must put their copy of the cell back to its state as of `to_txid`
([how-to-consume-the-change-feed.md](how-to-consume-the-change-feed.md), step 8).
Records of the target that were not published yet can arrive again with the
ids they had; deduplicate by `id` as always.

## 5. Know the cost

The rewind writes one segment as large as the database and needs two local
copies of it. On the test node a 50 MB cell took 3.6 s and a 200 MB cell 7.6 s,
with a peak of about 1 GB more resident memory for the larger one (design
section 20, "Cost"). The cell is unavailable for that time, except the
restore of the target, which runs before it is fenced.

The peak memory is about five times the database, so a rewind is refused when
the target or the current state is larger than `max_bytes` (256 MiB by default):
`{:error, {:too_large, bytes, max_bytes}}`. The current state is checked from the
store before the cell is fenced, the target once it is restored, and the
current state again where it is restored.

The instance option `rewind: [fence_attempts: 5, timeout_ms: 60_000, poll_ms: 100,
max_bytes: 268_435_456]` bounds the retries when the chain moves, the whole call,
the wait for the owner to hibernate and the size; `nil` for `max_bytes` removes
the limit, and `:timeout` overrides the second per call.

## 6. Repeat a call whose answer was lost

`{:error, :outcome_unknown}` and a timeout can hide a rewind that landed.
Repeating it would rewind again and discard what was written since. Read
`position/3` before the first try and pass it as `:if_head`:

```elixir
{:ok, %{txid: head}} = Alambique.position(:demo, "org-1")
Alambique.rewind(:demo, "org-1", {:txid, 3}, if_head: head)
```

A head that differs gives `{:error, {:head_moved, head}}` before the lease is
taken and again before the upload, with nothing changed. After a first try that
landed, the head is the rewind's own TXID, so a repeat is refused.

## 7. Undo only if no data was written since a report

Every wake commits an empty fence at a new TXID, so `if_head: head` would
refuse a cell that was only woken. Record `position/3` when you report, and
pass its TXID with `:data`:

```elixir
{:ok, %{txid: reported}} = Alambique.position(:demo, "org-1")
# ... later ...
Alambique.rewind(:demo, "org-1", {:txid, 3}, if_head: {reported, :data})
```

It succeeds if the cell was only woken since, and gives `{:error,
{:data_written, data_txid}}` when a write, a migration or a rewind committed
after `reported` (`data` of `position/3` is above it). It is checked before the
lease is taken and again before the upload.  `if_head: head` stays the exact check for
repeating a call.

## Troubleshooting

- **`{:error, {:target_not_behind, head}}`.** The target is the head or ahead
  of it, or the timestamp resolves to it. The output above, for `{:txid, 99}`:
  `{:error, {:target_not_behind, 8}}`.
- **`{:error, {:behind_retention, oldest_txid}}`.** Retention removed the files
  the target needs. `oldest_txid` is the lowest TXID a restore reaches.
- **`{:error, {:owned, owner, expires_at_ms}}`.** Another rewind or a relocation
  holds the cell, or the owner did not hibernate before `:timeout`. Try again
  after the lease expires.
- **`{:error, {:too_large, bytes, max_bytes}}`.** The database is larger than
  `rewind: [max_bytes:]`. Raise the option if the node has room for five times
  `bytes`, or restore the cell to a file with `mix alambique.restore`.
- **`{:error, {:capture, reason}}` or `{:error, {:attach, reason}}`.** A file
  of the rewind was gone or wrong before anything was sent, for example because
  the instance restarted and removed its work directories. Nothing changed;
  try again.
- **`{:error, {:fence, :txid_taken}}`.** Writers kept taking the TXID, more than
  `fence_attempts` times. Try again.
- **A crashed rewind.** The lease it held expires after `lease_ttl_ms`; the cell
  then serves the old state or the new one. A VM crash can leave a `rewind-*` directory
  in `<data_dir>/work`; the instance removes it the next time it starts. A restart of
  the instance during a rewind removes the files under it, and the rewind fails
  safely, as above.

## Next steps

- [how-to-operate-a-cell.md](how-to-operate-a-cell.md) for restoring a cell to a
  local file instead.
- [telemetry.md](telemetry.md) for `[:alambique, :cell, :rewind]`.
