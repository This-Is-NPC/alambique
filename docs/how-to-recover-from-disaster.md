# How to recover from the loss of a node and its disk

**The question:** a node is gone with its disk and only the bucket is left.
How do I bring its cells back on a new node, how long does it take, and what
do I check afterwards?

This guide stands alone. It rehearses the loss on your machine against the
disposable SeaweedFS of the project, and then lists what changes in
production. The background is in [design.md](design.md): the lease in section
5, the restore in section 6 and the early takeover in section 5.1.

## What survives

A write returns after the store has acknowledged its segment, so every write a
caller saw succeed is in the bucket (design section 6). The `:data_dir` of a
node is a cache: nothing in it is the only copy of an acknowledged write. A
write whose caller got `{:error, :timeout}` or `{:error, :outcome_unknown}` may
or may not be there; the application decides what to do with it, as it would
for any unknown outcome.

What the lost node leaves behind is its **leases**. A lease is held until its
`lease_ttl_ms` ends (30 s by default), and the new node takes it only after
`skew_margin_ms` more (2 s). Until then calls for that cell answer
`{:error, {:unavailable, owner, expires_at_ms}}`. Section 3 shows it, and
section 4 shows how to skip the wait.

This guide does not cover the loss of the bucket itself. For that, see
versioning in [storage-compatibility.md](storage-compatibility.md).

## Prerequisites

- A project that depends on `alambique` and compiles
  ([README](../README.md)).
- Docker, only for the rehearsal. In production you use your own bucket.
- `sqlite3`, only for the check in section 5.
- For the new node: the **same** bucket, `:prefix`, credentials, `:placement`
  and `:migrations` as the lost one, an empty `:data_dir`, and the same version
  of the application.

## 1. Rehearse the loss

Start the disposable store. In a checkout of the repository:

```sh
mise run seaweedfs:up
```

Without a checkout, use the `docker run` of the
[README](../README.md#example); the container name does not matter.

```text
SeaweedFS ready on 127.0.0.1:18333
```

In your project, save the settings that every node shares as `common.exs`.
They are the credentials of the disposable container; use your own in
production.

```elixir
defmodule Disaster do
  @s3 %{
    endpoint: "http://127.0.0.1:18333",
    bucket: "disaster-demo",
    access_key_id: "alambique",
    secret_access_key: "alambique-secret"
  }

  def child(node_id, data_dir) do
    {Alambique,
     name: MyApp.Cells,
     node_id: node_id,
     store: {Alambique.Store.S3, @s3},
     prefix: System.get_env("ALAMBIQUE_PREFIX", "myapp"),
     data_dir: Path.expand(data_dir),
     migrations: [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT NOT NULL)"}]}
  end

  # One line per cell: the rows and the total length of their text.
  def summary(cell) do
    {:ok, %{rows: [[n, sum]]}} =
      Alambique.query(MyApp.Cells, cell, "SELECT count(*), coalesce(sum(length(body)), 0) FROM notes")

    "#{cell}: #{n} rows, #{sum} bytes of text"
  end

  # Prints the events that show a recovery: the lease, the wake and the restore.
  def log_events do
    events = [[:alambique, :lease, :acquire], [:alambique, :cell, :wake], [:alambique, :replication, :restore]]
    :telemetry.attach_many("disaster-log", events, &__MODULE__.log/4, nil)
  end

  def log([_, :lease, :acquire], m, meta, _) do
    IO.puts("lease.acquire #{meta.cell} result=#{meta.result} epoch=#{meta[:epoch]} #{ms(m.duration)} ms")
  end

  def log([_, :cell, :wake], m, meta, _) do
    IO.puts("cell.wake #{meta.cell} result=#{meta.result} source=#{meta[:source]} txid=#{meta[:txid]} #{ms(m.duration)} ms trips=#{m.trips}")
  end

  def log([_, :replication, :restore], _, meta, _) do
    IO.puts("replication.restore #{meta.cell} result=#{meta.result} txid=#{meta[:txid]} files=#{meta[:segments]}")
  end

  defp ms(native), do: System.convert_time_unit(native, :native, :millisecond)
end
```

Save `write.exs`. It is node A: it writes 50 rows to each of three cells and
stops the VM without a drain, as a machine that loses power does.

```elixir
Code.require_file("common.exs", __DIR__)

{:ok, _} = Supervisor.start_link([Disaster.child("node-a", "data-a")], strategy: :one_for_one)

cells = ["org-1", "org-2", "org-3"]

for cell <- cells, i <- 1..50 do
  {:ok, _} = Alambique.execute(MyApp.Cells, cell, "INSERT INTO notes (body) VALUES (?)", ["#{cell} note #{i}"])
end

for cell <- cells, do: IO.puts(Disaster.summary(cell))
IO.puts("node A: stopping without a drain")
System.halt(0)
```

Run it, then delete the data directory, which is the lost disk:

```sh
mix run write.exs
rm -rf data-a
```

```text
org-1: 50 rows, 641 bytes of text
org-2: 50 rows, 641 bytes of text
org-3: 50 rows, 641 bytes of text
node A: stopping without a drain
```

Expected result: the three summaries, and no `data-a` directory. Everything
is now only in the bucket.

## 2. Start a new node on an empty directory

A new node needs no restore command. It starts the instance with the same
store and prefix, and a cell is restored by its first call. Save `read.exs`,
which is node B and calls each cell until it answers:

```elixir
Code.require_file("common.exs", __DIR__)

Disaster.log_events()
{:ok, _} = Supervisor.start_link([Disaster.child("node-b", "data-b")], strategy: :one_for_one)

cells = ["org-1", "org-2", "org-3"]

# The lost node still holds the leases, so a call is refused until each one has
# ended. A node takes a lease `skew_margin_ms` (2 s) after its expiry.
ask = fn ask, cell ->
  case Alambique.query(MyApp.Cells, cell, "SELECT count(*) FROM notes") do
    {:error, {:unavailable, owner, expires_at_ms}} ->
      left = expires_at_ms - System.os_time(:millisecond)
      IO.puts("#{Time.utc_now() |> Time.truncate(:second)} #{cell}: unavailable, held by #{inspect(owner)}, lease ends in #{div(left, 1000)} s")
      Process.sleep(max(left, 0) + 2_100)
      ask.(ask, cell)

    {:ok, %{rows: [[n]]}} ->
      {:ok, n}
  end
end

started = System.monotonic_time(:millisecond)
for cell <- cells, do: IO.inspect(ask.(ask, cell), label: "#{cell} rows at the first answer")
IO.puts("waited #{System.monotonic_time(:millisecond) - started} ms")

for cell <- cells, do: IO.puts(Disaster.summary(cell))
IO.inspect(Alambique.scrub(MyApp.Cells, cells: 3), label: "scrub")
IO.inspect(Alambique.drain(MyApp.Cells), label: "drain")
```

Run it right after the loss, within the lease time:

```sh
mix run read.exs
```

## 3. Wait for the leases

Output of the run on 2026-11-16 (a laptop with other work running, so the
times are not those of a quiet machine):

```text
06:31:41 org-1: unavailable, held by "node-a", lease ends in 7 s
lease.acquire org-1 result=ok epoch=2 19 ms
replication.restore org-1 result=ok txid=52 files=10
cell.wake org-1 result=ok source=restore txid=53 471 ms trips=16
org-1 rows at the first answer: {:ok, 50}
06:31:52 org-2: unavailable, held by "node-a", lease ends in 2 s
lease.acquire org-2 result=ok epoch=2 7 ms
replication.restore org-2 result=ok txid=52 files=10
cell.wake org-2 result=ok source=restore txid=53 358 ms trips=16
org-2 rows at the first answer: {:ok, 50}
06:31:57 org-3: unavailable, held by "node-a", lease ends in 0 s
lease.acquire org-3 result=ok epoch=2 8 ms
replication.restore org-3 result=ok txid=52 files=45
cell.wake org-3 result=ok source=restore txid=53 260 ms trips=51
org-3 rows at the first answer: {:ok, 50}
waited 19417 ms
```

What it says:

- **The refusal.** The first call for `org-1` came about 10 s after node A
  stopped, the time node B took to start, and was refused: the lease of node A
  ran 7 s more. The new node did not take
  it at once, because the lease of a node that may still be alive is not its to
  take (design section 5). Each cell has its own lease, so each waits for its
  own end: `org-2` and `org-3` were written a little later than `org-1`.
- **The wait is bounded.** It is the rest of `lease_ttl_ms` plus
  `skew_margin_ms`: here 19.4 s from the first call to the last answer, which
  is under 30 s + 2 s. Without a call, nothing happens: a lease is taken when a
  call needs the cell.
- **The restore.** `source=restore` says the cell was built from the store.
  `files=10` is the number of files applied (the newest snapshot, if there is
  one, and the files after it), `trips` the store requests of the wake, and
  `txid=53` the new head: the restored `txid=52` plus one empty file the wake
  writes to fence the old owner. `org-3` applied 45 files because the compactor
  had not merged them before node A stopped (it has `trips=51` against 16);
  compaction makes a wake read fewer files
  ([how-to-speed-up-a-wake.md](how-to-speed-up-a-wake.md)).
- **The epoch rose.** `epoch=2`: node A held epoch 1. A node of the old epoch
  that comes back can no longer write: its next upload is fenced (design
  section 6).

## 4. Skip the wait with an early takeover

If you know that node A is gone for good, for example because the machine was
deleted, take its cells at once instead of waiting. `Alambique.lease/3` reads
the lease without waking the cell, and `Alambique.take_over/4` replaces it. The
epoch you pass makes two nodes that try it together take the cell once
(design section 5.1). Node B, with `takeover.exs`:

```elixir
Code.require_file("common.exs", __DIR__)

Disaster.log_events()
{:ok, _} = Supervisor.start_link([Disaster.child("node-b", "data-b")], strategy: :one_for_one)

for cell <- ["org-1", "org-2", "org-3"] do
  {:ok, %{owner_id: owner_id, epoch: epoch, released: false}} = Alambique.lease(MyApp.Cells, cell)
  started = System.monotonic_time(:millisecond)
  {:ok, _node} = Alambique.take_over(MyApp.Cells, cell, :node_lost, epoch: epoch)
  IO.puts("take_over #{cell}: was #{owner_id} at epoch #{epoch}, done in #{System.monotonic_time(:millisecond) - started} ms")
  IO.puts(Disaster.summary(cell))
end

IO.inspect(Alambique.drain(MyApp.Cells), label: "drain")
```

Output of a second run on the same store, with a new prefix
(`ALAMBIQUE_PREFIX=myapp2` for `write.exs` and `takeover.exs`), node B started
right after the loss:

```text
take_over org-1: was node-a at epoch 1, done in 737 ms
org-1: 50 rows, 641 bytes of text
take_over org-2: was node-a at epoch 1, done in 161 ms
org-2: 50 rows, 641 bytes of text
take_over org-3: was node-a at epoch 1, done in 161 ms
org-3: 50 rows, 641 bytes of text
drain: {:ok, %{errors: [], hibernated: 3}}
```

The takeover includes the restore, and no call was refused. Do it only when
the node is really gone: the takeover fences the old owner, so no acknowledged
write is lost even if the suspicion is wrong, but the old owner then stops
serving ([how-to-operate-a-cell.md](how-to-operate-a-cell.md), section 5.1).

## 5. Check the result

Do all of these before you send traffic.

1. **The rows match.** Each cell answered the count it had before the loss: 50
   rows and 641 bytes of text. In production compare with what your
   application knows, such as a counter in another database, because the
   library has no other copy to compare with.

2. **`source=restore`, not `source=empty`.** A wake that finds nothing in the
   store does not fail: it starts a new empty cell. A typo in the prefix gives
   this, with no error. Starting node B as in `read.exs` with
   `ALAMBIQUE_PREFIX=mapp` and calling `Disaster.summary("org-1")` gave:

   ```text
   lease.acquire org-1 result=ok epoch=1 37 ms
   replication.restore org-1 result=ok txid=0 files=0
   cell.wake org-1 result=ok source=empty txid=2 597 ms trips=7
   org-1: 0 rows, 0 bytes of text
   ```

   `epoch=1` on a cell that had writes is the sign. Stop the node and fix the
   prefix and the bucket before anything writes to it.

3. **The lease and the head.** Look at the cell in the store, without a node:

   ```sh
   export ALAMBIQUE_S3_ENDPOINT=http://127.0.0.1:18333 ALAMBIQUE_S3_BUCKET=disaster-demo \
          ALAMBIQUE_S3_ACCESS_KEY_ID=alambique ALAMBIQUE_S3_SECRET_ACCESS_KEY=alambique-secret \
          ALAMBIQUE_PREFIX=myapp
   mix alambique.inspect org-1
   ```

   ```text
   Cell:       org-1
   Prefix:     myapp
   Frozen:     no
   Crashes:    none
   Lease:      epoch 2, owner node-b, released
   Head:       TXID 53, epoch 2, page size 4096
   Segments:
     level 0: 53 files, TXID 1-53, 35.1 KiB
     level 1: 6 files, TXID 1-48, 5.2 KiB
   Snapshots:
     none
   ```

   The epoch is one above the old one (`epoch 2`) and the head is one TXID
   above the last the old node acknowledged (`53`, from `52`). The lease says
   `released` here only because `read.exs` drained node B at its end.

4. **The files are sound.** `Alambique.scrub/2` restores sampled cells into a
   temporary directory and runs SQLite's integrity check on them (design
   section 6.1). Above, it checked one of the three cells:

   ```text
   scrub: {:ok, %{ok: 1, empty: 0, failed: 0, cells: 1, failures: []}}
   ```

   It samples, so for a short list of important cells restore them yourself
   and check the file with SQLite. This needs no node, and it uses the same
   code as a wake:

   ```sh
   mix alambique.restore org-1 check.db
   sqlite3 check.db "PRAGMA integrity_check; SELECT count(*), sum(length(body)) FROM notes;"
   ```

   `/path/to/project` stands for the directory where you ran the task.

   ```text
   Restored org-1 to /path/to/project/check.db: TXID 53, epoch 2, 11 files applied
   ok
   50|641
   ```

5. **Clean up the rehearsal.**

   ```sh
   rm -rf data-b check.db
   mise run seaweedfs:down
   ```

   ```text
   SeaweedFS removed
   ```

## In production

- **The old node may be alive but out of reach.** It keeps serving until its
  lease deadline, `lease_ttl_ms - skew_margin_ms` after its last renewal,
  28 s by default, and then stops (design section 5). A new node takes the
  cell only after that, so two nodes never serve it together. Use the early
  takeover only when you know the old node is down.
- **Expect a burst of wakes.** The first call to each cell
  restores it. Many cells waking at once queue at `awake: [max_wakes:]` and
  get `{:error, :overloaded}` past `wake_queue_ms`
  ([how-to-size-a-node.md](how-to-size-a-node.md)). Callers should retry.
  Large cells restore lazily, so their first answer does not wait for the
  whole database ([how-to-speed-up-a-wake.md](how-to-speed-up-a-wake.md)).
- **Put the new node in the router only when it is ready.** Start it with
  `join: false` and call `Alambique.join/1` after your own processes are up
  ([how-to-operate-a-cell.md](how-to-operate-a-cell.md), section 9).
- **Watch it.** The signals of [how-to-monitor-a-node.md](how-to-monitor-a-node.md)
  tell you whether the wakes are slow, whether the breaker opened and whether
  any lease was lost.

## Troubleshooting

- **Calls answer `{:error, {:unavailable, owner, expires_at_ms}}` for longer
  than `lease_ttl_ms` plus `skew_margin_ms`.** The owner renews its lease: it
  is alive. Run `mix alambique.inspect CELL` twice; if `expires in` goes up, a
  node still holds the cell. Find it, or stop it.
- **The cell is empty after the recovery.** See step 2 of section 5: the
  prefix or the bucket is not the lost node's.
- **The wake fails with `{:restore, reason}`.** A file in the chain is missing
  or damaged. Restore an earlier point with
  `mix alambique.restore CELL file.db --at txid:N` and read the troubleshooting
  list of [how-to-operate-a-cell.md](how-to-operate-a-cell.md). If the bucket
  is versioned, restore the versions of the missing objects first
  ([storage-compatibility.md](storage-compatibility.md)).
- **`{:error, {:http, 403, _}}`, or `{:error, {:http, 301, _}}`.** The
  credentials or the region are not the old node's
  ([how-to-configure-the-s3-store.md](how-to-configure-the-s3-store.md)).
- **`{:error, {:data_dir_in_use, path}}`.** Another instance holds the
  directory: give the new node its own.
- **The migrations differ from the lost node's.** A cell that is at a higher
  schema version than the new code reads is refused; deploy the code of the lost
  node, or one that reads that version
  ([how-to-roll-out-a-schema-change.md](how-to-roll-out-a-schema-change.md)).

## Next steps

- [how-to-monitor-a-node.md](how-to-monitor-a-node.md) for what to watch while
  the cells wake.
- [how-to-manage-the-bucket.md](how-to-manage-the-bucket.md) for what to keep
  and what never to delete in the bucket.
- [how-to-swap-a-node.md](how-to-swap-a-node.md) for the planned version of
  this, with a drain.
