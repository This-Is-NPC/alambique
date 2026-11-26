# How to operate a cell

**The question:** how do I run and maintain a cell that is in the store:
look at it, restore it, freeze it, move it, read the scrub results, restart a
node, change the options of an awake cell, and keep its disk from filling or
stalling?

This guide stands alone. It needs the store, and the running cluster for the
steps that call a node (`--connect`). The background (compaction, retention,
the freeze marker) is in [design.md](design.md) sections 9 and 10.

The names in the examples (`demo`, `org-1`, `a@127.0.0.1`, `compact-demo`) are
placeholders. The output blocks show the layout that each command prints. Their
values are placeholders too.

## Prerequisites

- A project that depends on `alambique`. Run the `mix` tasks in its directory.
- Credentials for the S3 bucket the instance uses.
- For the steps that call a node (`move`, `relocate`, `cleanup`, and
  `freeze --connect`): the node name, the cluster cookie, and the instance name
  (the `:name` given to `{Alambique, ...}`).

## 1. Tell the tasks where the store is

The tasks read the store directly and start no instance. Give them the same
store and prefix that the instance uses. Flags win over environment variables,
which win over the config.

Environment variables, used by every command below:

```sh
export ALAMBIQUE_S3_ENDPOINT=http://127.0.0.1:18333
export ALAMBIQUE_S3_BUCKET=compact-demo
export ALAMBIQUE_S3_ACCESS_KEY_ID=alambique
export ALAMBIQUE_S3_SECRET_ACCESS_KEY=alambique-secret
export ALAMBIQUE_PREFIX=demo
```

The flags are `--bucket`, `--access-key-id`, `--secret-access-key`,
`--endpoint`, `--region`, `--session-token`, `--no-path-style` and `--prefix`.
Their variables are `ALAMBIQUE_S3_BUCKET`, `ALAMBIQUE_S3_ACCESS_KEY_ID`,
`ALAMBIQUE_S3_SECRET_ACCESS_KEY`, `ALAMBIQUE_S3_ENDPOINT`,
`ALAMBIQUE_S3_REGION`, `ALAMBIQUE_S3_SESSION_TOKEN`, and
`ALAMBIQUE_S3_PATH_STYLE=false` for `--no-path-style`.

- `--endpoint`: without it the tasks use AWS, at `https://s3.<region>.amazonaws.com`.
- `--region`: default `us-east-1`.
- `--prefix`: default `alambique`.
- `--no-path-style`: virtual-hosted bucket addressing. Path style is the default.

Prefer the variables for the secret key, the session token and the cookie. A
flag shows in the process list (`ps`) and may stay in the shell history.

The same settings can go in the config. The store can be any module that
implements the store behaviour:

```elixir
config :alambique, :ops,
  store: {Alambique.Store.S3, %{endpoint: "...", bucket: "...", access_key_id: "...", secret_access_key: "..."}},
  prefix: "demo"
```

With none of them, a task stops and lists the three ways.

## 2. Look at a cell

```sh
mix alambique.inspect org-1
```

```text
Cell:       org-1
Prefix:     demo
Frozen:     no
Crashes:    none
Lease:      epoch 3, owner a@127.0.0.1, held, expires in 7.6 s
Head:       TXID 43, epoch 3, page size 4096
Segments:
  level 0: 43 files, TXID 1-43, 14.5 KiB
  level 1: 10 files, TXID 1-40, 3.6 KiB
  level 2: 2 files, TXID 1-32, 915 B
Snapshots:
  TXID 42, 693 B, epoch 1, state of <ISO 8601 time>
```

The `Lease` line has one of these states:

- `held`: the owner renews it.
- `expired`: the owner is gone and no node has taken over yet.
- `released`: the owner hibernated the cell cleanly.
- `none`: the cell has never woken.

The `Head` is the newest TXID in the store. Levels 1 and above are compacted
copies of level 0, which restore reads to open fewer files.

`Crashes` is `none`, or `N recorded, M in the window` for a cell that crashed
the VM (step 4).

- `--no-sizes` skips the size of each object. Use it for cells with many files.
- `--json` prints one JSON object with the same facts.
- `--location ID` picks a location when the placement has several (step 6).

## 3. Restore a cell into a file

```sh
mix alambique.restore org-1 ./org-1.db
```

```text
Restored org-1 to /home/you/project/org-1.db: TXID 43, epoch 3, 2 files applied
```

The output is an ordinary SQLite file. Open it with `sqlite3 ./org-1.db`. The
task builds the file next to the path and renames it into place. An existing
file at the path is replaced. The cell is not touched, awake or asleep.

`--at` picks the point in time:

- `latest` (default)
- `txid:N`
- an ISO 8601 UTC time, such as `2026-01-01T00:00:00Z`
- `ms:N`, Unix milliseconds

```sh
mix alambique.restore org-1 ./org-1-txid20.db --at txid:20
mix alambique.restore org-1 ./org-1-time.db --at 2026-01-01T00:00:00Z
```

```text
Restored org-1 to /home/you/project/org-1-txid20.db: TXID 20, epoch 1, 2 files applied
Restored org-1 to /home/you/project/org-1-time.db: TXID 24, epoch 1, 3 files applied
```

A time gives the last TXID stamped at or before it.

Retention keeps every point in its window, seven days by default
(`pitr_window_ms`). A point older than the window fails once the reaper has
deleted its files (see the troubleshooting list). Until then the files are only
marked, for `deletion_grace_ms` (one day by default), so older points still
restore (step 7).

## 4. Freeze a cell

A frozen cell refuses to wake, so nothing writes to it. Use this while you
investigate the cell or run a repair. Reads of the store, restores and
compaction still work.

```sh
mix alambique.freeze org-1 --instance demo --connect a@127.0.0.1 --cookie demo
```

```text
Froze org-1
org-1 is asleep
```

`--connect` also asks the cluster to hibernate the cell now. Without it, the
task only writes the marker. An awake cell then hibernates at its next lease
renewal, within a third of `:lease_ttl_ms`. A call to a frozen cell fails with
`{:error, :frozen}`.

Check the state with `mix alambique.inspect org-1`. It shows `Frozen: yes` and
`Lease: ..., released`.

Undo it:

```sh
mix alambique.unfreeze org-1
```

```text
Unfroze org-1
```

From inside the application, use `Alambique.freeze(MyApp.Cells, cell)`,
`Alambique.unfreeze(MyApp.Cells, cell)` and `Alambique.frozen?(MyApp.Cells, cell)`.

### A cell the instance froze

An instance freezes a cell by itself when the cell keeps crashing the VM. After
`:threshold` crashes (3 by default) within `:window_ms` (one hour), it writes
the same freeze marker, logs an error, and emits `[:alambique, :cell, :quarantined]`
(see [telemetry.md](telemetry.md)). Alert on that event. The other cells of the
node keep running. To handle one:

1. Look at it. `mix alambique.inspect org-1` shows `Frozen: yes` and a
   `Crashes:` line such as `3 recorded, 3 in the window`. From the application,
   `Alambique.crashes(MyApp.Cells, cell)` returns `{:ok, counter}`. The counter
   has `total`, and `recent`, a list of crashes with `at_ms`, `node` and
   `kinds`. A kind names the work in flight: `cell` (the cell process itself),
   `hydrator`, `compaction`, `query`, `rewind` or `relocate`.
2. Take a copy to reproduce the crash away from production:
   `mix alambique.restore org-1 ./org-1.db` (step 3). The cell is frozen, so
   nothing writes to it meanwhile.
3. Fix the cause, for example the statement the application sends or the
   version of the code, and deploy the fix.
4. Unfreeze the cell with `mix alambique.unfreeze org-1` or
   `Alambique.unfreeze/2`. This also forgets the recorded crashes, so the count
   starts again from zero. If the VM crashes again, the cell is frozen again
   after `:threshold` more crashes.

The instance records a crash when the node that had the cell starts again. If
the store is not reachable then, the record is kept in `<data_dir>/inflight` and
written again every `:retry_ms` (30 seconds). To change the numbers or turn the
feature off, see `:quarantine` in the documentation of the `Alambique` module.

## 5. Move a cell to another node

```sh
mix alambique.move org-1 b@127.0.0.1 --instance demo --cookie demo
```

```text
Moved org-1 to b@127.0.0.1
```

The current owner hibernates the cell, and `b@127.0.0.1` wakes it. The lease
then shows the new owner and a higher epoch:

```text
Lease:      epoch 5, owner b@127.0.0.1, held, expires in 8.6 s
```

Moving needs the Erlang cluster, not only the store. The task starts
distribution under a random name (a long name when the target has a dot after
the `@`) and sets the cookie. The cookie comes from `--cookie`, then
`ALAMBIQUE_COOKIE`, then the user's `~/.erlang.cookie`. The task connects to
`--connect NODE`, which defaults to the target, and calls `Alambique.move/4`
there. The target must run the instance. Use `--timeout MS` for slow cells
(default 15000).

From code, `Alambique.move(:demo, "org-1", node, only_if_idle: true)` moves
the cell only when no other call waits on it. A busy cell answers
`{:error, :busy}` and stays where it is, so a rebalance does not make callers
wait. Try again later.

## 5.1 Take over a cell whose owner is down

If a failure detector suspects that a node is gone, read the lease without
waking the cell. Pass its epoch to `take_over/4`. With the epoch, two nodes
cannot take the same cell from each other.

```elixir
{:ok, %{owner: _node, owner_id: owner_id, epoch: epoch, released: false}} =
  Alambique.lease(:demo, "org-1")

{:ok, served_by} =
  Alambique.take_over(:demo, "org-1", :owner_down, epoch: epoch)
```

- `owner` is the node, when that node still runs the instance. `owner_id` is
  the raw owner id in the lease. Use it while the detector still names the
  failed node.
- A released lease (`released: true`, after a clean hibernation) needs no epoch.
- A moved lease reports `moved_to`.
- A cell that never had a lease reports epoch 0 and `owner: nil`.
- `Alambique.lease/3` reads the lease only. It does not wake or freeze the
  cell. `:timeout` bounds the read.

## 6. Move a cell to another location

A placement can put a cell in one of several locations (a store and prefix
each). The placement of the instance decides where a cell lives (design section
18). To move a cell to another location, give the id of the destination as the
placement lists it:

```sh
mix alambique.relocate org-1 us --instance demo --connect a@127.0.0.1 --cookie demo
```

```text
Moved org-1 to location us
```

From a console, the same call is:

```elixir
{:ok, %Alambique.Location{id: "us", prefix: "demo"}} = Alambique.relocate(:demo, "org-1", "us")
{:ok, %{columns: ["body"], rows: [["hi"]]}} = Alambique.query(:demo, "org-1", "SELECT body FROM notes")
```

After the move, the old location keeps a released lease whose `moved_to` names
the new location. The new location holds the data and a held lease with a higher
epoch. Check each location with `mix alambique.inspect org-1 --location ID`.

While the move runs, calls to the cell fail with
`{:error, {:unavailable, "relocate", _}}`. If the task fails or is interrupted,
run it again with the same arguments. It continues from where it stopped. A node
that still caches the old location finds the marker there, asks the placement
again, and needs no restart.

After the move, the task reports what happens to the objects that the cell left
at the old location:

```text
Moved org-1 to location us
Location eu keeps the objects of org-1 until <ISO 8601 time>; the instance deletes them then, or run: mix alambique.cleanup org-1 eu
```

The objects stay for `moved_grace_ms` (an instance option, ten minutes by
default). This lets a query or restore that started before the move still find
its files. After that, the instance marks the segments and snapshots for
deletion, and the reaper deletes them `deletion_grace_ms` later (one day by
default, step 7). The frozen marker, the trashed key (the retention marker), the
crash counter and the schema index entry go at once, when the cleanup runs. The
compactor runs the cleanup on its own. It also scans for cleanups that a restart
left pending (`compaction: [moved_scan_ms:]`, 60 000 by default). Nothing is
needed by hand. To finish a cleanup early, or when compaction is off, run:

```sh
mix alambique.cleanup org-1 eu --instance demo --connect a@127.0.0.1 --cookie demo
```

```text
Cleaned <n> objects of org-1 at location eu (deleted after deletion_grace_ms)
```

The count depends on the cell's history.

- `--grace-ms 0` frees the storage at once. It can fail a reader that started
  before the move.
- The lease of the old location stays, marked `moved_to`. Nodes that still have
  the old location cached read it, ask the placement again, and never write there.
- A cleanup of the location where the cell lives is refused (`:live_location`).

A cell can move back to a location it left. The old location must be cleaned up
first. The cleanup must also be older than eight times `op_timeout_ms`, so a slow
cleanup cannot delete new objects. The cleanup does not have to wait for
`deletion_grace_ms`: the returning snapshot has a TXID above the marked files, so
it covers them. Until the cleanup is ready, `relocate` answers:

- `{:error, :destination_not_cleaned}`: no cleanup finished there yet, usually
  because the grace period is still running. The call queues the cleanup in the
  compactor. Run `relocate` again after the grace period. With compaction off,
  run the cleanup yourself first.
- `{:error, {:destination_settling, ready_at_ms}}`: the cleanup finished too
  recently. Wait until `ready_at_ms` (Unix milliseconds), then run `relocate`
  again. Do not clean up again, because that restarts the wait.
- `{:error, :destination_not_empty}`: data is there, but the lease does not say
  the cell moved. For example, the lease was lost on that store. Inspect the
  location before you decide.
- `{:error, {:destination_ahead, txid}}`: a data file ends beyond the restored
  state, so the move refuses rather than collide with a writer it does not know.
  Find and remove that file, then run the cleanup and `relocate` again.

Epochs keep growing across moves. If a move to one location did not finish, a
move to another location answers `{:error, {:move_pending, id}}`. Run the first
move to the end.

## 7. Get back files that retention removed

Retention and the cleanup of a moved cell do not delete at once. They mark the
files, and the reaper deletes them `deletion_grace_ms` later (one day by default,
design section 9.1). During that time:

1. **Restore a point before the window.** `mix alambique.restore` still sees the
   marked files. For example, `mix alambique.restore org-1 ./old.db --at txid:7`
   works for a point that retention already covered. Copy out what you need from
   the restored file.
2. **Keep the files.** From a console on a node of the instance, list the pending
   deletions and cancel the ones you want to keep:

   ```elixir
   {:ok, %{entries: entries}} = Alambique.Trash.pending(:demo)
   # each entry: %{key: ..., due_ms: ..., cell: "org-1"}
   :ok = Alambique.Trash.cancel(:demo, hd(entries).key)
   ```

   Without a console, delete the manifest object `<prefix>/trash/<due>/<cell>/<id>`
   with your S3 tool. A cancelled manifest is never processed. Retention marks
   covered files again in a later run, so set `retention: false`, or a longer
   `pitr_window_ms`, first if the files must stay.
3. **Check what is left.** `Alambique.Trash.pending/2` lists what waits and when
   it is due. `Alambique.Trash.reap/2` runs a deletion round now.

After the due time the files are gone, and the usual rules apply. If the store
has object versioning, the files may still be recoverable there (see
[storage-compatibility.md](storage-compatibility.md)).

## 8. Read the scrub results

With `scrub: [...]` set on the instance, or with `Alambique.scrub(:demo)` from a
console, the library restores sampled cells and checks them (design section 6.1).
The check does not wake or freeze a cell. It returns a summary, and emits one
telemetry event per cell:

```elixir
{:ok, summary} = Alambique.scrub(:demo)
# %{cells: <n>, ok: <n>, empty: <n>, failed: <n>,
#   failures: [%{cell: "<cell>", location: "<location>", reason: <reason>}]}
```

Each failed cell is also logged as an error, for example
`Alambique scrub: replica of <cell> at <location> is damaged: ...`. Watch
`[:alambique, :scrub, :cell]` for `result: :error`. The `reason` says what failed:

- `{:restore, reason}`: a file is missing, damaged or out of order. The reason is
  the one `mix alambique.restore` prints, such as `{:gap, txid}`. To read the last
  good state, restore with `--at txid:N`, where `N` is before the problem. Do not
  delete anything.
- `{:checksum_mismatch, expected, found}`: the files restored, but the result
  does not have the checksum that the chain ends with.
- `{:integrity_check, findings}` or `{:quick_check, findings}`: the files
  restored, but SQLite reports the database as damaged. The findings are its
  lines. The damage was probably there before the cell was replicated.
- `{:checksum, reason}` or `{:open, reason}`: the restored file could not be
  read or opened.

`empty` counts cells that are in the schema index but have no objects in the
store, for example a cell that moved away.

## 9. Restart a node without failed calls

A node that starts takes part in routing at once. Other nodes can then forward
calls to it before the processes that serve them are running. To keep the node
out until it is ready, start the instance with `join: false`, and join when
everything is up.

1. Before you stop the node, drain it. Stopping the instance runs the same drain
   (design section 8):

   ```elixir
   {:ok, %{hibernated: _, errors: []}} = Alambique.drain(MyApp.Cells)
   ```

   Other nodes stop choosing this node and serve its cells from the released
   leases.

2. Start the new release with `join: false`, and list `Alambique` before the
   processes that depend on it in the supervision tree:

   ```elixir
   {Alambique, name: MyApp.Cells, join: false, ...}
   ```

   Until step 3, the node is not a member. No other node prefers it, picks it as
   an owner, or lists it for `Alambique.move/4`. It still serves the calls made
   on it, and a cell whose lease it holds stays reachable from other nodes.

3. After your own processes have started, join. Put this in the last child of
   the tree, or in `Application.start/2` after `Supervisor.start_link/2`:

   ```elixir
   :ok = Alambique.join(MyApp.Cells)
   ```

   `join/1` returns `:ok` at once on a member, so it is safe to repeat. After a
   `drain/2`, it also ends the drain, and the node wakes cells again.

Expected result: after a moment, other nodes route new wakes to the node, and
`Alambique.move(MyApp.Cells, cell, node)` no longer answers
`{:error, :unknown_node}` for it.

## 10. Check that the disk does not stall writes

Every write waits for its SQLite commit on the node's disk and for its upload to
the store (design section 15). If the file system under either one stalls,
every cell that waits on it stalls too.

Attach a handler once per node. It logs each commit and each segment upload that
takes 1 s or more. Put it in `Application.start/2`, or run it from a console on
the node:

```elixir
defmodule StallWatch do
  require Logger

  @threshold_ms 1_000

  def attach do
    :telemetry.attach_many(
      "alambique-stall-watch",
      [[:alambique, :cell, :commit], [:alambique, :replication, :sync]],
      &__MODULE__.handle/4,
      nil
    )
  end

  def handle(event, %{duration: duration}, meta, _config) do
    ms = System.convert_time_unit(duration, :native, :millisecond)
    if ms >= @threshold_ms, do: Logger.warning("#{inspect(event)} #{meta.cell}: #{ms} ms")
  end
end

:ok = StallWatch.attach()
```

Expected result: no log line while the disk and the store answer quickly. A
line such as `[:alambique, :cell, :commit] org-1: 1450 ms` names a cell and a
duration in milliseconds. The `duration` of both events is in native time units,
which the handler converts.

A commit includes the upload, so the events do not tell a slow disk from a slow
store. Compare the lines with the store's latency and with the disk's own metrics.
Stalls can be intermittent, so keep the handler attached for a while before you
decide that the disk is clean.

**What to do.** If the handler logs commits or uploads at 1 s or more, the file
system under `:data_dir`, or the store, stalls writes. Move `:data_dir` to a file
system that does not stall, and check the store's disk as well.

## 11. Empty the WAL of an awake cell now

An awake cell moves its WAL into the database file when the `-wal` file passes
`wal: [checkpoint_bytes: ...]` (default 64 MiB). A cell that hibernates does the
same before it releases its lease. To free disk space without putting the cell
to sleep, for example when a monitor sees the disk filling up:

```elixir
:ok = Alambique.checkpoint(MyApp.Cells, "org-1")
```

The cell uploads what is pending, waits until the store has acknowledged every
upload, runs `wal_checkpoint(TRUNCATE)`, and keeps serving. It never truncates a
frame that is not yet replicated. It answers no other call meanwhile.

| Answer | Meaning | What to do |
| --- | --- | --- |
| `:ok` | The `-wal` file is empty. | Nothing. |
| `{:error, :busy}` | The uploads were not acknowledged within `deadline_ms` (default `wal: [checkpoint_deadline_ms: 10_000]`), or SQLite could not finish. The WAL stays, and the cell keeps serving. | Repeat the call later. If it keeps failing, check the store. |
| `:asleep` | Nobody owns the cell. It is not woken. | Nothing. Hibernation already emptied its WAL. |

`Alambique.checkpoint(MyApp.Cells, "org-1", deadline_ms: 30_000)` waits longer
for one call. The call goes to the node that owns the cell, like any other call.
The telemetry event `[:alambique, :cell, :checkpoint]` reports each checkpoint
(see [telemetry.md](telemetry.md)).

To keep the WALs of all awake cells under one limit, set
`wal: [max_bytes: 2_000_000_000]`. The default is `nil`, which means no budget.
A background round runs every `wal: [budget_interval_ms: 1_000]`. It adds up the
WAL sizes. If the sum is at or above the limit, it checkpoints the cell with the
largest WAL, then the next one, until the sum is below the limit. A cell that
answers `:busy` or `{:error, :disk_full}` waits for the next round.

- `[:alambique, :wal, :budget]` reports each round that acted (see
  [telemetry.md](telemetry.md)).
- `Alambique.cells(MyApp.Cells, details: true)` lists each cell with its
  `wal_bytes`.

The sum can exceed the limit by what the cells write in one interval.

## 12. Recover from a full disk

When the disk under `:data_dir` has no room, the calls that need it return
`{:error, :disk_full}`. These are writes, reads that need temporary space, and
wakes (a restore or a migration that cannot write its file). The call that gets
this error did not run.

1. Match on the atom, not on SQLite's message text:

   ```elixir
   case Alambique.execute(MyApp.Cells, cell, sql, params) do
     {:ok, result} -> result
     {:error, :disk_full} -> :retry_later
   end
   ```

2. Free space. The local files of hibernated cells are a cache of the store, so
   you can remove them. Or lower `:local_cache` `:max_bytes`, which evicts them
   (design section 7.1). Step 11 empties the WAL of an awake cell without putting
   it to sleep.
3. Repeat the call. A cell whose wake failed wakes again on the next call.

Expected result: the next write that fits is acknowledged with `{:ok, _}`. The
wake event shows `reason: :disk_full` (`[:alambique, :cell, :wake]`).

## 13. Change the options of an awake cell now

The per-cell options (`cell_options`, see `Alambique`) are read at each wake.
To apply a change to cells that are awake, without putting them to sleep:

```elixir
{:ok, :override, changed: [:cache_kib]} = Alambique.refresh_options(MyApp.Cells, "org-1")
```

The cell calls your function again, in a process of its own, and applies the
result: the page cache limit at once, and the idle timers from the time of the
cell's last call. It keeps serving meanwhile. The call goes to the node that
owns the cell.

| Answer | Meaning | What to do |
| --- | --- | --- |
| `{:ok, :override, changed: keys}` | The function returned options; `keys` are the ones whose value changed (`[]` when none did). | Nothing. |
| `{:ok, :node, changed: keys}` | The function returned `nil` or `[]`, or there is none: the cell uses the node's values. | Nothing. |
| `{:ok, :asleep}` | Nobody owns the cell. It is not woken; it reads the options at its next wake. | Nothing. |
| `{:error, {:cell_options, message}}` | The function raised (or asked for another `sql: [locking_mode:]`, which only a wake applies), did not return in `cell_options_timeout_ms`, or returned an invalid override, or the page cache could not be set. The cell keeps the options it had. | Fix the function and repeat the call. |

To refresh every cell awake on a node, for example after you changed the
function, call it with `:all` on each node:

```elixir
{:ok, %{cells: 120, refreshed: 120, changed: 7, asleep: 0, failed: 0, failures: []}} =
  Alambique.refresh_options(MyApp.Cells, :all, concurrency: 8)
```

At most `:concurrency` cells (default 8) are refreshed at a time. A cell that
fails keeps its options and is listed in `failures` with its reason; `changed`
counts the cells that got at least one new value.

The lookaside pair (`awake: [lookaside_bytes:, lookaside_count:]`) is stored and
listed in `changed`, and the cell uses it when its connection opens next, at a
wake or a reopen after an idle close.

A refresh is not a call to the cell: it does not keep it awake, does not give
back a shrunk cache and does not open a closed connection (the next request
that runs SQL does, with the new options). `[:alambique, :cell, :options,
:refresh]` reports each one (see [telemetry.md](telemetry.md)).

## Troubleshooting

- **`no store configured`**: see step 1.
- **`could not connect to NODE`**: check the node name (`name@host`, as the node
  prints it), the cookie, that `epmd` runs on both machines, and that the hosts
  resolve each other. Tasks that use only the store need none of this.
- **`--instance NAME is required`**: `move`, `relocate`, `cleanup` and
  `freeze --connect` call the running instance. Pass the name given to
  `{Alambique, name: ...}`.
- **`{:error, :unknown_node}` from `move` to a node that just started**: the node
  runs with `join: false` and has not called `Alambique.join/1` (step 9).
- **`{:error, :store_unavailable}` from calls**: the circuit breaker of the store
  is open. It opens after `breaker: [failures:]` failures in a row (5 by default:
  5xx answers, requests that got no answer, and connection errors). While it is
  open, it refuses requests without reaching the store for
  `breaker: [cooldown_ms:]` (5 000 by default, plus up to 20 %). Then it lets one
  request through, and it closes if that request succeeds.
  - Look for `[:alambique, :store, :breaker, :open]`. Its `reason` says whether
    failures or a probe opened it. See [telemetry.md](telemetry.md#store-events).
  - Look for `[:alambique, :store, :failure]`. It gives the `reason`, the
    `operation`, and whether the failure `counted` toward the breaker.
  - `Alambique.breaker_status(MyApp.Cells)` shows the state of each location
    (`:closed`, `:open` or `:half_open`), without asking the store. Use it for a
    health check. Each location has a second state under `feed`, for the
    objects of the change feed. If only `feed` is open, the store fails for
    those objects alone: writes and reads go on and the feed is published
    again when the store answers.
  - Check the store, not the node. Calls are safe to repeat once the store
    answers again.
  - Raise `breaker: [failures:]` if a store that fails now and then opens the
    breaker too often.
- **Writes wait, or time out, while the store fails**: a cell whose upload fails
  for a store reason keeps its batch and uploads it again. It waits
  `upload: [retry_backoff_ms:]` (500 by default) and doubles the wait up to
  `upload: [retry_backoff_max_ms:]` (5 000 by default), for as long as its lease
  is valid. Writers wait for it. A caller gets `{:error, :timeout}` when its own
  timeout passes. The write may still land, so treat its outcome as unknown. A
  caller gets `{:error, :busy}` when more than `awake: [max_pending:]` calls wait
  on the cell. Look for `[:alambique, :cell, :upload_retry]` events (see
  [telemetry.md](telemetry.md)). When the store stays down until the lease runs
  out, the cell stops with `{:sync_failed, reason}`, and its writers get
  `{:error, :outcome_unknown}`. The next call wakes the cell from the store.
- **`{:error, :frozen}` from a call**: unfreeze the cell (step 4). If you did not
  freeze it, look at its `Crashes:` line first. The instance may have frozen it.
- **Restore fails with `{:gap, txid}`**: a file in the middle of the chain is
  missing. This can happen for points older than the newest snapshot when a
  reaper round was cut short. Run `Alambique.Trash.reap/2`, or wait for the next
  round, to finish the deletion, then restore a newer point. A gap that is not
  at the tail means lost data. Restore `--at txid:N` with an `N` before the gap,
  and report it. A level-0 file missing among the newest 16 TXIDs gives no
  error. It reads as a tail (design.md section 6, "The tail").
- **Restore fails with `:history_truncated` or `{:history_coarse, txid}`**: the
  point is older than what retention keeps. Only newer points can be restored.
- **Calls for a cell fail at once with the same store error, and the store log
  shows few requests**: the wake failed because the store did. The node remembers
  the failure and waits before it retries, with `wake: [backoff_ms: 200,
  backoff_factor: 2, backoff_max_ms: 10_000, backoff_jitter: 0.2]` (design section
  7). Calls for the cell get the error until the delay ends. Then one call wakes
  the cell again. Each failure in a row makes the delay longer, up to the cap, and
  a successful wake resets it. Each delay is in the `delay_ms` of
  `[:alambique, :admission, :wake_backoff]`. Wait for the store to answer, or
  lower `backoff_max_ms` if a cell must retry sooner.
- **A cell fails to wake with `{:restore, {:epoch_regression, key, ...}}`**: a
  file of a lower epoch sits past the chain, and it is not a tail. A merged file,
  a snapshot, or a file more than 16 TXIDs past the head cannot be a tail. A
  lower-epoch level-0 file within that reach is ignored, and the next owner
  overwrites it. Inspect the object at `key`. Read the state before it with
  `mix alambique.restore --at txid:N`. See
  [design.md](design.md#6-replication-and-fencing), "The tail".

## Next steps

- [design.md](design.md) section 9 for the compaction and retention options
  (`:compaction` and `:retention` on `{Alambique, ...}`), section 9.1 for delayed
  deletions, section 6.1 for the scrub, and section 12 for the requirements.
- [telemetry.md](telemetry.md) for the events to watch, such as
  `[:alambique, :compaction, :pass]` and `[:alambique, :retention, :run]`.
