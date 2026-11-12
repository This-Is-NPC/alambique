# How to operate a cell

**The question:** how do I look at a cell in the store, restore it to a local
file, freeze it, deal with a cell the instance froze because it crashed the VM,
move it to another node or another location, get back files that retention
removed, read what the scrub found, restart a node without failed calls, check that the disk under the cells does not stall
writes, empty the WAL of an awake cell now, or tell that the disk is full and recover?

This guide stands alone. It needs the store and, for moving a cell and for
hibernating a frozen one at once, the running cluster. The background
reasoning (compaction, retention, the freeze marker) is in
[design.md](design.md) sections 9 and 10.

The command output below was produced on 2026-10-26 against the disposable
SeaweedFS 4.48 container from `mise run seaweedfs:up` (bucket `compact-demo`,
prefix `demo`) and two nodes, `a@127.0.0.1` and `b@127.0.0.1`, running one
instance named `demo` with the cell `org-1` (40 notes written through it).
Your hosts, keys and sizes will differ.

## Prerequisites

- A project that depends on `alambique`. Run the commands in its directory.
- Credentials for the S3 bucket the instance uses.
- For `move` and `freeze --connect`: the node names, the cluster cookie, and the
  instance name (the `:name` given to `{Alambique, ...}`).

## 1. Tell the tasks where the store is

The tasks use the store directly and start no instance. Give them the same
store and prefix the instance uses, in one of three ways. Flags win over
environment variables, which win over the config.

Environment variables (used for every command below):

```sh
export ALAMBIQUE_S3_ENDPOINT=http://127.0.0.1:18333
export ALAMBIQUE_S3_BUCKET=compact-demo
export ALAMBIQUE_S3_ACCESS_KEY_ID=alambique
export ALAMBIQUE_S3_SECRET_ACCESS_KEY=alambique-secret
export ALAMBIQUE_PREFIX=demo
```

Flags: `--endpoint`, `--bucket`, `--access-key-id`, `--secret-access-key`,
`--region` (default `us-east-1`), `--no-path-style` and `--prefix`. The
environment variable of `--no-path-style` is `ALAMBIQUE_S3_PATH_STYLE=false`.

Prefer the environment variables for the secret key and the cookie. A flag
is visible to every local user in the process list (`ps`) and may stay in the
shell history; a variable is not.

Config, which also accepts any other store module:

```elixir
config :alambique, :ops,
  store: {Alambique.Store.S3, %{endpoint: "...", bucket: "...", access_key_id: "...", secret_access_key: "..."}},
  prefix: "demo"
```

Without any of them a task stops and lists the three ways.

## 2. Look at a cell

```sh
mix alambique.inspect org-1
```

```text
Cell:       org-1
Prefix:     demo
Frozen:     no
Lease:      epoch 3, owner a@127.0.0.1, held, expires in 7.6 s
Head:       TXID 43, epoch 3, page size 4096
Segments:
  level 0: 43 files, TXID 1-43, 14.5 KiB
  level 1: 10 files, TXID 1-40, 3.6 KiB
  level 2: 2 files, TXID 1-32, 915 B
Snapshots:
  TXID 42, 693 B, epoch 1, state of 2026-10-26T05:59:34.431Z
```

The lease is `held` while its owner renews it, `expired` when the owner is
gone and nobody took over yet, `released` after a clean hibernation, and
`none` for a cell that never woke. The head is the newest TXID in the store.
Levels 1 and above are compacted copies of level 0 that restore uses to read
fewer files.

A `Crashes:` line follows `Frozen:` (`none`, or `3 recorded, 3 in the window`
for a cell that crashed the VM; see step 4). The output above was captured
before that line existed.

`--no-sizes` skips fetching every object to measure it, which is slow for cells
with many files. `--json` prints one JSON object with the same facts.

## 3. Restore a cell into a file

```sh
mix alambique.restore org-1 ./org-1.db
```

```text
Restored org-1 to ./org-1.db: TXID 43, epoch 3, 2 files applied
```

The result is an ordinary SQLite file; open it with `sqlite3 ./org-1.db`. The
cell is not touched, awake or not, and an existing file at the path is replaced.

`--at` picks the point: `latest` (default), `txid:N`, an ISO 8601 UTC time, or
`ms:N` for Unix milliseconds.

```sh
mix alambique.restore org-1 ./org-1-txid20.db --at txid:20
mix alambique.restore org-1 ./org-1-time.db --at 2026-10-26T05:59:34.415Z
```

```text
Restored org-1 to ./org-1-txid20.db: TXID 20, epoch 1, 2 files applied
Restored org-1 to ./org-1-time.db: TXID 24, epoch 1, 3 files applied
```

A time gives the last TXID stamped at or before it. Retention keeps every point
of its window (seven days by default) and refuses older ones once the reaper
deleted their files, with an error, never with an older state (see the
troubleshooting list). Until then, for `deletion_grace_ms` (one day by
default), the files are only marked and older points still restore (step 7).

## 4. Freeze a cell

A frozen cell refuses to wake, so nothing writes to it, for example while you
investigate it or run a repair. Reads of the store, restores and compaction are
not affected.

```sh
mix alambique.freeze org-1 --instance demo --connect a@127.0.0.1 --cookie demo
```

```text
Froze org-1
org-1 is asleep
```

`--connect` also asks the cluster to hibernate the cell now. Without it the
task only writes the marker, and an awake cell hibernates at its next lease
renewal, within a third of `:lease_ttl_ms`. A call to a frozen cell fails with
`{:error, :frozen}`:

```text
Frozen:     yes
Lease:      epoch 3, owner a@127.0.0.1, released
{:error, :frozen}
```

(The two lines above are lines 3 and 4 of `mix alambique.inspect org-1`, and the
result of `Alambique.query(:demo, "org-1", "SELECT count(*) FROM notes")` on
node `a`.)

Undo it:

```sh
mix alambique.unfreeze org-1
```

```text
Unfroze org-1
```

From inside the application the same is `Alambique.freeze(MyApp.Cells, cell)`,
`Alambique.unfreeze/2` and `Alambique.frozen?/2`.

### A cell the instance froze

An instance freezes a cell by itself when it keeps crashing the VM: after
`:threshold` crashes (3 by default) within `:window_ms` (one hour) it writes the
same freeze marker, logs an error and emits `[:alambique, :cell, :quarantined]`
(see [telemetry.md](telemetry.md)). Alert on that event. The other cells of the
node are not affected. To handle one:

1. Look at it. `mix alambique.inspect CELL` shows `Frozen: yes` and a `Crashes:`
   line, `N recorded, M in the window`. From the application,
   `Alambique.crashes(MyApp.Cells, cell)` returns the counter: `total`, and
   `recent` with the time, node and kind of work in flight for each crash (a
   `cell` is the cell process itself; `compaction`, `query`, `hydrator` and
   `relocate` are the other work on its data).
2. Take a copy to reproduce the crash away from production:
   `mix alambique.restore CELL ./CELL.db` (step 3). The cell is frozen, so
   nothing writes to it meanwhile.
3. Fix what crashed, for example the statement the application sends or the
   version of the code, and deploy it.
4. Unfreeze it: `mix alambique.unfreeze CELL`, or `Alambique.unfreeze/2`. This
   also forgets the recorded crashes, so the cell starts counting from zero. If
   it crashes the VM again it is frozen again after `:threshold` more crashes.

The crashes are recorded when the node that had the cell starts again. If the
store was not reachable then, the instance keeps the record on the node's disk
(`<data_dir>/inflight`) and tries again every `:retry_ms` (30 seconds). To
change the numbers or turn the feature off, see `:quarantine` in the
documentation of the `Alambique` module.

## 5. Move a cell to another node

```sh
mix alambique.move org-1 b@127.0.0.1 --instance demo --cookie demo
```

```text
Moved org-1 to b@127.0.0.1
```

The current owner hibernates the cell and `b@127.0.0.1` wakes it; the lease
shows the new owner and a higher epoch:

```text
Lease:      epoch 5, owner b@127.0.0.1, held, expires in 8.6 s
```

Moving needs the Erlang cluster, not only the store. The task starts
distribution under a random name (long names when the target has a dot after
the `@`), sets the cookie (`--cookie`, else `ALAMBIQUE_COOKIE`, else the user's `~/.erlang.cookie`), connects to
`--connect NODE` (default: the target) and calls `Alambique.move/4` there. The
target must run the instance. Use `--timeout MS` for slow cells.

## 5.1 Take over a cell whose owner is down

If a failure detector suspects a node is gone, read the lease without waking
the cell and pass the epoch to `take_over/4`, so two nodes cannot take the
same cell from each other:

```elixir
{:ok, %{owner: _node, owner_id: owner_id, epoch: epoch, released: false}} =
  Alambique.lease(:demo, "org-1")

{:ok, served_by} =
  Alambique.take_over(:demo, "org-1", :owner_down, epoch: epoch)
```

`owner` is the node when it still runs the instance; `owner_id` is the raw
owner id in the lease, useful while the detector still names the failed node.
A released lease (`released: true`, after a clean hibernation) needs no epoch.
A moved lease reports `moved_to`; a cell that never had a lease answers epoch
0 with `owner: nil`. `Alambique.lease/3` does not wake or freeze the cell, and
`:timeout` bounds the read. The old `mix alambique.inspect` output still shows
the same lease.

## 6. Move a cell to another location

Where a cell lives (which store and prefix) comes from the placement of the
instance (design §18). To move a cell to another location of it, give the id of
the destination as the placement lists it:

```sh
mix alambique.relocate org-1 us --instance demo --connect a@127.0.0.1 --cookie demo
```

```text
Moved org-1 to location us
```

The format above comes from the task's code. The same call from a console, run
against two in-memory stores named `eu` and `us` (observed):

```elixir
{:ok, %Alambique.Location{id: "us", prefix: "demo"}} = Alambique.relocate(:demo, "org-1", "us")
{:ok, %{columns: ["body"], rows: [["hi"]]}} = Alambique.query(:demo, "org-1", "SELECT body FROM notes")
```

Afterwards the old location holds a released lease marked `moved_to` the new
one, and the new location holds the data and a held lease with a higher epoch
(`Alambique.Ops.describe/3` of each, `lease` and `head`):

```text
eu: owner relocate, released, epoch 2, moved_to "us", head TXID 4
us: owner nonode@nohost, held, epoch 4, moved_to nil, head TXID 5
```

While it runs, calls to the cell fail with `{:error, {:unavailable,
"relocate", _}}`. If the task fails or is interrupted, run it again with the
same arguments: it finishes from where it stopped. A node that still has the
old location cached finds the marker at the old location and asks the
placement again, so no restart is needed.

After the move the task reports what happened to the objects the cell left at
the old location (format from the task's code; with the default grace period
it says this):

```text
Moved org-1 to location us
Location eu keeps the objects of org-1 until 2026-10-26T12:10:01Z; the instance deletes them then, or run: mix alambique.cleanup org-1 eu
```

Those objects stay for `moved_grace_ms` (instance option, default ten minutes),
so that a query or restore that started before the move still finds its files.
Then the instance marks the segments and snapshots for deletion and the reaper
deletes them another `deletion_grace_ms` later (one day by default; step 7).
The frozen marker, the crash counter, the retention marker and the schema index
entries go at once, at cleanup time: they belong to the old residency. The
compactor does this. `relocate` queues the cleanup, and `relocate` also leaves
a marker for it in the old location, so a node that restarts (or another node)
finds the pending cleanup by its periodic scan (`compaction: [moved_scan_ms:]`,
default 60 000) and waits for the grace period counted from the move, not from
the restart. Nothing is needed by hand. To finish a cleanup early, or when
compaction is off, run `mix alambique.cleanup` or
`Alambique.cleanup_moved/4`:

```sh
mix alambique.cleanup org-1 eu --instance demo --connect a@127.0.0.1 --cookie demo
```

```text
Cleaned 7 objects of org-1 at location eu (deleted after deletion_grace_ms)
```

(The count varies with the cell's history.)

`--grace-ms 0` frees the storage early, at the risk of failing a reader that
started before the move. The lease of the old location stays, marked
`moved_to`: nodes that still have the old location cached read it and ask the
placement again, so they never write there. Cleaning up the location where the
cell lives is refused (`:live_location`).

A cell can move back to a location it left, and again, as often as needed.
The old location must be cleaned up first, and the cleanup must be a few
operation timeouts old (eight times `op_timeout_ms`), so a slow cleanup cannot
delete the new objects. It does not have to wait for `deletion_grace_ms`: the
marked segments and snapshots may still be there, because the returning
snapshot has a TXID above them and covers them; the reaper deletes them later.
Until the cleanup is ready `relocate` answers:

- `{:error, :destination_not_cleaned}`: no cleanup finished there, usually
  because the grace period is still running. The call queues the cleanup in
  the compactor; `relocate` again once the grace period has passed. With
  compaction off, run the cleanup yourself first.
- `{:error, {:destination_settling, ready_at_ms}}`: the cleanup finished but
  is too recent. Wait until `ready_at_ms` (Unix milliseconds) and run
  `relocate` again. Do not clean again: that restarts the wait.
- `{:error, :destination_not_empty}`: data is there but the lease does not say
  the cell moved (for example a store where the lease was lost). Inspect the
  location before deciding.
- `{:error, {:destination_ahead, txid}}`: a data file ends beyond the restored
  state. The move refuses instead of colliding with a writer it does not know.
  Find and remove that file, then run the cleanup and `relocate` again.

Epochs keep growing across the moves. A placement without
`commit_move/4` answers `{:error, :relocation_unsupported}`. If a move to one
location did not finish, a move to the other answers `{:error, {:move_pending,
id}}`: run the first one again to the end.

## 7. Get back files that retention removed

Retention and the cleanup of a moved cell do not delete at once: they mark the
files, and the reaper deletes them `deletion_grace_ms` later (one day by
default; design section 9.1). During that time:

1. **Restore a point before the window.** `mix alambique.restore` still sees
   the marked files, so `mix alambique.restore org-1 ./old.db --at txid:7` works
   for a point retention already covered. Once the file is restored, copy out
   what you need.
2. **Keep the files.** From a console on a node of the instance, list the
   pending deletions and cancel the ones you want to keep:

   ```elixir
   {:ok, %{entries: entries}} = Alambique.Trash.pending(:demo)
   # each entry: %{key: ..., due_ms: ..., cell: "org-1"}
   :ok = Alambique.Trash.cancel(:demo, hd(entries).key)
   ```

   Without a console, delete the object `<prefix>/trash/<due>/<cell>/<id>`
   with your S3 tool. A cancelled manifest is never processed. Retention marks
   its files again at its next run that finds them covered: set
   `retention: false` (or a longer `pitr_window_ms`) first if you want them to
   stay.
3. **Check what is left.** `Alambique.Trash.pending/2` shows what waits and
   until when; `Alambique.Trash.reap/2` runs a round now.

After the due time the files are gone and the usual rules apply. If the store
has object versioning, they may still be recoverable there
([storage-compatibility.md](storage-compatibility.md)).

## 8. Read the scrub results

With `scrub: [...]` set (or `Alambique.scrub(:demo)` from a console) the
library restores sampled cells and checks them (design section 6.1). It
returns, and emits as telemetry, one result per cell:

```elixir
{:ok, summary} = Alambique.scrub(:demo)
# %{cells: 10, ok: 9, empty: 0, failed: 1,
#   failures: [%{cell: "org-7", location: "default", reason: {:restore, {:gap, 12}}}]}
```

Every failed cell is also logged as an error: `Alambique scrub: replica of
org-7 at default is damaged: ...`. Watch `[:alambique, :scrub, :cell]` for
`result: :error`. The `reason` says what broke:

- `{:restore, reason}`: a file is missing, damaged or out of order; the reason
  is the one `mix alambique.restore` prints, such as `{:gap, txid}`. Restore
  `--at txid:N` with an `N` before the problem to read the last good state, and
  do not delete anything.
- `{:checksum_mismatch, expected, found}`: the files restored, and the result
  does not have the checksum the chain ends with.
- `{:integrity_check, findings}` or `{:quick_check, findings}`: the files
  restored and SQLite found the database damaged; the findings are its lines.
  The cell was probably damaged before it was replicated.

The scrub never freezes or repairs a cell. `empty` is a cell in the schema
index without objects in the store, for example one that moved away.

## 9. Restart a node without failed calls

A node that starts takes part in routing at once: other nodes can forward
calls to it before the processes your application needs to serve them are
running. To keep it out until it is ready, start the instance with
`join: false` and join when everything is up.

1. Before stopping the node, drain it, or just stop the instance, which runs
   the same drain (design section 8):

   ```elixir
   {:ok, %{hibernated: _, errors: []}} = Alambique.drain(MyApp.Cells)
   ```

   Other nodes stop choosing it and serve its cells from their released
   leases.

2. Start the new release with the option, and list `Alambique` first in the
   supervision tree, before the processes that depend on it:

   ```elixir
   {Alambique, name: MyApp.Cells, join: false, ...}
   ```

   Until step 3 the node is not a member: no other node prefers it, picks it
   as an owner or lists it for `Alambique.move/4`. It still serves the calls
   made on it, which another member runs when it has to wake a cell; a cell
   whose lease it holds stays reachable from other nodes.

3. After your own processes started (put this in the last child of the tree
   or in `Application.start/2` after `Supervisor.start_link/2`), join:

   ```elixir
   :ok = Alambique.join(MyApp.Cells)
   ```

   It returns `:ok` at once on a member, so it is safe to repeat. After a
   `drain/2` it also ends the drain: the node wakes cells again.

Expected result: after a moment other nodes route new wakes to the node, and
`Alambique.move(MyApp.Cells, cell, node)` no longer answers
`{:error, :unknown_node}` for it. A node that never calls `join/1` only
serves the calls made on it.

## 10. Check that the disk does not stall writes

Every write waits for its SQLite commit on the node's disk and for its upload to
the store. If the file system under either one stalls writes, all the cells
stall with it. On btrfs we measured stalls of 1 to 3 s that hit every cell of a
node, and every node that shares the store, at once. Check your own disks
before you trust a latency figure.

Run the bench on a disk of the kind your `:data_dir` uses. `TMPDIR` picks the
disk for the cell files (`/tmp` may be a tmpfs):

```sh
TMPDIR=/path/on/the/disk/under/test ALAMBIQUE_BENCH_RATES=400 mise run bench:stall -- 60
```

It writes to 12 cells at a fixed rate and prints one line per second: the
requests, p50, p99 and max latency, the requests over 1 s, and what can explain
a stall, including the threads of the VM blocked in the kernel (`Dthr`). To
include the store, start the disposable one with `mise run seaweedfs:up` and add
`ALAMBIQUE_BENCH_STORE=seaweedfs`; the bench then does not test the disk of the
store's data, which is in the container. Stalls are intermittent: a 60 s run on
a disk that stalls can still show none, so repeat it, and give the VM fewer
CPUs and dirty IO threads to make them easier to see, for example
`ELIXIR_ERL_OPTIONS="+SDio 4" taskset -c 0,1`.

A clean run, with the cell files on btrfs and the SeaweedFS store (60 s at 400
writes per second, 2 CPUs with a 150 % quota, `+SDio 4`, measured on
2026-11-03):

```text
summary: n=24017 failed=0 p50=2.7 p99=21.6 p999=34.2 max=71.5 ms, over 1000 ms: 0
```

The same command with `TMPDIR=/tmp`, a tmpfs, on the same host and shortly
after:

```text
summary: n=23867 failed=0 p50=7.5 p99=135.8 p999=210.5 max=291.3 ms, over 1000 ms: 0
```

These two runs do not show a difference: the host was busy with other work, and
the btrfs run did not stall. A run that did stall (an earlier build of the
bench, same settings, 90 s, cell files on btrfs, SeaweedFS store) printed:

```text
  84   400    6.2 2758.7 2799.6   21 |  10/  1/  0 |   1981/ 121.1    405/   3.4     0 | 2793.0 *****   0.0 2742.5  3131.7 ***** |  2748.5 put:ltx
  85   417 2275.5 2725.9 2762.2  417 |   1/  0/  0 |   1173/ 121.1     78/   3.4     0 | 2728.9 *****   0.0    0.0  2717.4 ***** |  2718.9 put:ltx
  86   370 1239.5 1733.0 1740.5  271 |   1/  0/  0 |   1026/ 121.1      0/   3.4     0 |    0.0   0.0   0.0    0.0     0.0   0.0 |     0.0 -
summary: n=35946 failed=0 p50=3.4 p99=1987.4 p999=2712.5 max=2799.6 ms, over 1000 ms: 709
```

The columns after the latencies are the run queues, the NIF calls and their
longest, the longest commit, sync, checkpoint, lease, compaction and feed spans,
and the longest store request (`put:ltx` here: the upload took 2.7 s). The
measurements behind this advice are in
[design.md section 12](design.md#12-requirements-and-measurements).

**What to do.** Put `:data_dir` and the store's data on a file system that does
not stall writes this way. Both: moving only one did not help. We measured
tmpfs only (a lab setup that loses the data at a reboot). ext4 and xfs are the
usual choice, but we did not measure them. We also did not test nodatacow
volumes for the store, or btrfs mount options. If you cannot move the disk,
expect every acknowledged write to wait out the stall.

## 11. Empty the WAL of an awake cell now

An awake cell moves its WAL into the database file when the `-wal` file passes
`wal: [checkpoint_bytes: ...]` (default 64 MiB), and a hibernating cell does
it before it releases. To free the disk sooner, for example when a monitor sees
the disk filling, without putting the cell to sleep:

```elixir
:ok = Alambique.checkpoint(MyApp.Cells, "org-1")
```

The cell uploads what is pending, waits until the store has acknowledged every
upload, runs `wal_checkpoint(TRUNCATE)`, and keeps serving. It truncates no
frame that is not yet replicated. It answers no other call meanwhile.

| Answer | Meaning | What to do |
| --- | --- | --- |
| `:ok` | The `-wal` file is empty. | Nothing. |
| `{:error, :busy}` | The uploads were not acknowledged within `deadline_ms` (default `wal: [checkpoint_deadline_ms: 10_000]`), or SQLite could not finish. The WAL stays and the cell serves. | Repeat later; look at the store if it persists. |
| `:asleep` | Nobody owns the cell. It is not woken. | Nothing: hibernation already moved its WAL. |

`Alambique.checkpoint(MyApp.Cells, "org-1", deadline_ms: 30_000)` waits longer
for one call. The call goes to the node that owns the cell, as any other. The
telemetry event `[:alambique, :cell, :checkpoint]` reports each checkpoint
([telemetry.md](telemetry.md)).

## 12. Recover from a full disk

When the disk under `:data_dir` has no room, the calls that need it return
`{:error, :disk_full}`: writes, reads that need temporary space, and wakes (a
restore or a migration that cannot write the file). The write did not run. Cells
that are awake keep serving reads and the writes that fit, and nothing they
acknowledged is lost, because the store holds it.

1. Match on the atom, not on SQLite's text:

   ```elixir
   case Alambique.execute(MyApp.Cells, cell, sql, params) do
     {:ok, result} -> result
     {:error, :disk_full} -> :retry_later
   end
   ```

2. Free space. The local files of hibernated cells are a cache, so you can
   remove them, or lower `:local_cache` `:max_bytes`, which evicts them
   ([design.md](design.md) section 7.1). Section 11 empties the WAL of an
   awake cell without putting it to sleep.
3. Repeat the call. A cell whose wake failed wakes again on the next call.

Expected result: the next write that fits is acknowledged with `{:ok, _}`. The
wake event shows `reason: :disk_full` (`[:alambique, :cell, :wake]`).

## Troubleshooting

- **`no store configured`**: see step 1.
- **`could not connect to NODE`**: check the node name (`name@host`, as the
  node prints it), the cookie, that `epmd` runs on both machines, and that the
  hosts resolve each other. The tasks that only use the store do not need any
  of this.
- **`--instance NAME is required`**: `move` and `freeze --connect` call the
  running instance; pass the name given to `{Alambique, name: ...}`.
- **`{:error, :unknown_node}` from `move` to a node just started**: the node
  runs with `join: false` and has not called `Alambique.join/1` (step 9).
- **`{:error, :frozen}` from a call**: unfreeze the cell (step 4). If you did
  not freeze it, look at its `Crashes:` line first: the instance may have frozen
  it.
- **Restore fails with `{:gap, txid}`**: a file in the middle is missing. It
  happens for points older than the newest snapshot when a reaper round was cut
  short. Run `Alambique.Trash.reap/2` (or wait for the next round) to finish
  the deletion, and restore a newer point. A gap in the chain that is not a
  tail means lost data: restore `--at txid:N` with an `N` before the gap and
  report it. A level-0 file
  missing among the newest 16 TXIDs gives no error, because it reads as a tail
  (design.md section 6, "The tail").
- **Restore fails with `:history_truncated` or `{:history_coarse, txid}`**: the
  point is older than what retention keeps. Only newer points can be restored.
- **A cell fails to wake with `{:restore, {:epoch_regression, key, ...}}`**: a
  file of a lower epoch sits past the chain and it is not a tail (a merged file,
  a snapshot, or a file more than 16 TXIDs past the head), so the tail rule
  cannot explain it. A lower-epoch level-0 file within that reach is ignored
  and the next owner overwrites it, so it never gives this error. Retention
  does not delete such files. Inspect the object at `key`, and read the state
  before it with `mix alambique.restore --at txid:N`. See
  [design.md](design.md#6-replication-and-fencing), "The tail".

## Next steps

- [design.md](design.md) section 9 for the compaction and retention options
  (`:compaction` and `:retention` on `{Alambique, ...}`), 9.1 for delayed
  deletions, 6.1 for the scrub, and section 12 for what they cost.
- [telemetry.md](telemetry.md) for the events to watch (`[:alambique,
  :compaction, :pass]`, `[:alambique, :retention, :run]`).
