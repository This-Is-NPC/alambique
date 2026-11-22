# How to swap a node for another

**The question:** how do I replace node A by node B, with B started beside A
and no cluster between them, without losing or repeating a write, and what do
callers see while it happens?

This guide stands alone. It needs the instance of
[how-to-call-cells.md](how-to-call-cells.md) running on both nodes. To restart
one node inside a cluster, see
[how-to-operate-a-cell.md](how-to-operate-a-cell.md), section 9. This page is
for two separate instances that share only the store.

The two nodes are not a cluster. They never forward calls to each other and
need no Erlang distribution. They coordinate through the leases in the store
([design.md](design.md), section 5): a cell has one holder at a time, and a
released lease is taken at once.

## Prerequisites

- The same version of the application, and the same `:store`, `:prefix`,
  `:placement` and `:migrations`, on A and B.
- A distinct `:data_dir` for each node. A start on a directory that another
  live instance holds fails with `{:error, {:data_dir_in_use, path}}`.
- A router or load balancer in front that can send a request to B first and
  retry a refused one after a delay, such as a `503` with `Retry-After`.
- If you stop A with a signal, a stop grace period longer than the drain: for
  example `docker stop --timeout`, or the equivalent of your orchestrator. The
  drain is bounded at 60 s by default (step 3).

`node_id` may be the same on both nodes. It does not have to be unique: every
start of an instance also draws a random incarnation, and the lease names both
([design.md](design.md), section 5). See Troubleshooting.

If the instances use the `registry` option, one `node_id` is one registry
entry. B takes it over from A at once, and A leaves the list of peers. Use
different ids if both should stay visible
([how-to-discover-nodes.md](how-to-discover-nodes.md)).

## 1. Start B beside A

Start the instance on B as on A. Nothing changes on A.

```elixir
{Alambique, name: MyApp.Cells, store: {Alambique.Store.S3, s3_config},
 prefix: "myapp", data_dir: "/var/lib/myapp/cells-b", migrations: migrations}
```

B wakes each cell from the store on its first request. For a cell that A
holds, B does not wait and does not take it. It answers at once:

```elixir
{:error, {:unavailable, "node-a", expires_at_ms}} =
  Alambique.query(MyApp.Cells, "org-1", "SELECT 1")
```

`"node-a"` is the `node_id` in the lease, and `expires_at_ms` is the time the
lease expires. The call did not run, so a write that got this answer is safe to
send again. A cell that nobody holds, or whose lease was released, is woken by
B as usual.

If B must not receive calls from a cluster before it is ready, start it with
`join: false` and call `Alambique.join/1` when it is ready
([how-to-operate-a-cell.md](how-to-operate-a-cell.md), section 9).

## 2. Send traffic to B and retry what it refuses

Make your router try B first. When B answers `{:unavailable, owner,
expires_at_ms}`, return a `503` with a `Retry-After`. The expiry is an upper
bound. A releases the lease when it drains, usually well before that. Read the
lease on each refusal. It is one store read:

```elixir
retry_after_ms =
  case Alambique.lease(MyApp.Cells, "org-1") do
    {:ok, %{released: true}} -> 0
    {:ok, %{expires_at_ms: expires}} ->
      min(max(0, expires + skew_margin_ms - System.os_time(:millisecond)), 1_000)
    {:error, _} -> 1_000
  end
```

`skew_margin_ms` is the instance option (default 2 000). The cap of one second
makes clients retry often enough to see the release when it comes.

## 3. Drain A

On A, hand the cells over:

```elixir
{:ok, %{hibernated: 40, errors: []}} = Alambique.drain(MyApp.Cells)
```

For each cell, A writes what is pending, compacts a long tail of level-0 files,
releases the lease explicitly and hibernates the cell. B then wakes the cell
and reads a short chain. The defaults:

| Setting | Default | Effect |
| --- | --- | --- |
| `compaction: [release_min_tail:]` | 32 | A tail shorter than this is not compacted. |
| `compaction: [release_deadline_ms:]` | 3 000 | Time one release may spend compacting. A cell whose compaction reaches the deadline is handed over anyway. |
| `compaction: [release_reserve_ms:]` | 2 000 | A cell with less time left than this does not compact. |
| `compaction: [release_concurrency:]` | 4 | Cells compacted at once on the node. The rest wait for a place or are handed over. |
| `drain: [concurrency:]` | 64 | Cells released at once. |
| `drain: [deadline_ms:]` | 60 000 | End of the whole drain. The `deadline:` option of `drain/2` replaces it for one call. |
| `drain: [shutdown_ms:]` | 75 000 | Budget that a stop by signal gives the drain. |

Each compaction stops at `release_deadline_ms` or `release_reserve_ms` before
the end of the time the drain has left, whichever comes first. A cell with less
time left does not compact at all. Cells beyond the concurrency limit are
released in waves. A cell that gets no place in time is handed over with its
chain as it is.

When the drain ends, A no longer accepts cells. From the moment a lease is
released, B serves its cell on the next request. A call that reaches A now
gets `{:error, {:unavailable, owner, expires_at_ms}}` for a cell that B holds,
and `{:error, :draining}` for a cell that nobody holds.

A stop by signal also drains. `SIGTERM` makes the BEAM stop the applications,
and stopping the instance runs the same drain with the same settings. Nothing
else is needed on A, but the orchestrator must wait at least `shutdown_ms`.

Read the progress from another process with `Alambique.drain_status/1`:

```elixir
%{draining: true, running: false, cells_at_start: 40, done: 40,
  errors: [], left: 0} = Alambique.drain_status(MyApp.Cells)
```

The drain events are listed in [telemetry.md](telemetry.md), "Drain events".
Stop A when `errors` is empty and `left` is `0`.

## 4. Know what the drain reports

- `errors: []`: every cell was written, released and hibernated. Stop A.
- `{cell, {:release_failed, reason}}`: the cell is asleep and its data is
  durable, but the store refused to release the lease. B answers `:unavailable`
  for it until the lease expires, `lease_ttl_ms` plus `skew_margin_ms` after the
  last renewal (32 s with the defaults). Call `Alambique.hibernate/3` again, or
  wait.
- `{cell, :deadline}`: the drain ran out of its deadline. The cell is still awake
  on A, and A still serves it. The drain does not kill it or hibernate it by
  force. Call `drain/2` again, or stop A. Both hibernate what is left. If
  neither happens, the lease expires and B takes the cell.
- `{cell, :timeout}` or another reason: one cell failed. The cell may still have
  hibernated afterwards. Call `drain/2` again.

## What callers see

During a swap, callers get three kinds of answer:

- A normal result, from A or from B.
- `{:error, {:unavailable, owner, expires_at_ms}}`, from B, while A still holds
  the cell. The call did not run. Retry it after the `Retry-After` of step 2.
- `{:error, :draining}`, from A or B, for a cell that nobody holds during the
  drain. Retry it the same way.

A refused call did not run, so retrying it applies the write once. In a swap
that goes as planned, the lease errors above are the only errors a client sees.

## Troubleshooting

- **B answers `{:unavailable, ...}` for 30 s or more after A stopped.** A did
  not release the lease. It was killed, the drain reached its deadline, or the
  release failed. Read `Alambique.lease/3` on B: `released: false` and the
  expiry show how long is left. If you know that A is gone,
  `Alambique.take_over/4` on B replaces the lease at once
  ([design.md](design.md), section 5.1).
- **A and B have the same `node_id`.** This is safe. The default `node_id` of a
  VM without distribution is `nonode@nohost`. B treats A's lease as another
  instance's and refuses until A releases it. The owner string that `lease/3`
  shows is the same for both, so tell them apart by the data directory or your
  logs. A crashed node that restarts with the same id is a new instance too. It
  waits for its old leases to expire (`lease_ttl_ms` plus `skew_margin_ms`)
  instead of taking them back. A drain avoids that wait.
- **`docker stop` kills A before the drain ends.** The orchestrator's timeout is
  shorter than the drain. Raise it above `drain: [shutdown_ms:]`, or lower
  `drain: [deadline_ms:]`. Cells still held expire as after a crash.
- **Requests for a cell fail with `{:error, :draining}` on B.** B is draining
  itself, for example after its own stop. A node started with `join: false` is
  not draining.
- **The first calls on B take seconds.** B restores the chain that each cell had
  when A released it. A chain of N files costs about N GET requests. A busy A
  compacts less, so its chains are longer. See
  [a node swap with a saturated writer](how-to-speed-up-a-wake.md#a-node-swap-with-a-saturated-writer)
  for how to check the chains and for the `compaction: [concurrency:, urgent_concurrency:]`
  options.
- **A cell's first wake on B reads many level-0 files.** On a saturated CPU the
  compaction at release can be cut by `release_deadline_ms`, and B reads the tail
  that was left ([design.md](design.md), section 9). The
  `[:alambique, :cell, :hibernate]` event of A then has `compaction: :deadline`
  and a `tail` above 0. To shorten the tail, raise `compaction: [release_deadline_ms:]`,
  at the cost of a longer drain. Keep `drain: [deadline_ms:]` above it. For
  cells that you move one by one, give the call its own budget with
  `Alambique.move(inst, cell, node, timeout: 15_000, release_deadline_ms: 10_000)`.
  The `release_deadline_ms` of the call replaces the instance value for that move
  only.
- **A drain returns `hibernated: 0`, and the cells are awake on another node.**
  `drain/2` acts only on the cells that have a process on this node.

## Next steps

- Restart a node inside a cluster: [how-to-operate-a-cell.md](how-to-operate-a-cell.md), section 9.
- Why a lease is fenced, and what the incarnation is: [design.md](design.md), sections 5 to 8.
- The drain events: [telemetry.md](telemetry.md), "Drain events".
