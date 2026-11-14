# How to swap a node for another

**The question:** how do I replace node A by node B, with B started beside A
and no cluster between them, without losing or repeating a write, and what do
callers see while it happens?

This guide stands alone. It needs the instance of
[how-to-call-cells.md](how-to-call-cells.md) running on both nodes. Restarting
one node in a cluster is in
[how-to-operate-a-cell.md](how-to-operate-a-cell.md), section 9; this page is
for two separate instances that share only the store.

The two nodes are not a cluster. They never forward calls to each other and
need no Erlang distribution. They coordinate through the leases in the store
(design section 5): a cell has one holder at a time, and a lease that is
released is taken at once.

## Prerequisites

- The same version of the application, and the same `:store`, `:prefix`,
  `:placement` and `:migrations`, on A and B.
- A distinct `:data_dir` for each node. A start on a directory that another
  live instance holds fails with `{:error, {:data_dir_in_use, path}}`.
- A router or load balancer in front that can send a request to B first and
  retry a refused one after a delay, such as a `503` with `Retry-After`.
- A stop grace period longer than the drain, if you stop A with a signal:
  `docker stop --timeout` or the orchestrator's equivalent. The default
  drain is bounded at 60 s (step 3).

`node_id` may be the same on both nodes. It is not required to be unique:
every start of an instance also draws a random incarnation, and the lease
names both (design section 5). See "Troubleshooting".

If the instances use the `registry` option, one `node_id` is one registry
entry: B takes it over from A at once, and A leaves the list of peers. Use
different ids if both should stay visible
([how-to-discover-nodes.md](how-to-discover-nodes.md)).

## 1. Start B beside A

Start the instance on B as on A. Nothing else changes on A.

```elixir
{Alambique, name: MyApp.Cells, store: {Alambique.Store.S3, s3_config},
 prefix: "myapp", data_dir: "/var/lib/myapp/cells-b", migrations: migrations}
```

B wakes each cell from the store on its first request. For a cell A holds, B
does not wait and does not take it. It answers at once:

```elixir
{:error, {:unavailable, "node-a", 1793929098927}} =
  Alambique.query(MyApp.Cells, "org-1", "SELECT 1")
```

`"node-a"` is the `node_id` in the lease and the number is its `expires_at_ms`.
The call did not run, so a write that got this answer is safe to send again.
A cell nobody holds, or whose lease was released, is woken by B as usual.

If B must not receive calls from a cluster before it is ready, start it with
`join: false` and call `Alambique.join/1` when it is ready
([how-to-operate-a-cell.md](how-to-operate-a-cell.md), section 9).

## 2. Send traffic to B and retry what it refuses

Make your router try B first. When B answers `{:unavailable, owner,
expires_at_ms}`, return a `503` with a `Retry-After`. The expiry is an upper
bound, not a prediction: A releases the lease when it drains, which is
usually much earlier. Read the lease on each refusal; it is one store read
(11 µs in memory, one request of latency in a real store: design section 8,
measured by `mise run bench:lease`):

```elixir
retry_after_ms =
  case Alambique.lease(MyApp.Cells, "org-1") do
    {:ok, %{released: true}} -> 0
    {:ok, %{expires_at_ms: expires}} ->
      min(max(0, expires + skew_margin_ms - System.os_time(:millisecond)), 1_000)
    {:error, _} -> 1_000
  end
```

`skew_margin_ms` is the instance's option (default 2 000). The cap of one
second keeps clients retrying often enough to see the release when it comes.

## 3. Drain A

On A, hand the cells over:

```elixir
{:ok, %{hibernated: 40, errors: []}} = Alambique.drain(MyApp.Cells)
```

For each cell A writes what is pending, compacts a long level-0 tail
(`compaction: [release_min_tail:]`, default 32 level-0 files, for at most
`compaction: [release_deadline_ms:]`, default 3 000 ms), releases the lease
explicitly and hibernates. B then wakes the cell on a short chain; a cell
whose compaction hit the deadline is handed over anyway, and its
`[:alambique, :cell, :hibernate]` event says `compaction: :deadline`. First it leaves the members and refuses to wake any more cells.
Forty cells took 12 ms in the swap test. From the moment a lease is released,
B serves its cell on the next request. A call that reaches A now gets
`{:error, {:unavailable, owner, expires}}` for a cell B holds and
`{:error, :draining}` for one nobody holds.

The drain has a bound. Each compaction stops after `release_deadline_ms`, or
`compaction: [release_reserve_ms:]` (default 2 000 ms) before the end of the
time the drain has left, whichever comes first; a cell with less than that left does not compact
(`compaction: :none`). The drain ends within `drain: [deadline_ms:]` (default
60 000 ms). Up to `drain: [concurrency:]` cells (default 64) are released at
once, so their compactions run in parallel and the drain takes about the
longest one, at most `release_deadline_ms` plus the release; more cells go in
waves. On one CPU the parallel compactions share it, but stay under the same
deadlines. Twelve cells on one CPU (four with a tail of 200 files) took 5.2 s.

If the node ends by a signal, `SIGTERM` is enough. The library installs no
handler: the BEAM's default handling of `SIGTERM` calls `:init.stop/0`, which
stops the applications (unless your release overrides it), and stopping the
instance runs the same drain with the same settings, before the cells
supervisor, the registry and the groups go. Nothing else is needed on A, but
the signal gives the drain `:shutdown_ms` (default 75 000) and the
orchestrator must wait that long.

Read the progress from another process with `Alambique.drain_status/1`, or
attach to the telemetry events (`docs/telemetry.md`, "Drain events"):

```elixir
%{draining: true, running: false, cells_at_start: 40, done: 40,
  errors: [], left: 0} = Alambique.drain_status(MyApp.Cells)
```

Stop A when `errors` is empty and `left` is `0`.

## 4. Know what the drain reports

- `errors: []`: every cell was written, released and hibernated. Stop A.
- `{cell, {:release_failed, reason}}`: the cell is asleep and its data is
  durable, but the store refused to release the lease. B gets `:unavailable`
  for it until the lease expires, `lease_ttl_ms` plus `skew_margin_ms` (32 s
  by default) after its last renewal. Call `Alambique.hibernate/3` again or
  wait.
- `{cell, :deadline}`: the drain ran out of `:deadline_ms` (default 60 000;
  `deadline:` per call). The cell is still awake on A and A still serves it.
  It is not killed and not hibernated by force. Call `drain/2` again, or stop
  A: both hibernate what is left. Otherwise its lease expires and B takes it.
- `{cell, :timeout}` or another reason: one cell failed. The cell may still
  have hibernated afterwards. Call `drain/2` again.

## Expected result

In the swap test (`test/swap_cluster_test.exs`; 40 cells, eight clients, the
store with 2 ms of latency, a lease of 5 000 ms, `Retry-After` of at most
250 ms), measured on a shared machine on 2026-11-05:

```
swap with one node id (lease 5000 ms, skew 500 ms, Retry-After <= 250 ms, store latency 2 ms)
  swap (B ready to A stopped): 513 ms; drain of 40 cells: 12 ms
  requests: 6663, refused at least once: 8, refusals: 16
  latency ms: median 3, p99 4, peak 570
```

B was ready 500 ms before the drain on purpose, so the swap time is that wait
plus the drain. The peak is that wait plus one `Retry-After`: no client waited
for the 5 000 ms lease. Every acknowledged write was in the store once, and
the only errors were lease errors and `:draining`.

## Troubleshooting

- **B answers `{:unavailable, ...}` for 30 s or more after A stopped.** A did
  not release the lease: it was killed, the drain hit its deadline, or the
  release failed. Read `Alambique.lease/3` on B: `released: false` and the
  expiry say how long is left. `Alambique.take_over/4` on B replaces the lease
  at once if you know A is gone (design section 5.1).
- **A and B have the same `node_id` (the default `node()` of VMs without
  distribution is `nonode@nohost`).** It is safe. B treats A's lease as
  another instance's and refuses until A releases it, as in this page. In
  v0.1.0 a second instance with the same id took the cells A still held. The
  owner shown by `lease/3` is the same string for both, so tell them apart by
  the data directory or your own logs. A crashed node that restarts, with the
  same id, is also a new instance: it waits for its old leases to expire
  (`lease_ttl_ms` plus `skew_margin_ms`) instead of taking them back, which a
  drain avoids.
- **`docker stop` kills A before the drain ends.** The orchestrator's timeout
  is shorter than the drain. Raise it above `:drain` `:shutdown_ms`, or lower
  the `:deadline_ms`. Cells left held expire as after a crash.
- **Requests for a cell fail with `{:error, :draining}` on B.** B is draining
  itself, for example after its own stop. A node that started with `join:
  false` is not draining.
- **The first calls on B take seconds.** B restores the chain each cell had
  when A released it, and a chain of N files costs N GETs. A saturated A
  compacts less, so its chains are longer; see
  [a node swap with a saturated writer](how-to-speed-up-a-wake.md#a-node-swap-with-a-saturated-writer)
  for how to measure it and `compaction: [concurrency:, urgent_concurrency:]`
  for what to raise.
- **A cell's first wake on B reads more than 32 level-0 files.** On a
  saturated CPU the compaction at the release can be cut by
  `release_deadline_ms`, and B reads the tail that was left (design section 9,
  "Limit: a saturated CPU"). The `[:alambique, :cell, :hibernate]` event of A
  says `compaction: :deadline` and its `tail`. In the bench with the writer on
  one busy CPU, four of 12 cells were cut with tails of 208 to 250 files; the
  worst wake still took 0.37 s, and none took over 0.41 s in 5 runs. Raise
  `compaction: [release_deadline_ms:]` for a shorter tail, at the cost of a
  longer drain (0 turns the compaction off), and keep `drain: [deadline_ms:]`
  above it.
- **A drain returns `hibernated: 0` and the cells are awake on another
  node.** `drain/2` acts on the cells with a process on this node only.

## Next steps

- Restart a node inside a cluster: [how-to-operate-a-cell.md](how-to-operate-a-cell.md), section 9.
- Why a lease is fenced, and the incarnation: [design.md](design.md), sections 5 to 8.
- The drain events: [telemetry.md](telemetry.md).
