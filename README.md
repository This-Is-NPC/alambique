# Alambique

Alambique runs SQLite databases on demand and replicates them to
S3-compatible storage. It is an Elixir library that brings the idea of
Cloudflare Durable Objects to the BEAM, so a service can scale horizontally
with compute and object storage only, with no database server per region.

Each database is a *cell*: one SQLite file with a single owner node at a time,
chosen by a fenced lease in the store. A cell wakes on the first call and
sleeps when idle. A write is acknowledged only after it is durable in the
store.

Use it when you have many small databases, one per user, organization or
mailbox, and want them to move between nodes without a database server. It
replicates as LTX files (the format Litestream 0.5 writes; Litestream 0.5.18 restores them, with [one limit](docs/segment-format.md#litestream-compatibility)), restores lazily,
compacts its history, migrates schemas with an N-1 window, moves cells between
locations and publishes a per-cell change feed.

## Status

The current version is 0.14.0. <!-- x-release-please-version -->
It is not published on Hex: depend on a release by its git tag.
Before 1.0, a new minor version may change behavior. What changed since the
first release (0.1.0):

- 0.2.0: `drain/2` takes a deadline for the whole drain and reports through
  telemetry and `drain_status/1`. A lease carries an incarnation, so two
  nodes with the same `node_id` fence each other.
- 0.3.0: every SQL statement, migration and store request has a deadline, and
  a call can lower its statement deadline. Admission is exact at `max_pending`
  and hibernates the least recently used cell at `max_cells`. The start
  refuses a `data_dir` that another instance holds or that ignores case.
  Relocation cleanups survive a restart and the change feed keeps object
  keys in commit order.
- 0.3.1: a cross-cell query takes at most `query: [max_statement_share:]`
  (default 0.5) of the statement slots, and `query: [concurrency:]` is 16.
- 0.4.0: `Alambique.checkpoint/3` empties the WAL of an awake cell, and
  `wal: [max_bytes:]` bounds the WAL files of all awake cells. A full disk
  gives `{:error, :disk_full}`. A wake above `awake: [max_wakes:]` waits in a
  queue (`wake_queue_ms`, `max_wake_queue`) before it gives `:overloaded`.
  `cells(details: true)` lists the last use and the WAL size of each cell.
- 0.5.0: **breaking:** `restore: [prefetch_bytes:]` is removed; use
  `restore: [max_inflight_bytes:]` (default 64 MiB), which caps the bytes a
  restore holds in memory. A wake takes the lease before it reads the store.
  A circuit breaker (`breaker:`) answers `{:error, :store_unavailable}` while
  the store fails, a wake that fails on the store backs off (`wake:
  [backoff_ms:]`), and a cell keeps a failed upload and retries it while its
  lease is valid (`upload:`). `owner/3` takes `peers: true` to ask the nodes
  instead of the store.
- 0.6.0: nodes find each other through the store. The `registry:` option
  writes an entry under `<prefix>/nodes/`, and `Alambique.peers/1` lists the
  live peers ([how-to](docs/how-to-discover-nodes.md)).
  `Alambique.breaker_status/1` reports the state of the store circuit breaker
  of each location. The S3 store derives the AWS endpoint from the region and
  takes a `session_token` or a `credentials` function that renews temporary
  credentials ([how-to](docs/how-to-configure-the-s3-store.md)); the ops tasks
  take `--session-token`. There is no breaking change.
- 0.6.1: the README has a first try that runs against a local SeaweedFS, and
  the Rust version the crate needs is declared (1.97.1 or later). The native
  code builds with Rust 1.100.0 too. There is no breaking change.
- 0.7.0: the compactor takes the cells that waited longest first, so no cell
  is passed over while others are busy. A new option
  `compaction: [max_chain_segments:]` (default 256) puts a cell with that many
  uncompacted commits first, which keeps the chain a wake reads short. A
  restore of a chain larger than `restore: [max_inflight_bytes:]` keeps
  reading ahead as the budget frees, so it is as fast as one that fits (the
  table is in the [how-to](docs/how-to-speed-up-a-wake.md)).
  `mise run bench:restore-concurrency` measures it. There is no breaking
  change, except that a `compaction: [fanout:]` above 256 is now refused
  unless `max_chain_segments` is at least as large.
- 0.7.1: the compactor is woken when a cell reaches `max_chain_segments`
  instead of waiting for its next tick, and a new option
  `compaction: [urgent_concurrency:]` (default 4, 0 for none) adds passes
  while a cell is over the cap. A restore takes CPU linear in the length of
  the chain, a lazy restore that finds the chain small reuses the files it
  already read instead of asking the store again, and the S3 store has a
  connection pool of its own, `pool_size` (default 256). The how-to has a
  [node swap under load](docs/how-to-speed-up-a-wake.md#a-node-swap-under-load)
  (`mise run bench:swap`). There is no breaking change: a `req_options:
  [finch: ...]` still applies, and with a pool named there, `pool_size` does
  not.
- 0.8.0: a compaction pass no longer merges again a block that lies inside a
  file of a higher level, so a superseded file that was deleted is not
  rewritten and the writes it wasted are gone. `Alambique.load/2` answers
  `{:ok, %{pending: n, waking: waking}}`: `waking` is `true` from the start of
  the cell process until the cell starts serving, which tells a cell that is
  loading its data from one that is saturated. There is no breaking change: a
  match on `%{pending: n}` still works.
- 0.9.0: a node that hands a cell over on purpose (`hibernate/3`, `drain/2`,
  the stop of the instance) compacts the level-0 tail first, when it has
  `compaction: [release_min_tail:]` files or more, for at most
  `release_deadline_ms`, so the next node reads a short chain. On a saturated
  CPU the compaction can be cut and the tail left is longer
  ([how-to](docs/how-to-swap-a-node.md)). The compactor starts a pass as a
  slot frees, with no rounds, and `[:compactor, :tick]` reports a busy
  period. `[:cell, :wake]` reports `store_time` and `reductions`, and
  `[:cell, :hibernate]` the `tail` and `compact_duration`. **breaking:**
  `compaction: [batch_factor:]` is removed.
- 0.10.0: the store circuit breaker counts only the failures of the store. A
  request that fails while it waits for a pool connection, or that is killed
  before it sends, is `:local`: it neither counts nor resets the run of
  failures, so a node that takes more than it can serve no longer opens the
  breaker. `[:alambique, :store, :failure]` reports the `reason` and whether
  it `counted`. The freeze-marker check of each renewal and the replica marker
  read are no longer killed at 50 ms and counted as a timeout of the store:
  `retry: [check_timeout_ms:]` bounds them: 1 000 ms by default, capped at half
  of `lease_ttl_ms / 3` (less the renewal jitter) minus `skew_margin_ms`, so
  lower on a short lease; a cell with a slow store can wait that long in a
  renewal. A request that left and whose answer the node read late still
  counts. **breaking:** start refuses an explicit `check_timeout_ms` above that
  bound, and a `lease_ttl_ms` too close to twice `skew_margin_ms` to leave room
  for the check.
- 0.10.1: `[:alambique, :store, :failure]` gains `kind` (`:error`, `:throw`
  or `:exit`) and `detail`, which say what a crash of the store was (`nil`
  when the failure is not a crash). The `detail` is cut to `breaker:
  [detail_limit:]` characters (default 200), and a crash is logged at most
  once per `breaker: [log_interval_ms:]` (default 10 000) for each store. A
  crash that is the wait for a pool connection (the exit of
  `NimblePool.checkout!/4`, or the error Finch raises for it) is `:local`, so
  it does not count. The testkit `MemoryStore` no longer times out its calls
  to its server, which a starved server turned into a counted crash. There is
  no breaking change: only metadata keys and options are new.
- 0.10.2: `Alambique.owner/3`, and the peers check behind it, no longer name
  a local cell that already emitted its `[:alambique, :cell, :stop]` event
  (`:lease_lost`, `:fenced`) and is only closing its database. Before, right
  after a takeover, the old owner could answer itself for that moment. There
  is no breaking change and no new option.
- 0.11.0: the S3 store keeps only the code and message of an error answer
  (`:error_limit`), the start refuses a store without conditional writes
  (`check_store: false` skips it), and no read of the bucket is unbounded
  (`read_limits:`, `[:alambique, :store, :too_large]`). A lazy restore checks
  the checksum of small files first and holds the first writes until the
  hydration is verified (`lazy_restore: [verify_bytes:, verify_writes:]`,
  `[:alambique, :cell, :writes_released]`). A slow wake renews the lease it
  used up, and keys, leases, trash manifests and the ops tools refuse
  malformed input. `Alambique.members/1`, `Alambique.preferred/2`,
  `[:alambique, :membership, :changed]` and `move/4` with `only_if_idle: true`
  are new. **breaking:** an instance refuses a `prefix` with a leading or
  trailing slash, an empty segment, a `.` or `..` segment or a control
  character. A deployment that used one must copy its objects to a valid
  prefix with the store's own tools and point the instance at it
  ([how-to](docs/how-to-manage-the-bucket.md#7-choose-the-prefix)).
- 0.12.0: `query` no longer fails a cell with a read error when the cache
  trim removes that cell's empty directory while the query creates it
  ([design §14](docs/design.md#14-cross-cell-queries)). The load test now runs
  from one round file (`mise run load -- --config <file>`). There is no
  breaking change and no new library option.
- 0.13.0: a release compaction cut by its deadline keeps the merges it
  published, and the cut no longer opens the store breaker, which made the
  lease release fail and left the lease to expire. `tail` in
  `[:alambique, :cell, :hibernate]` is now what the chain still has after the
  compaction, and a cut before the chain is known gives `:deadline_error`.
  `move/4` and `hibernate/3` take `release_deadline_ms:` for one call, and
  `[:alambique, :admission, :overloaded]` names the limit behind each
  `{:error, :overloaded}` (design.md, sections 7 and 9). There is no breaking
  change to the API.
- 0.14.0: a compaction merge waits for 2 requests to the store instead of 4,
  and the merges of one level run in parallel
  (`compaction: [merge_concurrency: 4]`). Idle and evicted cells compact at
  release too (`release_on:`), at most `release_concurrency:` (4) cells of a
  node compact at release at once, and the S3 store bounds the wait for a
  pooled connection (`pool_timeout:`, 2000 ms) (design.md, sections 3 and 9).
  In `[:alambique, :cell, :hibernate]`, an eviction now reports
  `reason: :evict` (it was `:request`), and `compaction: :busy` is new.
- 0.15.0: on a node with 2 to 4 dirty IO schedulers, the default
  `sql: [max_concurrent_statements:]` keeps 2 slots, so reads are no longer
  refused with `:busy` while a scheduler is free. `lazy_restore:
  [verify_reads: true]` (default `false`) makes owner reads wait for the end
  of a lazy wake's download instead of holding a dirty IO scheduler on a
  missing page. A guide sizes a node with one or two CPUs (`+SDio`,
  [how to size a node](docs/how-to-size-a-node.md)). There is no breaking
  change.

The full list is in [CHANGELOG.md](CHANGELOG.md).

## Installation

Add Alambique to `mix.exs`, by tag:

```elixir
def deps do
  [
    {:alambique, git: "https://github.com/This-Is-NPC/alambique.git", tag: "v0.14.0"} # x-release-please-version
  ]
end
```

The native crate is compiled with your project, so you need a C compiler,
Elixir 1.17 or later and a Rust toolchain of 1.97.1 or later (tested with
1.97.1 and 1.100.0). The project itself is developed with Erlang/OTP 28 and
Elixir 1.20. With [mise](https://mise.jdx.dev) one command installs them:

```sh
mise use erlang elixir rust@1.97.1
```

The first install can take minutes: installing Rust with its components
(`clippy`, `rust-docs`, `rustfmt`) took 12 minutes on 2 vCPUs. `rustup`
works instead of mise for Rust.

Then create a project and add the dependency above to its `mix.exs`:

```sh
mix new my_app
cd my_app
mix deps.get
```

The first `mix compile` downloads the crates and builds the native library:
1 to 2 minutes on 2 vCPUs (1 min 52 s measured).

## Example

Try it against a local SeaweedFS. This container is disposable and its data
lives inside it. The first `docker run` pulls an image of about 725 MB:

```sh
docker run -d --name alambique-first-try -p 127.0.0.1:18333:8333 \
  -e AWS_ACCESS_KEY_ID=alambique -e AWS_SECRET_ACCESS_KEY=alambique-secret \
  chrislusf/seaweedfs:4.48 \
  mini -dir=/data -ip=127.0.0.1 -ip.bind=0.0.0.0 -s3.port=8333 -admin.ui=false
```

Alambique does not create the bucket; SeaweedFS creates it on the first
write. On AWS S3 create it first. Save this as `example.exs` in the root of
your project (next to `mix.exs`) and run `mix run example.exs` there:

```elixir
s3_config = %{
  endpoint: "http://127.0.0.1:18333",
  bucket: "demo-bucket",
  access_key_id: "alambique",
  secret_access_key: "alambique-secret"
}

# One instance per node, with the same name, store and prefix on every node.
children = [
  {Alambique,
   name: MyApp.Cells,
   store: {Alambique.Store.S3, s3_config},
   prefix: "myapp",
   data_dir: Path.expand("data"),
   migrations: [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]}
]

{:ok, _} = Supervisor.start_link(children, strategy: :one_for_one)

# Call any cell from any node.
IO.inspect(Alambique.execute(MyApp.Cells, "org-1", "INSERT INTO notes (body) VALUES (?)", ["hi"]))
IO.inspect(Alambique.query(MyApp.Cells, "org-1", "SELECT body FROM notes"))
IO.inspect(Alambique.drain(MyApp.Cells))
```

The output of the first run, and of the second with one more row:

```
{:ok, %{columns: [], rows: []}}
{:ok, %{columns: ["body"], rows: [["hi"]]}}
{:ok, %{errors: [], hibernated: 1}}
```

`drain/2` writes what is pending and releases the leases before the script
ends. Without it, a run within the next 30 s (`lease_ttl_ms`) gets
`{:error, {:unavailable, "nonode@nohost", expires_at_ms}}`, because the stopped
process still holds the lease. Wait until `expires_at_ms` (a Unix time in
milliseconds) and call again. Remove the container with
`docker rm -fv alambique-first-try`; `-v` also removes the anonymous volume
the container creates.

More calls, placement and the other features are in
[docs/how-to-call-cells.md](docs/how-to-call-cells.md).

If Elixir warns about a "native name encoding of latin1", the shell has no
UTF-8 locale. Run with `LANG=C.UTF-8`, for example
`LANG=C.UTF-8 mix run example.exs`.

## Requirements

- Elixir 1.17 or later, Rust 1.97.1 or later and a C compiler. The native
  crate in `native/alambique_nif` bundles SQLite and is built through Rustler.
- An S3-compatible store with atomic conditional writes (`If-None-Match: *`
  and `If-Match`). SeaweedFS 4.48 is the only one tested; AWS S3 and
  Cloudflare R2 have not been run
  ([docs/storage-compatibility.md](docs/storage-compatibility.md)). The S3
  store accepts an AWS endpoint derived from the region, session tokens and
  renewable credentials
  ([docs/how-to-configure-the-s3-store.md](docs/how-to-configure-the-s3-store.md)).
- For nodes that run many cells, the VM flags `+Mdai max +MMmcs 0`
  ([docs/design.md](docs/design.md), section 12).
- A local, case-sensitive file system for `:data_dir`, used by one instance
  only. The start refuses a directory that another live instance holds (a
  lock file, released when its OS process ends) and one that ignores case. The
  lock needs a local file system: on NFS `flock` may not exclude other hosts.
  A `node_id` need not be unique for safety:
  leases also carry a random incarnation per start (design section 5).
- Docker, only to run the SeaweedFS of the example and the SeaweedFS tests.
  A store that meets the S3 requirement above works instead.

## Limitations

- Write access to the bucket prefix is full control of its cells: checksums
  detect corruption, not forgery. Give each deployment its own prefix and
  readers a read-only credential
  ([docs/design.md](docs/design.md), section 3.1). No prefix may be a path
  prefix of another instance's in the same store (`app` and `app/cells`
  share keys).
- A file system whose writes can stall stalls every write of every cell. See
  [docs/how-to-operate-a-cell.md](docs/how-to-operate-a-cell.md), step 10.
- On a saturated CPU, the compaction at the release of a cell can be cut by
  `compaction: [release_deadline_ms:]`, and the next node reads a longer tail
  ([docs/design.md](docs/design.md), section 9). `move/4` and `hibernate/3`
  take a larger `release_deadline_ms:` for one call.
- Known defects and limits are listed in
  [docs/what-does-not-work.md](docs/what-does-not-work.md).

## Documentation

[docs/README.md](docs/README.md) maps the documentation. To run a node, see
how to [monitor it](docs/how-to-monitor-a-node.md), [manage the
bucket](docs/how-to-manage-the-bucket.md) and [recover from the loss of a node
and its disk](docs/how-to-recover-from-disaster.md). The history of
releases is in [CHANGELOG.md](CHANGELOG.md). To work on the project, read
[docs/contributing.md](docs/contributing.md).

## License

MIT. See [LICENSE](LICENSE).
