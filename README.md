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

<!-- x-release-please-start-version -->
The current version is 0.7.1. It is not published on Hex: depend on a release
by its git tag.
<!-- x-release-please-end -->
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

The full list is in [CHANGELOG.md](CHANGELOG.md).

## Installation

Add Alambique to `mix.exs`, by tag:

```elixir
def deps do
  [
    {:alambique, git: "https://github.com/This-Is-NPC/alambique.git", tag: "v0.7.1"} # x-release-please-version
  ]
end
```

The native crate is compiled with your project, so you need a C compiler and
a Rust toolchain of 1.97.1 or later (tested with 1.97.1 and 1.100.0). With
[mise](https://mise.jdx.dev), `mise use rust@1.97.1` installs it; `rustup`
works too. The first `mix compile` downloads the crates and builds the
native library: about 75 seconds on 2 vCPUs.

## Example

Try it against a local SeaweedFS. This container is disposable and its data
lives inside it:

```sh
docker run -d --name alambique-first-try -p 127.0.0.1:18333:8333 \
  -e AWS_ACCESS_KEY_ID=alambique -e AWS_SECRET_ACCESS_KEY=alambique-secret \
  chrislusf/seaweedfs:4.48 \
  mini -dir=/data -ip=127.0.0.1 -ip.bind=0.0.0.0 -s3.port=8333 -admin.ui=false
```

Alambique does not create the bucket; SeaweedFS creates it on the first
write. On AWS S3 create it first. Save this as `example.exs` in your project
and run `mix run example.exs`:

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
`docker rm -f alambique-first-try`.

More calls, placement and the other features are in
[docs/how-to-call-cells.md](docs/how-to-call-cells.md).

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
  ([docs/design.md](docs/design.md), section 3.1).
- A file system whose writes can stall stalls every write of every cell. See
  [docs/how-to-operate-a-cell.md](docs/how-to-operate-a-cell.md), step 10.
- Known defects and limits are listed in
  [docs/what-does-not-work.md](docs/what-does-not-work.md).

## Documentation

[docs/README.md](docs/README.md) maps the documentation. The history of
releases is in [CHANGELOG.md](CHANGELOG.md). To work on the project, read
[docs/contributing.md](docs/contributing.md).

## License

MIT. See [LICENSE](LICENSE).
