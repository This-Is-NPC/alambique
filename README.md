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
The current version is 0.5.0. It is not published on Hex: depend on a release
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

The full list is in [CHANGELOG.md](CHANGELOG.md).

## Installation

Add Alambique to `mix.exs`, by tag:

```elixir
def deps do
  [
    {:alambique, git: "https://github.com/This-Is-NPC/alambique.git", tag: "v0.5.0"} # x-release-please-version
  ]
end
```

The native crate is compiled with your project, so you need a Rust toolchain
and a C compiler (see Requirements).

## Example

Start one instance per node, with the same name, store and prefix on every
node of the cluster:

```elixir
children = [
  {Alambique,
   name: MyApp.Cells,
   store: {Alambique.Store.S3, s3_config},
   prefix: "myapp",
   data_dir: "/var/lib/myapp/cells",
   migrations: [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]}
]
```

Then call any cell from any node:

```elixir
{:ok, _} = Alambique.execute(MyApp.Cells, "org-1", "INSERT INTO notes (body) VALUES (?)", ["hi"])
{:ok, %{columns: ["body"], rows: [["hi"]]}} = Alambique.query(MyApp.Cells, "org-1", "SELECT body FROM notes")
```

More calls, placement and the other features are in
[docs/how-to-call-cells.md](docs/how-to-call-cells.md).

## Requirements

- Elixir 1.17 or later, a Rust toolchain and a C compiler. The native crate in
  `native/alambique_nif` bundles SQLite and is built through Rustler.
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
- Docker, only to run the SeaweedFS tests.

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
