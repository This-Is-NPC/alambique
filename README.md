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
replicates as LTX files (the format Litestream 0.5 writes), restores lazily,
compacts its history, migrates schemas with an N-1 window, moves cells between
locations and publishes a per-cell change feed.

## Installation

Add Alambique to `mix.exs`:

```elixir
def deps do
  [{:alambique, "~> 0.1.0"}]
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
  ([docs/storage-compatibility.md](docs/storage-compatibility.md)).
- For nodes that run many cells, the VM flags `+Mdai max +MMmcs 0`
  ([docs/design.md](docs/design.md), section 12).
- A local, case-sensitive file system for `:data_dir`, used by one instance
  only, and a `node_id` that is unique in the cluster. The library checks
  neither.
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
