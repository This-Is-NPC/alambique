# Alambique

Alambique is an Elixir library that runs SQLite databases on demand and
replicates each one to S3-compatible storage. It brings the idea of
Cloudflare Durable Objects to the BEAM: a cell is one SQLite database, for
example one user, organization or mailbox. It is awake on one node while it
is used and sleeps when it is idle, and any node that runs the instance can
serve any cell. A service scales with compute and object storage, with no
database server per region.

The current version is 0.18.0. <!-- x-release-please-version -->

## Install

Alambique is not published on Hex. Add it to `mix.exs` by its release tag:

```elixir
def deps do
  [
    {:alambique, git: "https://github.com/This-Is-NPC/alambique.git", tag: "v0.18.0"} # x-release-please-version
  ]
end
```

Then run `mix deps.get`. The first `mix compile` builds the native library.
Install Erlang/OTP, Elixir and Rust with your usual tools; `rustup` installs Rust.

## Start here

- Start an instance under your supervisor, with the same name, store and
  prefix on every node: `{Alambique, name: MyApp.Cells, store: ...}`.
- Call a cell from any node: `Alambique.execute(MyApp.Cells, "org-1", sql, params)`
  and `Alambique.query(MyApp.Cells, "org-1", sql, params)`.
- The full walkthrough, with a disposable SeaweedFS, the expected output and
  the cleanup, is in [docs/how-to-run-a-first-cell.md](docs/how-to-run-a-first-cell.md).

## Big ideas

**[Concepts](docs/concepts.md)** — what a cell, lease, epoch and segment are.

**[Design](docs/design.md)** — the model, components, state and protocols.

**[Call cells](docs/how-to-call-cells.md)** — start an instance and call its cells from your application.

**[Operate a cell](docs/how-to-operate-a-cell.md)** — inspect, restore, freeze or move a cell.

**[Swap a node](docs/how-to-swap-a-node.md)** — replace a node and see what callers get meanwhile.

**[Change feed](docs/how-to-consume-the-change-feed.md)** — read the row changes of all cells and resume where you stopped.

**[Cross-cell queries](docs/how-to-query-across-cells.md)** — read-only SQL over many cells without waking them.

**[Read replicas](docs/how-to-read-replicas.md)** — serve the reads of a hot cell from other nodes, with a bounded lag.

**[Schema changes](docs/how-to-roll-out-a-schema-change.md)** — change the schema while old and new code run together.

**[Rewind](docs/how-to-rewind-a-cell.md)** — undo what a cell did after a point in time.

**[S3 store](docs/how-to-configure-the-s3-store.md)** — point the store at AWS or a compatible server, with static or renewing credentials.

**[Monitoring](docs/how-to-monitor-a-node.md)** — the events, calls and host metrics that show a node is healthy.

**[Cost of a cell](docs/cost-of-a-cell.md)** — what one cell costs in CPU, memory and storage.

**[Known limitations](docs/known-limitations.md)** — what to know before you run a release in production.

**[Telemetry](docs/telemetry.md)** — the telemetry events Alambique emits, and their data.

**[Segment format](docs/segment-format.md)** — the bytes of a replicated segment.

## Interface

The public API is the `Alambique` module. Functions on a cell take the
instance name first and the cell id second:

- `{Alambique, opts}` starts an instance; `Alambique.child_spec/1` builds its child spec.
- `Alambique.query/4` runs a read statement and returns its columns and rows.
- `Alambique.execute/4` runs a write and returns after it is durable.
- `Alambique.transaction/4` runs a function in one SQLite transaction on the cell.
- `Alambique.move/4` moves a cell to another node of the instance.
- `Alambique.rewind/4` undoes what a cell did after a point in time.
- `Alambique.drain/2` writes what is pending and releases the leases of a node.

The documentation map, with one question per page, is in
[docs/README.md](docs/README.md).

## Requirements

- Elixir 1.17 or later, a recent stable Rust (the native crate declares its
  minimum in `rust-version`), and a C compiler. The native crate in
  `native/alambique_nif` bundles SQLite and is built through Rustler.
- An S3-compatible store with atomic conditional writes (`If-None-Match: *`
  and `If-Match`). The walkthrough uses SeaweedFS 4.48. See
  [docs/storage-compatibility.md](docs/storage-compatibility.md).
- A local, case-sensitive file system for `:data_dir`, used by one instance
  only. The start refuses a directory that another live instance holds.
- Docker, only to run the SeaweedFS of the walkthrough. A store that meets
  the conditional-write requirement works instead.

To build, test or work on the project, read
[docs/contributing.md](docs/contributing.md).

## License

MIT. See [LICENSE](LICENSE).
