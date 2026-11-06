# Contributing

**The question:** how do I build, test and measure Alambique?

This page is for contributors. The rules for contributors and coding agents
are in [AGENTS.md](../AGENTS.md); the architecture is in [design.md](design.md).
Development uses the versions pinned in `mise.toml`: Erlang 28.5, Elixir 1.20.4
and Rust 1.97.1.

```sh
mise run test            # default suite, no external services
mise run test:native     # Rust unit tests of native/alambique_nif
mise run test:coverage   # line coverage of Elixir and the NIF, fails below 90%
mise run test:miri       # native tests under Miri (Rust undefined behavior; SQLite tests skipped)
mise run test:asan       # native tests under AddressSanitizer, SQLite's C code included
mise run test:ubsan      # native tests with SQLite's C code built with UBSan
mise run fuzz [target] [seconds]  # libFuzzer targets, default all and 60 s each
mise run test:cluster    # failover properties and the tasks on peer nodes (about two minutes)
mise run bench:cells     # wake thousands of cells and report resource use
mise run bench:commit    # commit latency and throughput of one cell
mise run bench:lazy      # lazy restore of a large cell against a full restore
mise run bench:routing   # cluster-view reads of a forwarded call by group size
mise run bench:router    # routing path and awake.max_pending check of a local call
mise run bench:inflight  # cost of the in-flight marker per write
mise run bench:stall     # per-second latency, run queues, NIF calls and store requests under steady load
mise run test:seaweedfs  # store contract tests against a disposable SeaweedFS
mise run seaweedfs:down  # remove the SeaweedFS container
```

The Miri, sanitizer and fuzz tasks install the pinned nightly toolchain and
`cargo-fuzz` on first use; the sanitizer tasks also need `clang`, or `CC` set
to another compiler. `test:coverage` uses the pinned `cargo-llvm-cov` and
takes about four minutes. `ALAMBIQUE_FUZZ_SECONDS` and `ALAMBIQUE_FUZZ_MAX_LEN`
(default 4096) set the fuzz time and input size.

The test kit lives in [testkit/](../testkit/) as the separate package
`alambique_testkit`. Its sources are compiled into this project's test build
instead of being a path dependency, because the kit depends on `alambique`
and Mix rejects dependency cycles.

The cluster tests start peer nodes on `127.0.0.1` and need `epmd` on the
`PATH`. Functions passed to `Alambique.transaction/4` from a test must come from
a compiled module such as `test/support/cluster_funs.ex`, because peers cannot
load modules that exist only in memory.


## Releases

Releases are cut by [release-please](https://github.com/googleapis/release-please)
from the Conventional Commits on `master` (`.github/workflows/release.yml`).
Each push to `master` updates a release pull request with the next version and
its `CHANGELOG.md` entry. Merging that pull request tags `vX.Y.Z` and publishes
the GitHub release. Before 1.0, `feat` bumps the minor version and `fix` the
patch. The version lives in `mix.exs` and `testkit/mix.exs`. The Rust crates
under `native/` are internal and keep their own version.

Applications depend on a release by tag:

```elixir
{:alambique, git: "https://github.com/This-Is-NPC/alambique.git", tag: "v0.1.0"}
```
