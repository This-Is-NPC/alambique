# Contributing

**The question:** how do I build, test and measure Alambique?

This page is for contributors. The rules for contributors and coding agents
are in [AGENTS.md](../AGENTS.md); the architecture is in [design.md](design.md).
Development uses the versions pinned in `mise.toml`: Erlang 28.5, Elixir 1.20.4
and Rust 1.97.1.

```sh
mise run test            # default suite, no external services
mise run test:native     # Rust unit tests of native/alambique_nif
mise run check           # the full gate CI runs: quick checks, clippy, links, default suite
mise run check:quick     # the fast checks of the pre-push hook
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
mise run bench:restore-concurrency  # restore of a long chain by fetch_concurrency and budget
mise run bench:swap      # restore of 12 busy cells at once on SeaweedFS behind a delay proxy
mise run bench:breaker   # commits above a 1-CPU node's capacity: throughput, breaker opens and store failures by reason
mise run bench:lease     # cost of Alambique.lease/3, in memory and with store latency
mise run bench:stall     # per-second latency, run queues, NIF calls and store requests under steady load
mise run load:image      # build the image of the load test service
mise run load -- A commits  # load-test one node in a limited container (docs/how-to-load-test.md)
mise run test:seaweedfs  # store contract tests against a disposable SeaweedFS
mise run test:litestream # Litestream 0.5.18 restores replicas Alambique wrote (needs SeaweedFS)
mise run seaweedfs:down  # remove the SeaweedFS container
```

The load test is described in [how-to-load-test.md](how-to-load-test.md). Its
harness lives in `load/`; the code that decides outcomes is unit-tested in
`test/load/` and excluded from the coverage figures, because the rest of it
drives containers.

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


## The gate and CI

`master` requires the check `ci` of GitHub Actions on a pull request before it
merges, and the check must be green on the head commit. Pull requests merge by
rebase only. The workflow is `.github/workflows/ci.yml`; it runs on every pull
request to `master` and on every push to `master`, and it takes its tool
versions from `mise.toml`. A new push to a pull request cancels the run of the
older commit.

`ci` is the aggregate job: it passes when these jobs passed.

| Job | What it runs |
| --- | --- |
| `gate` | `mise run check`, below |
| `coverage` | `mise run test:coverage`: line coverage of Elixir and the NIF, fails below `ALAMBIQUE_COVERAGE_MIN` |
| `native` | `mise run test:native`: the Rust unit tests of the NIF |
| `services` | `mise run test:seaweedfs` and `mise run test:litestream` against a SeaweedFS container |
| `cluster` | `mise run test:cluster`: failover properties on peer nodes |

Install the hooks once per clone:

```sh
mise run hooks:install   # sets core.hooksPath to .githooks
```

The hooks run only what is fast:

- `pre-commit` runs the format checks, about a second.
- `pre-push` runs `mise run check:quick` (`scripts/quick-check`): an
  incremental compile with warnings as errors, `mix format
  --check-formatted` and `cargo fmt --check`. Tags and branch deletions pass.
  The compile is incremental, so a push with nothing new takes seconds.
  `scripts/quick-check --force` recompiles everything; the full gate uses it.

Skip them on purpose with `ALAMBIQUE_SKIP_HOOKS=1` for one command, or with
`git commit --no-verify` and `git push --no-verify`. CI runs the full gate
anyway.

`mise run check` is the full gate, the one the `gate` job runs. It stops at the
first failure and runs, in order:

1. `scripts/quick-check --force`: `MIX_ENV=test mix compile
   --warnings-as-errors --force` (forced, so a stale `_build` cannot hide a
   warning), `mix format --check-formatted`, and `cargo fmt --check` for
   `native/alambique_nif` and its `fuzz` crate
2. `cargo clippy --all-targets -- -D warnings` on the NIF
3. the documentation link check (`mise run check:links`): relative links and
   `#anchors` in `README.md`, `CHANGELOG.md`, `AGENTS.md` and `docs/*.md`
4. `mise run test`, the default suite

Run it by hand when you want the CI result on your machine; a loaded machine
makes timing tests in the suite fail, so rerun the failing file before
suspecting your change. `ALAMBIQUE_CHECK_CLUSTER=1` adds `mise run
test:cluster` and `ALAMBIQUE_CHECK_COVERAGE=1` adds `mise run test:coverage`.

These stay local, because they need nightly toolchains, clang, minutes to
hours of CPU or measurements that a shared runner would distort: Miri,
the sanitizers, fuzzing, the TLA+ model check, the benchmarks and the load
test.

The release pull request is opened by release-please with the workflow token,
and events of that token start no `pull_request` run. `release.yml` therefore
dispatches `ci.yml` on the release branch (`workflow_dispatch`), so `ci` runs
on the head commit of the release pull request.

`mise run local-check` runs the full gate on `HEAD` by hand and posts a
`local-check` commit status. `master` no longer requires it. `scripts/local-check
--post-only --state=success --sha=<commit>` posts the status without running
the gate, and `ALAMBIQUE_LOCAL_CHECK_DRY_RUN=1` (or `--dry-run`) prints the API
calls instead of posting them.


## Releases

Releases are cut by [release-please](https://github.com/googleapis/release-please)
from the Conventional Commits on `master` (`.github/workflows/release.yml`).
Each push to `master` updates a release pull request with the next version and
its `CHANGELOG.md` entry. Merging that pull request tags `vX.Y.Z` and publishes
the GitHub release. Before 1.0, `feat` bumps the minor version and `fix` the
patch. The version lives in `mix.exs`, `testkit/mix.exs` and the status and
installation lines of `README.md`, which carry `x-release-please` annotations
so the release pull request updates them. The Rust crates under `native/` are
internal and keep their own version.

Applications depend on a release by tag, as the README shows.
