# Contributing

**The question:** how do I build, test and measure Alambique?

This page is for contributors. The rules for contributors and coding agents
are in [AGENTS.md](../AGENTS.md); the architecture is in [design.md](design.md).
Development uses the versions pinned in `mise.toml`: Erlang 28.5, Elixir 1.20.4
and Rust 1.97.1.

```sh
mise run test            # default suite, no external services
mise run test:native     # Rust unit tests of native/alambique_nif
mise run check           # the full gate: quick checks, clippy, docs links, default suite
mise run check:static    # the same without the suite: what the CI gate job runs
mise run check:quick     # the fast checks of the pre-push hook
mise run check:links     # relative links and anchors of README.md, CHANGELOG.md, AGENTS.md and docs/
mise run check:readme    # release-please markers in README.md
mise run check:tla       # model-check the pipelined-commit protocol with TLC
mise run check:tla:mutations  # check that the TLA+ model finds each deliberate bug
mise run test:coverage   # line coverage of Elixir and the NIF, fails below ALAMBIQUE_COVERAGE_MIN (default 90)
mise run test:miri       # native tests under Miri (Rust undefined behavior; FFI-bound tests skipped)
mise run test:asan       # native tests under AddressSanitizer, SQLite's C code included
mise run test:ubsan      # native tests with SQLite's C code built with UBSan
mise run fuzz [target] [seconds]  # libFuzzer targets, default all and 60 s each
mise run test:cluster    # failover properties on a cluster of peer nodes (slow)
mise run test:seaweedfs  # store contract tests against a disposable SeaweedFS
mise run test:litestream # Litestream 0.5.18 restores replicas Alambique wrote (needs SeaweedFS)
mise run seaweedfs:up    # start the disposable SeaweedFS container on 127.0.0.1:18333
mise run seaweedfs:down  # remove the SeaweedFS container
mise run load:image      # build the image of the load test service
mise run load -- --config load/rounds/example.toml --dry-run  # plan a load test of one node in a limited container (docs/how-to-load-test.md)
mise run hooks:install   # set core.hooksPath to .githooks
mise run local-check     # the full gate on HEAD, by hand, posting a local-check status
```

The load test is described in [how-to-load-test.md](how-to-load-test.md). Its
harness lives in `load/`; the code that decides outcomes is unit-tested in
`test/load/` and excluded from the coverage figures, because the rest of it
drives containers. Load round files other than `load/rounds/example.toml`, and
the results of load runs, are not tracked by git (see `.gitignore`).

The Miri, sanitizer and fuzz tasks install the pinned nightly toolchain and
`cargo-fuzz` on first use; the sanitizer tasks also need `clang`, or `CC` set
to another compiler. `test:coverage` uses the pinned `cargo-llvm-cov`.
`ALAMBIQUE_FUZZ_SECONDS` and `ALAMBIQUE_FUZZ_MAX_LEN`
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

`ci` is the aggregate job: it passes when these jobs passed. Every test runs
once: the default suite runs with coverage, split into parts, and the cluster
suite runs in parts without coverage.

| Job | What it runs |
| --- | --- |
| `gate` | `mise run check:static`: the static part of the gate below, without the suite |
| `coverage-build` | `mise run test:coverage:build`: the NIF built once with coverage instrumentation, shared with the parts as an artifact |
| `coverage-run (i)` | one of N parts of the default suite: `mise run test:coverage:run -- --partitions N --slowest-modules 5 --export-coverage part-i` with `MIX_TEST_PARTITION=i` |
| `coverage` | `mise run test:coverage:report`: sums the parts and fails below `ALAMBIQUE_COVERAGE_MIN` |
| `native` | `mise run test:native`: the Rust unit tests of the NIF |
| `services` | `mise run test:seaweedfs` and `mise run test:litestream` against a SeaweedFS container |
| `cluster (i)` | one of M parts of the failover properties on peer nodes: `mise run test:cluster -- --partitions M --slowest-modules 5` (`scripts/test-cluster`) with `MIX_TEST_PARTITION=i` |

**The partitions.** N is the number of items of the `part` list of
`coverage-run` in `ci.yml` (default 4) and M the one of `cluster` (default 3).
ExUnit deals the test files out to the partitions round-robin in sorted order,
so every test runs in exactly one part. `scripts/test-cluster` deals out only
the files that mention the `:cluster` tag; dealing out every file of the project
puts the two slowest cluster files in one part. Each part runs on its own
runner and prints its seed.

**How coverage is summed.** llvm-cov can merge profiles only of the same
binary, so `coverage-build` builds the instrumented NIF once and the parts load
that file (they never compile one: `ALAMBIQUE_NIF_PREBUILT=1`). A part with
`--export-coverage` saves `cover/part-i.coverdata` and the NIF profiles
(`.profraw`, tagged with the part), and fails only when a test fails, never on
the threshold. `coverage` downloads all of them, runs `mix test.coverage` (it
imports the `.coverdata` files and enforces the threshold of `mix.exs`) and
`cargo llvm-cov report` over all profiles with `--fail-under-lines`, and
prints both totals in its job summary.

**Reproduce a failing part.** Take the seed from the log of the part and run
the same partition:

```sh
MIX_TEST_PARTITION=2 mix test --partitions 4 --seed 123456            # a coverage-run part
MIX_TEST_PARTITION=1 scripts/test-cluster --partitions 3 --seed 123456  # a cluster part
```

Add `--cover` or run `mise run test:coverage` for the coverage of a single
local run (it builds, runs and reports in one process).

Install the hooks once per clone:

```sh
mise run hooks:install   # sets core.hooksPath to .githooks
```

The hooks run only what is fast:

- `pre-commit` runs the format checks.
- `pre-push` runs `mise run check:quick` (`scripts/quick-check`): an
  incremental compile with warnings as errors, `mix format
  --check-formatted`, `cargo fmt --check` and the README marker check. Tags and
  branch deletions pass.
  The compile is incremental, so a push with nothing new takes seconds.
  `scripts/quick-check --force` recompiles everything; the full gate uses it.

Skip them on purpose with `ALAMBIQUE_SKIP_HOOKS=1` for one command, or with
`git commit --no-verify` and `git push --no-verify`. CI runs the checks
anyway.

`mise run check` is the full gate. It stops at the first failure and runs, in
order:

1. `scripts/quick-check --force`: `MIX_ENV=test mix compile
   --warnings-as-errors --force` (forced, so a stale `_build` cannot hide a
   warning), `mix format --check-formatted`, `cargo fmt --check` for
   `native/alambique_nif` and its `fuzz` crate, and the README marker check
   (`scripts/readme-check`)
2. `cargo clippy --all-targets -- -D warnings` on the NIF
3. the documentation link check (`mise run check:links`): relative links and
   `#anchors` in `README.md`, `CHANGELOG.md`, `AGENTS.md` and `docs/*.md`
4. `mise run test`, the default suite

`mise run check:static` runs steps 1 to 3 only: it is the `gate` job, because
CI runs the suite in the coverage parts. Run `mise run check` by hand when you
want the whole result on your machine; a loaded machine
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
`local-check` commit status. `master` does not require it. `scripts/local-check
--post-only --state=success --sha=<commit>` posts the status without running
the gate, and `ALAMBIQUE_LOCAL_CHECK_DRY_RUN=1` (or `--dry-run`) prints the API
calls instead of posting them.


## Store and format tests

These need a SeaweedFS container, or Litestream, which the tasks pin:

```sh
mise run seaweedfs:up        # disposable SeaweedFS on 127.0.0.1:18333, container alambique-seaweedfs
mise run test:seaweedfs      # store contract tests and the tests tagged :seaweedfs
mise run test:litestream     # Litestream 0.5.18 restores replicas Alambique wrote
mise exec -- mix test test/ltx_format_test.exs   # LTX format tests (also part of mise run test)
mise run seaweedfs:down      # remove the container
```

- On SeaweedFS 4.48, `mise run test:seaweedfs` ends with
  `Result: 8 passed, 22 excluded`.
- The versioning tests are in `test/versioning_seaweedfs_test.exs`. Each run
  uses a fresh bucket.
- Crash points: `AlambiqueTestkit.CrashPoints.arm/3` arms one, and
  `grep -rn 'Fault.point' lib/` lists them all.

## Benchmarks

The benchmarks stay local: they measure, so a shared runner would distort them.
Each task takes its arguments from `mise.toml`, and the variables of each script
are in its header (`bench/*.exs`). Tasks marked SeaweedFS start that container.

- `bench:cells`: wake thousands of cells on one node and report resource use.
- `bench:memory`: memory of awake cells by purpose, and read and write latency.
- `bench:query`: query thousands of cells cold and warm.
- `bench:query-refresh`: refresh a large cached query replica after a small write.
- `bench:schema`: create the same schema with one statement per call and with one script.
- `bench:commit` (SeaweedFS): commit latency and throughput of one cell by store latency, pipeline depth and load.
- `bench:amplification`: bytes put to the store and kept, by kind of object, for a simulated mailbox.
- `bench:wal`: one awake cell under steady writes: commits/s, p99, WAL size and memory.
- `bench:stall`: stalls of the write path (see below).
- `bench:sums`: commit of a single-row change in cells of many pages.
- `bench:lazy` (SeaweedFS): lazy restore of a large cell against a full restore.
- `bench:wake`: time of the first query after a wake, with latency on every store request.
- `bench:trips`: store requests of a job by kind, at two store latencies and two concurrencies.
- `bench:routing`: cluster-view reads of a forwarded call by group size.
- `bench:router`: routing path and `awake.max_pending` check of a local call.
- `bench:inflight`: cost of the in-flight marker per write.
- `bench:restore-concurrency`: restore of a long chain by `fetch_concurrency` and `max_inflight_bytes`.
- `bench:swap` (SeaweedFS): restore of 12 busy cells at once, behind a delay proxy.
- `bench:breaker` (SeaweedFS): commits above the capacity of a node pinned to one CPU: throughput and breaker opens.
- `bench:lease`: cost of `Alambique.lease/3`, in memory and with store latency.

**Stalls.** To check whether a disk stalls writes, run `bench:stall` in a checkout
on that disk:

```sh
TMPDIR=/path/on/the/disk mise run bench:stall -- 60   # 60 s per rate
```

- 12 cells receive writes at fixed rates: `ALAMBIQUE_BENCH_RATES`, default `60 100 200 400`.
- Each second prints the requests, p50, p99, max, the requests over 1 s and the
  VM threads blocked in the kernel (`Dthr`, Linux only).
- The last line, `summary: ... over 1000 ms: <n>`, counts the requests over 1 s.
  0 means no stall.
- `ALAMBIQUE_BENCH_STORE=seaweedfs` with `mise run seaweedfs:up` runs the store
  in SeaweedFS instead of memory.
- Stalls are intermittent: repeat the run before you conclude.

## Releases

Releases are cut by [release-please](https://github.com/googleapis/release-please)
from the Conventional Commits on `master` (`.github/workflows/release.yml`).
Each push to `master` updates a release pull request with the next version and
its `CHANGELOG.md` entry. Merging that pull request tags `vX.Y.Z` and publishes
the GitHub release. Before 1.0, `feat` bumps the minor version and `fix` the
patch. The version lives in `mix.exs`, `testkit/mix.exs` and the status and
installation lines of `README.md`, which carry the one-line marker
`x-release-please-version` so the release pull request updates them. Never use
a start and end block: `scripts/readme-check` (`mise run check:readme`) rejects
it. A block opened and closed on one line is never closed for the updater,
which then rewrites every version-like string to the end of the file. The Rust
crates under `native/` are internal and keep their own version.

Applications depend on a release by tag, as the README shows.
