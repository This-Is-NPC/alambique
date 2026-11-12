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


## The local gate

`master` requires the status `local-check` on a pull request before it merges.
Nothing on GitHub runs the tests: a local gate runs them and posts the status.
Pull requests merge by rebase only.

Install the hooks once per clone:

```sh
mise run hooks:install   # sets core.hooksPath to .githooks
```

`mise run check` is the gate. It stops at the first failure and runs, in order:

1. `MIX_ENV=test mix compile --warnings-as-errors --force`
2. `mix format --check-formatted`
3. `cargo fmt --check` for `native/alambique_nif` and its `fuzz` crate
4. `cargo clippy --all-targets -- -D warnings` on the NIF
5. the documentation link check (`mise run check:links`): relative links and
   `#anchors` in `README.md`, `CHANGELOG.md`, `AGENTS.md` and `docs/*.md`
6. `mise run test`, the default suite

The suite takes about four minutes on an idle machine and dominates the gate.
The forced compile, the format checks, clippy and the link check add about a
minute together once dependencies are built; clippy alone takes two seconds
when warm and under two minutes the first time. A first run in a fresh
checkout also compiles the dependencies, and a loaded machine makes timing
tests in the suite fail, so rerun the failing file before suspecting your
change. Two heavy checks are off by default:

```sh
ALAMBIQUE_CHECK_CLUSTER=1 mise run check    # adds mise run test:cluster
ALAMBIQUE_CHECK_COVERAGE=1 mise run check   # adds mise run test:coverage
```

The hooks:

- `pre-commit` runs only the format checks, about a second. The full gate is
  too slow to run on every commit.
- `pre-push` runs `scripts/local-check --pre-push` for each commit pushed. A
  red gate refuses the push. A green gate posts `local-check` on the commit
  through `gh api` once the push lands. The gate vouches only for the tree it
  ran on, so the pushed commit must be `HEAD` with a clean working tree. Tags
  and branch deletions pass. It needs `gh` logged in; without it the gate
  still runs but posts nothing.

`mise run local-check` gates `HEAD` and posts the status by hand. For a commit
you did not push from here, such as the release pull request release-please
opens, check out its branch, run `mise run check`, then post the result:

```sh
scripts/local-check --post-only --state=success --sha=<commit>
```

To skip on purpose, set `ALAMBIQUE_SKIP_LOCAL_CHECK=1` for one command, or
use `git commit --no-verify` or `git push --no-verify`. A skipped push posts no
status, so `master` refuses the merge until an admin bypasses it or you post
one with `--post-only`. `ALAMBIQUE_LOCAL_CHECK_DRY_RUN=1` (or `--dry-run`) runs
the gate but prints the API calls instead of posting them.


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
