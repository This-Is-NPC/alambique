# Alambique design

This page is for contributors. It describes the model, the storage layout,
the protocols and the constraints that every module relies on. Section
numbers are stable references.

## 1. Model

A **cell** is one SQLite database with its own file, identified by a cell id
(by default a string of `[A-Za-z0-9_.-]`, at most 128 bytes; the placement of
§18 can set another rule within the limits given there). A cell is **awake** on
exactly one node while it is used and **asleep** otherwise. An asleep cell
costs nothing on any node: no process, no open file, no timer, no ETS row
beyond a bounded cache.

An **instance** is one supervised Alambique tree inside the host
application, started with `{Alambique, opts}`. All nodes of a cluster that
serve the same cells run an instance with the same `:name` and the same
placement (§18), which says where each cell's store and prefix are.

**Data directory.** `:data_dir` holds the local files of the cells and belongs
to one live instance: a second one would share the files of cells with equal
ids, and its start removes `read-replicas` and `work` (§20, §21). The start
takes an exclusive `flock` on `<data_dir>/lock` (`dir_lock` in the NIF, through
`File::try_lock`) before it touches any other file, and the first child of the
instance (`Alambique.DataDir`) holds it. `flock` belongs to the open file
description, so a second open conflicts inside one VM as well as across OS
processes, and the kernel releases it when the process dies: a crashed VM
leaves no stale lock, and a stop (or a failed start) releases it at once. A
refused start returns `{:error, {:data_dir_in_use, path}}` and changed nothing
but the creation of the directory and its lock file. The lock excludes only
processes that share the kernel's view of the file: on NFS `flock` may be
local to the client, so the directory must be local, as it already must be
for SQLite. `test/data_dir_lock_test.exs` covers two instances in one VM, a
lock held by another OS process, and the release after a stop;
`test/data_dir_lock_properties_test.exs` checks a model of starts and stops.

The directory must also be case-sensitive: on a case-insensitive file system
ids that differ only in case (`Org`, `org`) have separate leases and one local
file. Each start creates a probe file with a lower-case name, looks for the
upper-case name, and removes the probe; a hit refuses the start with
`{:error, {:data_dir_case_insensitive, path}}`. The check refuses because
the failure is silent corruption of two cells. A real case-insensitive mount
needs root, so the tests replace the lookup of the flipped name
(`Alambique.DataDir.case_insensitive?/2`) and have not run on such a mount.

## 2. Packages and layout

| Path | Package | Purpose |
| --- | --- | --- |
| `/` | `alambique` | The library. |
| `native/alambique_nif` | Rust crate, loaded through Rustler | Bundled SQLite (connections, statements, the Alambique VFS of §16), WAL reading, LTX encoding and decoding, page application, checksums, compaction merge. |
| `testkit/` | `alambique_testkit` | Test-only helpers: in-memory store with fault injection, skewable clock, peer-node cluster helper, property generators. Depended on by path with `only: :test`. |

Rust runs SQLite and does pure byte work on local files and binaries. All network I/O,
including every S3 request, happens in Elixir through the store behaviour
(§3), so the testkit can inject faults into any request.

NIF functions that touch files or run longer than 1 ms are scheduled on
dirty I/O or dirty CPU schedulers.

The library allocates through the BEAM (`enif_alloc`), SQLite included, so
its memory is accounted in `:erlang.memory/0` and managed by the ERTS
allocators. It also keeps counters and gauges (memory per allocator, page
cache hits and misses, open connections and files, WAL frames read, LTX bytes
encoded and decoded, durations of dirty calls), which `Alambique.NativeStats`
reads in one call and emits as a telemetry event (`telemetry.md`).

**Limit: a failed `apply_ltx` can leave its file half written.**
`Native.apply_ltx/4` checks the LTX file, its pre-apply checksum and the pages
it carries before it writes. It computes the post-apply checksum while it
writes, so a file whose trailer does not match its pages returns `{:error,
{:post_apply_mismatch, expected, got}}` after the database file was changed.
Apply to a temporary file and discard it on error, as
`Alambique.Replication.Restore` and `Alambique.Query.Replica` do
(`native/alambique_nif/src/dbsum.rs`, `apply`).

### 2.1 Native crashes

A crash in the native library stops the whole VM, so the crate is built to
turn a Rust panic into an error and is checked with fuzzers, Miri and
sanitizers.

**Panic policy.** `Cargo.toml` sets `panic = "unwind"` for the dev and
release profiles, and nothing else (`.cargo/config.toml`, the Rustler options
in `native.ex`) changes it. With `abort` the first panic would stop the VM.
Rustler 0.38 runs every NIF inside `catch_unwind`, on normal and dirty
schedulers alike, and raises `:nif_panicked` in the caller: `catch_error/1`
returns `:nif_panicked`, and a process that does not catch it exits with
`{:nif_panicked, stacktrace}`. The panic message goes to stderr through
Rust's default hook.

**Rule for code that C calls.** A panic that leaves an `extern "C"` function
aborts the process (since Rust 1.81). So every callback runs its body through
`guard` (`guard.rs`), which catches the panic and returns the failure value
of the callback:

| Callback | File | Value after a panic |
| --- | --- | --- |
| VFS `xOpen` | `vfs.rs` | `SQLITE_CANTOPEN`; the file the default VFS opened is closed again |
| VFS `xClose`, `xRead`, `xWrite`, `xTruncate` | `vfs.rs` | `SQLITE_IOERR_CLOSE`, `_READ`, `_WRITE`, `_TRUNCATE` |
| VFS `xSync`, `xFileSize`, `xLock`, `xUnlock`, `xCheckReservedLock` | `vfs.rs` | `SQLITE_IOERR_FSYNC`, `_FSTAT`, `_LOCK`, `_UNLOCK`, `_CHECKRESERVEDLOCK` |
| VFS `xShmMap`, `xShmLock`, `xShmUnmap` | `vfs.rs` | `SQLITE_IOERR_SHMMAP`, `_SHMLOCK`, `SQLITE_IOERR` |
| VFS `xFileControl` | `vfs.rs` | `SQLITE_ERROR` |
| VFS `xSectorSize`, `xDeviceCharacteristics`, `xShmBarrier` | `vfs.rs` | 4096, 0, nothing |
| VFS `xFetch`, `xUnfetch` | `vfs.rs` | cannot panic (they set a pointer or return) |
| SQLite allocator `xMalloc`, `xRealloc` | `alloc.rs` | null, which SQLite reports as out of memory |
| SQLite allocator `xFree`, `xSize`, `xRoundup` | `alloc.rs` | nothing, 0, the argument |
| SQLite allocator `xInit`, `xShutdown` | `alloc.rs` | cannot panic |
| Authorizer | `sqlite.rs` | `SQLITE_DENY` (the statement is refused) |
| Destructors of `Conn`, `Stmt`, `LazyRes` | `sqlite.rs`, `lazy_nif.rs` | the resource leaks; see below |
| Thread that sends page-miss messages | `lazy_nif.rs` | the message is lost, the thread lives |

ERTS calls the destructor of a NIF resource from a C callback that Rustler
does not wrap, so a panic in a `Drop` would abort the VM. The `Drop` of every
resource of this crate runs inside `guard`.

**How it is proved.** `guard` has a test hook (test builds only) that makes
one named callback panic once. The Rust unit tests panic in every allocator
hook and check the failure value. `test/native_panic_test.exs` runs in the
VM: it calls NIFs that panic on a normal, a dirty CPU and a dirty I/O
scheduler, a resource whose destructor panics, and a NIF that opens a
database through the VFS while a panic is armed in each of the 23 callbacks
above that can panic. Each test then checks that SQLite still works. The
test NIFs exist only in a build with the cargo feature `test-panic`, which
`native.ex` turns on for `MIX_ENV=test`; a release build does not contain
them. Every Mix environment loads the artifact through the same linked
`priv/native`, so the module tracks it as an external resource: a switch of
environment rebuilds it with the right features instead of reusing another
environment's copy. Replacing `guard` with a plain call makes the same tests
abort the VM ("thread caused non-unwinding panic").

**Fuzzing.** `mise run fuzz [target] [seconds]` runs libFuzzer through
cargo-fuzz on the nightly toolchain pinned in `mise.toml`
(`ALAMBIQUE_NIGHTLY`; the task installs it and cargo-fuzz when missing).
Seconds default to 60 or to `$ALAMBIQUE_FUZZ_SECONDS`; `$ALAMBIQUE_FUZZ_MAX_LEN`
(default 4096) caps the input size. The target is `all` or one of:

| Target | Input | What it checks |
| --- | --- | --- |
| `ltx_decode` | an LTX file, or any bytes | decoder, verifier and lazy-restore parser never panic and agree on valid files |
| `lazy_plan` | up to four length-prefixed files | staging, `apply`, `ready` and the fetch of every page never panic; a chain that `dbsum::apply` restores gets a plan, and the plan fills a file with the same bytes |
| `ltx_roundtrip` | a description of a database and an increment | encode, decode, apply and merge give back the same pages and checksums |
| `page_apply` | a database image size and an LTX file | page application never panics, the file has the committed size, a snapshot leaves a database whose checksum is the trailer's |
| `ltx_merge` | up to four length-prefixed files | merging never panics and a merged file verifies |
| `wal_scan` | a WAL file, as it is and with its checksum chain repaired | the reader never panics, returns whole committed pages, and resuming or limiting at the returned position changes nothing |
| `capture_chain` | a script of transactions (page writes, growth, shrink), fences and batch boundaries, with a torn frame at the end of the WAL | chained captures (each fed the previous position, sums and checksum) have contiguous TXIDs and chained checksums, apply in order to the last database, are refused when missing, repeated or swapped, and do not change with the torn tail |

The harnesses are functions in `src/fuzzing.rs`; the targets in
`native/alambique_nif/fuzz/fuzz_targets` call them. Fuzz builds skip the one
CRC that a fuzzer cannot guess (the LTX file checksum, `cfg(fuzzing)`), and
the harnesses do not apply files that would make a database over 1 MiB.
Seeds are a few KB each in `fuzz/corpus/<target>` (files of the shapes the
format tests use, and the inputs that found the defects below); a run starts
from them and keeps what it finds in a temporary directory. The unit tests
replay the seeds, so they run under Miri too. A crash is kept in
`fuzz/artifacts/<target>` (not committed); turn it into a regression test and
a seed. `cargo test --lib write_seeds -- --ignored` rewrites the seeds.

**Miri and sanitizers.** `scripts/test-native-checked` runs the unit tests
under one checker; the tasks pass the checker:

| Task | Covers | Does not cover |
| --- | --- | --- |
| `test:miri` | undefined behavior, leaks and data races of all Rust code that Miri can run: LTX and WAL parsing, checksums, merge, page application, the lazy page map, the harnesses | everything that calls SQLite (the VFS and SQLite tests are skipped: Miri cannot run C), the allocator against ERTS |
| `test:asan` | out-of-bounds and use-after-free accesses and leaks in Rust (standard library rebuilt with ASan) and in SQLite's C code (built with `-fsanitize=address`), including the VFS tests with a lazily restored database | uninitialized reads, data races, the BEAM allocator (tests run without ERTS) |
| `test:ubsan` | undefined behavior in SQLite's C code (built with `-fsanitize=undefined`, any report aborts) and Rust overflow and unsafe-precondition checks | Rust has no UBSan; undefined behavior in Rust is Miri's job |

**Chained captures.** `capture::testing` (`src/capture.rs`) builds WALs from
generated transactions and captures segments back to back, each fed the
previous position, page sums and checksum. Property tests (`proptest`, 64
cases each; skipped under Miri, which replays one fixed script instead) check
contiguous TXIDs, the checksum chain, the database after applying in order,
and that segment bytes do not depend on a torn WAL tail. `dbsum::apply` checks
checksums, not TXIDs: a missing fence (`pre == post`) passes it, and only
`merge` (and the restore's TXID check) refuses it.

ASan and UBSan compile SQLite with `$CC` (default `clang`). All three use the
same nightly. None of the checks runs the crate inside the BEAM; the panic
tests and the Elixir suite do.

**`unsafe`.** Unsafe code is in `alloc.rs` (the allocator and the hooks that
give SQLite the BEAM's memory), `vfs.rs` (the callbacks and the file
structure SQLite hands over), `sqlite.rs` (the FFI calls and the `Send` and
`Sync` of raw handles) and one call in `stats.rs`. The parsers, the encoder,
merge and the WAL reader have none. The crate denies `unsafe_op_in_unsafe_fn`
and clippy's `undocumented_unsafe_blocks`: every block has a `// SAFETY:`
comment with its invariant, and every `unsafe fn` a `# Safety` section.

**Results (2026-10-27, one development machine, `nightly-2026-10-20`).**
One run of `mise run fuzz all 60`, started from the seeds:

| Target | Runs in 60 s | Executions/s | Coverage (edges / features) |
| --- | --- | --- | --- |
| `ltx_decode` | 3,717,116 | 60,936 | 579 / 1163 |
| `ltx_roundtrip` | 63,248 | 1,036 | 869 / 2577 |
| `page_apply` | 1,677,216 | 27,495 | 583 / 1170 |
| `ltx_merge` | 2,433,708 | 39,896 | 693 / 1563 |
| `wal_scan` | 783,073 | 12,837 | 477 / 1307 |

A second run used `mise run test:asan` and `mise run test:ubsan` (66 tests
each, no report) and `mise run fuzz all 30` (30 seconds per target, the same
pinned nightly). It found no panic or sanitizer report:

| Target | Runs in 30 s | Executions/s | Peak RSS |
| --- | --- | --- | --- |
| `ltx_decode` | 1,764,697 | 56,925 | 474 MB |
| `ltx_roundtrip` | 23,965 | 773 | 441 MB |
| `page_apply` | 933,168 | 30,102 | 486 MB |
| `ltx_merge` | 1,013,150 | 32,682 | 463 MB |
| `wal_scan` | 285,355 | 9,205 | 475 MB |

No panic, sanitizer report or failed property in these runs. Earlier runs
found these defects; each has a regression test and a seed:

- The decoder accepted a snapshot with missing or unordered pages, and
  applying it left a database whose checksum was not the file's. It now
  requires every page, in order (`ltx.rs`).
- Merging could write a snapshot with holes when a later input grew the
  database without carrying the new pages, and applying an increment did the
  same to a database. Both now refuse (`merge.rs`, `dbsum.rs`).
- Three additions could overflow on crafted headers: the next TXID in a merge,
  the end of a page frame in the lazy-restore parser, and the page after the
  commit in an apply. Builds with overflow checks (tests, fuzzing) panicked
  and release builds wrapped; they are now checked.

`test:miri` ran 60 tests (7 skipped because they call SQLite) in 110 s,
`test:asan` and `test:ubsan` ran 66 tests, all without a report. Mutation
checks: a deliberate out-of-bounds read in a test fails under `test:asan`, and
the panic tests abort the VM when `guard` does not catch.

## 3. Store behaviour

`Alambique.Store` is the only interface to object storage.

```elixir
@type key :: String.t()
@type etag :: String.t()
@type condition :: :none | :if_none_match | {:if_match, etag}
@type meta :: %{optional(String.t()) => String.t()}

@callback put(config, key, body :: iodata(), condition, meta) ::
            {:ok, etag} | {:error, :precondition_failed} | {:error, term}
@callback get(config, key) ::
            {:ok, body :: binary, etag, meta} | {:error, :not_found} | {:error, term}
@callback get_range(config, key, offset :: non_neg_integer, length :: pos_integer) ::
            {:ok, body :: binary, total_size :: non_neg_integer, meta} | {:error, :not_found} | {:error, term}
@callback head(config, key) ::
            {:ok, etag, meta} | {:error, :not_found} | {:error, term}
@callback list(config, prefix :: key, start_after :: key | nil, limit :: pos_integer) ::
            {:ok, [key], next_start_after :: key | nil} | {:error, term}
@callback delete(config, key) :: :ok | {:error, term}

# Optional: where the store keeps its data, without credentials (§8).
@callback location(config) :: term
```

- `:if_none_match` sends `If-None-Match: *`. `{:if_match, etag}` sends
  `If-Match`. A 412 maps to `{:error, :precondition_failed}`. A 409
  `ConditionalRequestConflict` is retried by the caller like any transient
  error, never treated as success.
- `meta` maps to `x-amz-meta-*` headers.
- `get_range` returns up to `length` bytes from `offset` and the size of the
  whole object and its metadata (S3: a `Range` request, the total of
  `Content-Range` and the `x-amz-meta-*` headers). The
  body is shorter at the end of the object and empty past it. Lazy restore
  (§16) reads page indexes and pages with it.
- Every other failure, including timeouts, is `{:error, reason}`. A caller
  must assume that a failed `put` may or may not have been applied.
- A store module that raises, throws or exits is treated as a failed request.
  The contexts that the library builds for a location (`Config.lease_ctx/2`
  and `Config.repl_ctx/2`, the only places that hand the store to the cell, the
  compactor and the router) wrap it in `Alambique.Store.Guard`, which answers
  `{:error, {:store_crashed, kind, reason}}`. A cell then retries a renewal
  while its lease is valid and stops with `:lease_expired` after, as for any
  error; a lost lease is still `{:error, :lost}`. `Alambique.Store.S3` does
  the same for its HTTP client with `{:error, {:request_crashed, kind, reason}}`,
  for example when the connection pool of Finch is exhausted; the pool settings
  go in its `:req_options` (`finch: [pool_timeout: ...]`).
- **Hard deadline.** Every store request of the library runs in its own
  process, linked to the caller and killed when its time passes, and then
  answers `{:error, :timeout}`; a store that never returns cannot hold a
  caller. Calls through `Replication.Retry.run/3` are bounded by the operation
  deadline (`op_timeout_ms`, or less); an attempt that starts at or past it
  (`deadline_in(0)` asks for exactly one) gets `ctx.backoff_ms`, 50 ms by
  default, before it is killed.
  The lease `GET` and `PUT` (`Retry.once/2`, one attempt, no loop) are bounded
  by `lease_ttl_ms - skew_margin_ms`, the time the lease can be used: a
  request that outlives it cannot help, and the answer is the store error that
  every caller already handles (a renewal reads the lease back,
  `reconcile_renew/3`; a cell whose lease runs out stops). The change feed's
  listings, reads and deletes are bounded by the `:timeout_ms` of `Feed.ctx/1`
  (`op_timeout_ms` for an instance, 10 000 otherwise). An attempt that raises,
  throws or exits does so in the caller. Cost, measured with a function that
  returns at once: 2.6 us per call against 0.07 us without the process, which
  is small next to a request. A killed `put` may have been applied, as for any
  failed `put`. `Alambique.Store.S3` ends a request after `:receive_timeout`,
  8 000 ms by default, below the default `op_timeout_ms` (10 000), so the
  client gives up before the library does; raise the two together.
- **Attempts.** A request that `Replication.Retry.run/3` repeats stops at
  `retry: [max_attempts:]` attempts (default 10) as well as at its deadline.
  With the default backoff (50 ms doubling to 1 000 ms) ten attempts end after
  about 5.5 s of waiting, before the 10 s of `op_timeout_ms`, so a store that
  fails fast is asked less often than before. The process per attempt stays:
  the hard deadline needs a process that can be killed, and it costs less than
  the request it bounds (above).
- **Circuit breaker.** Every context the library builds for a location
  (`Config.lease_ctx/2`, `Config.repl_ctx/2`) wraps the guarded store in
  `Alambique.Store.Breaker`, one breaker for each location of the instance,
  shared by all its cells, so a store that fails is seen once and not once per
  cell. Its state is four `:atomics`, found once when the context is built and
  read without a message: a request costs one atomic read while it is closed.
  `breaker: [failures: 5, cooldown_ms: 5_000, probe_timeout_ms: 10_000]`:
  - Closed, `failures` failures in a row open it; any success resets the run.
  - Open, a request answers `{:error, :store_unavailable}` at once and never
    reaches the store. `Retry.run/3` does not repeat that answer. A wake that
    fails with it fails as `{:error, :store_unavailable}`, not as
    `{:restore, ...}` or `{:lease, ...}`, and is not remembered
    (`wake: [failure_backoff_ms:]`): asking the breaker is cheaper than a
    restore. An upload that fails with it is repeated by the cell while its
    lease is valid (§6, "Upload failures"); a lease renewal fails as for any
    store error and the cell stops with `:lease_expired` when the lease runs
    out.
  - After `cooldown_ms` (plus up to 20 % at random, so the nodes of a cluster
    do not probe together) one request is the probe. It is claimed with a
    compare-and-swap, so there is never a second one; the others still get
    `:store_unavailable`. A success closes the breaker, a failure opens it
    again for another cool-down. A probe that never answers (its process was
    killed) is given up after `probe_timeout_ms` and the next request is the
    probe.
  - A failure is what says the store may be down: a timeout, including a
    request killed at its deadline, a transport error, a crash of the store
    module, and an HTTP 5xx. `:not_found` and `:precondition_failed` are
    answers of a working store, normal for leases, fences and `if_none_match`
    uploads, and an HTTP 4xx (a bad request, a refused credential) is a
    problem that another request would repeat, not an outage. A request killed
    at its deadline never answers, so `Retry` reports it; only a closed breaker
    counts it.
  - **Lease renewals bypass it** (`Lease.renew/2`). A cell renews every third
    of the TTL and its lease is valid for `lease_ttl_ms - skew_margin_ms`, so
    it has two tries after a failed renewal. A breaker that stayed open after
    the store recovered (up to 1.2 × `cooldown_ms`, plus a probe that another
    request may hold) would fail those tries and every cell would lose its
    lease together, which is the avalanche the breaker is there to prevent. A
    renewal is one request per cell per renewal interval, one attempt, and
    bounded by the lease time, so it needs no protection, and it neither waits
    for the breaker nor counts in it. Acquiring a lease at a wake goes through
    the breaker like the rest of the wake. A store that fails for less than
    the time a lease can be used, and recovers, costs no cell its lease
    (`test/store_breaker_test.exs`).
  - Requests that started before it opened and end after do not change its
    state. The change feed reads its own guarded stores and has no breaker.
  - **Status.** `Alambique.breaker_status/1` reads the atomics (a fourth one
    holds the wall-clock ms of the last change between closed and open) and
    answers, for each location, `:closed`, `:open` or `:half_open`, since when
    and the failures. It never asks the store and adds nothing to a request:
    the extra atomic is written only when the breaker opens or closes. The
    failures stay counted while it is open (and are the run that opened it),
    and a success of a request that started before it opened does not reset
    them. `since_ms` is wall-clock Unix time so that it means the same to the
    caller and to other nodes, not a monotonic reading; a half-open breaker
    reports when its cool-down ended, computed from the monotonic clock. A
    location not used yet has no breaker and reads as closed. The reason of
    the last failure is not kept: it is in the telemetry events and logs.
  Telemetry: `[:alambique, :store, :breaker, :open]` and `:close`
  ([telemetry.md](telemetry.md)).
- `Alambique.Store.S3` implements it with Req and AWS SigV4, with a
  path-style option. SeaweedFS is the only store it is tested on
  ([storage-compatibility.md](storage-compatibility.md)); the AWS paths are
  tested against a stub of the HTTP client and an independent SigV4 signer
  (`test/s3_credentials_test.exs`). Without `:endpoint` it derives
  `https://s3.<region>.amazonaws.com` from `:region`, and keeps the path style
  it uses for any endpoint (`/<bucket>/<key>`; `path_style: false` gives
  `<bucket>.s3.<region>.amazonaws.com`). `:session_token` is sent as
  `x-amz-security-token` and signed. A `:credentials` function returns
  `{key, secret, token | nil}` and optionally an expiry; the store caches it in
  `:persistent_term`, keyed by the function, with the refresh time computed when
  it is called (expiry minus `:credentials_refresh_margin_ms`, or
  `:credentials_ttl_ms` without an expiry; both default to 300 000). A 400 or
  403 whose body has the code `ExpiredToken` renews it, unless another request
  has already, and sends the request once more. Concurrent requests with an
  expired cache may each call the function; the last result is kept. The
  token and the key pair of a request always come from the same call.
  A `head` that is refused for an expired token is not recognised, because
  a HEAD answer has no body to hold the code: it is the plain `{:http, 403,
  _}` error, and the next request that has a body renews the credentials.
  `from_url/2` builds the config from `s3://bucket/prefix`. It does not follow
  redirects: Req would sign the request again for the new host and send it the
  body, so a 3xx is `{:error, {:http, status, body}}` and the endpoint must be
  the region of the bucket.
- **Credentials.** The cell state, the contexts and the placement hold the
  store config, and OTP prints them in a crash report. A store can implement
  the optional `conceal/1`, which the library applies to the `:store` option;
  `Alambique.Store.S3` moves `:access_key_id`, `:secret_access_key` and
  `:session_token` into closures (`:credentials` is one already), which print as `#Function<...>` in a crash report, `:sys.get_state`,
  `inspect/1` and `~p` alike, and resolves them only when it signs. A missing
  key raises an `ArgumentError` that names the key. A custom placement that
  builds its own store configs conceals them itself (`S3` accepts a function of
  no arguments for both). The Mix ops tasks pass the keys and the session
  token as given, without `conceal/1`, so a crash inside a task can print them.
  `test/secret_redaction_test.exs` checks the crash report, the state and the
  errors.
- `AlambiqueTestkit.MemoryStore` implements it in memory, shared by every
  node of a test cluster, with the faults in §11.

### 3.1 Trust model

The library trusts the store. Every check on what it reads (CRC-64 of pages,
the XOR-linear database checksum, the epochs and seqs, the strict key layout)
detects corruption and bugs; none authenticates. Lease tokens are random but
unsigned. Whoever can write to the prefix can forge segments, snapshots, leases
or the feed, take a cell's lease or change its content, and the library accepts
it when the checks agree. Nothing is encrypted at rest by the library; use the
store's own encryption.

What follows for a deployment: give writers a credential scoped to the prefix of
one deployment, readers (the Mix tasks that only look, `alambique.inspect`) a
read-only one, and do not share a bucket prefix with a party you do not trust.
Endpoints given as `http://` are sent in clear.

## 4. Storage layout

All keys of a cell live under the `prefix` of its location, in the location's
store (§18); the instance option `prefix` is the prefix of the single location
of the default placement. For cell `c`:

| Key | Content |
| --- | --- |
| `<prefix>/cells/<c>/lease` | Lease object (§5), JSON. |
| `<prefix>/cells/<c>/0000/<min>-<max>.ltx` | Level-0 segment: one replicated commit batch, with `min == max` (§6). |
| `<prefix>/cells/<c>/000<L>/<min>-<max>.ltx` | Compacted segment at level `L`, 1 to 8 (§9). |
| `<prefix>/cells/<c>/0009/0000000000000001-<max>.ltx` | Full snapshot as of TXID `max`: a file of the snapshot level, starting at TXID 1. |
| `<prefix>/cells/<c>/frozen` | Present while the cell is frozen (§10). |
| `<prefix>/cells/<c>/trashed` | The TXID of the newest anchor whose covered files retention marked for deletion, as decimal text (§9.1). Absent when files are deleted at once. |
| `<prefix>/trash/<due>/<c>/<id>` | Manifest of a delayed deletion, JSON `{"cell": c, "keys": [...]}`; `<due>` is the time the keys may go, in milliseconds, sixteen digits (§9.1). |
| `<prefix>/cells/<c>/crashes` | Crash counter of the cell: JSON, written with conditional puts (§10). |
| `<prefix>/schema/<version>/<c>` | Empty schema index entry of cell `c` at `<version>`, ten digits (§17). |
| `<prefix>/nodes/<node_id>` | Entry of a node in the node registry, JSON (§22). |
| `<prefix>/feed/<shard>/<time>-<txid>-<c>` | Change feed object of cell `c`, one per published group of one or more batches with row changes; `<txid>` is that of the last batch; `<shard>` is a hash of `c` (§13, §19). |

Every segment and snapshot carries object metadata: `epoch`, `data` (the data
TXID, §6) and `node` (the writer), `ts` (the stamp of the newest state in it,
in milliseconds) and, for snapshots, `created` (when the object was written).
Compaction and retention read ages and epochs from the metadata with one
`HEAD`, without fetching the file. A missing `epoch` counts as 0, a missing
`data` as the object's own maximum TXID (§6); a snapshot without `ts` is read
once for the stamp in its header.

The cell prefix follows Litestream's replica layout: levels are directories
of four hex digits and snapshots live at level 9 (see
[segment-format.md](segment-format.md) for what is checked, including the
Litestream 0.5.18 restore test and its limits).
Litestream lists only the level directories, so the lease, the frozen marker,
the `trashed` marker and the crash counter beside them are not part of a
replica; the schema index, the feed and the trash manifests live outside the
cell path.

TXIDs are written as 16 lowercase hex digits so lexicographic order equals
numeric order. The segment format decision and its LTX details are recorded
in [segment-format.md](segment-format.md).

## 5. Lease protocol

The lease object holds:

```json
{"cell": "c", "epoch": 7, "owner": "node@host", "incarnation": "<random>",
 "token": "<random>", "seq": 42, "expires_at_ms": 1790000000000, "released": false}
```

The holder of a lease is the pair `owner` (the `node_id`) and `incarnation`, a
random 128-bit value drawn when an instance starts. It is never stored on disk,
so every start of an instance is a new holder, even with the same `node_id` and
the same `data_dir`. A lease is "ours" only when both match. Two instances with
one `node_id`, started by a mistake or on purpose, are two holders: the second
treats the lease of the first as another node's, and takes the cell after the
release or the expiry, never before. A lease with no `incarnation` (an object
written by a tool that holds no cell) is never ours. Fencing therefore does not
depend on `node_id` being unique, and nothing checks that it is.

The cost is the restart of a crashed node. A VM that dies leaves the leases of
its awake cells unreleased, and the restarted instance is a new holder: it
serves those cells after `lease_ttl_ms` plus `skew_margin_ms` (32 s by default)
instead of at once. A graceful stop and `Alambique.drain/2` release the leases
and cost nothing. Persisting the incarnation in `:data_dir` would let a restart
on the same disk reclaim at once, but a copied disk would share an incarnation
between two instances, and the fence would rest on documentation again. The
incarnation stays in memory. A cell process that dies inside a running instance
is still reclaimed at once, because the incarnation is the instance's.

A lease can also carry `moved_to` (the id of the location the cell moved to;
the lease is then released and acquiring it answers `{:error, {:moved, id}}`
until a move back replaces it), `moved_at_ms` (when it was marked),
`cleaned_at_ms` (when the objects the cell left there were deleted) and
`moved_from` (on the free lease a move installs at the destination). They are
described in §18.

`token` is random per acquisition. `seq` grows by one on every write of the
object, whoever writes it, so no two writes have the same bytes (S3 etags
are content hashes). Readers ignore unknown fields.

- **Acquire.** `get` the lease.
  - Not found: `put` with `:if_none_match`, epoch 1.
  - Found and (`released` or expired by the acquirer's clock minus the
    configured skew margin): `put` with `{:if_match, etag}`, epoch + 1.
  - Otherwise: `{:error, {:owned, owner, expires_at_ms}}`.
  - A 412 means another node won: re-read and report the new owner.
  - Any other `put` error may hide an applied write: re-read, and report
    success only if the stored `token` is the acquirer's own.
- **Renew.** The owner `put`s its own lease with `{:if_match, last_etag}`,
  same epoch, new expiry, at `ttl / 3` intervals, each moved at random by up
  to `lease_renew_jitter` (default 0.1) of the interval, so cells woken
  together do not renew together. A 412 or a renewal that
  cannot complete before the local deadline `expires_at_ms - skew_margin`
  means the lease is lost: the cell stops serving immediately. After any
  other `put` error, re-read: if the stored object has the owner's token and
  epoch and a higher `seq`, a renewal landed and the owner adopts its etag;
  otherwise the lease is lost. The module does not fail a renewal because
  the local deadline passed: the cell checks `valid?` and stops serving, and
  a later renewal that still wins the `If-Match` revives nothing it did not
  already hold.
- **Release.** `put` with `{:if_match, last_etag}` and `released: true`,
  keeping the epoch. The lease object is never deleted, so epochs only grow.
  Releasing again, or after an unacknowledged release landed, is a success.
- **Take over.** `Alambique.take_over/4` replaces the lease at once, whoever
  holds it and whatever its expiry (§5.1). `get` the lease.
  - Not found: like acquire, `put` with `:if_none_match`, epoch 1.
  - `moved_to` set: `{:error, {:moved, to}}`.
  - Otherwise, held or not, live or expired, released or not: `put` a lease
    with epoch + 1, a new `token` and the caller as owner, with `{:if_match,
    etag}`. A 412 means the lease changed since the read: re-read and report
    `{:error, {:contended, owner, epoch}}`, and the caller may retry. Any
    other `put` error is reconciled by the token, as for acquire.
  - With the option `:epoch`, a stored epoch that differs gives `{:error,
    {:epoch_changed, stored_epoch}}` and nothing is written. It guards
    against taking a cell from a new, healthy owner on a suspicion about the
    old one.
  - The reason the caller gives is in telemetry and the log, never in the
    lease object.
- The owner treats the lease as valid only until `expires_at_ms -
  skew_margin` by its own clock. Other nodes treat it as expired only after
  `expires_at_ms + skew_margin` by theirs. Clock skew larger than the margin
  can create overlapping owners in time, which §6 fences.

**Invariant L1.** At most one node ever holds a given `(cell, epoch)`.
Conditional writes on the single lease key guarantee it.

### 5.1 Early takeover

A failure detector or the platform may suspect, or know, that an owner is gone
long before its lease expires. `Alambique.take_over(instance, cell, reason)`
runs on the calling node: if a process of this node holds the cell it answers
`{:ok, node()}`; otherwise it starts the cell process with a takeover flag.
That process wakes as in §7, except that step 1 is `take_over` instead of
`acquire` (and a lease that names this instance is not "reclaimed": it is simply
replaced). The call returns after the wake, so after the fence segment of the
new epoch is stored, and the owner hint of the cell is dropped on this node.

**Fencing.** The takeover raises the epoch with a conditional write, and the
fence of §6 lands at the first TXID after what the new owner restored. From the
return of `take_over/4` on:

- an upload of the old owner needs a TXID that the new owner holds or will
  hold, so it fails with `:precondition_failed` on different bytes, and the old
  cell stops with `:fenced`; a batch whose `put` collided was never
  acknowledged and is not in the chain (§6), so its writers are refused with
  `:fenced`;
- an upload of the old owner that landed before the fence was restored by the
  new owner, which is why no acknowledged write is lost, and nothing is applied
  twice (the fence retries after restoring a segment it found, §6);
- the old owner's renewal fails with a 412, so it stops with `:lease_lost`
  without releasing a lease that is no longer its own.

This holds when the suspicion was wrong and the old owner is alive and
unaware. A segment of the old owner that is acknowledged to its caller after
`take_over/4` returned (its acknowledgement was lost and retried) landed before
the fence, so its TXID is below the new owner's head.

**Notification.** When the lease it replaced named a node that still runs the
instance (its `node_id` and incarnation are in the group of §8), the new cell sends that node
an asynchronous message after the wake; the old cell then renews its lease at
once, finds the 412 and stops. Nothing depends on the message: it only
shortens the window below.

**Stale reads.** A cell answers a plain read from its local database while its
lease is valid by its own clock. After a takeover the old owner can therefore
still answer reads that miss writes the new owner acknowledged, until it learns
of the new epoch. It learns of it at the first of these:

- the notification above: within the time the message and one renewal request
  take after `take_over/4` returned (the message waits behind the calls already
  in the cell's mailbox);
- its next upload, which fails (`:fenced`);
- its next renewal, which runs every `ttl / 3` after the previous one, so at
  most `ttl / 3` plus the freeze check (one `HEAD`) and one renewal request
  after the takeover;
- the end of its own lease: it stops answering at `expires_at_ms -
  skew_margin_ms` by its clock, which is at most `ttl - skew_margin` after its
  last successful renewal, and no renewal succeeds after the takeover. This is
  the bound that holds when the old owner cannot reach the store.

So the stale window after `take_over/4` returns is the notification round trip
for an owner that is reachable, `ttl / 3` plus two requests when it is not told
but can reach the store, and `ttl - skew_margin` in the worst case. A read that
waits behind unacknowledged writes is answered after their batch, which fails
once the fence is there, so it is never stale. Tests measure these bounds with
the real clock (`test/takeover_properties_test.exs`).

**Checked reads.** `Alambique.query(instance, cell, sql, params, verify_lease:
true)` closes the window for callers that need it. The cell runs the
statement, then reads the lease object (one `GET`, in the cell process; one for
all the checked reads of a batch that waited for its upload) and answers only
if the stored lease still has the cell's `epoch` and `token` and is not
released. Otherwise the cell stops with `:lease_lost` without answering; the
call exits with the cell, and the router looks again (§8), which reaches the
new owner, or an error while nobody can serve. If the lease cannot be read the
answer is `{:error, {:lease_unreadable, reason}}` and the cell keeps serving
(`reason` is `:store_unavailable` while the circuit breaker of the store is
open, §3).

Reading the lease after the statement, not before, is what makes the answer
linearizable. Let `R` be a checked read issued after a write `w` was
acknowledged. If `w` was acknowledged by this epoch, the cell's database holds
it. If a newer epoch acknowledged `w`, its owner had stored the fence first,
which needs the lease of the newer epoch to have been written before `w`'s
acknowledgement, so before `R` was issued and before the cell reads the lease;
the lease protocol already relies on a store whose reads see completed
conditional writes, so the read sees the newer lease and the cell does not
answer. A lease never goes back to an older
token, so a read that finds the cell's token has seen no takeover so far. The
cost is one `GET` per checked read, which a plain read does not pay.

The `GET` cannot be answered from the lease the cell already knows (valid by
its own clock, minus the skew margin): that lease says nothing about a newer
one. A takeover (§5.1) or a move writes a newer lease at once and does not wait
for the old one to expire, and the window above (`ttl - skew_margin` at worst)
is exactly the time during which the cell's own view is valid and wrong. A
checked read exists to close that window, so only the stored lease can answer
it.

## 6. Replication and fencing

The awake cell is the only writer of its SQLite file. It opens it in WAL
mode, and frames stay in the WAL until the cell checkpoints after they are
replicated. Every connection that `Alambique.Sqlite` opens is set up in the
NIF so that nothing else can move frames into the file:

- it does not checkpoint when it closes (`SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE`),
  which includes a connection that is garbage collected or whose owner
  crashed;
- automatic checkpoints after a commit are off (`sqlite3_wal_autocheckpoint`
  with 0);
- an authorizer refuses, when a statement is prepared and whatever its
  syntax, `PRAGMA wal_checkpoint`, `PRAGMA wal_autocheckpoint` with a value
  and `PRAGMA journal_mode` with any value but `wal`. The same authorizer
  enforces the rules of statements in §7 ("Statements of callers").

`Alambique.Sqlite.checkpoint/1` (`sqlite3_wal_checkpoint_v2`, `TRUNCATE`) is
the only checkpoint, and `Replicator.checkpoint/2` the only caller in a
cell. The cell calls it at hibernation and, while awake, when the `-wal` file
passes `wal.checkpoint_bytes` (default 64 MiB; "The WAL of an awake cell"
below) or when `Alambique.checkpoint/3` asks.

**Commit pipeline.** A write call is acknowledged to the caller only after
its transaction is durable in the store:

1. The cell runs the caller's transaction on the local database.
2. Transactions that commit while the pipeline is full are grouped into the
   next batch (group commit).
3. The NIF reads the WAL frames of the batch and encodes one LTX segment
   with `min == max == next TXID`, carrying the pre-apply and post-apply
   checksums. The replicator state advances at the encode
   (`Replicator.encode/4`), so the next batch can be encoded before this one
   lands.
4. A linked upload process `put`s `0000/<txid>-<txid>.ltx` with
   `:if_none_match` and metadata `epoch` and `node` (`Replicator.upload/3`)
   and reports to the cell.
5. Callers are answered in TXID order, when the batch and every batch before
   it landed (the pop loop, below). On `:precondition_failed`, the upload
   process `get`s the object and applies the put rule below. On any other
   error it retries the same bytes with backoff until the lease deadline,
   then reports a failure and the cell stops.

**The pipeline.** The instance option `pipeline_depth` (1 to 16, default 8;
16 is `Chain.window/0`) bounds the batches that are encoded and not yet
acknowledged. The cell keeps them in a FIFO queue (`Alambique.Cell.Pipe`)
next to `acked`, the newest acknowledged TXID, epoch and data TXID. The
replicator state is the *encoded* head and is ahead of `acked` by the queue.

- A flush encodes the pending commits as a batch and sends it to an upload
  process if the queue has a free slot; otherwise it waits for the next
  acknowledgement and the commits meanwhile grow the batch.
- When an upload process reports `:ok` the cell marks the batch as landed.
  The only place that answers writes pops the head of the queue while it has
  landed, so a batch is acknowledged only after every earlier one: the
  acknowledged TXIDs are always a prefix of the chain. The compactor
  notification, `:after_ack`, the `[:cell, :commit]` event and the feed
  publication follow each pop; the feed is bounded by `acked`, because rows of
  a batch that is not acknowledged may never become durable.
- A read waits for the newest batch (the open one when it has items), which
  implies every earlier write is acknowledged.
- Upload failures. A put that fails for a store reason (an error, a timeout,
  a 5xx, `:store_unavailable`) says nothing about the lease, so it does not
  stop the cell. The upload process has already retried until its deadline
  (`op_timeout_ms`, cut at the end of the lease; `retry: [max_attempts:]`);
  when it reports the error the cell keeps the batch in its place in the
  queue and, after a backoff, starts a new upload process for the same
  `upload` (`upload: [retry_backoff_ms: 500, retry_backoff_max_ms: 5_000]`:
  the wait doubles up to the maximum; `[:cell, :upload_retry]`). This goes on
  while the lease is valid by this node's clock and the skew margin, which is
  the deadline of the retries: a store that is down for less than a lease does
  not stop any cell, and one that is down longer stops each cell when its
  lease runs out. Why a retry is safe:
  - The key of a batch is a function of the cell and its TXID, and the put is
    `:if_none_match`: a second put of the same batch cannot create a second
    object. If the first one landed and its answer was lost, the retry gets
    `:precondition_failed`, reads the object, finds the same bytes and epoch
    and answers `:ok` (the put rule below). If another owner took the TXID it
    gets `:taken`, which is not a store failure and stops the cell as before.
  - A retry keeps the place of its batch in the queue and only the head is
    acknowledged, so the answers stay in TXID order whatever the order of the
    landings; the retried batch is never re-encoded, so no later batch can
    take its TXID.
  - New writes meanwhile commit locally and wait for the next batch, behind
    the unconfirmed ones; a full pipeline defers the flush, as without
    failures. They count in `max_pending`, so the callers waiting on a cell
    that cannot upload meet `{:error, :busy}` at the limit and nothing grows
    without bound.
  - A writer waits for the acknowledgement of its batch for as long as its own
    call timeout. A caller that stops waiting gets `{:error, :timeout}`; the
    write is still in the cell, may land with a later retry, and must be
    treated as unknown.
  The stop that remains: a put that returns `:taken` (`:fenced`), an upload
  process that exits, a capture error, a lost or expired lease, or a retry
  that finds the lease no longer valid (`{:sync_failed, reason}`, the reason of
  the last failure). A drain (hibernation, `position`, migrations) waits for
  the retries too, so it can last until the lease runs out. The stop kills the
  upload processes. Nothing is re-encoded and
  nothing is acknowledged out of order. The callers get these answers:
  - A write of a batch that was encoded and sent gets
    `{:error, :outcome_unknown}`, whatever stopped the cell and whether or not
    its own upload failed: the put may have landed before or after the process
    was killed, and the caller must not repeat it. A batch behind the failed
    one may have landed too, and its object is a tail (below).
  - The one exception is the batch whose own put returned `:taken`. Its key
    holds another writer's object, or was replaced by one: an earlier attempt
    of ours that landed would have found its own bytes and epoch there and
    returned `:ok`, and a zombie is replaced only by a higher epoch, which
    removes it from the tail. Its writers get `{:error, :fenced}` and may
    repeat the write. A batch behind it is not covered by this: a put of the
    old owner may still land past the gap, so its writers get
    `:outcome_unknown`.
  - A write that was committed locally and not yet encoded (a full pipeline,
    or the flush that failed) was never sent: it gets the reason, as does a
    read, which changes nothing.
  The model checks the rule (`tla/Pipeline.tla`, `NoRefusedDurable`).
- `position`, `migrate`, hibernation, drain, shutdown and the checkpoint of an
  awake cell (below, also on request) first encode what is pending and wait until the queue is
  empty (`drain`). The cell answers no
  other call meanwhile, for at most the upload deadlines (`op_timeout_ms`). The
  checkpoint of the hibernation runs on an empty queue, so every frame it
  moves into the file was replicated and acknowledged.
- Fault points: `:repl_after_put` runs in the upload process (a handler that
  kills `owner_pid` crashes the cell), and the cell adds `:cell_after_land` and
  `:cell_before_ack`.

`test/upload_retry_test.exs` and `test/upload_retry_properties_test.exs` check
the retries: random store failures and a lease that runs out leave the segments
as TXIDs 1..N once each, the rows of the acknowledged writes in commit order,
and a stopped cell only when the lease was lost.
`test/pipeline_properties_test.exs` checks the pipeline with a gated store that decides
the landing order, crashes at the fault points and takeovers: every
acknowledged write is in the state a restore gives, the restored state only
grows and nothing is deleted. `tla/Pipeline.tla` models the same rules.

**The WAL of an awake cell.** With automatic checkpoints off, only a
checkpoint resets the WAL, so an awake cell that never sleeps would grow it
with every distinct page it writes (60 s of `mise run bench:wal` reached 2 to
4 GB). After each landed upload the cell reads the size of the `-wal` file.
At or above `wal.checkpoint_bytes` it runs `drain` (encode what is pending,
wait for the queue to empty) and then `Replicator.checkpoint/2`, which refuses
unreplicated frames and restarts the replicator's WAL position, as at
hibernation: the next capture scans the new WAL from its start, whose salts
differ from the old one's. The cell answers no other call during the `drain`
and the `TRUNCATE`, as for `position`; with the default limit that is once per
64 MiB of WAL. A checkpoint that reports `:checkpoint_busy` or unreplicated
frames leaves the WAL for the next acknowledgement. A cell whose file is
still hydrating (§16) waits for the end of the hydration. The cell emits
`[:alambique, :cell, :checkpoint]` and reaches the crash point
`:cell_after_checkpoint`; a cell killed there restores from the store at its
next wake, like any cell that dies awake (no marker, §7).

**The WAL budget.** `wal.max_bytes` (default `nil`, none) bounds the disk
that the WALs of all awake cells use together, which `wal.checkpoint_bytes`
per cell does not: 20 000 cells at 64 MiB each is far more than any disk. A
process per instance (`Alambique.WalBudget`) wakes every
`wal.budget_interval_ms` (default 1 000, first round after a random part of
it) and adds the sizes the cells publish after each landed upload (the same
stat the checkpoint check takes, slot 4 of the load counter), one registry
scan and no file access. When the sum is at or above the budget it calls
`Alambique.checkpoint/3` on the cell with the largest WAL, then on the next,
until the sum is under the budget. The cell runs its usual `drain` and
`TRUNCATE`, so no frame that is not replicated is truncated, and only the cell
being checkpointed answers no other call meanwhile; the round runs in its own
process and no cell waits for it. A cell that answers `{:error, :busy}`
(uploads not acknowledged within `wal.checkpoint_deadline_ms`, or SQLite
busy) or `{:error, :disk_full}` (a checkpoint that finds no room) is skipped until the next round; a round therefore takes at most one
deadline per busy cell. The default is off because the right value depends on
the disk, and a value derived from `checkpoint_bytes` would make existing
nodes checkpoint without being asked; size it as the free space of the
`:data_dir` volume minus the database files. The sum is only as fresh as the
last upload of each cell and the interval, so the WAL files can pass the
budget by what the cells write in one interval. Each round that reached the
budget emits `[:alambique, :wal, :budget]`.

`Alambique.checkpoint/3` runs the same two steps on request and keeps the cell
awake. Its `drain` is bounded by `wal.checkpoint_deadline_ms` (default 10 s,
`:deadline_ms` per call): when the queue has not emptied by then, the call
answers `{:error, :busy}` and the uploads go on untouched (the cell does not
stop, unlike a drain that exceeds the upload deadlines). The drain is what keeps
the rule: `Replicator.checkpoint/2` only refuses frames that no batch has
encoded yet, so frames of a batch in flight would be truncated before the store
acknowledged them. `test/checkpoint_test.exs` holds a put in a gated store and
requires the truncation to wait for it (it fails if the checkpoint runs
before the drain). A cell nobody owns answers `:asleep` and is not woken, as for
`migrate` without `wake: true`. A hydrating file (§16) answers `{:error,
:busy}`.

`test/wal_bound_test.exs` checks the bound with sequential and concurrent
writers, a restore from the store and a kill after a checkpoint.

**Put rule.** The same rule serves the commit, the wake fence and a rewind's
replace segment (§20). After `:precondition_failed` the writer reads the
object at the key and compares its epoch with the *floor epoch*, the epoch of
the segment before it in the chain (`last_epoch` of the replicator before the
put; for a replace, the epoch of the head):

- same bytes and same epoch as the attempt: an earlier attempt landed and its
  acknowledgement was lost, so the put succeeded;
- an epoch below the floor: a *zombie*, an object that a fenced-out owner
  left past the chain (see the tail below). The writer overwrites it with
  `{:if_match, etag}` of the object it read. If that fails with
  `:precondition_failed` the object changed meanwhile and the writer reads
  it again;
- anything else: the TXID is taken. The cell has lost ownership, fails the
  batch, and stops (the fence restores again instead).

Writers only create (`:if_none_match`), so an old owner can never replace a
segment. Only the owner of a higher epoch replaces, and only an object that
no acknowledged batch can be. Nothing deletes a zombie: a delete by key would
race with this overwrite.

TXID 1 is always the whole database, because LTX files that start at TXID 1
are snapshots (see [segment-format.md](segment-format.md)). The TXID that the
next batch will take is known before the transaction runs
(`Alambique.Replication.Replicator.next_txid/1`).

**Fence on wake.** After acquiring epoch `e` and restoring up to TXID `n`,
the new owner writes an empty fence segment at TXID `n + 1` with
`:if_none_match` and `epoch = e`. If it gets 412, a stale owner wrote
`n + 1` first: the new owner restores again, which adopts that segment, and
retries the fence. Once the fence lands, every write by an older epoch collides on a TXID the new owner
holds.

**Late puts.** When a cell stops it kills its upload processes and releases
its lease, but a request that was already sent is not recalled: the store may
apply it after the release, after a new owner woke and after that owner
committed. Nothing waits for it; no code needs to, because the keys and the
conditions make a late put harmless. The upload process puts one kind of
object, a level-0 segment at `0000/<txid>-<txid>.ltx`; the cell's other
processes that put are the feed sender, which is covered below, and nothing
else (snapshots and merged files come from the compaction, which reads the
chain with the rules above and does not depend on the lease). Let `h` be the
head the new owner restored, so its fence is TXID `h + 1`, and `t` the TXID of
the late segment:

- `h < t` and the new owner already holds `t` (the fence or a commit): the key
  exists, so the put fails with `:precondition_failed`. Nothing is replaced,
  whatever the epochs.
- The put landed before the new owner restored (`t <= h`): the segment is part
  of the chain the new owner continues, whole, because a segment is one object
  and one transaction. Its writers were told `:outcome_unknown`, and it is
  there. A restore never applies half a batch.
- `t` is past the new owner's head and lands first: an object of an older
  epoch in a TXID the new owner has not used. A reader ignores it: it follows
  a segment of a higher epoch (the fence), which R2 excludes from the chain,
  and it is a tail by its key (above). The new owner's own put at `t` meets
  it, sees an epoch below its floor and replaces it with `{:if_match, etag}`
  (the put rule); after that, nothing of the old epoch remains at `t`.
- A put that fails its condition is a no-op in the store: the old owner never
  overwrites, and the new owner's objects and later writes are untouched.

So the rule that makes a late put harmless is the pair "one key per TXID,
created only with `:if_none_match`" and "a reader and a writer treat an epoch
below the preceding segment's as outside the chain". The epoch is in the
object metadata, not in the key; the TXID is the key. No deadline or wait is
needed before the release.

One case rests on time instead of on a key. `:if_none_match` succeeds when
the key does not exist, so a put that outlives the deletion of its key would
recreate it. Only retention deletes level-0 files (compaction keeps them,
§9): a file whose last TXID is at or below the anchor, once the anchor is
`pitr_window_ms` old (default seven days) and `grace_ms` old (default ten
minutes), and `deletion_grace_ms` after it was marked (default one day; 0
deletes at once, §9.1). So with the defaults a put must stay in flight for
more than a day to find its key gone, against `op_timeout_ms` (10 s) for the
call and the `receive_timeout` of the S3 store (8 s) for a request. Every
store call is killed at its deadline (§3), so a call a cell gave up on does not
outlive it, though a killed `put` may still have been applied. The cell does
not wait for or cancel a request after that. If it happens, the resurrected
object does not change what a restore returns. It has an old epoch and a TXID
at or below the anchor; restore starts at a snapshot at or above the anchor
and walks only files after it, and the compaction plans only blocks after the
newest snapshot, so neither reads it, and it is not a tail (tails lie past
the head). Retention counts it as covered by the anchor it already recorded
and does not mark it again, so with a deletion grace it stays in the store as
an inert object (one segment); with `deletion_grace_ms: 0` the next run
deletes it. Points before the anchor are already gone (§9).

A late feed object (§13) is keyed by the time of
its own batch and read after `settle_ms`; the new owner publishes the rows
the old one left, so a late object duplicates records and loses none.
`test/late_upload_test.exs` holds the old owner's requests inside the store
(`Alambique.DetachedStore` keeps a put alive after its caller is killed,
`GatedStore` holds it) until the owner stopped and released and a second
instance committed, then lets them go. It checks the cases above for
segments (refused at the fence and at a commit, landing past the head and
replaced by the next commit, landing before the takeover as a whole batch of
two rows) and for a feed object. After each it compares the rows restored
from the store with the new owner's state and with its later writes. With
`:if_none_match` replaced by an unconditional put in `Replicator`, three of
the four tests fail.

**Data TXID.** A fence takes a TXID but commits no data, so the head TXID
cannot tell a woken cell from a written one. The data TXID of a cell is the
TXID of the newest commit in its chain that changed data. Every segment,
compacted file and snapshot carries it as object metadata `data` (decimal
string, like `epoch`):

- A segment with pages (a batch) has its own TXID. A wake that runs
  migrations commits data, so it moves it; a transaction that changes no page
  uploads nothing.
- A fence has the data TXID of the state it follows, so a wake never moves it.
  A rewind's replace segment (§20) has its own TXID.
- A snapshot, and a compacted file, has the `data` of the newest state it
  holds (for a merge, that of its last input, §9).
- A restore sets the data TXID of the state to the `data` of the last file it
  applies, and the head read does the same. A new owner therefore continues
  from the head file, and its fence carries the value.
- An object without a readable `data` counts as its own maximum TXID. That
  can flag a wake as data but never hide data. A fence written on top of such
  a chain carries the conservative value forward.

`Alambique.position/3` returns it as `data`. No data was committed after TXID
`t` exactly when `data <= t`.

**Restore.** List the cell prefix and pick the newest at or below the target,
then list segments after it across levels, choose the covering chain, and
apply in TXID order. The chain must be contiguous, each segment's pre-apply
checksum must match the database checksum before applying it, and segment
epochs must never decrease along the chain. Restore stops at the target
TXID or timestamp (point-in-time restore): a timestamp target stops before
the first segment stamped later than it. A candidate segment from a lower
epoch than the chain so far is skipped in favour of another candidate; if
none is left, restore fails with `{:error, {:epoch_regression, key, epoch,
chain_epoch}}` instead of returning a shorter chain, unless what is left is a
tail (below). The result is written to a temporary file and renamed into
place.

**The tail.** An owner that has several uploads in flight when it crashes or
is fenced out can leave files that landed out of order: a gap, and files past
it. Every reader (restore, the head read, the replicas of cross-cell queries)
ends the chain at its head and ignores the *tail*, the unreachable files that
are all of this shape, judged by their keys alone: level 0, one TXID
(`min == max`) and no further than 16 past the head (the cap of the
pipeline depth, `Alambique.Replication.Chain.window/0`). When nothing starts
at the next TXID, or every candidate there is from a lower epoch, restore
ends at the head if the files left are a tail. Anything else keeps failing as
below, so a real loss of history is still loud. A mismatch of the pre-apply
checksum is never a tail.

The head read (`Alambique.Replication.head/3`, used by the wake to reuse a
local file, by rewind and by `Alambique.position/3` of a sleeping cell) is
the head of the chain, not the highest key. It follows the keys from the
newest snapshot, reads the epochs of the last 17 files and stops before the
first epoch that goes back.

A tail is safe because every reader ignores it. An owner with
`pipeline_depth` above 1 can leave one when it crashes or loses its lease with
several uploads in flight. Nothing lists the cells that have a tail, and the
scrub does not report one.

Restore never returns an older state as if it were the target, which matters
once retention has deleted files (§9). It fails instead:

- `{:gap, txid}`: nothing starts at `txid` although later files exist inside
  the target and they are not a tail, for example after an interrupted
  retention run. A file lost among the newest 16 level-0 files looks like a
  tail and is not detected;
- `:history_truncated`: files exist but none starts at TXID 1 and no snapshot
  fits the target, so the history before the oldest snapshot is gone;
- `{:history_coarse, txid}`: a timestamp target where every file that could
  come next is a merged file stamped after the target, so a missing
  single-TXID file would be needed to place it.

A store with nothing, or a timestamp before the very first file, still gives
an empty database at TXID 0.

**Invariant R1.** Restoring from the store after any crash yields exactly
the state of the last acknowledged batch, or a later one whose segment
landed but whose acknowledgement was lost. No acknowledged batch is ever
missing.

**Invariant R2.** A segment written by epoch `e` is never part of the
restored chain after a segment of epoch `e' > e`.

**Invariant R3.** A tail is ignored and only lower-epoch objects are
overwritten. A reader ends the chain at its head and ignores a tail past a
gap, and a writer replaces an existing object only when its epoch is below
the epoch of the segment before it (the put rule). An acknowledged segment
therefore never has a lower epoch than the segments below it, so ignoring or
overwriting a zombie cannot lose an acknowledged batch.

**Invariant S1.** A snapshot that `Replicator.snapshot/3` uploads has the
checksum and content of the replicated state, whatever runs beside it
(`test/snapshot_properties_test.exs`: eight replicators writing, syncing,
checkpointing and taking snapshots at once, compaction and retention on the
same store, store faults, SQLite work on other connections). The snapshot
reads the database file and the replicated WAL frames, so it depends on the
rule that only `checkpoint/2` checkpoints (above). A checkpoint made any other
way while frames are not replicated moves them into the file, and the
snapshot then refuses with `:snapshot_checksum_mismatch` instead of uploading
a state that was never replicated.

**Model check.** `tla/Pipeline.tla` is a TLA+ model of the lease, the fence,
the restore chain and acknowledgement from a FIFO of batches with up to
`Depth` of them in flight. TLC checks R1, R2 and R3 on it, together with the
other properties listed in
[how-to-check-the-pipeline-model.md](how-to-check-the-pipeline-model.md);
run it with `mise run check:tla`. The model covers batches that are encoded
ahead of the store, as the commit pipeline above does.

### 6.1 Scrub

A replica nobody reads can rot unnoticed: an object damaged or lost in the
store is found when the cell wakes. The scrub looks for it earlier.
`Alambique.Scrub` (instance option `scrub`, off by default; `Alambique.scrub/2`
runs one round now) restores sampled cells into a temporary directory under
`data_dir`, without a lease and without waking them, the way a cross-cell
query does (§14), and checks:

1. the restore: every file, the chain (no gap, no epoch regression) and the
   pre- and post-apply checksums that restore verifies (above);
2. that the checksum of the restored file, computed again from its pages,
   equals the checksum the chain ends with;
3. SQLite's `PRAGMA integrity_check` (or `quick_check`, option `check`) on the
   restored file, opened read-only.

A restore that fails is tried again (`attempts`, default 2) before the cell
counts as damaged, because a restore can race with a deletion. A damaged cell
is logged as an error and emitted as `[:alambique, :scrub, :cell]` with the
reason; the cell is not frozen, because a damaged replica is still the only
copy and freezing would stop writes that may repair nothing. A cell without
objects is `:empty`. The directory is removed in every case.

Sampling: each location has a cursor that walks the ids page by page across
rounds. A draw reads `page_size` ids after it, picks one at random and moves
the cursor on; each round makes `cells` draws, so it never lists a location.
Per pass over the ids every cell has the same chance, `1 / page_size`. The
cursor is in an ETS table owned by the instance (shared by the background scrub and `Alambique.scrub/2`) and starts at a random id after a restart. A round takes
`cells` restores (default 10) every `interval_ms` (default one hour), at most
`rate` cells per second (default 1) and `concurrency` at a time (default 1):
the cost is `cells` times the size of a cell in `GET`s per round, per node.
Every node scrubs on its own cursor.

### 6.2 A full local disk

A full local disk is `{:error, :disk_full}` to the caller, told by code and
never by SQLite's text: `SQLITE_FULL` (what an `ENOSPC` on the database, the
WAL or a temporary file becomes), `ENOSPC` and `EDQUOT` from the native
library (`Error::DiskFull`) and `:enospc` and `:edquot` from Elixir's file
operations. A step that wraps the reason of the step below it
(`{:migration, version, reason}`, `{:restore, key, reason}`) gives
`:disk_full` when the reason it wraps is one (`Alambique.DiskFull`).

What the cell does:

- A statement or transaction that hits it rolls back and answers
  `{:error, :disk_full}`. The cell stays awake and serves: the failure is the
  same kind as `:statement_timeout`. Nothing acknowledged is lost, because a
  write is acknowledged only after it is durable in the store, and the refused
  write left no row in the file or the WAL. The next write that fits goes
  through; the node recovers when space is freed.
- A wake that cannot write the file (restore, migrations) fails with
  `:disk_full`. The cell stops and the caller gets the error. The failure is
  not remembered (`Admission.failed/3` keeps only the failures a retry would
  repeat and the store's), so the next call wakes the cell again and succeeds once space is
  free. Telemetry carries `reason: :disk_full` on `[:cell, :wake]`.
- A checkpoint or the marker of a hibernating cell that the disk refuses leaves
  the WAL in place or the file unmarked. No caller is waiting and nothing is
  lost: the store has every acknowledged write, and the next wake restores
  instead of reusing the file.
- A page the hydrator of a lazy restore cannot write is retried with backoff,
  like a failed fetch (§16); a statement that waits for it ends at its deadline.

`PRAGMA max_page_count` makes SQLite refuse growth the way a full disk does,
without filling a disk, which is how `test/disk_full_test.exs` and
`test/disk_full_properties_test.exs` reproduce it. The crash point
`:restore_install` (§11) fails the last step of a restore with `{:error,
:enospc}`.

### 6.3 Memory of a restore

A restore, and the hydration of a lazily restored cell, hold at most
`restore: [max_inflight_bytes:]` (default 67 108 864, 64 MiB) of what they
read from the store. `Replication.Budget` counts it: a read takes its weight
before it is made and gives it back when the bytes are no longer needed. An
object larger than the cap weighs the cap, so it is read alone and the
ceiling is `max(max_inflight_bytes, largest object)`.

- **Full restore.** The store gives no size before a read, so the first range
  of a file (`min(edge_bytes, max_inflight_bytes)` bytes) says it, and is the
  whole file when the file is that small. Larger files cost one more request,
  a `GET` after the range. A task reads the files the walk is expected to
  need, in its order and `fetch_concurrency` ranges at a time, while the walk
  runs. It claims every file first, so the walk waits for a file the task is
  reading instead of reading it again. A file is kept when it fits the budget
  and every file before it is kept or being read; when it does not fit, the
  task waits for the walk to give weight back and takes it for the next files,
  so the read ahead keeps going through a chain longer than the budget (the
  measurement is in how-to-speed-up-a-wake.md). It ends for good at a file
  that is larger than the budget, which the walk reads alone, or that failed
  to read, and when the walk asks for a file it did not claim (a wrong guess of
  the chain): the walk then waits for the requests in flight and reads alone,
  so the walk and the task never exceed `fetch_concurrency` requests together.
  The walk takes a kept file from memory (its weight moves to the file it works
  on) and gives the weight of the previous file back before it reads the next.
  When the walk waits for a file, it first drops the files kept before it that
  it skipped, so a stale file never holds the room that file needs.
  A file the read ahead did not claim is read when the walk reaches it, after
  dropping what was read ahead and not used (a wrong guess of the chain), so
  that the file is alone in the budget. The LTX body is decoded in the native
  library one page at a time; the budget counts the body, not that working
  set.
- **Lazy restore.** The edges of the files (the first range and the trailer,
  each `edge_bytes`) read ahead stay under the budget too; the walk reads the
  rest as it reaches them. A page index larger than the trailer read is read
  whole and counted past the budget (it is sized by the object, see §16).
- **Hydration.** Every ranged read that installs pages (`Lazy.fetch_run/3`:
  background runs of `batch_bytes`, misses, the pages the replicator needs)
  waits for room in the budget of the cell. Under the defaults four runs of
  4 MiB never wait; with a small cap they take turns, and a read that waits
  past its deadline fails like a slow one.

What is outside the budget is fixed: the first ranges of one read-ahead round
(`fetch_concurrency × edge_bytes`, 1 MiB by default) that wait for room, and
the edges of the file the walk reads of a lazy restore (`2 × edge_bytes`). A restore and a hydration of one cell do not
overlap, so the memory of one wake is

```
max(max_inflight_bytes, largest file) + (fetch_concurrency + 2) × edge_bytes
```

and the memory of N wakes in progress is N times that, N being at most
`awake: [max_wakes:]`. Not covered: the refresh of a replica
(`@round_bytes`, 32 MiB, plus the files of a round), compaction, retention,
rewind and the scrub, which have their own bounds.

## 7. Cells

Each awake cell is one process (`Alambique.Cell`) under a
`DynamicSupervisor`, registered in a local `Registry`, holding one SQLite
connection. The process registers its name first and wakes in
`handle_continue`, so calls for the cell queue in its mailbox and concurrent
first calls on one node share one process and one wake. Before the wake the
process creates its in-flight marker (§10). Wake order:

1. Take the lease, then look for the freeze marker and read the state of the
   store at the same time. The wake reads nothing of the cell before it holds
   the lease, so a lease held elsewhere or a store that fails costs one
   request, not a restore (§7.2):
   - Acquire the lease (§5), or take it over (§5.1) when the wake was started by
     `Alambique.take_over/4`. A lease that names this instance (`node_id` and
     incarnation, §5) while no process of this node holds the cell is a
     leftover of a dead cell process: the wake releases it on its behalf and
     acquires again. A lease of the same `node_id` and another incarnation is
     another instance's: the wake fails with `{:error, {:owned, owner,
     expires_at_ms}}` and the lease stays. The lease that the router read to
     route the call is passed on, so the object is read once; the write is
     conditional on what was read, and an error with it is decided again from
     a fresh read.
   - Look for the freeze marker (§10): a frozen cell fails the wake with
     `:frozen` and releases the lease.
   - Read the store: restore into `<data_dir>/<cell>.cell/cell.db`, or find the
     head of the chain to reuse that file. The directory is created with mode
     0700 (SQLite gives `-wal` and `-shm` the mode of the database file, so the
     directory is what keeps them private). A restore is lazy (§16) when the
     chain is large: the file is then filled while the cell serves. A wake
     first removes the `cell.db.restore-*` files of interrupted restores. A
     clean hibernation writes a marker (`cell.db.meta`: TXID, database
     checksum, page size). A restore deletes it before it touches the file; a
     wake that reuses the file deletes it at the fence. The file is
     reused when the marker equals the newest object in the store
     (`Alambique.Replication.head/2`) and `Replicator.from_restore/3` finds the
     file's own checksum equal to it. Anything else, including a missing marker
     or a damaged file, restores. Every wake writes a fence, so the file is
     reused only when this node hibernated the cell last and nobody has written
     to it since.

   The wake goes on when both are done. A failed lease ends it there. The
   error it reports after that is the freeze marker's, then the store's, and
   what the read made and no cell will use (a lazy plan and its file) is
   released.
2. Open the connection (WAL mode, §6) and write the fence
   (§6). On `:txid_taken` the connection is closed and the wake restores
   again and retries the fence, up to five times. The lease is renewed first
   when less than half of its usable window is left. A cell with no migration
   to run writes its schema index entry (§17) in the same round trip as the
   fence.
3. Start the hydrator, if the restore was lazy.
4. Check the cell's version against the window (§17), then run the
   cell's migrations in order: the instance's list, or the list its
   `{module, function, extra_args}` returns for the cell id. `PRAGMA user_version`
   holds the highest applied version; each pending migration runs in its own
   transaction that also sets `user_version`, and all of them are uploaded as
   one batch before the cell serves (the batch is a normal commit, so the
   after-ack callback sees it). The schema index entry (§17) moves with it.
   A migration that fails rolls back and fails the wake; the ones before it
   that committed are uploaded and indexed first, so the durable cell and the
   index agree on the last version that applied (§17, "Migrating an awake
   cell"). `Wake.remigrate/1` runs this same step on a serving cell.
5. Install the change-feed triggers and hand the feed rows that the
   previous owner left unpublished to the sender (§13). The wake does not wait
   for the puts, and a failing feed prefix does not fail it.
6. Serving starts.

A failed wake releases the lease if it holds one, closes the connection and
stops the process; callers waiting on it get the reason, for example
`{:owned, owner, expires_at_ms}`. A failure that a retry repeats at the cost
of a restore (`{:restore, _}`, `{:hydration, _}`, `{:open, _}`,
`{:local_file, _}`, `{:migration, ...}`, `{:schema_too_new, _}`) is
remembered for `wake.failure_backoff_ms` (5 s): calls for that cell on this
node get the same error after the lease read, without a listing or a
download (`Alambique.Admission`).

A wake that fails because the store does (a timeout, an HTTP answer, a
crashed request, a dropped connection, `:store_unavailable` from an open
circuit breaker), whether in the lease request, the frozen check or the
restore, is remembered with an exponential backoff instead: the first delay is
`wake.backoff_ms` (200 ms), each failure in a row multiplies it by
`wake.backoff_factor` (2) up to `wake.backoff_max_ms` (10 s), and a random
share of up to `wake.backoff_jitter` (0.2) is taken from each delay so that the
cells that failed together do not return together. Calls for the cell that
arrive meanwhile get the remembered error at once and start no wake and
read nothing from the store, not even the lease. The router's own lease read
that fails with a store error (`{:lease_unreadable, _}`) is remembered the same
way; a cell that another node owns (`{:owned, _}`, `:unavailable`) is not. The count
starts over when a wake succeeds, or when the next failure comes more than
`backoff_max_ms` after the backoff ended; `take_over/4` also ends it, as the retry it is. The defaults give 200, 400, 800 ms,
1.6, 3.2, 6.4 s and then 10 s at most (less the jitter). The cap is below the
lease TTL (30 s), so a cell is not kept away longer than a lease lives, and
above the 5 s cool-down of a circuit breaker, so the wakes that follow it do
not probe the store faster than the breaker lets requests through. The
`[:alambique, :admission, :wake_backoff]` event reports each delay. Only the
shapes of store errors that the library knows (`Alambique.Admission.store_error?/1`)
count; a reason that is none of them is retried at once.

The memory of both kinds is one ETS table entry per cell. When it passes
`hint_limit` entries, the ones whose backoff and quiet time ended go first, and
all of them only if that was not enough. Failures that depend on the lease, the
owner or the moment (`{:owned, ...}`, `:frozen`, `:draining`, `:disk_full`)
are never remembered.

### 7.2 Round trips of a wake

A cell's first call after it hibernated waits for the wake, and the wake waits
for the store. With 25 ms per request (`mise run bench:wake`, a mailbox of 5 MB
with 12 single-commit files after the last snapshot), the steps are:

| Step | Request | Depends on | Round before | Round after |
| --- | --- | --- | --- | --- |
| Router reads the lease | `GET lease` | nothing | 1 | 1 |
| The wake reads it again | `GET lease` | the router's read | 2 | none (passed on) |
| Take the lease | `PUT lease` (if-match) | the lease read | 3 | 2 |
| Frozen marker | `HEAD frozen` | the lease | 4 | 3 |
| List the cell's files | `LIST` | the lease | 5 | 3 |
| Chain head, local file reused | `HEAD` of the last 16 files (together), then `GET` range of the last | the listing | 6, 7 | 4 (together) |
| Chain, full restore | `GET` of the snapshot, then of each file after it, one at a time | the listing | 6 to 20 (14 files) | 4 (together) |
| Chain, lazy restore | `GET` range of the first and the last 64 KiB of each file | the listing | 6, 7 (after a second `LIST` for a small chain) | 4, 5 |
| Page 1, lazy restore | `GET` range | the edges | 8 | 6 |
| Fence | `PUT` segment (if-none-match) | the lease and the state | 8 (local), 21 (full), 9 (lazy) | 5 (local, full), 7 (lazy) |
| Schema index entry | `PUT` | the open (the version) | 22 (full, lazy) | with the fence |

The rounds above follow the order of §7.1 step 1: the lease comes first. The
latencies below were measured before that change, when the lease, the frozen
marker and the listing shared one round; the wake now takes one round more
(25 ms here) and was not measured again.

Measured with 25 ms per request, p50 of the call that wakes the cell and
lists the 50 newest messages (30 wakes of 3 cells, `mise run bench:wake -- 10`):

| Wake | Rounds before | Rounds after | p50 before | p50 after | p95 before | p95 after | p99 before | p99 after |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Local file reused | 9 | 4 | 217 ms | 114 ms | 244 ms | 139 ms | 247 ms | 141 ms |
| Full restore | 22 | 4 | 701 ms | 166 ms | 811 ms | 173 ms | 812 ms | 174 ms |
| Lazy restore | 8 + query | 6 + query | 495 ms | 215 ms | 497 ms | 219 ms | 497 ms | 219 ms |

The requests per wake until the first answer went from 23.5, 25.5 and 82.4 to
21.5, 24.5 and 37.5: the gain is in the rounds, not the requests. The lazy
count includes the requests of the hydrator and the feed in that time.

The "After" column counts rounds, not requests. The requests that share a
round start together and the wake waits for the slowest.

Why each overlap is safe:

- **The lease before any read of the cell.** A node that cannot take the lease
  (held elsewhere, the store failing on the put) pays one request. Reading in
  parallel with the lease saved a round, but after a store failure every
  cell woke together and each restored what it could not use. The state is
  now read after the lease, so it is never older than it; the fence still
  fails with `:txid_taken` if a previous owner wrote in between (step 2 of
  the list above).
- **The frozen marker.** It is one `HEAD`, not data of the cell, and a frozen
  cell fails with `:frozen` and releases the lease. It runs beside the read
  instead of before it, so a frozen cell costs a restore that is released
  when the marker is found; asking first would add a round to every wake.
- **The lease from the router's read.** The put is `if-none-match` when no
  lease was read and `if-match` on the etag that was read otherwise. If the
  object changed in between, the put fails and the lease is read again, so a
  stale read can only cost a round trip.
- **The head of the chain.** The last file of the chain is read with a ranged
  `GET` whose answer carries its epoch, so its `HEAD` is not needed; when a
  fenced owner wrote it with a lower epoch the epoch rule picks an earlier
  file, which is read after.
- **The schema index entry.** It is written beside the fence only when no
  migration is to run, so it records the version the file already has. If the
  fence fails the entry stays; the sweeper (§17) removes an entry that is not
  the cell's newest.

**Admission.** A node limits what it takes (`awake`; replies are
`{:error, :overloaded}` and `{:error, :busy}`):

- `max_cells` (default 20 000) is the `max_children` of the cell supervisor,
  so the count is exact. Before it reserves a wake slot, the router checks
  whether the awake cells fill `max_cells` (the registry count, confirmed by
  the supervisor, which drops an exited cell sooner than the registry does).
  If they do, it hibernates the least recently used cell with no caller
  waiting, the same way as for `max_bytes` (below; one implementation,
  `Router.make_room/1`), so an idle cell gives its place to a new one. When
  every awake cell has callers waiting, the wake is refused with
  `{:error, :overloaded}`. Two wakes may both hibernate a cell for the same
  place; the supervisor limit still holds, and the node is left one cell
  below the ceiling. A host that chooses cell ids freely should size
  `max_cells` by the memory (about 165 KB per cell without data, up to
  `cache_kib` plus about 150 KiB with it, §12) and the file descriptors (3 per
  cell) of the node, or set `max_bytes`.
- `cache_kib` (default 512) is the page cache of each awake cell's SQLite
  connection, set with `PRAGMA cache_size = -cache_kib` when the cell opens its
  file. It is the largest part of an awake cell's memory: SQLite's default is
  2 000 KiB per connection, and the NIF hands SQLite's allocations to the BEAM
  (`enif_alloc`, `alloc.rs`), so they count in `:erlang.memory(:system)`.
  The bundled SQLite is built with `SQLITE_ENABLE_MEMORY_MANAGEMENT`, which
  gives all connections of the VM one page cache pool: the bound is the sum of
  the connections' `cache_size`, and one connection can hold more than its own
  share while the others hold less (`cache_used` of a cell can pass
  `cache_kib`; the sum does not). `test/sqlite_memory_test.exs` is not async
  for this reason.
- `max_bytes` (default `nil`: no budget) bounds the memory that the awake
  cells hold. Each awake cell records, in the admission table, the sum of
  `SQLITE_DBSTATUS_CACHE_USED`, `SCHEMA_USED` and `STMT_USED` of its connection
  (`Alambique.Sqlite.memory/1`), after every call and message, and at the end of its wake. The
  total is a counter in the same table; the entry goes away when the cell ends
  (by the cell, or by `Membership` for a cell killed). Nothing is measured when
  `max_bytes` is `nil`. The router checks the total before it reserves a wake
  slot: while it is at or over `max_bytes` (or at `max_cells`, above) it
  hibernates the cells in the order of their last call, oldest first, skipping a cell with callers waiting, and
  re-reading the total after each (the same hibernation as
  `Alambique.hibernate/3`, so pending writes are uploaded first). When no cell
  is left to hibernate and the total is still over, the wake is refused with
  `{:error, :overloaded}`. A record is as old as the
  last call or message of its cell (a lease renewal is a message, every
  `lease_ttl_ms / 3`), so a cell that was emptied by a checkpoint is counted
  at its old size for up to that long; this errs towards hibernating too
  much, never too little. The woken cell may pass the budget as its cache
  fills; the next wake makes room. Each hibernation emits
  `[:alambique, :admission, :evict]`. The time of the last call is the third
  slot of the load counter of each cell (also listed by `cells(details: true)`, §8; set when its wake ends, so a cell
  that is still waking is never chosen). Finding the oldest cells reads the
  counter of every awake cell and sorts them (O(n log n), n up to
  `max_cells`), which is paid by each wake at the ceiling of `max_cells` or
  over the `max_bytes` budget, and by no other.
- `max_wakes` (default 256) counts wakes in progress. One process per
  instance, `Alambique.Admission.Wakes`, holds the slots, so each has one
  holder at a time and every change is one message to it. The router asks it
  for a slot (a call) before it starts a cell; the holder is that calling
  process. The cell, in its own process before any wake work, takes the slot
  over from the process that woke it (a call), and gives it back when its
  wake ends. The server monitors every holder, so the slot comes back when its
  holder dies: a caller killed between the grant and the start of the cell, or
  a cell killed mid-wake (even by `:kill`). Taking a slot, handing it to a
  cell and giving it back are each applied once: a later `end` from a process
  that holds nothing changes nothing. If the caller died before the cell took
  the slot over, the slot was already freed; the cell then asks for one of its
  own like any wake (it waits in the queue, within the same cap and
  `wake_queue_ms`), and if it gets none it stops without restoring, and the
  callers already routed to it get `{:error, :overloaded}` as for a failed
  wake. So the wakes in progress, cells included, never pass `max_wakes`.
  The messages cost microseconds against a wake that restores a file.
  A wake that finds no free slot does not fail at once: it waits in a FIFO
  queue of at most `max_wake_queue` places (default four times `max_wakes`,
  1 024 with the default `max_wakes`), for at most `wake_queue_ms` (default
  500) and never longer than its own call timeout. A slot that frees goes to
  the head of the queue in the same step, so no slot sits idle while a wake
  waits, and a wake takes a free slot directly only while the queue is empty,
  so no later arrival passes it. A queued caller that dies leaves the queue
  with its monitor, so it holds no place. Above the cap, or when the wait
  ends, the reply is `{:error, :overloaded}` as before. The wait does not
  count in `max_pending` or `max_cells`: it happens after `make_room`, before
  the cell exists, so a queued wake holds no cell. Each wake that queued, or
  met a full queue, emits `[:alambique, :admission, :wake_queue]`. The
  defaults come from the case that motivated them: a burst of 63 cold wakes
  on a node with few slots. A wake takes tens to hundreds of milliseconds (an
  estimate, not measured here), so 500 ms covers a few rounds of slots
  without holding a caller past the point where it is better to answer
  `:overloaded` and let it retry; the cap of four times `max_wakes` is what
  the slots can drain in about those rounds. If the server itself restarts,
  it starts with no holders; wakes in progress then end without effect and the
  count is exact again as soon as they have.
- `max_pending` (default 10 000) bounds the calls waiting on one cell: the
  calls sent and not yet run by the cell plus the callers it accepted
  and has not answered (`Cell.load/2`). Other messages of the mailbox, such as
  the exit messages of the linked tasks of a wake, do not count. The load
  counter is an `:atomics` array; the router takes a place with one atomic
  `add_get` before it sends a call (`Cell.admit/2`) and gives it back, with
  `{:error, :busy}`, if the count passed the limit, so the check and the
  reservation are one step and the limit is never passed, however many callers
  arrive together. The cell gives the place back when it has answered or
  parked the call, so a call counts while it waits in the mailbox or runs,
  even after its caller gave up, and a read that waits for a held upload holds
  its result only while it counts. A hibernation is never refused. The router
  stamps each call with the time its caller stops waiting (the call timeout);
  a cell that takes from its mailbox a call stamped in the past drops it
  without running it and gives the place back. The stamp is made and compared
  on the node of the cell (`deliver` runs there; a call from another node
  arrives by `:erpc` and is stamped on arrival), so the clocks of different
  nodes are never compared. A call that already runs
  cannot be stopped: a write is durable or not, as in §6.

**Serving.** A call checks `Lease.valid?` first and stops the cell with
`:lease_expired` if it is false. A timer at `ttl / 3` first looks for the freeze marker (one `HEAD`, one
attempt without retries, because the cell serves nothing while it waits and a
retry could use up the lease; a failed check counts as not frozen; a frozen
cell hibernates, §10) and then renews the lease: `:lost` stops the cell with `:lease_lost`; another error keeps the cell until the
lease stops being valid. A sync that returns `:fenced` stops it with `:fenced`. A cell that is told that
it was taken over (§5.1) renews at once. A cell that stops for one of these
reasons answers every waiting reader, and every writer whose batch was not
sent, with `{:error, reason}`, and every writer of a sent batch with
`{:error, :outcome_unknown}` (§6, except a batch refused with `:fenced`). It
closes the connection, and releases the lease unless it was fenced or lost. A
sync that fails for a store reason is repeated while the lease is valid
(§6); when the lease runs out, or the sync fails for any other reason, the cell
stops with `{:sync_failed, reason}` and the writers are answered the same way.

**Statements of callers.** Everything a caller's SQL may do is decided by
the connection, in the NIF, and not by looking at the SQL text:

- *Read-only.* `Alambique.query/5` runs through `Db.read/4`, which calls
  `sqlite_run` with the read-only flag of the connection set for the whole
  call. While it is set the authorizer refuses transaction and savepoint
  statements, every `PRAGMA` with an argument except the introspection ones
  (`table_info`, `table_xinfo`, `table_list`, `index_list`, `index_info`,
  `index_xinfo`, `foreign_key_list`, `foreign_key_check`, `function_list`,
  `collation_list`, `module_list`, `pragma_list`, `database_list`,
  `integrity_check`, `quick_check`, `compile_options`), and `ATTACH`. The
  connection is also `query_only`, which refuses the writes. A refusal is
  `{:error, :read_only}`. The flag is up for the whole call because
  `pragma_*` table functions prepare their statement while the outer one
  runs. In a read-only call SQLite's value length limit is lowered to
  `max_result_bytes`, so `zeroblob(900000000)` is refused before it is built.
- *No ATTACH.* `ATTACH`, `DETACH` and `VACUUM` (which raises the same
  authorizer action, also for `VACUUM INTO`) are refused on every connection
  unless it was opened with `attach: true`. An attached file is another
  cell's data, or a file that the SQL creates. Only the rewind's reconcile
  (§20) opens its connection to the target file that way, to attach the
  current state read-only. A cell cannot run `VACUUM`, which it could not
  inside its write transaction either.
- *Deadline.* The NIF installs a progress handler (every 1 000 VM
  instructions) on each connection. `sqlite_run` arms a deadline for the
  statement, and a stopped statement is `{:error, :statement_timeout}`: no
  process per statement. Two options give it:
  `sql: [statement_timeout_ms: ...]` for the statements of callers (the
  owner's reads, `execute/5`, `Alambique.Tx.query/3` and `execute/3` in
  `transaction/4`, replicas and cross-cell queries, whose deadline is the
  caller's and whose error stays `:timeout`), and
  `sql: [script_timeout_ms: ...]` (default 60 000) for the rest:
  the statements of migrations, `Alambique.Tx.script/2` and the library's own
  statements. The second is the deadline of the connection (`sqlite_open`
  takes it): every `sqlite_run` and `sqlite_execute` call that sets none
  gets it, so a statement the library adds later is bounded without asking.
  It is a separate key because the two want different values: a caller
  waits `call_timeout_ms` for the answer, while a migration may build an
  index over a large table, and its wake has no caller at stake. Migrations
  and scripts are interrupted like any statement; the wake fails with
  `{:migration, version, :statement_timeout}` and `Tx.script/2` returns the
  error. A call can lower the first deadline (`statement_timeout_ms:` on
  `query/5`, `execute/5` and `transaction/4`, a positive integer, else
  `{:error, :bad_option}`); it never raises it, the `:sql` value is the
  ceiling, because the `:sql` limit is what protects the cell from host-supplied
  SQL. The scrub's check shares the restore budget (`scrub: [timeout_ms:]`).
  SQLite rolls back a transaction whose write statement was interrupted, so
  the write call returns the error and the cell keeps serving.
  `Sqlite.close/1` interrupts a running statement before it waits for it.
  Limits of SQLite, not defects:
  - A statement that spends its time in one VM instruction (a huge
    `zeroblob` in a write, or a sort inside a single step) is not
    interrupted until that instruction ends; the handler runs between
    instructions, so a shorter interval would not help and is not an option.
    Reads are protected differently: SQLite's value length limit is lowered to
    `max_result_bytes` (see the read-only item above), so such a value is refused before it
    is built. In a write the limit stays SQLite's default (1 000 000 000
    bytes), which a legitimate row may need, so the instruction is bounded by
    that size only.
  - The deadline is checked between VM instructions, so a statement ends a
    little after it, not exactly at it.
  - `:max_result_rows` and `:max_result_bytes` count what the statement
    returns, so a scan that returns no rows is bounded by the deadline only.
    A statement can scan a large table inside SQLite until its deadline.
  - `sqlite_run` and `sqlite_execute` are the only calls that run a
    statement, so all of them are armed.
    The checkpoint (`Sqlite.checkpoint/1`) is not a statement: it is bounded
    by the readers that keep it from finishing (`{:error, :busy}`).
- *Result size.* `sqlite_run` counts rows and bytes while it builds the
  result and stops with `{:error, :result_too_large}` over
  `max_result_rows` (default 100 000) or `max_result_bytes` (default 64 MiB;
  text and blobs by length, 8 bytes for other values). The one NIF call
  serves the owner path, replicas and cross-cell queries, so all of them are
  capped. The cap bounds the result a caller gets, not the work of the
  statement (see "Deadline").
- *Slots.* Every SQLite call runs on a dirty IO scheduler (`+SDio`, 10 by
  default), shared with the commits, leases and the library's own
  statements. `sqlite_run` takes a slot before it touches the connection and
  gives it back when it returns: a node-wide counter in the NIF, raised by a
  compare-and-swap that never passes the limit. A user statement with no free
  slot gets `{:error, :busy}` at once, so the cell process never waits for
  one. Owner reads (checked or not), replica reads and `Alambique.Query` (whose cells queue behind a per-query gate, §14, so the query itself never takes more slots than the limit) use
  `sql: [max_concurrent_statements:]` as the limit (default: the dirty IO
  schedulers minus two, at least 1). The count is VM-wide (the dirty IO pool
  is per VM, so the counter is shared by every Alambique instance in the
  BEAM), while the limit is per instance: each instance refuses its reads when
  the shared count reaches its own limit. Instances in one VM should use the
  same value; with different values the lower one is refused first and the
  higher one can use the slots the others leave. The statements of a write transaction
  are counted too, but their limit is the whole pool: reads never make a write
  fail, while writes take slots from reads. The library's statements (`BEGIN`,
  `COMMIT`, migrations, `Tx.script/2`, scrub, restore) take none, and the
  schedulers left over are theirs. The slot belongs to the call inside the
  NIF, not to the caller, so nothing needs a monitor: a caller killed
  mid-call leaves a statement that still runs on the scheduler, and its slot
  comes back when that statement ends, at the deadline at the latest.
  `:max_concurrent` made no measurable difference to a `SELECT 1` (about
  2.3 us per call, differences within 0.1 us). Statements that wait in the
  dirty queue are not counted: a user call that starts with no free slot
  returns at once, so the wait of a commit is bounded by the calls that
  already run. `test/statement_slots_properties_test.exs` checks that the
  running statements never exceed the limit and that the count returns to 0
  with callers killed mid-call.
- *Prepare cost.* Every call prepares its statement again; there is no
  statement cache in the NIF. Measured through `Sqlite.run/4` on a file
  database (1 000 rows, an indexed `SELECT ... WHERE w = ? AND n > ? ORDER BY n
  LIMIT 10`): preparing and releasing takes about 4 us and the whole call
  about 11 us. Next to the cell call, and for a write the upload of its
  batch, that is not measurable, so a cache (which would also have to be
  dropped at every schema change) is not built.
- *Parameters.* A wrong number or type of parameters is
  `{:error, {:bad_params, message}}` from `Db.run/4` and `Db.read/4`; the
  cell process does not crash. Lengths over 2 GiB are refused in the NIF.
- *Hardening.* Every connection is opened with `SQLITE_DBCONFIG_DEFENSIVE`
  (no `PRAGMA writable_schema`, no writes to shadow tables), with
  `SQLITE_DBCONFIG_TRUSTED_SCHEMA` off (views, triggers and constraints in
  the schema may use only innocuous functions) and with extension loading
  off. Host migrations that put a function marked unsafe by SQLite in a
  trigger, view or generated column fail.

Reads run on the cell's connection. A read that arrives while local commits
are waiting for their upload is answered only after the newest of their
batches is acknowledged, and fails with it, so no caller observes a write
that could still be lost. Writes are `BEGIN IMMEDIATE` transactions run in
the cell process. A write commits locally, then waits for a flush message that is queued behind everything
already in the mailbox, so all writes queued before it share one upload
(§6, group commit); calls that arrive while the pipeline is full form the next
batch. The upload's deadline is the shorter of `op_timeout_ms` and the time
left on the lease, fixed when the batch is sent.

**Hibernation.** After `idle_timeout_ms` without calls (and with an empty
mailbox), on `Alambique.hibernate/3`, on a drain (§8) and so on instance
shutdown, and on supervisor shutdown as a last resort, the cell
uploads what is pending, runs `Replicator.checkpoint/2`, closes the
connection, removes the emptied WAL and its index (`-wal`, `-shm`), writes
the `cell.db.meta` marker, releases the lease and exits; the in-flight marker (§10) is
removed when the process ends. A release the store refuses is logged, and
`hibernate/3` and the drain report it as `{:release_failed, reason}`: the cell
is asleep and its data durable, but its lease stays until it expires. An awake cell has two timers
(renewal and idle); a hibernated cell has no process, file descriptor, timer
or ETS row.

**Hooks.** A transaction sees the TXID of the batch it joins through
`Alambique.Tx.txid/1`, taken from `Replicator.next_txid/1` when it runs. The
instance option `:after_ack` is called with `(cell, txid)` in the cell process
after the callers of each acknowledged batch were answered, for write batches
and migration batches but not for fences. The change feed (§13) publishes in
the same place, right after `:after_ack`.

### 7.1 Local cache

The file of a hibernated cell stays in `<data_dir>/<cell>.cell/` so the next
wake can reuse it, but the store holds everything: the files are a cache. The
`:local_cache` option (`max_bytes`, default 20 GiB; `interval_ms`, default
60 000; `false` turns it off) bounds it. It is on by default because an
instance that serves many cells would otherwise fill its disk with the files of
every cell it ever woke, including those a rebalance moved away; 20 GiB is
small for a node meant to hold many cells and large enough that a small
deployment never evicts. The total counts every directory; only the ones of
cells without a process can be removed.

One process runs a round every `interval_ms`: it adds up the size of every
`*.cell` directory and, while the total is over `max_bytes`, removes the
directory whose marker (or, without one, newest file) is oldest. The cost of a
round is a `stat` per file, in one task off every cell's path.

*Safety against a wake.* The eviction of a cell runs in a process that first
registers the cell id in the instance's registry, the same unique name a cell
process takes. If a cell process exists or is waking (it registers before it
touches its directory, §7), the registration fails and the directory is left
alone. If the eviction holds the name, a cell that starts finds it taken and a
call that reaches the evicting process sees it exit with `:normal`, which the
router answers by looking again (§8); the retry starts the cell after the
directory is gone, and the wake restores from the store. So no wake and no
eviction ever work in one directory, and an evicted cell loses nothing that the
store does not have. A directory without a valid marker, which no wake would
reuse, is evicted by the same rule.

Each round emits `[:alambique, :local_cache, :sweep]` (telemetry.md). Work
directories (`<data_dir>/work`, §20) are not part of the cache; `inflight`,
`replicas` and `read-replicas` have their own bounds.

## 8. Routing

- The **ring** maps a cell id to a preferred node by rendezvous hashing: the
  node with the highest `:erlang.phash2({cell, node}, 2^32)` among the nodes
  currently running the instance. Membership is a `:pg` scope per instance;
  each node's `Alambique.Membership` process joins the group of all members
  and a group named by its `node_id` and incarnation, which maps a lease holder
  back to a node. The same `node_id` with another incarnation is another group,
  so a lease of another instance never resolves to this node.
  A node joins its store identity group before the group of members.
  `:pg` drops a member when its node goes down or disconnects.
- **Join.** With the instance option `join: false` the node starts without
  joining the group of members and joins it on `Alambique.join/1`. Until then
  every place where a node chooses a node (the ring and `Membership.preferred/2`,
  `Membership.nodes/1`, the validation of a hint, and the target of `move/4`)
  leaves it out, so no other node prefers it, picks it as an owner or moves a
  cell to it. It is not drained: calls made on it are served, forwarded to the
  preferred member when it is not the node itself, or woken here when no member
  exists. The `node_id` group and the store identity group are joined at start, because they only describe the node: a
  cell it wakes by a local call is found through its lease, so peers forward to
  the real owner.
- **Store identity.** Every node also joins the `:pg` group
  `{:alambique, :store, identity}`. The identity says where the data lives,
  not how it is reached: the first 8 bytes of the SHA-256 of the sorted list
  of `{id, prefix, store location}` over the placement's `locations/1`, hex
  encoded and computed once in `Alambique.Config` at start (a placement that
  answers an error or no location fails the start). The store location is the
  optional callback `c:Alambique.Store.location/1`: a term that names where the
  store keeps its data, equal on every node that shares it and free of
  credentials. `Alambique.Store.S3` answers scheme, lowercase host, port, path
  and bucket, so credentials, `:receive_timeout` and `:path_style` do not
  change the identity; the memory store of the testkit has no credentials and uses its config
  (its server) as it is.
  A store that wraps another delegates to `Alambique.Store.location/1`. A
  store without the callback is identified by its whole config, credentials
  and option order included, so such a store should implement it. The hash
  keeps everything else off the wire.
  Nodes in a group of another identity are *foreign*: `Membership.nodes/1`
  leaves them out, so they are never preferred or chosen as owners, and
  `move/4` answers `:unknown_node` for them. A forward toward a foreign node
  (a stale hint, for example) is not sent: the call answers
  `{:error, {:foreign_store, node}}` and drops the hint. A node in no identity
  group, one still joining, is not foreign. Two instances of one name over different stores therefore
  stay apart on a connected cluster. Changing the locations of the placement,
  the endpoint name or the bucket changes the identity: during such a rollout
  the nodes split into two groups. Leases keep that safe, but routing answers
  `:foreign_store` until the rollout finishes.
- Every lease read and every wake goes to the location of the cell
  (`Alambique.Placement.locate/3`, §18). The answer is cached per instance in
  a bounded ETS table (a sixteenth of the rows is evicted at `:hint_limit`
  rows, and a row that is already stored is not written again) and dropped when the
  location it names refuses with `moved`; the wake then asks the placement
  again, once.
- A call for cell `c` on any node: look up `c` in the local registry; if
  absent, forward to the node in the owner-hint cache, or to the preferred node
  if there is no hint. Forwarding is `:erpc.call/5` with the call's timeout
  plus one second, and the call carries a hop budget of two. A node that is
  asked and has no process for `c` reads the lease:
  - a live owner (the lease is unreleased, not expired by this node's clock
    plus the skew margin, and its holder is in the group) gets the call,
    unless the hops ran out, which answers `{:error, {:owned, node}}`;
  - an owner that is not in the group answers `{:error, {:unavailable, owner,
    expires_at_ms}}` until the lease expires;
  - a free lease (none, released, expired, or naming this instance) means waking
    `c` here when this node is the preferred one or the hops ran out, and
    forwarding to the preferred node otherwise. If that node cannot be reached,
    this node wakes the cell.
- A call that reaches a cell which stopped without running it (the cell was
  hibernating, lost its lease, or failed to wake because another node owns
  it) is looked up again, at most three times. A crash during a call is
  reported as `{:error, {:cell_down, reason}}` and never retried, because the
  call may have run.
- **A forwarded call that may have run is not run again.** A call counts as
  *sent* once `:erpc.call` was issued to a node that was connected; a node
  that is not connected is connected first, and a failed connect means the call
  was not sent. If the connection fails after the call was sent
  (`noconnection`), the target may have served it before the failure was
  noticed: after a silent partition, data that the kernel had buffered still
  reaches the target, which runs the call, and a second run would apply a write
  twice. `execute/5` and `transaction/4` then answer
  `{:error, :outcome_unknown}`, as for a timeout, and the router never sends
  them again. A call that was not sent, and a sent call that is safe to
  repeat (reads, the checked read, `touch`, `hibernate`, `migrate`), is
  routed again as before. The caller can accept the repetition for a write with
  `idempotent: true`. The rule applies at every hop (first hop, a free lease
  that forwards to the preferred node, a live owner); `move/4` only sends
  `touch`, which is safe to repeat. A connection that is lost between the
  check and the send is reported as sent: the router then answers
  `:outcome_unknown` for a call that did not run, which is the conservative
  error. A `:timeout` is unknown as before.
- **A stopped instance refuses like a draining node.** A forwarded call
  that reaches a node whose instance has stopped, while the VM is up, finds no
  configuration. `Router.remote/5` answers `{nil, {:error, :draining}}`
  without serving, so the call did not run and it is safe to repeat. The
  caller treats it as the answer of a draining node: a free lease that
  forwards to the preferred node looks the cell up again; the first hop and a
  live owner hand the refusal to the caller. It is no longer an `ArgumentError`
  that `:erpc` reports as `{:forward_failed, _}`.
- **Requests between nodes.** A write forwarded from another node may carry
  the `{:idempotent, request}` wrapper, which marks it as safe to repeat if the
  forward is lost after it was sent. `:position` and the `{:isolated, request}`
  wrapper of a write are forwarded like any other request. Every node of an
  instance runs the same code, so the router sends each request as it is.
- A bounded ETS table keeps owner hints, set from the node that actually
  served a forwarded call. A stale hint costs one extra hop; a hint whose node
  is gone is dropped. A hint that is already stored is not written again. When
  the table reaches `:hint_limit` rows a sixteenth of the rows is evicted
  (`Alambique.BoundedTable`), so the hints that remain keep serving; emptying
  the table sent every following call to the store for its lease.
- The cluster view (the member nodes and the nodes of another store) is
  computed from `:pg` and kept in a `:persistent_term`. A forwarded call reads
  it without scanning the groups (`mise run bench:routing`, §15). `Membership` refreshes it
  when the scope reports a join or a leave, and `sync/1` asks it to refresh for a
  change made on this node (only the `Membership` process stores the view, so
  a slower caller cannot overwrite a newer one), so the view follows a change of another node after
  the scope's message, not before. A node that joins makes several events (one
  per group), and a `:persistent_term` put scans every process, so a burst
  makes one refresh for the events that are waiting, and a view that did not
  change is not put.
- **Take over** (§5.1) runs on the calling node and starts the cell there
  with a hop budget of zero, whatever the hint or the ring say. It drops the
  hint for the cell on that node; other nodes keep a hint to the old owner
  until a routed call corrects it. A call that reaches the old owner's node
  finds no process, or a process that stops (`:lease_lost`, `:fenced`) and is
  looked up again, and the lease then names the new owner's node.
- **Drain.** `Alambique.drain/2` takes the node out of service: it leaves the
  group of members, so the ring and `Alambique.move/4` no longer choose it; it
  refuses to wake cells (`{:error, :draining}`; a call that reaches it for a
  cell nobody holds is forwarded to the preferred node among the remaining
  members, and a forwarded call that is refused is looked up again); then it
  hibernates every cell with a process on the node, a few at a time (the
  instance option `:drain` sets concurrency, per-cell timeout, the deadline of
  the whole drain, passes, listing page and the supervisor's shutdown time),
  uploading
  pending writes and releasing the leases, so other nodes serve the cells at
  once instead of after the lease expires. The group of the `node_id` and incarnation stays, so
  calls for cells that are still awake on the node keep reaching them. A
  drained node serves nothing new until it restarts or until
  `Alambique.join/1`, which joins the group of members again and ends the
  drain (it is also the call that makes a `join: false` node a member; on a
  member it does nothing). The state of the node, `:not_joined`, `:member` or `:left`
  (drained), lives in a `:persistent_term` of the instance: set from `:join`
  when the instance starts, written by `join/1` and the drain, erased when the
  instance stops. A restart of `Alambique.Membership` or of the drain child
  therefore puts the groups back as they were (`Membership.sync/1`), and a
  node that never joined is not draining. Only the group of members is read by
  peers; the state is local, and `draining?/1` reads it without a `:pg` lookup. The last child of the
  instance's supervision tree runs the same drain when the instance stops
  (`Application.stop`, `System.stop`), before the cells' supervisor, the
  registry and the groups go; the supervisor's shutdown still hibernates what
  the drain did not finish. A cell whose lease the store would not release is
  asleep but not released: the drain lists it as `{:release_failed, reason}` in
  `errors`, not in `hibernated`, because another node waits for the lease to
  expire. **Deadline.** `:deadline_ms` (default 60 000, under the 75 000 of
  `:shutdown_ms`, so the supervisor does not kill a drain that ends on time)
  bounds the whole drain. After it no new hibernation starts, a call in flight
  gets the time left, and every cell still awake is reported as `{cell,
  :deadline}`. Those cells are left awake, not killed and not hibernated by
  force: forcing gains nothing, since an acknowledged write is already durable
  and a kill would only leave the leases unreleased. The node keeps serving them; another `drain/2` or the stop of
  the instance hibernates them, and otherwise their leases expire as after a
  crash. A drain emits `[:alambique, :drain, :start]` and `[:alambique, :drain,
  :stop]` (`docs/telemetry.md`), and `Alambique.drain_status/1` reads its
  progress from an ETS table that the drain child owns, so a caller on another
  process sees it while the drain runs.
- **Swap.** Two instances over one store with no cluster between them hand
  cells over through the lease alone. The new node reads the lease of a cell
  the old one holds, finds its holder (node id and incarnation, §5) in no
  group of its own, and answers `{:error, {:unavailable, owner,
  expires_at_ms}}` without waiting; the old node's drain releases the lease and
  the next call wakes the cell. Nothing is forwarded, so it also works when the
  two share a `node_id`. `test/swap_cluster_test.exs` runs it under load; the
  procedure is in [how-to-swap-a-node.md](how-to-swap-a-node.md).
- **Move**: the owner hibernates the cell, and the target wakes it with a hop
  budget of zero, so it wakes the cell itself. The target must run the instance.
- Node join changes the preferred node only for future wakes. Node death
  leaves leases to expire (§5); the cell is served again from another node
  once the lease has expired by that node's clock plus the margin.
- `Alambique.touch/3` is a call that does nothing: it routes like any other,
  wakes the cell where a call would, and returns the node that served it.
- `Alambique.owner/3` answers from the local registry, or from the lease. With
  `hint: true` it answers from the owner-hint cache when the hinted node runs
  the instance, without a store request; a hint to a node that is gone is
  dropped, and a lease answer naming another node is stored as the hint. The
  answer can be stale until a routed call corrects the hint. With `peers:
  true` it asks the member nodes' registries in parallel (`:erpc.multicall/5`
  with `call_timeout_ms`, target `Router.holds?/2`) and makes no store request
  and no wake: `{:ok, node}` for the node that holds the cell awake, `:none`
  otherwise. It sees only processes, so a cell that is waking, a member that
  does not answer, and a lease held by a node that is not a member all give
  `:none`; the lease read does not have these gaps.
- `Alambique.lease/3` is the public read of the lease for takeover callers:
  `{:ok, %{owner, owner_id, epoch, expires_at_ms, released, moved_to,
  moved_from}}` without waking or freezing the cell. It resolves the location
  like `owner/3`, answers epoch 0 for a cell that never had a lease, keeps the
  moved markers, and returns the guarded store error or `:timeout`. The epoch
  is the one `take_over/4` expects. It is one store read of the lease object
  and nothing else (`test/lease_api_test.exs` counts the requests), so a caller
  may run it on every `{:unavailable, owner, expires_at_ms}` to compute a
  retry delay: `expires_at_ms + skew_margin_ms - now`, or none when the lease is
  released. `mise run bench:lease` measured 11 µs per call (median of 1 000)
  on the in-memory store, against 3 µs for the bare `Lease.read/2`, and 26 003
  µs with 25 ms of added latency on every store request, against 25 997 µs for
  the bare read: one request, with no cache in between (p99 26 ms in one run
  and 74 ms in another, on a shared machine).
- **Live cells.** `Alambique.cells/2` lists the cells with a process on this
  node. `Alambique.Live` keeps an ordered ETS set `{id, pid}` per instance,
  fed by a registry listener that starts before the registry (it monitors each
  pid and removes only the exact `{id, pid}` pair, so a new process under the
  same id survives the old one's exit). A page walks the set with
  `:ets.next/2`, so it costs its size and not the number of cells, and
  `count` is `Registry.count/1`. The set can lag the registry by a message; a
  walk skips entries whose process is gone and keeps going to fill the page.
  The row is `{id, pid, load}`, with the load counter the registry holds as the
  cell's value. With `details: true` a page reads two slots of that counter for
  each entry and nothing else: slot 3, the monotonic ms of the last call (the
  value admission sorts to pick the least recently used cell, `nil` while the
  wake lasts), and slot 4, the size of the `-wal` file. Monotonic time because
  the values are only compared on this node and a clock step must not reorder
  them. The cell writes slot 4 with the `File.stat` it already makes after each
  landed upload for the checkpoint check (§6), at the end of its wake and
  after a checkpoint, so a call to `cells/2` costs no file access and the size
  can lag a write by the upload that is still in flight.
- **Cell load.** `Alambique.load/2` answers `{:ok, %{pending: n}}` for a cell
  with a process on this node and `:none` otherwise, without a call into the
  cell. Each cell registers a `:counters` reference as its registry value; the
  cell adds one when it accepts a write or a read that waits for a commit and
  subtracts the batch when it answers. A second slot counts the calls in
  flight to the mailbox: the router adds one before it sends a call and the
  cell subtracts it when the call is answered or parked. `pending` is the two
  slots, so callers parked in the pipeline (§6) count although they are no
  longer in the mailbox, and internal messages (`:flush`, `:landed`, timers,
  exit messages) do not count. A call that parks is briefly in both slots, so
  the value can be one over for an instant.

## 9. Compaction and retention

Compaction makes restore read few files; retention deletes what no restore
point in the policy needs. Both run per cell, on any node, whether the cell is
awake or not, and neither ever changes what a restore returns.

**Compactor.** Each instance starts one process (`Alambique.Compaction.Compactor`)
and one timer. Cells do not have either. A cell tells the compactor about every
acknowledged batch and every wake fence with one `:ets.update_element` on a
bounded public table: a row `{cell, first_txid, last_txid, due_ms}`.
A row is ready when `fanout` TXIDs were committed since it appeared, or when
`max_age_ms` passed. Every `interval_ms` the compactor takes up to
`concurrency * batch_factor` ready rows, deletes them, and runs one pass for
each, `concurrency` at a time, each with its own deadline. When more rows are
ready than fit in a batch, the choice is not left to the hash order of the
table: the rows with `max_chain_segments` (default 256) TXIDs or more go first,
then the rows with the smallest `due`, the ones that waited longest. A row that
is taken is deleted and a new commit makes a new row with a later `due`, so a
ready row is overtaken only by the rows that were ready before it: with `R`
ready rows and a batch of `B`, it is taken within `ceil(R / B)` ticks, however
busy the other cells are. This reads every ready row each tick and sorts them,
`O(R log R)` with `R` at most `queue_limit` (10 000 by default). A full batch
makes the next tick follow at once. A cell nobody writes has no row. When the
table holds `queue_limit` rows, notifications for new cells are dropped; the
next commit of such a cell makes a row. A failed pass is logged and queued
again after `retry_ms`; a pass that ran out of merge budget is queued at once.
The compactor starts last in the instance tree, so its crash restarts nothing.

**Chain cap.** A row counts the TXIDs from its first commit to its last, one
per batch, so `last - first + 1` is the number of level-0 files the cell added
since the compactor last took its row. Rows with `max_chain_segments` or more
(at least `fanout`, so they are ready anyway) go ahead of all other rows. A
cell is therefore not left with a growing chain because other cells are busier:
while fewer cells than a batch are over the cap, a cell waits one tick, and
its chain is the cap plus what it commits during that tick and its pass. That
bounds the level-0 files a restore reads to about the cap, on top of the
merged files of "Levels" below. It does not hold when the compactor is down, when more
cells are over the cap than a batch (they are served oldest first, in
`ceil(over / batch)` ticks), or while a pass fails: a row taken for a failed
pass is queued again after `retry_ms` with no TXID count, so its chain is not
measured until it commits again after a pass that worked.

**Levels.** A pass merges level-0 files, one per batch, `fanout` (default 8)
at a time into level 1, level 1 into level 2, up to `max_level` (default 2).
Merged files cover *aligned blocks*: level `L` covers blocks of `fanout^L`
TXIDs, `[j * span + 1, (j + 1) * span]`. The key of a merged file is therefore
fixed by the TXIDs, and two compactors that see the same files plan the same
keys. Every write is `:if_none_match`; a 412 means another compactor got there
first and is a success. A block is merged only when all its inputs exist and
the block ends at or before the end of the chain, so the tail that does not
fill a block stays at the level below. Level-0 files are kept; restore picks,
at each step, the file that starts at the next TXID and reaches furthest, so
it reads at most `fanout - 1` files for each level below the top one, plus the
top-level files since the newest snapshot, for example 13 files for a chain of
1 000 batches and 16 for 5 000.

**Superseded files.** A file of level `L` (1 to `max_level - 1`) is redundant
for restore once the file of level `L + 1` that contains its block exists:
restore picks the one that reaches furthest. When a pass writes a merged file of
level 2 or above, it marks the files it merged, which are at level 1 or above,
for deletion through the trash (§9.1), so they leave the store after
`deletion_grace_ms` and a restore that listed them before still finds them.
`compaction: [delete_superseded: false]` keeps them. Level-0 files are never
marked this way: a rewind to any TXID of the window (§20) applies level-0 files
up to the target, because a merged file cannot be applied halfway. A file that
was already there (`:exists`, another compactor wrote it) is not marked by this
pass. What the pass fails to mark, because it crashed between the put and the
mark, is deleted with the rest when retention covers it. In steady state a cell
holds its level-0 files for the window, the top level for the window and at most
`fanout - 1` files of each level in between. Without the deletion it held every
level for the window, which for `max_level` 3 is four copies of the pages
written; with it, two (measurements in `docs/how-to-size-storage.md`).

**Snapshots.** A pass merges the newest snapshot and the chain after it into a
new snapshot, `0009/0000000000000001-<max>.ltx`, when one of three things is
true: the level-0 bytes written since the newest snapshot reach
`snapshot_ratio` (default 2.0) times its size, `snapshot_interval_txids`
(default 4096) TXIDs passed, or `snapshot_interval_ms` (default seven days)
passed. The state in the snapshot is the state of that TXID, no matter which
node's compactor wrote it. Without a snapshot, the age is that of the first file
of the chain.

The bytes are counted by the compactor, which is the only writer that sees the
size of a level-0 file: a level-1 merge adds the sizes of the level-0 files it
read and wrote (once per file, only for a merge it wrote itself). The count is
the object `<cell prefix>/chain`, `"<snapshot TXID>:<snapshot bytes>:<bytes
since>"`, put again after a pass that merged and after a snapshot. It is not
read when a TXID or age limit already makes a snapshot due. A lost update only
delays a snapshot. A count whose TXID is not the newest snapshot's, and a
snapshot with no count at all, give no
base: only the two limits apply until the next snapshot writes a count. A cell
with no snapshot, or a snapshot smaller than `snapshot_min_bytes` (4 MiB), is
compared with that size, so a small hot cell is not snapshotted at every
batch. The count lives in the store, not in the cell, so it survives a wake on
another node and a restart, and it is a fixed key of the cell like `trashed`
(a move deletes it with the others).

A snapshot costs the size of the database, written again each time, so the
bytes written for snapshots are the level-0 bytes divided by the ratio,
whatever the size of the database, and a restore reads at most about `1 +
ratio` times the snapshot (the snapshot and the files after it). The wake
reads that from the store whenever its local file cannot be reused; with the
default 2.0 the worst case is three snapshots, and a lower ratio shortens the
chain and writes more (measurements in `docs/how-to-size-storage.md`). The
TXID limit also bounds the number of files in the chain.
A block that a snapshot cuts in two is never merged, so more snapshots mean
fewer merges.

**Thinning snapshots.** A new snapshot replaces the previous one as the base of
a restore: the new one is built from it, and every reader that wants the
newest state takes the newest. The previous one is kept only as the base of a
restore to a point before the new one. When it is less than `snapshot_keep_ms`
(default three days) after the snapshot before it, the pass marks it for
deletion through the trash, so what remains is spaced at least that far apart
and a restore to any point in the window reads at most that much more chain.
It is not marked when its age is above 90% of the retention window: retention
(below) may take it as its anchor, and the files before it would then be gone.
Deleting it does not stop retention from moving forward: the anchor is the
newest snapshot that is old enough among those left, so history is kept up to
the gap between two kept snapshots past the window.

The readers of snapshots: `Restore` takes the newest snapshot at or before the
target (an older one only when the newer is damaged, then the chain from
scratch if the first file is still there, so a damaged newest snapshot now
falls back less far); `rewind`, the lazy restore, the scrub, the read replicas
and the cross-cell queries restore through it, with the latest state or a
target; replicas and queries rebuild when a snapshot newer than theirs appears
and use only the newest; `Head` and the relocation to a location read the
newest snapshot's TXID; compaction builds from the newest. None of them needs
an interim snapshot for a state inside the window, because the files after the
snapshot before it are all kept: level 0 and the top level, and the
superseded ones until their grace ends.

**Data TXID.** A merged file or snapshot carries the `data` of its last
input (§6); without one, its own maximum TXID.

**Epochs (R2).** A merged file or snapshot has the highest epoch of its
inputs. A group is merged only if the epochs of its inputs do not decrease and
do not fall below the epoch of the file right before it. Otherwise it is
skipped (`:epoch_regression` in the telemetry), so a write of a fenced-out
epoch can never be moved into a file with a higher epoch and so into the
restored chain. The skipped group is merged once the next owner has
overwritten the zombie (§6, put rule). The merge itself (§2, `merge.rs`) verifies every input and
refuses inputs that are not contiguous or whose pre-apply checksum does not
follow the previous post-apply checksum.

**Memory.** The merge decompresses one page at a time and copies the
compressed page blocks into the output. A merge holds its inputs, its output
and a page index; it does not hold the database decompressed. Measurements are
in §12.

**Retention.** `Alambique.Retention.run/3` marks the covered files. The anchor
is the newest snapshot stamped at or before `now - pitr_window_ms` (default
seven days). Every point from the anchor on stays restorable, including every
point stamped after the window start. Snapshots older than the anchor and all
files that end at or before it are marked for deletion (§9.1). With no
snapshot that old, nothing is marked; the newest snapshot and everything after
the anchor are never touched. Timestamps come from different nodes' clocks, so
the window should be larger than the clock skew.

Retention does not delete orphans, the files of a fenced-out epoch that landed
after the new owner's fence. Every reader ignores them as a tail (§6, R3) and
the next owner overwrites the one it needs, with a conditional put. A delete by
key would race with that overwrite and could remove a live segment. The wake
does not ask the compactor for a retention run.

The compactor runs retention after a pass that took a snapshot and after a
pass that waited `max_age_ms`. Covered files are marked and deleted by the
reaper (§9.1). Because they only touch files before the anchor, any part of
the deletions leaves a store that restores as before. Compaction deletes only
the superseded files above, which are redundant for every restore.

**Concurrent restores.** A restore lists the store and then fetches what it
listed, so a deletion between the two would fail it. Covered files are
therefore deleted only after the anchor snapshot has existed for `grace_ms`
(default ten minutes, far above `op_timeout_ms`): a restore that listed before
the anchor existed has finished by then, and one that listed after it starts
from the anchor or a newer snapshot. An anchor without a `created` stamp is not
trusted and nothing is deleted for it. Requests per compaction without held bytes: one `LIST` per pass, one `HEAD`
for the predecessor and `fanout` `GET`s per merge, a `PUT`, and per snapshot
one `HEAD` for its age plus a `GET` per file in the chain.

**Held bytes.** The cell that owns a cell's chain has every segment it
uploaded in memory at the moment it acknowledges it, and the compactor of the
same node would read exactly those bytes back. Each instance therefore keeps a
node-local ETS table (`Alambique.Compaction.Hold`, created with the compactor)
of the files it just replicated: `{cell, level, min, max}` to the object
metadata and the body, which is the binary of the upload, not a copy. The cell
puts a row when it acknowledges a batch (and for the fence of a wake and the
sync of a drain), before it tells the compactor. A pass uses a row instead of a
`GET` for an input, and instead of a `HEAD` for the epoch of the predecessor
or the age of the newest snapshot. A merged file is held as the input of the
next level, and the consumed inputs and the chain of a new snapshot are
dropped. Level 9 snapshots are never held.

A row is an optimisation, never a source of truth. A pass uses a row only for
a key in the listing it made at its start, and checks the epochs of held
files with the same R2 rule as fetched ones. A new owner that overwrote the
file of a fenced-out owner has a higher epoch than the held bytes of the old
one, so the group is skipped. The merge itself verifies every input and the
checksums between them, so a damaged body is refused. A row is missing, and
the store is read as before, after an eviction, a wake on another node, a
restart of the cell or of the compactor, and for files over
`hold_object_bytes`. Rows are mixed freely per input. The cell drops its rows
when it stops, for any reason, and when it wakes.

`compaction: [hold_bytes: 33_554_432, hold_object_bytes: 1_048_576]` bounds
the table per instance; the oldest rows of any cell go first when it is over
`hold_bytes`, and 0 turns holding off. A cell with small writes keeps about
`(fanout - 1)` rows per level plus the rows of the next block. In the
benchmark (`scripts/bench-trips`) a hot cell does no compaction `GET` or
`HEAD` for held inputs. The `[:compaction, :merge]` event reports `held` and
`fetched` inputs.

### 9.1 Delayed deletions

Retention and the cleanup of a moved cell (§18) do not delete their data at
once. They hand the segment and snapshot keys to `Alambique.Trash`, which
writes them into a *manifest* and leaves the objects where they are. A reaper
deletes the objects of a manifest once its time has passed. The instance option `deletion_grace_ms` (default 86 400 000,
one day) is the wait; `0` deletes at once through the same code. This protects
against a retention or cleanup that is wrong (a window set too short, a bug, a
cell relocated by mistake): the files are still there to restore from or to
keep, for the grace period. The feed is out of scope: its objects are not cell
state and the feed has its own retention (§13). Orphans are deleted at once,
because they keep the cell from being written and restore never applies them.

**Layout.** A manifest is the object `<prefix>/trash/<due>/<cell>/<id>`, where
`<due>` is `now + deletion_grace_ms` in milliseconds, zero-padded to sixteen
digits, and `<id>` is random. Its body is JSON: the cell and the keys, at most
`trash.batch_keys` (default 1 000) per manifest. Keys sort by due time, so the
reaper lists `trash/` from the start and stops at the first manifest that is
not due. One manifest per batch, not one object per key, keeps the marking
to one `PUT` for up to 1 000 files, and recovery to one object. It costs the
exactness of a cancel: an operator cancels a whole manifest (below).

**Cell keys.** A manifest names only segments and snapshots. The fixed keys of
a cell (`frozen`, `trashed`, `crashes`) are not a TXID range and can belong to
a new residency after a move back, so the cleanup deletes them at once. The
reaper ignores a fixed key in a manifest and deletes the manifest; leaving the object is harmless, because the next cleanup
of that location removes it. A pending manifest therefore can never delete the
frozen marker, the retention marker or the crash counter of a returning cell.

**Marking is idempotent.** A restore during the grace sees the marked files
as before, and retention sees them in its listing too. Retention would mark
them again at every run, so it writes `<cell>/trashed` with the TXID of the
anchor whose covered files it recorded, after the manifests, and plans only
what the previous anchor did not cover. Cost of a run that marks: one `GET`
of the marker, one `PUT` per manifest, one `PUT` of the marker, instead of one
`DELETE` per file. A crash between the manifest and the marker records the
files twice; a manifest that finds nothing left is harmless. A covered file
that is rebuilt after it was marked (see below) stays unmarked until the next
anchor covers it.

**Reaping.** Every instance starts one reaper per node (`trash: [reaper:
false]` turns it off on a node); several may take the same manifest, and
deleting twice is harmless. Every `trash.interval_ms` (default 300 000) it
lists `trash/` of each location in pages of `trash.page_size` (default 100),
and for each due manifest: reads it, deletes the keys in order, `trash.concurrency`
(default 8) at a time and at most `trash.rate` (default 200) per second, then
deletes the manifest. A manifest with a key outside the cell it names is
refused and left (`:bad_manifest`). A failed manifest stays and is tried again
at the next round. Per manifest: one `GET`, one `DELETE` per key and one
`DELETE`; per page one `LIST`.

**Crash safety.** The fault points are `:trash_before_mark` and
`:trash_after_mark` (around the manifest `PUT`), `:retain_before_marker`,
`:trash_before_delete` and `:trash_after_delete` (around each object `DELETE`)
and `:trash_before_entry_delete` (all objects gone, manifest still there). A
crash at any of them leaves the objects, or some of them, and the manifest,
so the next round finishes; the manifest is deleted last, so an object is
never deleted without its manifest still being processable. Keys are deleted
in key order, which puts lower levels before the merged files built from them,
so an interrupted reap never leaves the inputs of a block with its output
missing, which compaction would rebuild. A manifest never names a key that is
later re-created for another purpose: keys are TXID ranges, and a cell
that returns to a location writes TXIDs above everything it wrote there before
(§18).

**Storage cost.** Covered objects stay for the grace period, so the store holds
up to `pitr_window_ms + deletion_grace_ms` of history on top of the snapshot
and the chain, plus the manifests (about 65 bytes per key). Measured on the
test history of `retention_test.exs` (15 TXIDs in three days, snapshots every
five, 24 covered objects): 51 921 bytes in the store before retention; with a
grace of one day, 53 531 bytes after it (the manifest and the marker) until the
reaper ran, 2 492 after; with a grace of 0, 2 490 at once. Requests of one
retention run on that history: 24 `DELETE`s with a grace of 0; with a grace of
one day, 2 `PUT`s, 1 `GET` and the same `HEAD` and `LIST`, and a reaper round
of 1 `LIST`, 1 `GET` and 25 `DELETE`s.

**Interaction with other rules.** A restore to a point before the anchor works
during the grace, and `mix alambique.restore` sees the marked files: that is
the recovery path (`docs/how-to-operate-a-cell.md`). Raising `pitr_window_ms`
after files were marked does not unmark them; cancel the manifests (below).
`Alambique.Trash.pending/2` lists the manifests, `Alambique.Trash.cancel/3`
deletes one so its objects stay, and `Alambique.Trash.reap/2` runs a round now.
After a cancel, retention marks the files again at its next run that finds
them covered, unless it is turned off. Deletion order and parallelism are
unchanged for a grace of 0.

**Limit: store reads and writes are not streamed.** Compacting or
snapshotting a cell holds about two to three times its compressed snapshot in
memory per concurrent pass. Lower the compactor's concurrency on nodes that
serve very large cells.

## 10. Operations

`mix alambique.inspect`, `restore`, `move`, `relocate`, `freeze` and `unfreeze`
are thin wrappers over `Alambique.Ops`, `Alambique.Freeze`,
`Alambique.move/4` and `Alambique.relocate/4`; the
steps are in [how-to-operate-a-cell.md](how-to-operate-a-cell.md).

- **Store settings** of the tasks come, highest first, from flags, from
  `ALAMBIQUE_*` environment variables (S3 endpoint, bucket, keys, region, path
  style, prefix), and from `config :alambique, :ops, store: {module, config},
  prefix: ...` or `placement: {module, opts}`. Store settings name the one
  location of a single store. With a placement, a task asks it for the location
  of the cell, or takes the one named by `--location ID`. The tasks start the
  dependencies of the application but no instance, and use the store directly.
- **inspect** reads the freeze marker, the lease, the head and the listing of
  the cell and, unless told not to, fetches every object to measure it.
- **restore** is `Alambique.Replication.restore/5` into a local path, with the
  target `latest`, `txid:N`, an ISO 8601 time or `ms:N`.
- **freeze** puts the object `<prefix>/cells/<c>/frozen` (a JSON note with the
  time and the node). Waking checks it right after the lease is acquired, so a
  frozen cell fails the wake with `:frozen` and releases the lease without
  touching the data. An awake cell checks it on every lease renewal and
  hibernates, so a marker written by a task takes effect within `ttl / 3`;
  `Alambique.freeze/3` and `mix alambique.freeze --connect` also hibernate the
  cell at once. A check that cannot read the store fails the wake rather than
  guess. Restores, compaction and retention keep working on frozen cells.
  `unfreeze` deletes the marker.
- **quarantine** freezes a cell that crashes the VM, so one bad cell cannot
  take down node after node. See "Quarantine" below. `unfreeze` also forgets
  the crashes of the cell, and `inspect` shows them.
- **relocate** (`mix alambique.relocate CELL LOCATION_ID`) needs the nodes
  like `move` and runs `Alambique.relocate/4` on the one it connects to (§18).
- **move** needs the nodes: the task starts distribution under a random name
  (long or short names as the target's), sets the cookie, connects to a node that
  runs the instance and calls `Alambique.move/4` there. A VM that cannot reach
  the cluster over distribution cannot move cells; the other tasks need only
  the store.

### Quarantine

A NIF or SQLite that misbehaves on the data of one cell can stop the whole VM.
The supervisor then restarts the node, the cell wakes again, and the next node
dies the same way. The quarantine records which cell the VM was working on
when it died and freezes the cell after a few crashes.

**Markers.** A process that runs native code on the data of a cell holds a
file `<data_dir>/inflight/<os time>-<n>` (`Alambique.Inflight`). The file is
created without a sync, with a first byte that is 0 (idle) and a JSON body
`{"cell": ..., "kind": ...}`. The process writes 1 to the first byte, with a
`pwrite` on a descriptor it keeps open, before it runs native code and 0 after.
A VM crash keeps the page cache, so it leaves the byte at 1. A crash of the
whole machine loses the page cache, and may lose the file and the flag: no
marker is fsynced, and fsyncing every busy transition would add a disk flush
to every callback (design §12 measures the pwrite at about 1 microsecond;
an fsync is milliseconds). Another node sees only that the lease expired; it
cannot tell a machine crash from a network partition, a paused VM or a dead
process, and it does not know which cell was inside a native callback.
Blaming the lease owner or the last cell state would freeze healthy cells
after ordinary partitions. The marker stays a local VM-crash record. The files are removed by
`Alambique.Inflight.Tracker`, which monitors the processes that hold them, when
the process ends for any reason (an Elixir crash, a kill, a clean stop) and
for the live ones when the instance stops. Only a VM crash leaves a busy file
behind, and only a busy file counts.

| Kind | Held by | Busy during |
| --- | --- | --- |
| `cell` | the cell process, from its start to its end | every callback: wake (restore, open, fence, migrations), calls (the SQL of reads, writes and migrations), flush (WAL capture and encode, upload), checkpoint, close, renewal |
| `hydrator` | the cell process of a cell whose lazy phase is running (§16), one per hydration | the whole lazy phase: every page fetch, its install, and the checksum of the finished file |
| `compaction` | the task of one compaction pass, with its retention run (§9) | the pass |
| `query` | the task of one cell of `Alambique.Query` (§14) | building or refreshing the replica, and the statement |
| `relocate` | the caller of `Alambique.relocate/4` | restoring, fencing and snapshotting for the move |
| `rewind` | the caller of `Alambique.rewind/4` | restoring, reconciling and encoding the rewound state (§20) |

A file per cell process, busy only inside callbacks, blames the cells that
were working when the VM died: a handful, not every awake cell. A flag that
stayed 1 for as long as the cell is awake would blame them all, and three
unclean restarts of a busy node would freeze every cell on it. The hydrating
cell holds a second marker for the whole lazy phase, so a crash in any fetch
task is attributed to that cell; the tasks themselves open no files (§16).
The cost is two `pwrite` calls per callback (about 1.3 microseconds each on
tmpfs), one file creation per wake plus one per lazy phase, and one removal
per process end; a write is a call and
a flush. The measurement is in "Quarantine" of section 12.

**Recording.** `Alambique.Inflight.Tracker` is a child of the instance that
starts before the membership, so before this node takes any call. At start it
reads the markers the previous VM left: busy files count, idle files are
removed. For each cell, several files count as one crash. It adds the crash to
the counter of the cell with a conditional write, emits
`[:alambique, :cell, :crash_recorded]`, deletes the files, and, when the
crashes inside the window reach the threshold, freezes the cell and emits
`[:alambique, :cell, :quarantined]` and logs an error. Cells are settled eight
at a time within `startup_timeout_ms`. A cell the store did not answer for keeps
its files; the tracker tries again every `retry_ms`, and so does the next
start. A freeze that failed after the crash was recorded is retried without
counting the crash again.

**Counter.** `<prefix>/cells/<c>/crashes` is JSON: `total`, the crashes since
the last reset, and `recent`, the newest crashes inside the window, at most
`threshold` of them (`id`, `at_ms`, `node`, `kinds`). `at_ms` is the time of
the restart that recorded the crash, on the instance's clock. A write is a
`PUT` with `If-Match` (`If-None-Match` for the first), and a lost race reads
the object again at once, so nodes that restart together lose no increment. A
write whose outcome is unknown finds its own `id` in `recent` when it reads the
object again and is not counted twice. The cell is frozen when `recent` has
`threshold` entries after a write: crashes older than `window_ms` drop out, so
one crash a day never freezes a cell at the default of three crashes in an
hour.

A clean hibernation does not reset the counter, because a poison cell can
serve for a while before it crashes; only the window and the operator do.
`Alambique.unfreeze/2` and `mix alambique.unfreeze` delete the object, so a cell
that was frozen starts again from zero, which is the decision of whoever
unfroze it. `Alambique.crashes/2` and `mix alambique.inspect` read it.

**Limits.**

- A crash is recorded when the node that had the cell starts again. A node
  that never comes back records nothing; the node that wakes the cell next
  counts its own crash the same way.
- A VM killed from outside (`kill -9`, the out-of-memory killer) while a cell
  is busy is counted as a crash of that cell: the files cannot tell it from
  a NIF crash. Idle cells are not blamed.
- The counter lives at the location of the cell. `relocate` carries the
  freeze marker but not the counter, so the cell starts at zero at its new
  location, frozen if it was frozen.
- The options are in the `Alambique` module documentation (`:quarantine`);
  `false` turns the markers, the tracker and the counter off, and files left
  by an earlier run with the quarantine on are then ignored.

**Limit: the in-flight marker costs two writes per callback.** Each callback
of a cell writes the busy byte of its marker and then the idle byte, two
`pwrite`s on the dirty IO pool. A write runs three callbacks, so six;
`mise run bench:inflight` measures about 34 us of 197 us per write with the
marker on (one run, loaded machine). Keeping the byte busy while the mailbox is
not empty would save most of those writes, but a VM crash is attributed to the
cells whose byte is 1. Under load nearly every hot cell would be busy nearly
all the time, so a crash caused by anything else would count against all of
them and could freeze healthy cells. Batching the writes changes what
quarantine means. `quarantine: false` removes the marker and the protection.
Setting the byte only around native calls, from inside the NIF or with a counter
of nested native calls, would need native changes.

**Limit: a whole-machine crash is not recorded.** The markers are local files
whose busy byte is not fsynced. A power loss, kernel panic or host OOM can lose
the page cache, so the next start cannot tell which cell was inside a native
callback. Keep the node's data directory on storage that survives the failure
if the VM itself restarts, and inspect the node's logs. The quarantine still
covers a VM crash, where the page cache survives.

## 11. Testkit faults

`AlambiqueTestkit.MemoryStore` can, per node and per key pattern:

- return an error, time out, or add latency;
- apply a `put` and then report an error (lost acknowledgement);
- partition a node from the store;
- raise or exit in the calling process, as a store module with a bug or a
  crashed client would (`kind: :raise` and `kind: :exit`);
- crash the calling cell at a named crash point.

`AlambiqueTestkit.GatedStore` wraps another store and holds the puts whose key
matches a pattern until the test answers each one: applied (`release`), failed
without being applied (`fail`) or applied with an error returned
(`lose_ack`). A test waits for a held put with `await` and so decides in which
order uploads land, which a fixed latency cannot do. `test/tail_test.exs` uses
it to land a zombie before and after a restore.

Compaction and retention add the crash points `:compact_before_put`,
`:compact_after_put`, `:compact_before_snapshot`, `:compact_after_snapshot`,
`:retain_before_marker`, `:trash_before_mark`, `:trash_after_mark`,
`:trash_before_delete`, `:trash_after_delete` and `:trash_before_entry_delete`
(§9.1); the cell adds
`:cell_after_land`, `:cell_before_ack` and `:cell_after_ack` around the
acknowledgement of a batch and `:cell_after_checkpoint` after the checkpoint of
an awake cell (see [segment-format.md](segment-format.md)). The restore adds
`:local_prepare`, before a wake makes its directory, `:restore_install`, before the restored file is put in place, and the read
replica adds `:query_publish` (§14); a handler that returns `{:error, :enospc}`
at either makes the step fail as a full disk does (§6.2).

The router adds the crash points `:router_after_serve`, reached on the node
that served a forwarded call before it answers (a handler that closes the
links of the node makes the caller see `noconnection` for a call that ran), and
`:router_forward_lost`, reached on the sender after it sent a call and lost the
connection, before it decides what to do (a handler that waits lets the target
rejoin first, as a sender does that notices the loss late).

`AlambiqueTestkit.Placement` is a placement over named locations whose record
of moved cells lives in a memory store of its own, so every node of a test
cluster sees a committed move at once. Relocation adds the crash points
`:relocate_after_acquire`, `:relocate_after_install`, `:relocate_after_snapshot`,
`:relocate_before_mark`, `:relocate_after_mark`, `:relocate_after_commit` and
`:relocate_before_cleanup`; the cleanup of the source adds `:cleanup_before_mark`
and, through the trash, the `:trash_` points above.

A rewind adds the crash points `:rewind_after_acquire`, `:rewind_after_head`,
`:rewind_before_reconcile`, `:rewind_before_put` and `:rewind_after_put` (§20).

`AlambiqueTestkit.Clock` gives each node an adjustable offset for clock skew.
`AlambiqueTestkit.ManualClock` is a timeline that only moves when the test
advances it, with an offset per clock, so many simulated nodes in one VM
can have different clocks.
`AlambiqueTestkit.Cluster` starts peer nodes, each running an instance over
one memory store on the test node. It kills and restarts a node, partitions a
node from the store (`MemoryStore.partition/2`), partitions two nodes by
disconnecting them and changing the cookie each uses for the other, so that no
reconnect succeeds, skews one node's clock, and collects the telemetry of all
nodes.

## 12. Requirements and measurements

- **Thousands of cells per node.** An awake cell is one process, one SQLite
  connection and three file descriptors (database, `-wal`, `-shm`) plus two
  timers; an asleep cell has none of them. `mise run bench:cells` wakes N
  cells (default 5 000) with one write each, 256 at a time, against
  `AlambiqueTestkit.MemoryStore` in the same VM, then waits until the idle
  timeout (20 s) has put them all to sleep. Measured on an AMD Ryzen AI 5 340
  (12 schedulers, 46 GB), Erlang/OTP 28, Elixir 1.20.4, data directory on the
  local disk:

  | Cells | Wake all | Processes awake / asleep | Open fds awake / asleep | BEAM memory awake / asleep | RSS awake / asleep |
  | --- | --- | --- | --- | --- | --- |
  | 5 000 | 1.06 s (4 700 cells/s) | 5 141 / 141 | 15 019 / 19 | 889 MB / 75 MB | 1 216 MB / 267 MB |
  | 20 000 | 7.33 s (2 700 cells/s) | 20 141 / 141 | 60 019 / 19 | 3 370 MB / 120 MB | 4 388 MB / 527 MB |

  An awake cell with one row costs about 165 KB of BEAM-accounted memory: 25 KB
  for the process and the rest for the SQLite connection, which allocates that
  much whether or not the page cache is small. A cell with data costs more, up
  to its page cache (next bullet but one). After hibernation the process count
  and open files are back at their starting values; the remaining BEAM memory
  is the objects of the in-memory store, and the RSS stays above its starting
  value because the allocator keeps its carriers. The store is in memory, so
  the wake time contains no network latency: against S3 it is dominated by the
  requests of §7 (lease, listings, fence, migrations).

  With the change feed (§13), the same benchmark on the same machine gave:

  | Cells | Wake all | Processes awake / asleep | Open fds awake / asleep | BEAM memory awake / asleep | RSS awake / asleep |
  | --- | --- | --- | --- | --- | --- |
  | 5 000 | 5.44 s (920 cells/s) | 5 144 / 144 | 15 019 / 19 | 1 011 MB / 82 MB | 1 357 MB / 286 MB |
  | 20 000 | 21.7 s (920 cells/s) | 18 108 / 144 | 54 730 / 19 | 3 492 MB / 150 MB | 4 648 MB / 467 MB |

  In the 20 000 run the wake took longer than the idle timeout, so 2 036 cells
  were asleep again when the awake row was taken. Every cell here wakes for
  the first time, which creates the two internal tables and generates the
  triggers (about 3 s of the 5 000-cell run: with the triggers left out the
  run took 2.2 s); a cell that already carries them only compares a
  fingerprint at wake. The triggers and tables add about 24 KB of BEAM memory
  per awake cell, their schema in the SQLite connection. An earlier design
  with `TEMP` triggers and a `TEMP` context table opened a temp database per
  connection and added about 100 KB per cell; it was dropped. Each write
  adds a trigger insert, one stamping `UPDATE` per batch and, after the caller
  is answered, one feed object `put`.

  The feed also starts one sender process per awake cell (§13), which doubles
  the process count in the awake row (10 149 for 5 000 cells in a later run).
  An idle sender hibernates, which brings its memory to under 1 KB (816 bytes
  against 2 624 for a plain waiting process in a quick check); the awake rows
  of the later runs (BEAM memory about 760 MB, RSS about 1 390 MB, one run on a
  loaded machine) did not move measurably with it, so the sender process is
  not the main part of the difference to the table above.
- **Memory of an awake cell.** `mise run bench:memory` (`bench/memory.exs`)
  gives every cell a schema of three tables and three indexes and `ROWS` notes,
  runs 40 mixed calls on it (point reads, an aggregate, a join, a full table
  scan every tenth call, a write every fifth), and then, with the cells awake,
  prints the average of `sqlite3_db_status` over the connections and the growth
  of the BEAM counters. Measured on an AMD Ryzen AI 5 340 (12 schedulers, 46 GB),
  Erlang/OTP 28, Elixir 1.20.4, in-memory store (its ETS and binaries are left
  out of "rest of system"). SQLite allocates through `enif_alloc`, so its memory
  is in `:erlang.memory(:system)` (the `driver_alloc` allocator), not in
  `processes`. A cell of 8 000 notes (a 4.6 MB database), 100 cells, per cell:

  | KiB per awake cell | `cache_kib` 2 000 (SQLite's default) | `cache_kib` 512 (default) |
  | --- | --- | --- |
  | Page cache (`CACHE_USED`) | 2 058 | 528 |
  | Schema (`SCHEMA_USED`) | 45 | 45 |
  | Prepared statements (`STMT_USED`), lookaside slots in use | 0 | 0 |
  | Rest of the SQLite connection (lookaside buffer, connection object) | 83 | 73 |
  | `sqlite3_memory_used` per cell | 2 186 | 646 |
  | Rust allocations of the NIF | 10 | 10 |
  | Other `system` memory (not itemized) | 55 | 45 |
  | `:erlang.memory(:system)` growth per cell, store left out | 2 251 | 701 |

  The WAL index is not on the heap: SQLite maps the `-shm` file (32 KiB), so it
  counts as file-backed memory of the process, not in `system`; with the data
  directory on a tmpfs the database and WAL files are RAM as well. The
  per-cell NIF buffers are the 10 KiB of Rust allocations plus the
  8-byte headers of SQLite's blocks. A cell of 3 000 notes (a 1.7 MB database)
  at 500 cells measured 1 713 KiB of page cache and 1 889 KiB of `system`
  growth per cell at 2 000, and 528 KiB and 692 KiB at 512: the cache holds
  the database up to its limit and no more, so small cells cost what they
  hold. The 3 MB per awake cell that a downstream application saw in `system` matches
  this (inference: 2 000 KiB of cache per connection plus the rest of the
  connection, with a larger schema than the bench's).

  Latency with the smaller cache, 100 cells of 8 000 notes, one call at a time
  (p50 / p99 in microseconds), on a shared host, the runs back to back:

  | `cache_kib` | Reads | Full scans of the 4.6 MB table | Writes |
  | --- | --- | --- | --- |
  | 2 000 | 31 / 265 | 670 / 1 852 | 244 / 17 943 |
  | 1 024 | 51 / 611 | 881 / 3 680 | 389 / 23 933 |
  | 512 | 34 / 285 | 718 / 2 605 | 265 / 19 717 |
  | 256 | 40 / 398 | 775 / 3 472 | 301 / 18 932 |
  | 128 | 44 / 503 | 851 / 2 879 | 342 / 22 015 |

  The runs differ by the noise of a shared host (the 1 024 run is slower than
  the 512 and 256 runs). The only effect that follows the cache is the full
  scan of a table larger than the cache, which is about 7 % slower at 512, 16 %
  at 256 and 27 % at 128 than at 2 000: the pages come from the operating
  system's file cache instead of SQLite's. Point reads and writes did not move
  beyond that noise. 512 KiB cuts the memory of a cell with a large database to
  a third at that price; it is the default. A host whose cells scan databases
  larger than the cache all the time should raise `cache_kib`, and size the
  node by the sizing rule of [how-to-size-a-node.md](how-to-size-a-node.md).

  With `awake.max_bytes` of 100 MiB, 500 cells of 3 000 notes seeded 64 at a time,
  the node kept 62 cells awake and recorded 100.2 MiB for them; every cell
  that was hibernated woke again with its data when called. Not measured: the
  time of an eviction, which is the time of a hibernation plus a sort of the
  awake cells' records.
- **Failover.** `mise run test:cluster` runs the properties on peer nodes
  (§11): acknowledged writes survive random node kills, store faults, node and
  store partitions and clock skew within the margin, epochs never overlap in
  acknowledged batches, and cells are served again within the lease expiry
  plus margin.
  `test/ack_durability_cluster_test.exs` pins the sharpest case: the owner
  dies right after the acknowledgement (the VM halts at the crash point
  `:cell_after_ack`, or the test kills the node at the first acknowledgement),
  another node takes the lease, and every acknowledged write is present. The
  failover property also kills the owner right after random writes.

- **Compaction.** Measured on a laptop (AMD Ryzen AI 5 340) against the
  disposable SeaweedFS container (`mise run seaweedfs:up`), one cell with one
  small batch per TXID, `fanout` 8 and `max_level` 3 (the default is 2):

  | Batches | Restore, level 0 only | Restore after compaction | Restore after a snapshot | Compaction time |
  | --- | --- | --- | --- | --- |
  | 1 000 | 443 ms, 1 000 files | 23 ms, 13 files | 16 ms, 1 file | 0.6 s |
  | 5 000 | 2 373 ms, 5 000 files | 90 ms, 16 files | 70 ms, 1 file | 7.2 s |

  The compaction time is one compactor doing every merge of a cell from
  scratch, one request at a time; a cell that is compacted as it is written
  does a few merges per pass.
- **Commit latency.** `mise run bench:commit` (`bench/commit.exs`) runs N
  writers on one cell, each looping single-row `INSERT`s, 3 s per case, with
  the change feed on and compaction and retention off. A call returns after
  its batch is durable (§6). The first three stores are
  `AlambiqueTestkit.MemoryStore` with 0, 20 and 60 ms added to every segment
  upload (about local disk, R2 and cross-region S3); the last is the
  disposable SeaweedFS on the same machine (AMD Ryzen AI 5 340, 12
  schedulers, Erlang/OTP 28, Elixir 1.20.4, local disk):

  | Store | Writers | Commits/s | p50 ms | p95 ms | p99 ms | Batches/s | Writes per batch |
  | --- | --- | --- | --- | --- | --- | --- | --- |
  | memory +0 ms | 1 | 9 125 | 0.1 | 0.1 | 0.2 | 9 125 | 1.0 |
  | memory +0 ms | 8 | 28 405 | 0.3 | 0.3 | 0.4 | 3 551 | 8.0 |
  | memory +0 ms | 64 | 39 965 | 1.6 | 1.8 | 2.0 | 625 | 64.0 |
  | memory +20 ms | 1 | 48 | 21.0 | 21.0 | 21.1 | 48 | 1.0 |
  | memory +20 ms | 8 | 377 | 21.0 | 22.0 | 22.0 | 47 | 8.0 |
  | memory +20 ms | 64 | 2 774 | 23.0 | 24.0 | 25.0 | 44 | 63.6 |
  | memory +60 ms | 1 | 16 | 61.0 | 61.9 | 62.0 | 16 | 1.0 |
  | memory +60 ms | 8 | 131 | 61.0 | 62.0 | 62.0 | 16 | 8.0 |
  | memory +60 ms | 64 | 1 000 | 63.0 | 64.9 | 81.4 | 16 | 63.5 |
  | SeaweedFS | 1 | 560 | 1.7 | 2.4 | 3.0 | 560 | 1.0 |
  | SeaweedFS | 8 | 3 946 | 1.8 | 2.4 | 7.6 | 493 | 8.0 |
  | SeaweedFS | 64 | 13 360 | 4.6 | 6.1 | 7.6 | 209 | 63.9 |

  The group commit shows in the last column and in the batches per second:
  with 60 ms per upload one writer commits 16 times a second, and 64 writers
  commit 1 000 times a second in the same 16 uploads a second, because every
  writer that arrives during an upload joins the next batch. Latency stays at
  about one upload (p50 63 ms) instead of growing with the number of
  writers. The latency of a commit is the store latency plus about 1 to 3 ms
  here; the in-memory store with latency only delays the `put` of the
  segment, so real stores add their own variance to the tail. These are
  single runs on a laptop, not a statistical benchmark.
- **Merge memory.** `merge.rs` verifies each input page by page and copies the
  compressed pages into the output. A merge of a snapshot of a database with
  half incompressible content, and an incremental file that rewrites one page
  in 50, raised the peak resident memory of the VM above its baseline by:

  | Database | Snapshot file | Before (decompress all) | After (stream pages) |
  | --- | --- | --- | --- |
  | 123 MB | 63 MB | +190 MB | +65 MB |
  | 491 MB | 252 MB | +758 MB (2.0 s) | +254 MB (1.8 s) |

  The figures are measured above a baseline that already held the two input
  files, so the increase is the output. `Alambique.Store.get/2` returns whole
  bodies, so a pass that builds a snapshot holds its inputs and its output,
  which is two to three times the compressed size of the snapshot (inference
  from these figures); the compactor runs `concurrency` passes at once, so size
  that for the largest cell. Streaming gets and puts would need a store
  behaviour change and are not done.
- **Schema creation.** `mise run bench:schema` (`bench/schema.exs`) creates 4
  tables and 7 indexes in N fresh cells, one at a time, in one transaction
  each, against the in-memory store, and alternates two modes in three rounds:
  eleven `Tx.execute/3` calls (one capture cycle each: drop triggers, change,
  fingerprint, generate triggers) and one `Tx.script/2` (one cycle). AMD
  Ryzen AI 5 340, 12 schedulers, other processes running; the figures include
  the wake and the commit. Rounds with 1 000 cells per mode:

  | Mode | ms per cell | cells per second |
  | --- | --- | --- |
  | `Tx.execute/3` x 11 | 7.96 / 7.81 / 8.34 | 126 / 128 / 120 |
  | `Tx.script/2` | 1.78 / 1.85 / 2.25 | 563 / 540 / 445 |

  The script is about four times faster, and the remaining time is the wake,
  the commit and one cycle. Rounds with 500 cells gave 7.4 to 8.2 ms and 1.7
  to 2.2 ms.
- **Cells per node with SQLite in the NIF.** `mise run bench:cells` (5 000
  cells, one write each, 256 at a time, in-memory store), median of three
  interleaved runs on an AMD Ryzen AI 5 340 (12 schedulers, load average 5
  to 6 from other processes). "glibc" is SQLite allocating through the C
  allocator; the other rows are the current code (§2) without and with VM
  flags (`ERL_FLAGS`):

  | | Wake all | RSS baseline / awake / asleep | BEAM memory awake / asleep |
  | --- | --- | --- | --- |
  | glibc | 2.69 s | 116 / 1 028 / 754 MB | 121 / 88 MB |
  | `enif_alloc` | 4.70 s | 118 / 1 078 / 297 MB | 683 / 88 MB |
  | `enif_alloc`, `+MMmcs 0` | 5.46 s | 104 / – / 181 MB | – / 87 MB |
  | `enif_alloc`, `+Mdai max +MMmcs 0` | 3.50 s | 106 / – / 199 MB | – / 87 MB |

  Open fds are 15 019 awake and 19 asleep in every row; processes are 5 139 /
  139 with glibc and 5 143 / 143 otherwise (the four extra belong to
  `telemetry_poller`, which the test build loads). The benchmark prints where
  the memory is; anonymous RSS after hibernation, MB:

  | | RSS anon | ERTS allocators (used) | Store ETS | SQLite holds | Outside the allocators |
  | --- | --- | --- | --- | --- | --- |
  | glibc (one run, load 6 to 12) | 710 | 141 (72) | 12 | n/a | 569 |
  | `enif_alloc` | 250 | 140 (70) | 12 | 0 | 110 |
  | `enif_alloc`, `+MMmcs 0` | 136 | 132 (70) | 12 | 0 | 4 |
  | `enif_alloc`, `+Mdai max +MMmcs 0` | 155 | 158 (71) | 12 | 0 | -3 |

  An asleep cell has no process, no file descriptor and no hydrator, and
  SQLite holds nothing (`sqlite3_memory_used` is 0 after the last connection
  closes). With glibc, the 569 MB outside the allocators were free memory
  that malloc kept, not a leak: `MALLOC_ARENA_MAX=1` left the RSS at 742 MB,
  and `malloc_trim(0)` after hibernation gave it back (236 to 268 MB, 34 to
  44 ms). Since SQLite allocates through `enif_alloc` (`SQLITE_CONFIG_MALLOC`,
  set when the library loads), its memory is ERTS memory: it shows in
  `:erlang.memory/0` and `recon_alloc` (the awake BEAM figure above), and ERTS
  releases carriers when they empty. Rust code in the library allocates the
  same way. The 110 MB outside the allocators without flags are carriers that
  ERTS keeps mapped in its segment cache for reuse; `+MMmcs 0` turns the
  cache off and they go. What remains above the baseline is the in-memory
  store and BEAM data. The 12 MB of ETS are the objects of the in-memory
  store, which a real store would not hold in the VM.

  ERTS serializes the allocations of all dirty scheduler threads on one
  allocator instance. A cold wake makes about 6 million small SQLite
  allocations for 5 000 cells, so with a plain `enif_alloc` the wake took 2.5
  times as long as with glibc. Each thread therefore keeps the blocks of up to
  1 KiB that it frees and reuses them, and empties that cache when the NIF
  call ends, because cached blocks would keep carriers from being released.
  The wake is then 1.75 times the glibc wake. `+Mdai max` gives each dirty
  scheduler its own allocator instance and brings it to 1.3 times; with the
  segment cache on it keeps more carriers mapped (594 MB asleep), so the two
  flags go together. The commit path is unchanged within noise
  (`bench:commit`, memory store +0 ms, best of three: 64 writers 33 099
  commits/s against 32 687 with glibc; 1 writer 8 555 against 7 635). A wake
  against a real store waits for the requests of §7, so the extra CPU of a
  wake shows only in bursts of cold wakes.

  Decision: SQLite stays on `enif_alloc`, without mimalloc. The extra CPU of
  a burst (under 1 s per 5 000 cold cells with `+Mdai max +MMmcs 0`) is the
  price of memory that the BEAM accounts for and gives back.

  The live-cell listing (`Alambique.cells/2`, §8) keeps one ETS row and one
  monitor per awake cell. Median of three runs before and after it, without
  VM flags: wake all 3.92 s and 3.94 s, BEAM memory awake 685 and 693 MB,
  asleep 89 MB in both. A page of 100 in the middle of 5 000 cells takes 36
  to 41 µs, and all 50 pages 3 ms. The asleep RSS medians were 294 and
  326 MB: the ERTS allocators held 136 and 144 MB with the same 72 MB used,
  and 113 to 115 MB before against 123 to 138 MB after were outside them.
  With the BEAM's own memory unchanged, the difference is mapped memory that
  ERTS keeps for reuse (inference; not measured with the flags above).

  Options that did not bring the RSS down, each measured with glibc, three
  runs, asleep RSS (median): `PRAGMA cache_size = -64` on every connection
  755 MB; `MALLOC_ARENA_MAX=2` 758 MB; `sqlite3_soft_heap_limit64` of 128 MiB
  670 MB (it limits the awake peak, not the memory kept afterwards). SQLite
  through mimalloc gave 588 MB (it returns pages lazily and its memory is not
  in `:erlang.memory/0`), with a wake of 4.2 s. ERTS carrier flags with
  SQLite on `enif_alloc` (`+MSsmbcs`/`+MSlmbcs`, `+MSas aoffcbf`,
  `+MSmbcgs 1`, `+MSsbct`) left the RSS at 287 to 302 MB.
- **Lazy restore.** `mise run bench:lazy` (`bench/lazy.exs`) generates a
  database of incompressible rows, replicates it as TXID 1 and 20 small
  batches, restores it in full, and restores it lazily: time to the page map,
  to the first query (open, a point lookup and an indexed count) and until the
  file is complete and its checksum verified (hydration: 8 background reads of
  4 MiB at a time). 2 382 MB (606 633 rows, 609 000 pages), single runs on
  the same laptop; `+20 ms` adds 20 ms to every `get`, `get_range`, `head` and
  `list`:

  | Store | Full restore | Page map | First query | Complete file | Pages a query waited for |
  | --- | --- | --- | --- | --- | --- |
  | memory +0 ms | 13.0 s | 22 ms | 29 ms | 1.0 s | 5 |
  | memory +20 ms | 13.5 s | 147 ms | 228 ms | 2.2 s | 5 |
  | SeaweedFS (local) | 14.9 s | 65 ms | 148 ms | 3.8 s | 5 |

  A cell serves after 0.2 % of the full restore time. The complete file is
  ready before the full restore finishes, because the full restore decodes
  and checksums every file in one thread, in order, and syncs the result,
  while hydration decodes frames on all dirty schedulers and only checks the
  database checksum at the end. The threshold: at 4 MiB of data a full
  restore (28 ms memory, 89 ms at +20 ms) and a lazy first query (4 and
  92 ms) cost the same with 20 ms per request, because the page map needs a
  listing and the edges of every file; at 16 MiB the lazy wake is 2.5 times
  faster at +20 ms (96 against 238 ms). The default `:min_bytes` is
  therefore 4 MiB.

- **Quarantine cost.** The in-flight markers (§10) add one file descriptor
  per awake cell, one file creation and one removal per wake, and two
  `pwrite` calls per callback of the cell process: two for a read, four for a
  write (the call and its flush). Measured with
  `AlambiqueTestkit.MemoryStore` in the same VM, one cell, three alternating
  rounds with `quarantine: false` and with the defaults (microseconds per
  operation, mean of the rounds; the rounds differ by up to 15 %, so read the
  differences as an order of magnitude):

  | Operation | Off | On |
  | --- | --- | --- |
  | read of one row | 13.9 | 14.1 |
  | write, one caller | 128.7 | 140.5 |
  | write, 64 callers (group commit) | 37.2 | 41.5 |
  | wake and hibernate a cell | 1 263 | 1 404 |

  A cross-cell query of 1 500 cells (`Alambique.Query.all/4`) took 465 to 519
  microseconds per cell, cold and warm, with the quarantine off and on alike.

- **Hardening cost.** `mise run bench:cells` (5 000 cells, one write each,
  in-memory store), median of three interleaved runs of the code without the
  hardening (guards, quarantine markers, background reaper) and with it, on the same machine (AMD Ryzen AI 5 340,
  12 schedulers, Erlang/OTP 28, Elixir 1.20.4). The 1-minute load average read
  before the runs was 5.1 to 10.7, from other work on the machine and the
  previous run of the set, so read the small wake difference as noise:

  | | Wake all | Processes awake / asleep | Open fds awake / asleep | BEAM memory awake / asleep | RSS awake / asleep |
  | --- | --- | --- | --- | --- | --- |
  | Without | 5.39 s (928 cells/s) | 5 145 / 145 | 15 019 / 19 | 694 MB / 88 MB | 1 040 MB / 294 MB |
  | With | 5.14 s (973 cells/s) | 5 148 / 148 | 20 019 / 19 | 699 MB / 90 MB | 1 093 MB / 336 MB |

  The code with the hardening holds one in-flight quarantine marker per awake cell (one
  file descriptor, §10) and three more processes per instance (the tracker,
  the trash reaper and the scrub cursor). The rest of the difference is
  consistent with the panic guards on the SQLite allocator hooks and in the
  VFS callbacks (§2.1), which run on every allocation and I/O call: about
  5 MB of BEAM memory and 50 MB of RSS awake, 40 MB asleep. These runs do not
  separate the two costs, and the wake time is unchanged.

- **Stalls on btrfs.** One host (AMD Ryzen AI 5 340, 12 schedulers, btrfs on
  an encrypted NVMe root, a shared load that varied from 5 to 62 during the
  runs), so these are measurements, not guarantees. Two setups.

  *One node and SeaweedFS* (`mise run bench:stall`, 12 cells, 400 writes per
  second, 90 s, VM limited to 2 CPUs, a 150 % CPU quota and `+SDio 4`, cell
  files on btrfs, the store's data in the container on btrfs). Of 7 runs,
  2 had requests over 1 s (maximum 2 134 ms and 2 800 ms), 1 had a maximum of
  839 ms, and 4 stayed under 230 ms. While a run stalled, the sampled
  dirty IO threads were in uninterruptible sleep, mostly in `pwrite` to a
  `-wal` file (151 samples in one run, outside start-up and shutdown) and in
  page faults inside the NIF (45 in the same run); the kernel wait channel was
  `folio_wait_bit`, with a few `btrfs_start_ordered_extent_nowriteback`,
  `wait_current_trans` and `btrfs_sync_log`. With the cell files on tmpfs, a
  90 s run had a maximum of 110 ms and no thread in that state. With the
  in-memory store and the cell files on btrfs, a 90 s run reached 847 ms.
  Making the cell directory nodatacow (`chattr +C`) did not help: 3 runs
  reached 3.5, 5.4 and 5.9 s. A checkpoint every 2 MB of WAL (`wal:
  [checkpoint_bytes: 2_000_000]`) gave one clean run and one of 8.1 s.

  *A container cluster* (3 nodes at 1.5 CPUs, SeaweedFS, HAProxy, 12
  rooms, 8 workers, 40 s per run, p99 and maximum in ms, requests over 1 s):

  | Data of the store / of the nodes | Rate | Runs |
  | --- | --- | --- |
  | btrfs / btrfs | 120 | 205, 5 751, 37 over; 2 010, 4 021, 55 over |
  | tmpfs / btrfs | 120 | 173, 2 095, 40 over; 340, 4 092, 24 over; 417, 2 673, 41 over |
  | btrfs / tmpfs | 120 | 654, 2 025, 28 over; 517, 1 355, 5 over; 138, 1 308, 9 over |
  | tmpfs / tmpfs | 120 | 44, 83, 0 over; 58, 199, 0 over; 60, 85, 0 over |
  | tmpfs / tmpfs | 200 | 74, 1 005, 1 over; 129, 1 012, 1 over |

  In the slow windows, the uploads and lease reads of all three nodes finished
  at the same instant after 0.6 to 1.6 s, HAProxy showed no retry and no
  connect delay, and the VMs logged no long schedule. Only moving both the store's data
  and the nodes' data off btrfs removed the stalls. What was not measured: ext4
  or xfs, nodatacow volumes for the store, btrfs mount options, and why the
  stalls cluster near 2 s in the cluster.

## 13. Change feed

Every committed row change of the application is published so other systems,
such as cross-organization reports, can follow cells without waking or
reading them. The analytics store that consumes the feed is outside this
library.

**Record.** A record carries the cell id, the table, the row key, the
operation (`insert`, `update`, `delete`), the new values (none for `delete`)
and the commit position `{txid, n}`: the TXID of the replicated batch (§6) and
the index of the change inside that batch, starting at 0. The idempotency key is
`<cell>:<txid>:<n>`. The row key is a map of the primary key columns to their
values, or `{"rowid": n}` for a rowid table that declares no primary key; a
`WITHOUT ROWID` table always has one. Values are the table's columns in
declaration order (generated columns are left out); a blob is
`{"$blob": "<lowercase hex>"}` because JSON cannot hold bytes. In this section
"position" alone means the **feed position** of a consumer (below); the record
field `position` is the commit position.

**Capture.** Three triggers per user table (`AFTER INSERT`, `UPDATE` and
`DELETE`) insert one row per change into the internal table
`_alambique_feed(seq, txid, ts, tbl, op, key, vals)` in the same transaction
as the change, with the key and values built by SQLite's `json_object`. The
rows are therefore replicated and restored with the data they describe, and a
rolled back transaction leaves none. The triggers are stored in the file, so a
wake does not generate them again; it only checks a fingerprint (below). The
internal tables are `_alambique_feed` and `_alambique_feed_state(published_txid,
last_time, schema)`; they are created, in the same batch as the migrations,
once the file has a user table (a cell without tables has none), and a table
whose name starts with `_alambique_` or `sqlite_` is never captured. Virtual
tables and their shadow tables are not captured either.

**No feed.** `feed: false` captures no table: the instance generates no trigger,
creates no feed table in the cells, hands nothing to a sender and starts no
janitor, and a rewind (§20) writes no marker. A cell that had triggers loses
them at its next wake, as when all its tables became internal (below); rows
already captured and not yet published stay in its file and are never read.
`Alambique.Feed` on such an instance finds nothing new. The option exists for a
service that does not consume the feed: capture is the largest single cause of
store traffic for cells with large rows, because every row change is written
into the cell's file a second time, as JSON, and replicated with the data
before it is published, then written once more as the feed object (measurements
in `docs/how-to-size-storage.md`).

**Host tables.** `feed: [internal_prefixes: [...]]` (default `[]`) adds
prefixes of the host's own tables that are never captured, with the same
effect as the built-in two, which stay internal. The match is
`String.starts_with?` on the exact table name, done in Elixir, so no `LIKE` or
`GLOB` escaping is involved; the option is validated as a list of non-empty
valid UTF-8 strings. Only `Alambique.Feed.Capture` uses the prefixes, when it
lists the tables to capture (the fingerprint, the trigger generation and the
check of a wake). Queries, `Alambique.Query`, migrations, the schema index
and `mix alambique.check_deploy` do not look at table names and are
unchanged, and the feed producer only reads objects. The fingerprint is of the
captured tables, so a changed option changes it: the next wake, or the next
DDL in a transaction, regenerates the triggers (tables that match gain or lose
them), and rows already captured stay on the feed. A cell whose only tables
are host tables and which had triggers is regenerated too, because an empty
set of tables matches only a file that never had triggers. Nodes with different
values regenerate the triggers each time they wake a cell, so the cell follows
the node that woke it last; the guides say to deploy one value.

**Selection.** `feed: [tables: %{name => spec}, other_tables: :capture |
:skip]` decides what the triggers capture. The spec of a table is `true`
(the whole row, the default), `false` (no triggers), `[omit: columns]` (the
values of every column but those) or `[only: columns]` (the key and those). A
table without a spec follows `other_tables` (default `:capture`), so the
default captures what it always did. The columns of the key are never
omitted. The trigger writes `json_object` of the selected columns only, so an
omitted column is not read, and not in `_alambique_feed`, in the segments or
in the feed object; a record has no key for it (absent, with no marker: the
consumer decides what a missing column means, and `key` always identifies
the row). A name the table lacks is ignored. When columns are omitted the
update trigger has a `WHEN` that tests the captured columns only, so an
`UPDATE` of an omitted column alone leaves no row. The selection is part of
the fingerprint (`{name, create statement, spec}` of each captured table), so a
change regenerates the triggers at the next wake, as `internal_prefixes`
does, and the nodes should agree on it. `Capture` functions take the `feed`
map of the configuration (`internal_prefixes`, `tables`, `other_tables`); the
values are checked when the instance starts.

**Scripts.** `Alambique.Tx.script(tx, sql)` splits `sql` with
`Alambique.Cell.Sql`, refuses with `{:error, {:not_ddl, statement}}`, before
running anything, a statement that `Capture.ddl?/1` does not recognize, and
runs the script once inside `Capture.around_ddl/3`: the triggers are dropped,
the script runs between `SAVEPOINT` and `RELEASE`, and the triggers are
generated again for the final schema. Only DDL is allowed because rows
written while the triggers are dropped would escape the feed; a caller that
needs rows calls `Tx.execute/3` before or after, where triggers exist. The
savepoint makes the script all or nothing inside the transaction: on error
it is rolled back to the savepoint and the error returned, while the earlier
statements of the transaction stay, and the transaction still commits or rolls
back as its function decides. The result is `:ok`. In a transaction opened
with `capture: false` (migrations' own feed step) no cycle runs. A string
migration still goes statement by statement through `Tx.execute/3`; a
function migration can use `Tx.script/2` (how-to-roll-out-a-schema-change.md).

- **Batch TXID.** The triggers insert each row as pending: a negative `txid`
  and the time 2^40. Right before the cell uploads a batch that contains
  writes, it runs `UPDATE _alambique_feed SET txid = <next TXID>, ts = <batch
  time> WHERE txid < 0`. The next TXID is `Replicator.next_txid/1`, which is
  what the upload takes, and nothing is uploaded in between, so every row of a
  batch gets the batch's TXID and time. The triggers need no per-transaction
  state, and a rollback needs no cleanup.
  SQLite stores an integer in the fewest bytes its value needs, and an update
  that leaves the record the same size is written in place: only the page of
  the record's header, not the overflow pages of a large value. (Stamping a
  row born with TXID 0 shrank the record to a small number, and SQLite wrote
  the row again: a message of 75 KB had 66 to 69 pages in its segment
  instead of 49 to 52.) So the pending TXID has the width of the TXID the
  next batch takes: `_alambique_feed_pending` holds a negative number of
  1, 2, 3, 4 or 6 bytes, and the stamp saves the one for the TXID after its
  own, which writes only when the width changes (at TXIDs 128, 32 768, 8 388
  608 and 2^31). A time in milliseconds is always six bytes, like 2^40. The
  one batch that crosses a width writes its rows again, and so does a clock
  below 2^40 ms (a test clock).
- **`n`.** Rows are numbered in the order of their `seq` (the rowid of the
  internal table) inside their batch. Transactions run one at a time in the
  cell process, so `seq` is the commit order. `n` is derived, not stored, so
  every owner that reads the rows gets the same value.
- **Updates that change the key** are published as a `delete` of the old key
  followed by an `insert` of the new one. `PRAGMA recursive_triggers = ON`
  makes `INSERT OR REPLACE` publish the delete of the row it removes. Rows
  removed by `DROP TABLE` are not row changes and are not published.
- **Schema changes.** The fingerprint is a hash of the `CREATE TABLE` text of
  the user tables and of the trigger format, saved in `_alambique_feed_state`
  when the triggers are generated. After the migrations of a wake, the cell
  compares it with the file and, when they differ (the first wake, a new
  table or column from a later release, a new library version), drops and
  generates every trigger in a transaction that joins the migration batch.
  Migrations are captured like any other write: a SQL migration is split into
  statements (`Alambique.Cell.Sql`, which respects quotes, comments and
  trigger bodies) and each statement, like each `Alambique.Tx` call of a
  function migration, goes through the transaction, so its DML (seed rows,
  backfills) reaches the feed with the batch TXID of the migration batch, and
  its DDL regenerates the triggers as below. Row changes are on the feed, DDL
  is not. DDL inside a transaction
  (`Alambique.Tx.execute/3` or `query/3` with a statement that starts with
  `CREATE`, `DROP` or `ALTER`) drops the triggers, runs the statement, and
  generates them again for the new schema, all inside the same transaction.
  Rows written later in that transaction are captured, and a rollback undoes it
  all. `Alambique.Tx.script/2` does the same for a whole script with one cycle
  (see "Scripts" below). Detection is by statement text, because the only way to write is
  through `Alambique.Tx`; `Alambique.query/5` is read-only and refuses writes
  with `{:error, :read_only}`: the connection holds `PRAGMA query_only = ON`
  except inside the cell's write path, and the native authorizer refuses the
  rest (§7, "Statements of callers").

**Publication.** After a batch is acknowledged (§6, right after the callers
were answered and `:after_ack` ran) the cell reads the rows of the acknowledged
batches it has not handed over, in TXID order (`txid <= pipe.acked`: rows of a
batch still in the pipeline are never published, they are not durable), and
sends them to its **sender**, a process of its own that lives as long as the
cell. The sender writes each batch, or each group of batches (below), as one
feed object with `:if_none_match`, in TXID order, and reports each object to
the cell. The put is not part of a commit:
the cell does not wait for it, so the latency of the feed prefix adds nothing
to the answer of a write (with +20 ms on every feed put, 8 writers at 0 ms
segment latency fell from about 6 000 to about 313 commits/s when the cell
published inline). A 412 means an earlier attempt landed (a lost
acknowledgement, or the previous owner) and counts as published. Each attempt
retries until `op_timeout_ms`; a batch that still fails is tried again by the
sender after `feed: [publish_retry_ms:]` (default 1 000), doubling after each
failure up to `publish_retry_max_ms` (default 30 000), for as long as the cell
lives. It never blocks the cell or the callers. Each failure emits a
`[:alambique, :feed, :publish]` event with `result: :error`, `failures` and
`retry_in_ms`, and the sender logs a warning for the first one and then at
most one per `publish_retry_max_ms`.

**Groups of batches.** `feed: [object_batches: n, linger_ms: ms]` (defaults 1
and 0: one object per batch) lets
the sender put up to `n` queued batches in one object, which cuts the feed
puts of a busy cell. When fewer than `n` are queued it waits up to `ms` from
the arrival of the oldest for more, and never waits when the queue is at
`max_backlog_batches` or when the next batch is a rewind marker, which is
always alone. `n` is at most `max_backlog_batches`. The object of a group is
`{"cell": c, "batches": [{"txid": T1, "records": [...]}, ...]}`, without the
top-level `txid` and `records`; the `n` of a record counts inside its own
batch, so `position` and the idempotency key do not change and a record
repeated in another object dedupes. A group of one batch keeps the old shape.
The key is the one the last batch of the group would have had alone (its time
and TXID). A 412 then means that an object ending at that batch landed, and
with it every earlier batch, because the sender puts in order; with the key of
the first batch, a new owner that regroups after a crash could find an earlier
object at the key and drop the later batches of its group. The
feed position stays the key of the last object read, so a consumer that
crashed inside an object reads the whole object again and dedupes. The time
rules below hold because the key time is that of the last batch. A reader that
decodes the objects itself must handle both the single-batch shape and the
group shape; `Feed.read/3` does. Fault points
`:feed_before_group_put` (group chosen, nothing put) and `:feed_after_put`
(landed, not reported) cover a crash that makes the next owner regroup.

**Object format.** The body of a feed object is `"ALF"`, the version byte
`1` and the JSON text of the object compressed with zlib at level 6. There is
no other form: a body that does not start with `ALF` and a version the reader
does not know come as one `Rejected` (`:bad_format`, `{:unsupported_format,
n}`). The publisher splices the JSON text SQLite produced and compresses the
result, so it decodes nothing. A reader inflates at most `feed:
[max_object_bytes:]` bytes (default 268 435 456; the `Feed.ctx/1` option of a
reader outside the instance has the same name): a hostile writer cannot
exhaust its memory, and an object that inflates further or is damaged comes as
one `Rejected` (`:too_large`, `:bad_compression`).

*Choice of the encoding* (measured once with a throwaway script on the objects
of the amplification bench: a message of 75 KB with its label and counters,
then the read mark that writes the row again; run on a host with other load, so
the times are indicative, the sizes exact):

| Encoding of one message (2 objects, 153 KB of JSON) | Bytes | Compress | Decompress |
| --- | --- | --- | --- |
| JSON | 152 940 | | |
| zlib 1 | 71 987 (0.47) | 1.2 ms | 0.46 ms |
| zlib 3 | 71 184 (0.47) | 1.4 ms | 0.47 ms |
| zlib 6 | 66 831 (0.44) | 2.3 ms | 0.42 ms |
| zlib 9 | 66 822 (0.44) | 2.5 ms | 0.41 ms |
| Erlang terms, uncompressed (JSON decoded first) | 151 321 (0.99) | 0.29 ms | 0.003 ms |
| zstd 3 (command line, per object of 76.5 KB) | 2 x 31 008 (0.41) | 0.17 ms | 0.04 ms |
| zstd 19 | 2 x 30 067 (0.39) | 8.9 ms | |
| lz4 1 (the codec of the segments) | 2 x 53 827 (0.70) | 0.05 ms | 0.008 ms |
| lz4 9 | 2 x 41 266 (0.54) | 0.8 ms | 0.013 ms |

Without the body (a selection that leaves it out) the two objects are 1 054 bytes of
JSON and 560 with zlib 6; text that does not compress (a base64 attachment)
gives 0.76 at every level. Grouping both batches in one object gives 0.42 with
zlib 6 and 0.20 with zstd 3 (its window sees the second copy of the body; the
window of zlib is 32 KB).

- **JSON compressed, not a binary encoding.** A term encoding does not shrink
  text, which is almost all of a row: 0.99 of the JSON, and 1.23 for the objects
  without a body, where the keys repeat. It needs the JSON decoded on the
  publisher (the splice avoids that), and it ties every consumer to the BEAM.
- **zlib, not zstd or LZ4.** It is in the OTP the library runs on, so no
  dependency and no native code is added; the object is a standard zlib stream
  that any language reads. zstd gives about 7% fewer bytes per object than zlib
  6 (31 000 against 33 400) and compresses about 7 times faster; that is under 1% of the
  bytes put in the benchmark, and needs the `zstd` crate (a C library built into
  the NIF) or a NIF binding. At 13 messages an hour the 2.3 ms of zlib 6 per
  message is not measurable on the node; revisit it for a cell that publishes
  megabytes a second. LZ4 is in the NIF for the segments, but its ratio is
  1.5 times worse than zlib's on text, and the NIF has no entry for it on
  arbitrary bytes.
- **Level 6.** Levels 6 and 9 give the same size; 6 is 6%
  smaller than 3 for twice the CPU of 1. It is the default of zlib itself.

A stop does not wait for the linger. A cell that stops while its lease is
valid (hibernation, idle, a frozen cell, a shutdown, or a failure other than
the three below) publishes first, after its last acknowledgement and before it
closes the file and releases the lease: it hands the sender every
acknowledged batch not yet handed over, whatever `max_backlog_batches`, and
sends `{:flush, ref}`. The sender then puts its whole queue in objects of up
to `n` batches, with no linger, through the same path as any put (the late
and floor rules and the 412 dedupe hold), and answers `{:feed_flushed, ref}`.
The cell waits for it at most `feed: [stop_publish_ms:]` (default 5 000; 0
turns the flush off) and only while its lease is valid. On a timeout, a lost
lease or a sender that ends, the cell kills the sender and stops as before;
the rows stay in the file and the next owner publishes them at its wake. A
stop with nothing to publish skips the exchange, so it costs nothing, and one
with a queue costs about one put. Fault point `:feed_before_stop_flush` (rows
handed over, flush not sent) covers a crash of the cell there. No flush runs
when the lease was lost or fenced (the new owner is awake and publishes from
the confirmed position), when it expired (the cell may no longer publish), when
the file failed to hydrate, or when the cell crashed. A store that fails makes
a stop with a queue wait the whole `stop_publish_ms`.
`mise run bench:trips` takes `ALAMBIQUE_BENCH_FEED_OBJECT_BATCHES` and
`ALAMBIQUE_BENCH_FEED_LINGER_MS`.

The backlog is bounded: at most `feed: [max_backlog_batches:]` (default 256)
batches are with the sender. While it is full the cell hands over nothing
more; the rows wait in the file (that is where they were anyway) and are handed
over as the sender reports progress. A batch is one object, however many rows
it has, and is never split between objects: a transaction that changes a million rows makes a million-row batch,
read into memory in one piece and put as one very large object.
`max_backlog_batches` bounds the number of batches in flight, not their size.
While publication is behind (a store outage on the feed prefix) the table keeps
growing until it catches up. Change large tables in chunks of transactions, and
keep the feed prefix as available as the data prefix. Splitting objects and
stamping by range would change the object format and are not built.

The cell removes published rows lazily: the next write transaction starts with
`DELETE FROM _alambique_feed WHERE txid BETWEEN 1 AND published` and saves the
latest time in the row of `_alambique_feed` with `seq` 0, `txid` 0 and `op`
`floor` (no batch reads it), inside the user's transaction, so a rollback
keeps the rows and the delete is replicated with the next batch. The time
lives in the feed table so that the delete and the save write the same page: a
small write dirties the page of its table, one for each index, and two for the
feed (the table and its TXID index), 4 KiB each (measurements in
`docs/how-to-size-storage.md`). Files of the first release saved the time in
`_alambique_feed_state.last_time`, which a wake still reads.
`published` is what the sender last reported, so rows of a batch reported
after a write are removed by the next one. Hibernation does not clean up, and
a stopping cell drops what its sender had not put. On wake (§7 step 5) the new
owner hands over every row left, whether the previous owner got to publish it
or not, and serves without waiting for the puts. A sender that ends before the
cell does (a bug) is started again and gets the batches that were not reported.
A batch whose statements changed no row of a user table (the fence,
migrations, the cleanup alone, a transaction that wrote nothing) has no rows
and produces no object.

**Undecodable text and objects.** SQLite does not check that a TEXT value is
UTF-8, and `json_object` copies the bytes. The sender replaces every byte
that is not valid UTF-8 with U+FFFD before it puts the object (it is lossy for
such values, and the object is always valid JSON). A reader never trusts an
object either: one that is not JSON, or has an unknown `op`, a wrong shape or
a malformed rewind marker (a faulty or hostile writer of the store) is
returned as one `Alambique.Feed.Rejected` in place of its records, with a
`[:alambique, :feed, :read, :rejected]` event, and the position moves past it.
A key whose time or TXID has a sign is foreign and ignored, so it cannot
stall the shard. Records of other objects keep arriving.

**Cost of capture.** Each changed row is written to `_alambique_feed` by the
trigger, updated by the stamp and deleted by the cleanup, and so appears three
times in the WAL and the segments. `_alambique_feed(txid)` is indexed, so the stamp, the read and the cleanup do
not scan the table, which matters when publication is behind. The three
writes and the single object per batch remain.

**Cuts.** A TXID names a batch, and every transaction that commits before the
next encode shares it, so `Alambique.Tx.txid/1` (the batch a transaction will
join) minus one is not the state before the transaction: it also drops the
transactions that committed earlier in the same pending batch. Two calls give
a boundary without that loss:

- `Alambique.position/3` is a request to the cell (`:position`). The cell
  drains its pipeline (as `:migrate` does: it encodes the pending commits and
  waits until every batch is acknowledged), then answers the acknowledged
  TXID, epoch and data TXID. It runs in the cell's
  mailbox order, so every write committed before the call has a TXID at or
  below the answer and every later one above it. A cell nobody owns is not
  woken: the router answers from `Replication.head` (one `LIST` and one `GET`;
  `txid: 0` for a cell without data) with `awake: false`.
- `isolated: true` on a write sends `{:isolated, request}`. The cell encodes
  the pending commits, runs the transaction, and encodes its batch before it
  takes another call (it flushes directly instead of queuing a flush behind
  the mailbox; with a full pipeline it waits for a free slot). The batch is
  not durable when the cell takes the next call, only encoded, but nothing
  else can join it. The transaction owns TXID `txid`, `Alambique.Tx.before/1`
  answers `txid - 1`, and a restore of that TXID holds exactly what was
  committed before the transaction. The price is one extra upload; transactions
  that are not isolated keep the group commit of §6.

`position/3` and `before/1` cannot cut inside a batch: the replicated state
has no boundary there. The guarantee is only for a cut made with one of them.

**Positions are not contiguous.** A cell's TXIDs include fences, migrations
and batches without row changes, so the commit positions on the feed skip
numbers and a consumer cannot tell a missed batch from a quiet one by counting.
It does not need to. It reads every object after its feed position in key
order, and the rules below guarantee that no object appears behind a position
that has settled. To audit a cell against the feed, compare its segments in the
store (§4) with the feed objects of the same TXIDs.

**Discovery.** The objects of a location live under `shards` time-ordered
prefixes (§19), one per hash bucket of the cell id:

```
<prefix>/feed/<shard>/<time>-<txid>-<cell>
```

`<shard>` is `:erlang.phash2(cell, shards)` as three decimal digits (`feed:
[shards:]`, default 16, at most 1 000); it is stable across nodes and releases,
and fixed for the life of the feed, because a cell that changes shard would
lose its order.

`<time>` is a batch time in Unix milliseconds, 13 digits, zero padded;
`<txid>` is 16 lowercase hex digits (as in §4); `<cell>` is the cell id. A
key sorts, within a shard, by time, then TXID, then cell.

- **Time of a batch.** The cell stamps a batch with `max(clock now, floor)`
  and every row of the batch carries it. `floor` is the
  latest time the cell has used: it is kept in memory, and read on wake as the
  largest of `last_time` in `_alambique_feed_state` (files of the first
  release) and the `ts` of the rows left, the `floor` row included. So a later TXID of a cell never takes a time below an earlier one,
  even when the clock moved back or an owner's clock is behind the previous
  owner's by up to the skew margin. (Across owners it also holds because a
  publisher only puts while its lease is valid, so the next owner's clock, which
  acquired the lease at least the margin after, reads later than anything the
  previous owner stamped.) Within the same time the TXID orders the batches.
- **Late batches.** A batch is published at its own time only while `now -
  time <= feed window` (`feed: [window_ms:]`, default 10 000). A later
  publication, which happens after a long outage or when a new owner finds rows
  from a dead one, would land behind consumers that have moved on. It is
  therefore published under the time `max(now, its own time)`. Before it
  puts a batch under a time other than its own the publisher looks for the
  object at its original key (`head`), because the previous owner may have
  published it in time. A late batch can still be duplicated, for
  example when an earlier attempt landed under a late key and its
  acknowledgement was lost; that is the at-least-once case, and the duplicate has
  the same content and the same idempotency key.
- **Key floor.** The time of an object key is `max(its own time, floor)`, or
  `max(now, its own time, floor)` when late, and `floor` is the latest time
  any key of the cell took. The sender keeps it for as long as it lives, also
  when its queue is empty, and raises it to the time of every object it
  puts. A new sender starts at the cell's `floor` (above), which holds the
  times of the objects published and of the batches stamped. So the times of
  the keys never decrease along the TXIDs: a batch that is acknowledged after
  a late one was published, though stamped before it, takes a key at or after
  the late one's, and the TXID orders equal times. Without the floor, the
  late rule applied to each queue run alone: once the queue drained, a batch
  stamped earlier than the late key but still on time got a key before it
  (seen with `window_ms: 1`). Across a restart of the sender or an owner
  change, an object that landed but was not recorded is published again from
  the file, under the same or a later key. Keys written by v0.1.0 are read as
  before: the key format has not changed.

  *Limit.* If a late put lands, the sender ends before the cell records it,
  and the clock then reads below that late key (it went back, or the new
  owner's is behind), the batches still in the file can take keys below the
  landed object. A consumer whose position is already at that object skips
  them, and the first occurrence of an idempotency key is then not in commit
  order. No test or property covers this case.
- **Reading.** A consumer's feed position is a JSON map from each list (a
  shard of a location) to the key of the last object it read there, an opaque
  string (`nil` is the beginning). `Alambique.Feed.read/3` lists every shard of
  every location with its key as `start_after` and a limit, in parallel, merges
  the pages by (time, TXID, cell) and loads the first `limit` objects. A page
  that came back full bounds the merge: objects after its last key wait for
  the next read, because the shard may still hold earlier ones. A read costs
  `locations x shards` listings plus one `get` per new object, however many
  cells exist.
  It ignores objects whose time is newer than `now - settle_ms`: an object
  that is still being put (a put can last up to `op_timeout_ms`, and the
  publisher's clock can differ by the skew margin) must not appear behind a
  position that moved past it. `Alambique.Feed.ctx/1` for an instance sets
  `settle_ms` to `window_ms + op_timeout_ms + 2 * skew_margin_ms`; a consumer
  outside the instance chooses its own (default 30 000), and must keep it above
  that sum for the instance's settings. `Alambique.Feed.position_at/2` gives a
  position that starts at a time. A location added to the placement starts at
  its beginning in an old position.
- **Order a consumer sees.** Within a cell, the first occurrence of each
  idempotency key follows the commit order, also across a relocation (§18):
  the restored feed state carries the time floor, so the batches published at
  the destination are never older than those at the source, and one read
  merges both locations by key. Across cells, objects follow their times,
  which are the clocks of the publishers.

**Retention.** Feed objects are deleted by age: `feed: [retention_ms:]`
(default seven days, `nil` keeps them for ever) makes every node of the instance run `Alambique.Feed.prune/2`
every `prune_interval_ms` (default one hour), which lists each shard of each
location from the start and deletes objects whose time is older than `now - retention_ms`. A
consumer that has been away longer than the retention continues from the oldest
object left and has lost the rest. Pruning never touches the segments or snapshots of a cell
and is independent of compaction (§9).

**Delivery.** At least once. A consumer resumes from a stored feed position;
`Alambique.Feed.Producer` is a Broadway producer, compiled only when the
optional `broadway` dependency is present, that polls the feed, stores the
position through a callback once every message up to it was acknowledged, and
delivers again after a restart whatever was not.

**Invariant F1.** Every acknowledged commit appears on the feed exactly once
after deduplication by idempotency key, in commit order per cell, across
crashes and owner changes. A batch whose upload failed after the segment
landed (an unknown outcome for the caller) is durable and so is published; a
write that never reached the store never is. `test/feed_properties_test.exs`
checks it on peer nodes (`mise run test:cluster`): after random kills, store
faults, lost acknowledgements of feed objects, clock skew and crashes between
acknowledgement and publication, the deduplicated feed of every cell equals
the rows the cell holds, in the same order, with a random `object_batches`.
`test/feed_batching_properties_test.exs` checks it on one node with groups of
varied sizes, crashes of the sender and resumes from saved positions.
`test/feed_order_test.exs` checks the order of the keys alone, with held
uploads and a manual clock that also goes back.


## 14. Cross-cell queries

Operators and reports sometimes need SQL over many cells: every cell with its
row counts, mailboxes over quota, the data of one organization. `Alambique.Query`
runs a read-only statement on each cell without waking it, taking its lease or
touching the owner's file. Analytics across all organizations is the change
feed's job (§13), not this one: a query costs work proportional to the cells it
reads, the feed costs work proportional to the changes.

**Source of truth.** A read-only replica built from the store: `Replication.restore/5`
(§6) into a local cache, never the owner's live file and never a lease. A
replica is a complete SQLite file as of one TXID, which every result row
carries with the cell id.

**Staleness.** A write is acknowledged only after its segment is in the store
(§6), so a replica built after an acknowledgement contains it: a query sees
every write acknowledged before the cell's replica was refreshed, plus possibly
a batch whose segment landed but whose acknowledgement is still in flight. The
only thing an awake owner can show that a replica cannot is a transaction that
is committed locally and still waiting for its upload. That is at most one
batch in flight, bounded by `:op_timeout_ms` (default 10 s) and in practice one
store round trip; a read on the owner waits for that upload (§7), so
the owner never shows it either. A query always asks the store, unless the
caller passes `:max_age_ms`, which allows a cached `:latest` replica that many
seconds old (resolution one second). `at: {:txid, n}` and `at: {:timestamp, ms}`
read a past state.

**Cache.** `<data_dir>/replicas/<cell>.r/<txid hex>-<epoch>-<checksum hex>.db`, one file
per replica; the checksum is the database checksum of the LTX chain (§6) at
that TXID. A file is built under a `.tmp-*` name in the same directory, flushed,
and renamed, so a name that parses is always a complete file; a crash leaves
only temporary files, which `trim` deletes after ten minutes. The temporary
file is created empty before anything is downloaded: `trim` also removes cell
directories that hold no file, and would otherwise remove the directory of a
replica still being built (the restore then failed with `{:io, "No such file
or directory"}` on its first segment). The modification time is the last use. Nothing in memory is kept per cell: no process, no ETS
row, no open file while idle. A query opens a cached file for the length of its
statement (`immutable=1`, so no locks and no `-wal`/`-shm`), and closes it.

- **Refresh.** With a replica at TXID `t` the refresh lists the snapshot level (`0009/`) and the
  level-0 segments after `t` (`0000/` with `start_after`), two listings and no
  reads when nothing is new. The two listings run at the same time, so a cell
  costs one round trip to the store, not two, before it reads anything. New
  segments `t+1, t+2, ...` must be contiguous,
  each with an epoch not below the chain's (R2), and are applied with the
  checksum check of restore to a copy of the cached file; the copy is renamed
  to its new TXID and the older replicas of the cell are deleted. The copy is
  needed because other queries may have the old file open. It is a
  copy-on-write clone (`ioctl(FICLONE)`, Linux btrfs, xfs and the like) when the
  filesystem has them, so it shares the unchanged blocks and costs the
  metadata, not the database; otherwise a plain copy.
  `query: [reflink: false]` always copies. `trim` deletes the files of a cache
  directory whose name is not a replica's, such as those a version before
  the checksum in the name left. The starting checksum is the one in
  the cache name, and each segment's trailer gives the next, so no pass reads
  the file; the only data written are the pages of the segments, and the flush
  before the rename writes back only those on a clone. A refresh costs one
  clone, one `get` per new segment and the pages they change; on a filesystem
  without clones it still copies and flushes the file. Not done: reflinks on
  other platforms (`clonefile` on macOS), and skipping the copy when no query
  holds the old file, which would need a count of open readers shared by every
  process of the cache. The run ends at a gap or
  at a file of a lower epoch when the files left are a tail (§6, R3), so a
  refresh during an upload in flight keeps the replica at the prefix. Anything
  else unexpected falls back to a full restore, which has the final say and returns its errors
  (`:epoch_regression`, `{:gap, txid}`, `:history_truncated`,
  `{:history_coarse, txid}`) as the error of that cell: a newer snapshot than
  the replica (compaction and retention, §9, may have removed the segments in
  between), a gap, an epoch that goes back, a checksum mismatch, a missing
  file (a refresh whose segment was deleted falls back as well). A store
  error is not retried through a restore. A restore that fails because the
  listing went stale (`:not_found`, `{:gap, txid}`, `:history_truncated`:
  compaction or retention replaced files between the listing and the reads)
  lists again and starts over, `query: [restore_attempts:]` times in all
  (default 3); when they are used up the cell reports the last error, which
  is safe to retry. "Nothing new" relies on retention never deleting level-0
  files after the newest snapshot (§9), so they are all listed.
- **Concurrency.** `query: [concurrency:]` (default 16) is the number of cells
  built and read at once. It is a fixed number and does not follow the CPUs
  or the dirty IO pool: a cell spends most of its time waiting for the store
  (the two listings, the `GET`s of a refresh), so the time of a query over
  many cells is about `ceil(cells / concurrency)` round trips, whatever the
  quota of the node. With the default tied to the schedulers, 20 up-to-date
  cells on a 1-CPU node cost 40 trips in sequence at 25 ms each, 1.1 s. SQLite
  work is still bounded by the statement slots (§7): the cells of one query
  build and refresh `concurrency` at a time, but a gate of
  `min(concurrency, max(floor(slots * share), 1))` places lets only that
  many of them run their statement at once, so a query never gets `:busy` from
  its own cells. `share` is `query: [max_statement_share:]`, a number above 0
  and at most 1 (default 0.5), also a per-call option like `concurrency:`.
  With every slot taken by one query (share 1) a second query arriving while
  the first ran got `:busy` at once, about 1.5% of the queries at one per
  second in the load test; with half the slots two queries fit together and
  slots remain for cells. 20 up-to-date cells at +25 ms per trip (`MemoryStore`
  latency on the listings, 8 slots): one query took 58-61 ms with share 1 and
  56-59 ms with 0.5; two overlapping queries took 63-66 ms with 4-8 `:busy`
  per run with share 1, and 65-69 ms with none with 0.5. The places only bound
  statements, not the wait for the store, so the share does not slow a query
  down. The gate orders the cells of one query and nothing else: a
  cell that finds the slots taken by other callers still reports
  `{:error, :busy}` at once, as in §7, and the caller retries. A slot is held
  only while a statement runs, never during the waits for the store.
- **Bound.** `query: [cache_bytes:]` (default 1 GiB). A run trims the least
  recently used replicas when the replicas it built since the last trim add up
  to a tenth of the bound (a trim lists the whole cache, so it does not run at
  every few results) and when it ends, also when the consumer stops early.
  Replicas still being built count against the bound and are not evicted. The
  cache can pass the bound by a tenth of it plus the replicas of the cells
  being built at that moment (at most `query: [concurrency:]`); concurrent runs
  each trim for themselves. A replica evicted between
  being chosen and being opened is rebuilt once. Frozen cells (§10) are read
  like any other: freezing only stops waking.
- **Point in time.** A `{:txid, n}` read starts from the newest cached replica
  at or below `n`, and a replica at exactly `n` is used without asking the store
  (history does not change). A `{:timestamp, ms}` read always restores. Other
  replicas of the cell are kept; a `:latest` refresh deletes them.

**Cell listing.** Keys live under `cells/<cell>/...` and the store has no
delimiter listing, so ids are found by listing `cells/` (or `cells/<prefix>`)
in pages of 1 000 keys. After each page the listing jumps to `cells/<last
id>/~`, which skips the rest of the last cell (every name below a cell sorts
before `~`). The cost is the number of pages of keys, at most one request per
cell, and for cells with few segments about one per 1 000 keys. No index
object is kept: it would be a second source of truth that every wake would
have to update. Keys sort by key, not by id (`a-b` before `a`).

**Location.** A listed cell is read in the location the placement gives it
(§18). `location:` overrides that with an `Alambique.Location`, the id of one,
or `{store, prefix}`, and a list of them for scans. The cache names carry a hash
of the location, so the same cell id at two locations never shares a file.
Listing cells (`cells/2`, `:all`, `{:prefix, p}`) is an explicit scan of every
location of the placement, or of those named: paginated, streamed, and never
done unless the caller asks. With millions of cells, pass the ids you want.

**Running.** `Query.stream/5` and `Query.all/5` take a list or stream of cell
ids, `:all` or `{:prefix, p}`. Each cell is a task under a `Task.Supervisor` of
the instance (one idle process), `:concurrency` at a time (default 16), each with `:timeout_ms` for building its replica and running the
statement (default 30 s). The statement is interrupted at the deadline
by the progress handler of the connection (§7, "Statements of callers"). A
cell that fails
(timeout, store error, SQL error, a crash, an invalid id) gives `{:error, cell,
reason}` and the others go on. The connection is `query_only`, and the
statement also goes through `Alambique.Cell.Db.read/4` (§7), so writes,
`ATTACH`, `PRAGMA` with a value and transaction statements give `:read_only`,
and a result over the `:sql` caps gives `:result_too_large`. The statement
takes a slot of the node-wide limit on user statements (§7, "Slots"), so
queries and owner reads together never hold more than
`sql: [max_concurrent_statements:]` dirty IO schedulers; a cell that finds none
free gives `{:error, cell, :busy}`. A query alone never meets it: it holds at
most `query: [max_statement_share:]` of the slots (default 0.5), so two
queries fit together.
Rows of a cell are returned when that cell is done; a cell's result is held in
memory.

**Invariant Q1.** For every row of a result, the statement run on a fresh full
restore of the cell at the row's TXID gives the same rows.
`test/query_properties_test.exs` checks it for random workloads, hibernations
and earlier queries (so any cache state), and that the last result also equals
the owner's. `test/query_test.exs` crashes refreshes and restores at their crash
points and checks every cached file against a restore.

**Measurements.** `mise run bench:query` (1 000 cells with three rows each,
in-memory store without latency, AMD Ryzen AI 5 340, 12 schedulers,
default 1 GiB cache):

| Phase | Time | `get` | `list` |
| --- | --- | --- | --- |
| cold (no replica cached) | 0.44 s | 5 000 | 2 000 |
| warm, nothing changed | 0.46 s | 0 | 2 000 |
| warm, `max_age_ms: 60_000` | 0.45 s | 0 | 0 |
| after 10 cells wrote once | 0.46 s | 20 | 2 000 |
| `Query.cells/2` only | 0.00 s | 0 | 7 |

Warm time is the local work: opening a file and running the statement. Against
S3 the cold run is dominated by its 5 000 reads, the warm run by 2 000 listings.

`mise run bench:query-refresh -- <MiB>` refreshes one large replica with one
small segment (AMD Ryzen AI 5 340, in-memory store, `ALAMBIQUE_BENCH_DIR` on
btrfs; file sizes are what the cell grew to; single runs, the flush of a plain
copy varies a lot with the filesystem's write-back):

| Replica | Before (copy, read, flush) | After (clone) |
| --- | --- | --- |
| 134 MiB | 307 ms (copy 33, read 256, flush 6) | 17 ms (122 MiB file) |
| 254-281 MiB | 17 097 ms (copy 2 133, read 512, flush 18 085) | 38 ms and 46 ms |
| 256 MiB, tmpfs (plain copy) | 846 ms | 94 ms (read pass gone, copy kept) |

The "before" column is the code that read the whole file for its checksum and
copied and flushed it; the passes were timed on the final file.

## 15. Durability and commit latency

Durability is synchronous by default: a write is acknowledged only after the
segment of its batch is durable in the store and every earlier batch is (§6),
so the recovery point objective is zero. Group commit per cell amortizes the
store round trip, and `pipeline_depth` above 1 overlaps the uploads of
consecutive batches.
`mise run bench:commit` measures commit latency (p50, p99) and throughput
per cell against the in-memory store with injected latency and against
SeaweedFS; the numbers live in §12. Measured: the latency of a commit is one
store upload (p50 21 ms at 20 ms, 63 ms at 60 ms with 64 writers), and
throughput per cell is the batch size divided by that upload, so 64 writers
reach about 2 800 commits/s at 20 ms and 1 000 commits/s at 60 ms, against
48 and 16 for a single writer. An asynchronous mode is allowed only as
an explicit opt-in with a documented RPO; it is not built.

**Pipeline depth (measured).** `bench/commit.exs` runs the same cell at
`pipeline_depth` 1, 2, 4 and 8, with closed-loop writers and with open-loop
arrivals (Poisson), against the memory store with the latency added to every
segment upload (3 s per case, AMD Ryzen AI 5 340, OTP 28; reproduce with
`ALAMBIQUE_BENCH_*` variables, see the header of the file). Excerpt, p99 in
ms and commits/s ("done/s"), 16-byte rows:

| store | load | depth 1 | depth 2 | depth 4 | depth 8 |
| --- | --- | --- | --- | --- | --- |
| +20 ms | 64 writers | 1 504 /s, 45.0 | 2 179 /s, 41.1 | 2 788 /s, 24.8 | 2 696 /s, 27.0 |
| +20 ms | open 100/s | 42.0 | 36.3 | 25.0 | 22.1 |
| +20 ms | open 500/s | 42.0 | 36.0 | 30.7 | 27.3 |
| +60 ms | 64 writers | 521 /s, 124.1 | 1 023 /s, 64.3 | 1 016 /s, 63.7 | 1 026 /s, 63.3 |
| +60 ms | open 500/s | 121.1 | 117.1 | 103.1 | 88.6 |
| +100 ms | 64 writers | 311 /s, 204.1 | 625 /s, 103.1 | 622 /s, 104.2 | 622 /s, 106.8 |
| +100 ms | open 500/s | 201.0 | 197.0 | 190.1 | 173.1 |
| +0 ms | 8 writers | 20 188 /s, 0.7 | 18 881 /s, 0.9 | 14 834 /s, 1.4 | 18 261 /s, 1.7 |

With a store latency of 20 ms or more, a deeper pipeline lowers the latency of
staggered arrivals and raises the throughput of 64 and 256 writers (a closed
loop reaches W/L only when its calls fall into one batch). With no added
latency the depth made no consistent difference: the same 0 ms closed-loop
case varied up to 2.3x between two runs at the same depth, so the slower
depth-4 number in the table is noise, not an effect. The price of the gain at
20 ms or more is the number of PUTs: open-loop arrivals sent 2 to 8 times more per second at depth 8 than
at depth 1 (`puts/s` of the bench; 320 against 48 at 20 ms and 500 arrivals a
second), and each is a level-0 file for the compactor.

**Default depth.** The default is 8: the smallest depth with at least 90% of
depth 16's open-loop gain at 20, 60 and 100 ms of store latency, with
`puts/s` under 500 at 20 ms. A second run used open-loop Poisson arrivals,
10 s per case, the same machine and memory store with added put latency. It
ran while the load average was about 40 on 12 schedulers from unrelated work,
so p99 is noisy and not monotonic; p50 is the stable signal. p50 / p99 in ms:

| store | rate | depth 1 | depth 4 | depth 8 | depth 16 | puts/s at depth 1 / 4 / 8 / 16 |
| --- | --- | --- | --- | --- | --- | --- |
| +20 ms | 100/s | 34.3 / 54.1 | 23.3 / 46.1 | 23.6 / 48.0 | 24.1 / 53.2 | 41 / 85 / 82 / 79 |
| +20 ms | 500/s | 38.8 / 106.9 | 26.3 / 43.5 | 24.6 / 100.5 | 26.6 / 61.2 | 40 / 163 / 228 / 200 |
| +60 ms | 100/s | 96.3 / 131.4 | 66.1 / 91.1 | 61.2 / 76.4 | 62.2 / 75.1 | 16 / 59 / 84 / 85 |
| +60 ms | 500/s | 97.3 / 170.3 | 70.8 / 104.9 | 66.1 / 89.3 | 63.3 / 85.2 | 16 / 65 / 128 / 237 |
| +100 ms | 100/s | 158.4 / 208.8 | 114.8 / 143.2 | 105.1 / 139.4 | 103.1 / 110.0 | 10 / 38 / 67 / 84 |
| +100 ms | 500/s | 154.6 / 207.9 | 132.0 / 192.0 | 111.0 / 141.9 | 107.5 / 150.9 | 10 / 37 / 77 / 148 |

Depth 8 reaches at least 90% of depth 16's p50 gain at all three latencies;
depth 4 reaches about half of it at 100 ms and 500/s. Depth 8 also reaches 90%
of the p99 gain at 60 ms. At this load p99 is too noisy to apply the
threshold alone (depth 8 at 20 ms and 500/s is an outlier, and depth 16 is
worse than 8 at 100 ms and 500/s). `puts/s` at 20 ms stays under 500 (228).
Closed-loop writers (64) roughly double commits/s at 60 and 100 ms from depth
2 on (table above). Set `pipeline_depth: 1` to commit one batch at a time.

**Local cost benchmarks.** Three more variables and two benchmarks isolate
costs that the store latency hides, all against the memory store:

- `ALAMBIQUE_BENCH_FEED_LATENCIES` (`bench:commit`) adds latency to the put
  of every feed object, so the cost of publishing the feed shows; and
  `ALAMBIQUE_BENCH_READ_RATIO` makes that fraction of the calls
  `Alambique.query` reads of the same cell.
- `mise run bench:wal -- 60` (`bench/wal.exs`) keeps one cell awake under
  steady writes and prints, every 2 s, commits/s, p99, the size of the
  `-wal` file and the BEAM memory, to show whether the cost of a commit
  follows the growth of the WAL.
  Before the checkpoint of an awake cell (§6) three runs ended at 2.0, 4.0 and
  4.2 GB of WAL, with commits/s falling to about a third in two of them; after
  it the WAL peaked at 61 to 63 MB (the default limit, 64 MiB) in all three
  runs and commits/s did not fall. The machine was loaded (load average 20 to
  90) in both series, so the throughput of the two series is not comparable.
- `mise run bench:router`, `bench:inflight` and `bench:routing` time the pieces
  of the routing and write paths listed below (measurements, "Routing and write
  path costs").
- `mise run bench:sums` (`bench/sums.exs`, pages from
  `ALAMBIQUE_BENCH_PAGES`) builds cells of 10 000 to 1 000 000 pages and
  times a commit that changes one row, to show what the page-sums vector
  (8 bytes per page) costs per commit. Before the sums became a persistent
  native resource (below), the median grew with the page count: 338, 650 and
  1 658 us at 12 036, 100 258 and 300 763 pages. After it, 347, 262 and
  388 us (medians of 3 runs on a loaded, shared machine, so only the trend
  counts; the 300 k figure ranges 309 to 911 us).

  The sums are a Rust resource (`sums.rs`) in 256-sum chunks behind `Arc`s.
  A capture clones the chunk pointers (one per 2 KiB of sums) and copies
  only the chunks of the pages it changes, so a commit costs
  `O(changed pages * 256 + pages / 256)` and no sums cross the NIF boundary
  as a binary. The resource never changes, which keeps a state reusable after
  a failed upload (`Replicator`). Only the whole-file operations (a snapshot,
  `file_state`, `lazy_finish_sums`) are linear, and they run on dirty
  schedulers.

### Routing and write path costs

Local-cost measurements against the memory store on a shared machine (load
average 4 to 12), medians of three repetitions per run. Only the trend counts.

- **Cluster view** (`mise run bench:routing`). One node cannot fake other
  nodes, so the benchmark adds fake members to the `:pg` groups; the cost of
  the old reads grew with them. Microseconds per `Membership.nodes/1`,
  `Membership.foreign_nodes/1` and `Membership.preferred/2` with 1, 100 and 1 000
  members: 1.3, 3.2 and 22.3; 1.6, 3.6 and 23.2; 1.4, 3.2 and 23.6 before the
  cached view; 0.03 to 0.2 at every size after it.
- **`awake.max_pending` check** (`mise run bench:router`). The router takes
  a place in the load of a cell before each local call. It uses the load
  counter from the lookup the call already makes; a second registry lookup would cost 0.4 to
  0.6 us of a call of 10 to 14 us.
- **In-flight marker** (`mise run bench:inflight`). A closed-loop writer on
  one cell with the marker on and off: the marker costs about 34 us of 197 us
  per write (one run, three repetitions), six `pwrite`s for the three
  callbacks of a write. It is not reduced (§10, "the in-flight marker costs two writes").
- **SQLite calls per write** (`test/cell_nif_calls_test.exs`). Each call is a
  dispatch to a dirty IO scheduler. A write makes 6: `PRAGMA query_only = OFF`
  shares a call with `BEGIN` and `PRAGMA query_only = ON` with `COMMIT`; the
  rest are the feed cleanup, the statement, the status check and the stamp of
  the feed rows, which is one script.
- **Head of a chain** (`test/replication/head_cost_test.exs`). `Replication.head/2`
  reads the first 4 KiB of the newest file, and for a larger file the last 16
  bytes, which is all the header and trailer check needs. A file of a few KiB
  is one request. It sends a `HEAD` for each of the last 17 files of the chain, because their epochs are
  only in the object metadata.
- **Listing a chain** (`test/replication/list_scale_test.exs`). Listing 1 000 000
  keys took 2.4 s with the pages appended to the accumulated list, 0.4 s with
  the pages collected in reverse and joined once.

**Limit: the head of a chain costs a request per recent file.**
`Replication.head/2` reads the header and trailer of the newest file (two
ranged reads), but it also sends a `HEAD` for each of the last 17 files of the
chain, because the epoch that cuts off the files of a fenced owner is only in
the object metadata. A wake that reuses a local file and a `position` call on a
sleeping cell cost those requests. This is not a correctness problem; a store
that returned object metadata with its listing would remove them, and the store
behaviour has no such callback (`test/replication/head_cost_test.exs`).

**Limit: there is no asynchronous durability mode.** Every write waits for its
segment to be durable in the store; the opt-in mode described above is not
built.

## 16. Lazy restore

A waking cell serves before its file is downloaded. It is on by default for a
wake that has no reusable local file (§7) and whose files are larger than
`:min_bytes`; `lazy_restore: false` turns it off.

**SQLite and the VFS.** SQLite is bundled in `alambique_nif` (`sqlite.rs`)
because the VFS must live in the same SQLite library that
runs the queries. Connections and statements are NIF resources and every
call that can block runs on a dirty scheduler. The Alambique VFS (`vfs.rs`)
is the default unix VFS with one difference: for the main database file of a
cell that is hydrating it checks a page bitmap (`lazy.rs`) before every read.
The WAL, its shared memory, journals and locks are the default VFS's, so WAL
mode and WAL capture (§6) work as before. Only
`Cell.Db.open/1` uses it; the files of the query cache (§14) are opened with
the default VFS. A file that is not hydrating costs one null check per call.

**Page map.** A restore with the `:lazy` option walks the chain exactly as a
full restore does (§6: snapshot, then the file that starts at the next TXID
and reaches furthest; same checks of contiguity, pre-apply checksums, epochs
and timestamps) but reads only the edges of each file: its first and last
64 KiB with ranged reads (the first read also returns the size and the
metadata, so the epoch needs no `HEAD`), and more of the tail when the page
index is larger. The edges of the files the walk will most likely need are
read in parallel before it starts: first the first range of each file, which
says its size, then the trailers of those that have one. The LTX page index (docs/segment-format.md)
gives each page's frame offset and size. A file contributes its pages to the
map as `dbsum.rs` would apply them to a database file: newest version wins,
a snapshot replaces the map, pages past a file's `commit` are dropped. The
database file is created sparse at its final size. A file must
carry every page the database grows by, and its page index may not be larger
than `commit` entries can take: the map and the tail read are sized by the
objects, never by a header field alone. A first object that is not an LTX
file (short, invalid page size) fails the restore with
`{:error, {kind, key, reason}}`. The checksum the
chain ends with (`Restore.result.checksum`) is what the finished file must
produce. Per page the map costs about 33 bytes (frame location, CRC, flags,
bitmap, fetch order): 20 MB for the 2.4 GB database of §12.
Small chains are restored in full: if the files add up to less than
`:min_bytes` (default 4 MiB, §12) the lazy plan is dropped and the restore
repeats as a full one, from the same listing and without reading the
trailers (the sizes come with the first ranges). A full restore reads the
whole files of the chain it expects in parallel, `restore: [fetch_concurrency:]`
(16) at a time, in the order of the walk and as many as fit the byte budget of
the restore (§6.3); the walk then takes each from memory instead of waiting
for it. A guess that is
wrong costs requests, and the walk reads what is missing. The files themselves are not checksummed when they are
read lazily (their file checksum covers all pages); every page is covered by
the database checksum check at the end instead.

**Misses.** A read of a page that is not local blocks the dirty I/O scheduler
thread that runs the statement. The VFS asks a single global thread to send
`{:page_miss, pgno, file, offset, size}` to the cell's hydrator (a scheduler
thread cannot send messages itself), then waits on a condition variable.
The hydrator reads the page with `get_range` through the store behaviour, so
testkit faults apply, together with up to `:miss_pages` (default 16) pages
that follow it in the same object, decodes the LZ4 frames in the NIF and
installs them: a page is written to the file and marked local under one lock,
so a reader never sees a partial page. Installing a page (`lazy_deliver`)
runs on a dirty CPU scheduler, and the fetch tasks open no files. This is
what makes a VM with a single dirty I/O scheduler (`+SDio 1`) work: the read holds that scheduler while it waits, the fetch
task runs on a normal scheduler, and the install runs on a dirty CPU
scheduler, so the page arrives and the read continues. A read fails with
`SQLITE_IOERR` ("disk I/O error", only that statement) when the hydrator
reports that the fetch failed after retrying for `op_timeout_ms`, or when
`op_timeout_ms` plus one second passed without an answer. The next statement
that needs the page asks again. The lazy restore fetches page 1 before the
hydrator starts and before the file is opened; if that fetch fails, it
discards the plan and restores the chain in full.

**Hydrator.** Each awake cell that restored lazily has one hydrator process
(`Alambique.Replication.Hydrator`, linked to the cell; none for an asleep
cell). It reads the pages nobody asked for in runs of frames of the same
object, newest object first (the pages written last are the ones the cell
reads first) and in offset order, up to `:batch_bytes` (4 MiB) each and
`:concurrency` (4) at a time. A run reads across the frames of older versions
of pages that another object replaced, when they are at most 64 KiB, which
costs less than one more request. The hydrator starts before the fence, so it
has begun with the pages of the latest writes when the cell serves. It retries
with backoff, and gives up on what no retry can fix (an object that is gone, a frame that does not
decode). When every page is local it compares the database checksum of the
pages as the store holds them with the restored checksum, tells the cell
(`{:hydrated, pid}`) and ends; the bitmap and the page map are freed and the
file is a normal database. A mismatch or a permanent failure stops the cell
(`{:hydration_failed, reason}`). The cell holds one in-flight marker, busy
for the whole lazy phase, so a crash in any fetch task is attributed to the
cell and the tasks need no file of their own (design §10). Telemetry:
`[:alambique, :replication, :hydrate]`.

**Writes during hydration.** A write goes to the WAL as usual. Before a
checkpoint writes a whole page, the VFS marks it local, and a fetch that
arrives later never replaces a local page (but still records the CRC of the
page as the store holds it). A write to part of a page that is not local waits
for it. A truncation marks the pages past the new end local. The replicator
keeps one CRC per page (§2, `sums`) to compute checksums without reading the
file; while hydrating it does not know the CRC of pages nobody read, so before
capturing a batch `sync/3` fetches the old pages that the batch replaces or
drops (`wal_changed`, `lazy_prepare_sums`, which copies only the chunks it
fills). At the end `hydrated/1` fills the
remaining CRCs (a dirty NIF) and checks that they add up to the state's checksum. `snapshot/3`
returns `{:error, :hydrating}` meanwhile; the compactor builds snapshots from
the store and is not affected.

**Wake.** Step 2 of §7 uses the lazy restore when no local file can be
reused. The restore fetches page 1 and only then starts the hydrator; the
open reads page 1 from the file and does not wait. The cell serves as soon as
the fence and the migrations are done; those and the feed publication (§13)
read pages as needed, and a miss is served by the hydrator on a normal
scheduler.

**Hibernation and crashes.** A cell that hibernates while pages are missing
uploads what is pending, checkpoints, closes and *deletes* the file without a
marker; the next wake restores lazily again. The alternative, saving the
bitmap and the page map so that the next wake continues, was left out: it
needs the files of the map to still exist when the cell wakes again, a
second format to keep in step, and only helps a cell that goes idle within the
minutes hydration takes. A cell or VM that dies leaves a file without a marker
(§7), which no wake reuses; the next wake builds a new plan and fetches page 1
before opening it. If the page-1 fetch fails, that wake discards the partial
file and restores the chain in full, so an interrupted lazy restore cannot
leave a wake failing on a page whose fetch cannot run.

The complete-file marker (`cell.db.meta`) needs no extra fsync for a killed
VM: the checkpoint fsyncs the database before the marker is written, and the
page cache survives the kill. A power cut can lose the marker, which only
makes the next wake restore again; it cannot make a marker describe bytes
that never reached the disk. Retention (§9) deletes files only after a grace
period (`grace_ms`, ten minutes) and only those that a snapshot older than the
window covers, so the objects of a plan are there as long as hydration takes
less than the PITR window; a hydration that finds an object gone fails the
cell.

**Query cache.** The replicas of §14 are read in full by a query that
scans, are cached and never written, and are built by `Restore` without a
connection to wait on; they keep full restores. A lazy replica would help
point lookups on huge cells, but it would need a hydrator per cached replica,
which breaks the rule that cached cells have no process.

**Invariant H1.** Reads served during hydration equal reads after a full
restore at the same TXID, and the checksum of the file once complete equals
that of a full restore. `test/lazy_restore_properties_test.exs` generates
databases, histories with merged files and snapshots, and workloads of point
lookups, ranges, aggregates, full scans, schema reads and integrity checks
interleaved with writes (including transactions, rollbacks, `DROP TABLE` and
`VACUUM`), syncs, checkpoints, store errors, timeouts and latency on ranged
reads, and a crash; every read is compared with the same read on a fully
restored copy that ran the same writes, a read or write may fail with an I/O
error only while a fault is injected, and the final database checksum, the
restored checksum and the content are compared with a full restore of the
store. 1 500 runs passed.

**Limit: a cell idle during hydration starts over.** A large cell that
hibernates or crashes before its file is fully downloaded discards the partial
file, and the next wake starts the lazy restore again. Pages read on demand are
verified only by the database checksum at the end of hydration. A page-1 range
read that fails falls back to a full restore; a wake still fails when the
store is unreachable or an object of the chain is gone. Raise `idle_timeout_ms`
for very large cells.

## 17. Versioned migrations

Status: built.

**Version.** `migrations:` are `{version, sql | fun}` (or a module),
forward-only. They are validated when the instance starts: positive, unique
and contiguous from 1. The highest version is the instance's schema version N;
an instance without migrations has N = 0 and runs no migrations or window checks, but still keeps the schema index. A cell's
version is its `PRAGMA user_version`.

**Per-cell migrations.** `migrations:` can also be `{module, function,
extra_args}`, where `{module, function}` means `extra_args = []`; the config
keeps only the three-element form. At every wake the cell calls
`apply(module, function, [cell_id | extra_args])` (exported with arity
`1 + length(extra_args)`, checked when the instance starts) after the lease is
taken and before the file is restored, and the result must be a list valid under the rules above (`[]` is
version 0). The cell's N is the highest version of that list, so the window
rule, the migrations that run and the index entry below all use the N of the
cell, and a list that grows between wakes (workers of one namespace deployed
while others keep running) takes effect at the next wake. A raise, throw or
exit in the function, or an invalid list, fails the wake with
`{:error, {:bad_migrations, message}}` and nothing is migrated or uploaded.
The instance has no single schema version: `config.schema_version` is `nil`.

**Migrating an awake cell.** A list that grows takes effect at the next wake;
`Alambique.migrate/3` applies it to a cell that is awake now. The request is
routed like a call. A cell nobody owns answers `{:ok, :asleep}` without
waking (it migrates at its next wake); with `wake: true` it is woken, and the
wake migrates it. In the owning cell process the request:

1. uploads the pending writes first (as a read behind them waits), so their
   callers are answered before the migrations start;
2. runs the migration step of the wake again (§7, step 4) with the list that
   `Config.migrations_for/2` gives now: window check, pending migrations each
   in its own transaction, feed schema, index put, one batch uploaded, index
   delete of the previous version, after-ack, then the feed publication of
   the rows the migrations wrote;
3. answers `{:ok, user_version}`.

Calls that arrive meanwhile wait in the mailbox and are served afterwards, so
a long migration blocks the cell for its duration. `{:schema_too_new, v}` and
`{:bad_migrations, message}` leave the cell serving. When migration `k` fails
it rolls back and the call answers `{:error, {:migration, k, reason}}`; the
migrations before it in the same call have already committed locally, so they
are indexed and uploaded as their own batch and the cell serves at version
`k - 1`. The cell never has local commits that are neither uploaded nor
discarded, and the index lists the durable version. A failure to put the
index, to install the feed schema or to upload stops the cell like a failed
commit (`:fenced`, or `{:sync_failed, reason}`); the next wake restores the
durable state and migrates again.

**Window on wake.** After the file is restored and fenced, the wake reads the
cell's version `v`:

| `v` | Result |
| --- | --- |
| `v < N` | migrate to N (§7, step 4) |
| `v == N` | serve |
| `N < v <= N + w` | serve without migrating |
| `v > N + w` | the wake fails with `{:error, {:schema_too_new, v}}` |

`w` is the instance option `migration_window` (default 1). The N+1 case is code
for N-1 serving a cell that code for N migrated during a rollout. It works only
if migration N is expand-only (additive tables and nullable columns), which the
caller guarantees; the library cannot check SQL. The same `w` bounds the deploy
check below, so a deploy never runs ahead of the sweeper by more than the code
tolerates.

**Schema index.** Each cell has an empty object
`<prefix>/schema/<version>/<cell>` in its location, with the version in ten
zero-padded digits so that key order is numeric order. The wake keeps it
consistent through crashes by ordering the writes:

1. the entry for the target version is put after the migrations ran locally and
   before the batch is uploaded (a failed put fails the wake and nothing was
   uploaded);
2. the batch is uploaded;
3. the entry of the previous version is deleted (best effort). A cell that
   the wake found empty in the store has only the entries its own process
   wrote, so a new cell skips this request.

At every instant the durable version of a cell has an entry; the index may
briefly show two versions of one cell, never none. A crash between 1 and 2
leaves an entry ahead of the cell (harmless: the old entry is still there); a
crash or failure between 2 and 3 leaves one behind, and the sweeper removes it
the next time it reaches the cell. That can take until the lease of a crashed
cell expires (§5), so the double listing is allowed until then.
A wake that reuses the local file without migrating writes nothing; any other
wake puts the current version again, which repairs a lost entry. Cells created
before the index existed have no entry until they wake once. Every instance
keeps the index, including instances without migrations and cells at version 0
(`schema/0000000000/<cell>`), so it lists every cell that has woken. A new
cell, or a cell that migrates from version 0, costs one more put (and a
delete of the entry at its previous version, also for version 0) than before.
The deploy check ignores version 0: such a cell is never too old. The sweeper
lists version 0 too, so a stale entry left by a crash during a migration from
it is removed; a cell whose namespace still has no migrations is not woken.

The oldest live version of a location is the first key under `schema/`. The
other versions are found by listing again after the last key of each version
(`schema/<v>0` sorts after every `schema/<v>/...` and before `schema/<v+1>/`),
so a survey costs one request per distinct version, not one per cell, and
needs no delimiter from the store.

**Listing every cell.** `Alambique.list_cells/2` pages through the cells of one
location, asleep or awake, by id prefix, from the same index (the index is the
only place that names a cell without a scan of `cells/`). A page surveys the
versions (one request per version plus one), then lists each version with the
prefix `schema/<v>/<prefix>`, `start_after` the previous page's last id and
`limit + 1` keys. The per-version lists are merged in byte order, a cell with
entries at two versions counts once, and the page is the first `limit` ids;
`next` is the last id of the page when the merge held more than `limit`. A page
costs `2 * V + 1` requests for `V` versions with entries, each returning at most
`limit + 1` keys, so the cost does not depend on the number of cells
(`test/list_cells_test.exs` counts the requests). An `after` that sorts before
the prefix starts the listing at the prefix; one that sorts after every id with
the prefix ends it without a request. The survey is repeated on every page
rather than carried in the cursor, which keeps `next` a plain cell id. Options
`list_cells: [page_size:, max_limit:, timeout_ms:]` set the defaults. A cell
that has never woken since the index exists is missing until it wakes once.

**Relocation (§18).** The relocator reads the version of the fenced copy, puts
the destination entry before the source is marked as moved, and deletes the
source entry after the placement commits. A retry that finds the move already
marked does not know the version and leaves the source entry to the sweeper or
to the cleanup of the source (§18), which removes the entries of every version.

**Sweeper.** The `sweeper:` option (off by default) starts one process per
instance with one timer. A round lists the index of every location from
`Placement.locations/1`, for versions from 0 below N, page by page (`page_size`), and
for each cell: asks the placement for its location (a cell that moved has a
leftover entry, which is deleted); finds out whether it is awake; reads
`PRAGMA user_version` through `Alambique.query/5`, which wakes and migrates it
through the normal path (lease, fencing, feed capture); hibernates it again if
it was asleep; and deletes the listed entry, which is stale because the cell is
at another version. Cells go `concurrency` at a time and the round keeps to
`rate` cells per second. Frozen cells are skipped. A round with errors doubles
its wait up to `max_backoff_ms`. Entries leave the listing as cells migrate, so
the cursor is the last cell of a page. `Alambique.sweep/2` runs one round. Every
node with the option sweeps the whole index; enable it on few nodes.

With per-cell migrations there is no single N to filter versions by. The round
lists every version of each location, pages the cells of each, calls the
function once per listed cell and wakes only the cells whose listed version is
below their own N. Cells at or above their N are not counted in `cells`; a
function that fails for a cell counts one error for it and the round goes on.
The cost is one function call per listed cell on every round, plus the listing
requests. Version 0 is not listed: a cell at it migrates at its next wake.

**Deploy check.** `Alambique.check_deploy/2` and `mix alambique.check_deploy`
survey the index of every location and compare the oldest live version with the
oldest readable version, `max(N - w, 1)`. They need only the store, so they run
before the new code does, but only against the same placement configuration as
production. Exit codes: 0 passes, 1 refuses, 2 could not run. Locations that
cannot be listed are an error, never a pass.

With per-cell migrations the oldest version of a location says nothing, since
each cell has its own N. The check pages every cell of every version, calls the
function for each, and compares the cell's version with `max(N_cell - w, 1)`.
The report has `code_version: nil` and `oldest_readable: nil`, and each
location lists `too_old_cells`. The cost is one function call and a share of a
listing request per live cell, so it grows with the number of cells; a function
that fails for a cell is an error of the check.

**Invariant M2.** With per-cell migrations, an acknowledged write is durable, a
cell that woke is at the N of its namespace or ahead of it, and the index
lists every cell that woke at its durable version. After one sweep
every listed cell is at its N and listed only there.
`test/cell_migrations_properties_test.exs` checks it with several namespaces
whose lists grow while the instance runs, restarts, store faults and crash
points during the wakes.

**Invariant M1.** A cell on N-1 is served correctly by code for N, and
migrating it never loses rows.
`test/migration_properties_test.exs` checks it with three releases (N-2, N-1, N)
over one store that wake the same cells in random order, with expand-only
migrations (columns, backfills, seeded rows, tables), store faults and crash
points during the wakes. After every operation the durable rows hold every
acknowledged write and exactly the rows that migrations up to the cell's
version seed, and the index lists the cell at its durable version. At the end
the sweeper of the newest release leaves every cell at N and listed only there,
and the feed holds each row exactly once after deduplication. Telemetry:
`docs/telemetry.md`. Guide: `docs/how-to-roll-out-a-schema-change.md`.

## 18. Placement

The library knows no hierarchy: no regions, organizations or tenants. The
caller supplies an `Alambique.Placement` behaviour as the instance option
`placement: {module, opts}`; every callback gets `opts` first.

- `validate_id/2` (optional) decides how cell ids are structured. The default
  is the rule of §1. Whatever a placement accepts, the library also requires a
  non-empty binary of at most 200 bytes without `/` or NUL, and not `.` or
  `..`, because ids are used in keys and file names (a store or proxy may fold
  dot segments in a key).
- `locate/2` returns `{:ok, %Alambique.Location{id, store: {module, config},
  prefix}}` or an error. The id names the location and must stay stable.
- `locations/1` returns every location, for whole-store work: the change feed
  and its janitor, cross-cell queries, the sweeper and the deploy check.
- `commit_move/4` (optional) records a completed move; without it
  `Alambique.relocate/4` answers `{:error, :relocation_unsupported}`.
- `validate_options/1` (optional) checks the options when the instance
  starts.

`Alambique.Placement.Single` (one store, one prefix, the location id
`"default"`) is the default. `store:` and `prefix:` as instance options are
its shorthand; there is no other path. Examples a caller can build: one bucket
for everything, a bucket per region, a prefix per tenant.

Every per-cell object (lease, segments, snapshots, frozen marker, feed
objects) lives in the cell's location. A cell process, a compaction pass, a
freeze or a lease read asks `locate/3` for the location and builds the lease
and replication contexts from it. The schema index of §17 lives there too.

**Caching.** Answers of `locate/2` are cached per instance (§8). A stale
answer cannot let a cell be written in two places: a wake never uses the cache,
it asks the placement before it takes any lease, and a location that the cell
left refuses (see below). Operations that change state elsewhere, such as
freezing and relocating, ask the placement afresh too. After `commit_move/4`
returned, `locate/2` must answer the destination on every node.

**Guarantee P1.** A cell id maps to exactly one location at a time, and only
that location accepts its writes.

**Move between locations** (`Alambique.relocate/4`, `mix alambique.relocate`).
Given the destination, as built in `Alambique.Relocate`:

1. Take the lease at the source with owner `"relocate"`, as epoch `e`: ask the
   owner to hibernate and wait, or wait for a dead owner's lease to expire. A
   lease of an earlier relocation is released on its behalf. Restore, write
   the fence (§6) and so close the source to older epochs.
2. At the destination: check that it holds nothing of the cell, or only what an
   earlier try of this move left, or the moved lease of a cell that lived there
   before and is cleaned up (see "Moving back"); install a free lease with
   `moved_from` the source and epoch `e + 1` (above every epoch of the old lease
   there); upload a snapshot of the fenced state as of its TXID. Copy the frozen
   marker if there is one. Renew the source lease.
3. Mark the source lease `moved_to` the destination with `If-Match`. If anyone
   took the cell since step 1 the mark fails and the move stops. The source
   lease is released and every acquire gives `{:error, {:moved, id}}` from then
   on.
4. Call `commit_move/4`. From now on `locate/2` returns the destination.
5. Clean up the source (below). This step cannot fail the move: the cell is
   already at the destination.

**Deviation from the first draft of this section.** The draft marked the
source after committing the move. Then a crash between the two leaves the
source lease to expire while the placement already points at the destination,
and a node with a stale answer could write at the source while the destination
also accepts writes. Marking first makes the order safe: the destination is
reachable only after the source can never be woken again. The price is that the
cell is unavailable between steps 3 and 4 (and from step 1 to 3, when calls
get `{:error, {:unavailable, "relocate", _}}`), a placement write long.

**Failures.** A crash or error at any step leaves exactly one writable
location: the source until the mark, the destination after the commit, and
nowhere in between (calls fail with `moved` or `unavailable`). Run
`Alambique.relocate/4` again with the same destination until it returns `{:ok,
location}`: it finishes from wherever the previous try stopped, including when a
mark was written but the commit was not. A node that wakes the cell at the
source after the mark gets `{:error, {:moved, location}}` when the placement
still names the source, because the placement is not caught up.

**Feed across a move.** The restored database carries the feed state, so the
time floor of the cell (§13) continues at the destination and its first batches
are never older than those at the source. Rows the source had not yet cleaned up
are published again at the destination, with the same idempotency keys. A
consumer reads both locations (`Alambique.Feed.ctx/1` takes every location of
the placement) and deduplicates, as always.

**Cleanup of the source.** After the commit the objects at the source are of
no use except to readers that started before the move: a cross-cell query that
restores the cell from its cache, a restore that listed the source, a
`mix alambique.restore`. The cleanup (`Alambique.Relocate.Cleanup`, called by
`Alambique.relocate/4` as step 5 and by `Alambique.cleanup_moved/4`) removes,
once `moved_at_ms + moved_grace_ms + skew_margin_ms` has passed (instance option
`moved_grace_ms`, default ten minutes, as the `grace_ms` of retention in §9):

- every segment and snapshot under `<prefix>/cells/<c>/`, for every level;
- the fixed keys `frozen`, `trashed` and `crashes`, at once, because they name
  a residency rather than a TXID range;
- the schema index entries of the cell at that location (§17).

"Removes" means marks for deletion (§9.1) for the data: segments and snapshots
go `deletion_grace_ms` later (default one day), so they can be restored or kept
during that time. The two graces add up: a data object is deleted
`moved_grace_ms + deletion_grace_ms` after the move at the earliest. With
`deletion_grace_ms` 0 the cleanup deletes at once. `cleaned_at_ms` is stamped
after the data is marked and the fixed keys are gone.

The lease stays with `moved_to`, because a node with an old view of the
placement refuses to wake the cell only through it. Feed objects stay too: they
belong to the feed's own retention (§13), and consumers read every location.
The cleanup refuses `{:error, :live_location}` for the location the placement
gives (read afresh) and `{:error, :not_moved}` for a lease without `moved_to`,
so it can never delete at the live location. It claims the lease object with a
conditional write (`claim_cleanup`), deletes with a deadline of six times
`op_timeout_ms`, and stamps `cleaned_at_ms` (`mark_cleaned`, also conditional).
A second cleanup that starts meanwhile makes the first one's stamp fail with
`{:error, :contended}`. Nothing is lost by repeating any part of it.

**Pending cleanups survive a restart.** The queue of the compactor is in
memory, so the store holds the truth. Before step 3 the relocator writes an
empty marker `<prefix>/moved/<cell>` at the source; the cleanup deletes it as
its last step, after `cleaned_at_ms` is stamped. A marker therefore means "this
cleanup is not finished", whichever node or step stopped. Every
`compaction: [moved_scan_ms:]` (default 60 000) the compactor of each node
lists the markers of every location, one page of `moved_scan_page` (default
100) per tick, and queues the cells it finds. The grace is not kept in memory:
the cleanup reads `moved_at_ms` from the moved lease, so a restart neither
deletes early (the cleanup answers `:waiting` and is queued again for
`ready_at_ms`) nor forgets. The marker is in a prefix of its own because
finding moved cells by listing leases would read the objects of every cell.
The body of the marker is the deadline of the move that wrote it: its time on
the instance's clock (not the store's, whose clock may differ) plus the
`timeout` that move uses, whether the instance's `relocate: [timeout_ms:]` or a
longer one passed to `relocate/4`. A move that failed before the mark leaves a
marker for a cell that is live or not moved at that location: the cleanup of
such a cell answers `:live_location` or `:not_moved` and deletes nothing, and
the compactor then deletes the marker once its deadline plus `skew_margin_ms`
has passed, because before that the marker may belong to a move between the
marker and the mark.

Several nodes may find the same marker. Only one deletes at a time: the claim
on the lease object (`claim_cleanup`) is conditional, a second claimant that
overtakes the first makes its stamp fail with `{:error, :contended}` (the
compactor drops that job; the marker brings it back at the next scan if the
cleanup did not end), and every step (marking, deleting, dropping the index) is
idempotent, so a repeated or overlapping run loses nothing. The invariant that
nothing is deleted at the live location or before the grace holds for every
caller because `Cleanup.run` checks both against the store, never against the
queue. `test/moved_cleanup_properties_test.exs` checks it with random moves,
restarts and clock steps.

A relocation that crashed before step 5 is not repeated by `relocate` (the
placement already names the destination); the marker lets the compactor finish
it, or `cleanup_moved` by hand. With compaction off nothing scans: run
`cleanup_moved`.

**Moving back.** A cell can move to a location it left while the data its
cleanup marked is still pending for the reaper. The destination check of step
2 accepts a lease with `moved_to` when:

1. `cleaned_at_ms` is set (otherwise `{:error, :destination_not_cleaned}`, and
   the move queues the cleanup in the compactor instead of cleaning inline:
   the move has a deadline and the cleanup waits for the grace, so the caller
   retries)
   and older than eight times `op_timeout_ms` plus the skew margin (otherwise
   `{:error, {:destination_settling, ready_at_ms}}`, so a caller waits instead
   of cleaning again, which would restart the gap). The stamp is written after
   a cleanup marked the data and deleted the fixed keys. An earlier cleanup
   that is still running (slow, or overtaken by this one) stops at its
   deadline, six times `op_timeout_ms` after it started, which is before the
   gap ends, so no marking or deletion by a cleanup can reach objects the new
   move writes, and
2. every segment and snapshot there ends at or before the TXID the relocator
   restored from the source (otherwise
   `{:error, {:destination_ahead, txid}}`). One `LIST` of the cell's prefix
   answers this; the trash prefix is never listed. The incoming snapshot has
   that TXID and covers every file at or below it, so restore cannot pick an
   older state, and a compaction that wrote a merged file or snapshot after
   the cleanup is below it too.

No lease at the destination is different: the move still requires the prefix
to hold no data (`{:error, :destination_not_empty}`), because nothing vouches
for why it is there. `Lease.install` then replaces the marker with a free
lease whose epoch is the
larger of `e + 1` and the marker's epoch + 1, without `moved_to`. Epochs
therefore grow along any sequence of moves (L1 and the fencing of §6 rely on
it): the relocator's lease at the source is above everything the source ever
held, and the destination continues above both. Until the placement commits,
the destination holds a free lease that no one takes, because wakes ask the
placement first; a stale node whose cache still says the destination is sent to
the real location, and one that does reach it only finds `moved_to`.

The pending manifests cannot touch the returning data: they name segment and
snapshot keys, whose TXIDs are at or below the old maximum, and the cell writes
above the TXID it carried away. Fixed keys are absent from new manifests, and
the reaper ignores such a key in a manifest. During
the grace the old location still restores its last state, as §9.1 says; after
the move back the newest snapshot is the incoming one, and a point-in-time
target between the old maximum and the incoming TXID answers
`{:error, {:target_not_reached, txid}}` rather than an older state. Epochs grow
across the move, so R2 holds. The cost of a move back is the same lease read
and restore as before, one `LIST` of the cell prefix and the snapshot upload;
listing the trash prefix is not needed.

A `relocate` to the location the placement already names answers `{:ok,
location}`, unless its lease is marked as moved elsewhere: that is a move to
finish first and the answer is `{:error, {:move_pending, id}}`.

**Not done.** The relocator renews the source lease only between steps, so a
snapshot that takes longer than the lease ttl ends in a failed mark and a
retry (nothing is lost). A reader that holds
a restore of the source for longer than `moved_grace_ms` can fail with a
missing file and retries.

`test/placement_properties_test.exs` checks P1 on peer nodes (`mise run
test:cluster`). Random scenarios mix writes from every node, relocations to
either location (so cells move back and forth), cleanups, crashes at each step
above and in the cleanup, node kills, faults in both stores and clock skew, with
nodes holding stale answers. A sampler checks that a cell is never writable at
both locations (the placement names one while the other location's lease is
neither a moved marker nor a free lease installed by an uncommitted move).
After healing, the last relocation asked for each cell is run again to
completion, no acknowledged write is lost, the epochs of a cell's commits never
decrease and never appear in both locations, every location can be cleaned up
without losing a row, and the feed read from both locations satisfies F1. Each
scenario runs twice, with `deletion_grace_ms` 0 and with 60 000: the first
reaches the trash deletion crash points, the second moves cells back while the
marked objects are still there and reaches the marking crash points.

## 19. Scale

Design target: zero to millions of cells per continent.

- Per-cell keys keep each cell's traffic on its own prefix.
- Feed objects are spread, within each location, over `feed: [shards:]` prefixes
  (default 16) chosen by hashing the cell id, so no single prefix carries every
  cell's feed writes (S3 limits requests per prefix; 16 prefixes give 16 times
  the write rate of one) and a cell always maps to the same shard, which keeps
  its order. A consumer lists the shards in parallel, so a read costs
  `locations x shards` listings, independent of the number of cells. The
  default is a compromise: more shards raise the write ceiling and the cost of
  every poll, fewer do the opposite. Choose it before the first write and do not
  change it.
- No operation lists every cell on the request path. Whole-store scans
  (operator views, the sweeper, the deploy check) page through listings or
  use the schema index.

## 20. Rewind

`Alambique.rewind(instance, cell, target, opts)` replaces the state of a cell
with its state at `target`, `{:txid, n}` or `{:timestamp, ms}`, as one new
TXID. `Alambique.Rewind` implements it; the guide is
[how-to-rewind-a-cell.md](how-to-rewind-a-cell.md).

**The switch.** The state changes with one conditional put of a level-0
segment at TXID `head + 1`, under the epoch of the rewinder's lease. The
segment lists every page of the rewound database (`capture_replace` in the
native crate, the encoder of a snapshot with `min_txid = max_txid = head + 1`),
has the checksum of the head as its pre-apply checksum and the checksum of the
rewound file as its post-apply checksum. Applied to the head, it gives the
rewound state exactly, pages beyond the new size dropped; the restore, the
merge in the compactor, the lazy restore and the replicas of §14 treat it as
any segment. It is a compare-and-swap on the whole chain:

- it is written with `:if_none_match`, so a writer that took `head + 1`
  before (a stale owner, a taker's fence) makes it fail with a 412 and
  nothing is damaged; the rewinder reads the head again and starts over, up
  to `rewind: [fence_attempts:]` times;
- a writer that comes later fails the same way, on a TXID the rewind holds;
- a segment only applies to the state whose checksum it names, so it can never
  be mixed with a chain it was not built from.

A snapshot at the new TXID would not have this property: its key differs from
a segment's, so a taker's fence would not collide with it and a restore would
pick the snapshot and then reject the taker's segments. Because the put is the
guard, the lease is not what makes the rewind correct. It keeps readers away.

**Steps.**

0. Check the size: `rewind: [max_bytes:]` (default 256 MiB, `nil` for none)
   against the pages of the current head, read from its header, and again
   against the target once restored and the current state once restored:
   `{:too_large, bytes, max_bytes}`, before the lease for the first two.
1. Restore the target into `<data_dir>/work/rewind-<n>/target.db`, without a lease:
   history is immutable. Anything restore refuses (`:history_truncated`,
   `{:gap, _}`, `{:history_coarse, _}`, `{:target_not_reached, _}`, §6) is
   `{:behind_retention, oldest_txid}`, where `oldest_txid` is the lowest TXID
   a restore reaches. A `{:txid, n}` at or above the head, or a timestamp that
   resolves to it, is `{:target_not_behind, head}`. `:if_head` is compared to
   the head here: an integer to the head TXID, `{txid, :data}` to the data
   TXID (§6), refusing with `{:data_written, data}` when it is above `txid`.
   The segment of step 4 is data: its `data` is its own TXID.
2. Take the lease with owner `"rewind"` (`Alambique.Exclusive`, the loop that
   `Relocate` also uses; it takes the lease right after the owner hibernated,
   not after a poll interval, so a call cannot wake the cell in between, which
   also shortens a relocation's wait): the owner is asked to hibernate, a dead owner's lease
   is waited out, a stale `"rewind"` lease is released on its behalf. When it
   returns no process serves the cell and calls get `{:unavailable, "rewind",
   _}`. The lease is not taken over (§5.1): the old owner could still answer
   reads after the rewind. A moved cell is `{:moved, id}`. A frozen cell is
   rewound and stays frozen, because freezing only stops waking.
3. Read the head again (`:if_head` again), restore the current state into
   `current.db`, and compare the schema versions of both files
   (`PRAGMA user_version`, from the files, so the check needs the current
   state): `schema: :same` refuses a difference with `{:schema_differs,
   target, current}`.
4. Reconcile the target file in one transaction (`Alambique.Rewind.Reconcile`):
   carry the host tables over, add the feed marker (below). The current state
   is attached read-only by URI, so a file that is gone is
   `{:error, {:attach, reason}}`, where a plain `ATTACH` would create an empty
   database and carry the host tables over empty.
5. `Lease.renew`, then put the schema index entry of the target's version
   (when it differs), then the segment. The encoding refuses a missing main
   file (a snapshot of a cell with no file is still empty) and a page count that
   is 0 or differs from the one the reconciled connection reports
   (`{:error, {:capture, reason}}`, nothing sent), so an empty or foreign file
   is never put. On `:txid_taken` go to 3. An error that
   leaves the outcome open is `:outcome_unknown`.
6. Delete the old index entry (best effort; the sweeper removes a stale one),
   release the lease (best effort: it only expires otherwise) and remove the
   work directory. The cell stays asleep; its next call wakes it, restores
   through the segment and writes its fence at the next TXID.

**Crash points.** The points `:rewind_after_acquire`, `:rewind_after_head`,
`:rewind_before_reconcile`, `:rewind_before_put` and `:rewind_after_put` (§11) are reached in the rewinding
process. Before the put the store holds the old chain; the lease of the dead
rewind expires after `lease_ttl_ms` (or the next rewind or relocate releases
it) and the cell serves the old state, with at most an extra schema index entry
for the sweeper. After the put the store holds the new chain and the cell serves
the new state once the lease expires. A put whose answer was lost is adopted
when the stored object is the rewinder's own (same bytes and epoch), as for any
segment. Nothing local has to be undone: a wake restores from the store, and
the file of a hibernated cell is reused only when its marker equals the head,
which the segment changed. A VM crash leaves work directories (`rewind-*`, `relocate-*`, `scrub-*`) in
`<data_dir>/work`, a name no cell directory has because those end in `.cell`;
the instance removes `work` when it starts (`Cell.Local.clean_work_dirs/1`,
before any operation can run: the data directory belongs to one instance, as
the files of its cells do). A restart of the instance while a rewind runs in a
caller's process removes the files under it. The rewind then fails, never
installs less: the current state is attached read-only, the encoding refuses a
missing file and a wrong page count, and a segment that was already encoded is
a valid one. `test/rewind_test.exs` deletes the files at `:rewind_before_reconcile`
and `:rewind_before_put`. `Inflight` records the work of kind `rewind` (§10).

**Readers.** Calls to the cell go to its owner, so they see the old state
until the owner hibernates, are refused while the lease is the rewinder's, and
see the new state afterwards. Cross-cell queries (§14) and restores read the
store, so they see the old chain or the new one: a replica built at the head
applies the segment because its pre-apply checksum is the head's.
`test/rewind_properties_test.exs` samples the cell from every node during
random rewinds and accepts only the state before and the state after.

**Host tables.** `keep: :internal` (default) carries from the current state
into the rewound one every table whose name starts with one of
`feed: [internal_prefixes:]`, with its rows, `sqlite_sequence` entry and the
indexes, triggers and views that sit on it, and drops the target's copies
first. Objects whose name starts with `_alambique_` are never carried (a
capture trigger from before the prefix was listed would otherwise follow the
table), and nothing is captured: the copy runs with `capture: false`, and the
host tables are not on the feed. Captured tables are never carried, which
would give the feed rows that match no record. `keep: :none` rewinds
everything. Virtual tables are refused (`{:keep_unsupported, table}`); foreign
keys and triggers of host tables that name rewound tables are not reconciled.
Carried tables are not migrated, so `schema: :migrate` (which accepts another
`user_version` and lets the next wake migrate forward) is refused with them
(`:keep_needs_same_schema`): a migration that already ran on the carried table
would run again.

**Feed.** The reconcile step creates the internal tables if the target has
none and inserts one row into `_alambique_feed` with `txid = head + 1`,
`tbl = ''`, `op = 'rewind'` and the marker as JSON in `vals`, and raises
the time of the `floor` row to the rewind's time, the larger of the clock, the time floor of
the current state and the floor of the target. The row is part of the segment,
so it exists exactly when the rewind does, and the first owner publishes it in
step 5 of the wake (§7) like any row left by a previous owner: after the rows of
the target that were unpublished (TXID order, with the late-publication time
rule of §13), then the marker, then everything written after. The object is

```json
{"cell":"c","txid":N,"records":[],"rewind":{"from_txid":H,"to_txid":T,"target":{"txid":T},"kept":["_myapp_jobs"]}}
```

`records` is empty. Readers get an
`Alambique.Feed.Rewind` among the `Alambique.Feed.Record`s (`id` is
`"<cell>:<N>:rewind"`). A consumer that reads the feed must handle it: after
the marker the cell is in its state as of `to_txid`. Its meaning, with the
case of a rewind to a state that an earlier rewind undid, is in the module
documentation. The key time of the object is not below any record of the undone
history (the floor is carried), so within the shard of the cell the marker
sorts after them. Rows the target had not published are published again by the
next owner with the ids they had (`<cell>:<txid>:<n>`; `n` is the order of the
rows of the batch, which the table keeps), so a subscriber sees duplicates of
records already delivered, before the marker, and deduplicates by id as always;
`test/rewind_test.exs` checks it after the feed objects were pruned. Rows of
the undone history that were still unpublished are lost and are void anyway.
F1 holds after deduplication, with the undone records removed
(`test/rewind_properties_test.exs`). TXIDs are never reused, so an id names one
change for ever.

**Late publication by a stale owner.** The feed already covers it (§13): a
publisher puts only while its lease is valid, an object is keyed by the
publisher's own time, which the next owner's clock (it acquired the lease at
least the skew margin later) does not precede, and a reader ignores objects
younger than `settle_ms`, which covers a put that is still running when the
lease ends. The marker takes `max(clock, feed floor)` as its time, where the
floor includes everything the replaced state published, so a stale record put
after the marker sorts before it, and a consumer with the instance's
`settle_ms` reads it before either object. A consumer with a shorter
`settle_ms` can miss it, as for any late object.
`test/rewind_test.exs` moves the clock of the rewinding node back and checks
that the marker still sorts after the undone records.

The segments of a rewind are ordinary: any node restores and serves a rewound
cell.

**Cost.** The rewind writes a segment as large as the database and needs room
in `data_dir` for two copies of it (target and current). The encoder, the
upload, the restore and the compactor's merge hold a whole segment in memory,
as they do for a snapshot. Measured on one node over the in-memory store, with
random 1 MB values:

| database | `capture_replace` alone | whole rewind |
| --- | --- | --- |
| 200 MB | resident memory +700 MB at the peak (segment 201 MB) | 7.6 s; +1 030 MB at the peak, the in-memory store's own copy included |
| 50 MB | | 3.6 s; +145 MB at the peak |

`apply_ltx` of the same segment added no memory beyond the binary, and
`merge_ltx` of one input +190 MB over the input. None of them streams. This
is the existing limit of snapshots and compaction (§9, §16); only the lazy
restore reads pages by range, and it is not used for a rewind, which needs the
whole file.

**Telemetry.** `[:alambique, :cell, :rewind]` (telemetry.md); the upload is also
a `[:alambique, :replication, :sync]` with `kind: :replace`.

## 21. Read replicas

A hot cell is read from many nodes, and every read costs a hop to the owner.
A **read replica** is a copy of the cell's database on a node that does not
own it, kept up to date by polling the store. A caller that accepts a bounded
lag asks for it with `Alambique.query(inst, cell, sql, params, consistency:
{:bounded, ms})` and gets a result that contains every write acknowledged more
than `ms` milliseconds before the read started. The owner is not involved: no
message is sent to it.

The guarantee below is the point of the design, so it comes before the
mechanics.

**Guarantee.** For a read that starts at monotonic time `q` on a node with
bound `B`, the result contains every write whose acknowledgement the caller
received before `q - B` (a duration measured on one node's monotonic clock; no
wall clock and no other node's clock is read). Proof sketch. A replica
refresh takes `s = monotonic time` before it sends its first request and
records `confirmed_at = s` when its listing is applied. A read is served from
the replica only if `q - confirmed_at <= B - margin`. Take a write `w`
acknowledged before `q - B`. Then `w` was acknowledged before `s` (the check
gives `s >= q - B + margin`), so its segment had landed in the store before
the refresh sent its `LIST` (§6: a write is acknowledged only after its
segment is stored). With pipelined commits an acknowledgement also means that
every lower TXID of the pipeline landed (§6, answers come in TXID order), so the run of segments
the listing shows after the replica's head is contiguous up to and including
`w` (invariant R3: a tail past a gap is ignored, and it never contains an
acknowledged write). The replica applied that run before it set
`confirmed_at`. So the file contains `w`. This relies on the assumption that
restore and the fence already make (§6): a `LIST` sees every object whose `PUT`
completed before the `LIST` was sent. The two clocks that matter are both the
replica node's own monotonic clock (`q` is read on the node that serves the
read, which is where the replica is), so nothing needs agreeing across nodes.
Clock rate error enters only through the elapsed time of `B` on the caller's
side; `margin` is `drift_ppm` of the bound (default 200 ppm) plus one
microsecond, in the units the replica measures in.

**What is not guaranteed.**

- *Monotonic reads across the owner and a replica.* A caller that reads from
  the owner and then from a replica (a stale refresh, a cell whose replica is
  not ready, an `on_stale: :owner` fallback) can see time go back by up to
  `B`. Within one node a replica only moves forward.
- *`{:bounded, 0}` is not accepted*, and a small bound is not linearizable
  either: the bound says "every write acknowledged `B` ago", and a write
  acknowledged at `q - B + 1 ms` may be missing or present. Use `:strong`, or
  `verify_lease: true`, for a read that must see every write acknowledged
  before it started (§5).
- *A bound below the cost of a refresh is served by waiting*, not faster: with
  `on_stale: :refresh` the read waits for a refresh that started after
  `q - B + margin`, and goes to the owner if none finishes within
  `refresh_timeout_ms`.
- *Rewinds are writes.* A rewind (§20) removes data that was acknowledged; a
  replica follows it like any other segment, so a read after the rewind is
  bounded against the rewound history.

**Process and file.** `Alambique.Replica` is one process per (instance,
location, cell) on a node that does not own the cell, under a
`DynamicSupervisor` in the instance tree and registered in a `Registry` by
`{location id, cell}`. Its file is
`<data_dir>/read-replicas/<cell>@<hash of the location id>/replica.db`, with one
read-only connection (`immutable=1`, `query_only`, `cache_size` set from
`replicas: [cache_kib:]`, `mmap_size` 0). The directory name is not the
`<data_dir>/replicas` of the cross-cell cache (§14), which belongs to
`Alambique.Query` and survives restarts; the whole `read-replicas` directory is
removed when the instance starts, and a replica removes its own directory when
it stops. A crash leaves at worst a file that nothing reads and the next start
removes. The replica never writes to the store, the lease or any file but its
own.

A replica is built with `Replication.restore/5` (a full, not a lazy, restore:
the lazy page map and an in-place apply do not combine without more native
work). Reads during the build go to the owner. The build is followed at once by
a refresh, and the replica serves only after that refresh, because a restore
does not read the lease: a cell that moved would otherwise look current.

**Refresh.** Every `poll_ms` after the previous refresh ended while the
replica is read, then less often as it idles (the interval is a quarter of the
time since the last read, up to `poll_max_ms`, never below `poll_ms`), and on
demand when a read is stale, a task takes `s` and sends, concurrently, a `LIST` of the
level-0 files after the head, a `LIST` of the snapshot level and a `GET` of the
lease. (The snapshot listing is not in the first sketch: without it, a replica
whose next files were deleted by retention would see an empty run and confirm a
state that is behind. Query's refresh makes the same check, §14.) Then:

1. `Chain.run/3` takes the contiguous single-TXID run after the head. Files left
   over that are not a tail (`Chain.tail?/3`) mean a gap; a snapshot newer than
   the head means the history was cut. Either rebuilds the replica with a full
   restore.
2. The new segments are fetched 16 at a time, each with a deadline of its own
   (`missing_final`: a deleted file rebuilds at once), and their ranges
   checked. A refresh keeps the segments it fetched before one it cannot read,
   and stops after 32 MiB of bodies; it applies them, does not confirm the
   replica, and starts the next round at once. A replica far behind (after a
   bulk write, say) therefore advances at every round, holds a bounded number
   of bodies, and does not fetch the same segments again at every poll.
3. The replica process closes the connection, applies the run in order with
   `Native.apply_ltx` (the pre-apply checksum is checked; a mismatch rebuilds),
   reopens the connection so that SQLite cannot serve a cached page, and sets
   `confirmed_at = s`. A segment whose epoch is lower than the chain's ends the
   run if only a tail follows (a zombie of a fenced owner), and rebuilds
   otherwise, as a restore would fail (R2, R3).
4. If the lease has `moved_to`, or the placement now gives another location,
   the replica stops (`reason: :moved`); a moved cell's old chain would look
   fresh for as long as it exists. If the cell is awake on this node it stops
   too (`:owner_here`): the node serves it from the cell.

The fetch runs in a task, so reads are served from the file while the store
answers; the process only does local work (apply, reopen, read). Segments that
landed but were not yet acknowledged are applied too: a segment on the chain
stays on the chain (R3), so the replica never shows a state that later
disappears.

**Reads.** `Alambique.query/5` with `{:bounded, ms}` runs on the calling node
(`Alambique.Replica.Read`): the owner path if the instance has no `:replicas`
option or the cell is awake here; the replica if there is one for the cell's
location; otherwise the owner, and a replica is requested if the cell's switch
is on. The owner path is the existing `{:query, sql, params}` request. A read
in the replica runs inside the replica process (one statement at a time), by
`Db.read_within/5`: the read-only checks of `Db.read/4` (`query_only` and the
authorizer of the native library) and an interrupt at the caller's
`:timeout`. `execute/5` and `transaction/4` have no way to reach a
replica and refuse the option (`{:error, :bad_option}`).

**The switch.** The object `<cell prefix>/replicas` (written by
`Alambique.replicas/3`) says that nodes may keep replicas of the cell. It sits
beside `frozen` and `trashed` in the cell's prefix; the listing of a cell drops
keys that are not segments (§4), so it is not taken for one, and the cleanup
of a moved cell deletes it with the other fixed keys (§18). A node reads it
with one `HEAD` when a bounded read finds no replica, and remembers the answer,
positive or negative, for `marker_ttl_ms`; a store error counts as off and is
not remembered. A replica reads it again every `marker_ttl_ms` with a refresh and
stops when it is gone, so turning the switch off takes at most `marker_ttl_ms`
plus a poll (`poll_max_ms` for an idle replica) plus the refresh's own time. A node that sees the marker on starts
the replica for the next read: the read that found the switch is answered by the
owner. A move to another location (§18) does not carry the switch: the cleanup
of the old location deletes it, and the host turns it on again for the new one.

**Limits.** Admission goes through one process per node
(`Alambique.Replica.Manager`), so the limits hold whatever the number of
readers. `max_cells` bounds the replicas on the node; at the limit the replica
read longest ago is stopped to make room if nobody read it for
`marker_ttl_ms`, and otherwise the new cell is refused (its reads go to the
owner) for `marker_ttl_ms`. `max_bytes` bounds the sum of the database files: a
build reserves `commit * page_size` of the head before it downloads anything,
and a refresh reserves the size its last segment announces before it applies
it, so the limit is never passed, not even while a build runs. A replica that
does not fit is not built (or, when it grew, stops with `:max_bytes`), and the
cell is refused for `marker_ttl_ms`. A replica nobody read for `idle_ms` stops
and removes its file.

**Costs.** Three requests per poll and replica (`LIST` of level 0, `LIST` of the
snapshot level, `GET` of the lease), one `GET` per new segment, one `HEAD` per
`marker_ttl_ms`: with the defaults about 12 requests per second per replica
while it is read, and 0.6 once it has idled for 20 s (`poll_max_ms`, 5 s), and
each node that reads the cell pays them again. Measured with `poll_ms: 20`
over 1.2 s, starting 0.4 s after the last read: 177 store calls at a fixed
interval, 22 with the backoff (`test/replica_test.exs`). Slowing the poll of an
idle replica does not change the bound of a read: the first read after the idle
time is stale, waits for one refresh (`on_stale: :refresh`) or goes to the
owner, and the replica polls at `poll_ms` again. Local work per
change: one reopen of the connection, which empties SQLite's page cache of the
replica (at most `1000 / poll_ms` times per second, and only when something
changed). Memory: `cache_kib` plus the process, and a replica's copy of the
database on the disk of each node that reads the cell. The build is a full
restore: its time and memory are those of a wake (§6, §16) without the lease.

**Not done.** The owner does not push to replicas, because a push could not
carry the guarantee and would need the owner to know its replicas; if polling
turns out too costly, a push can later be a hint that starts a refresh early
without changing any guarantee. The change feed cannot rebuild DDL or page
state, and `Query.Cache` copies the file on every refresh, which is right for
reports and too expensive every few hundred milliseconds. A zombie that sits at
the head's next TXID is fetched again on every poll until its owner overwrites it
(the sketch of this design remembered it by etag).

**Verification.** `test/replica_properties_test.exs`: R-bound (random writes,
store latency, kills, takeovers, rewinds and reads with bounds from 1 ms; the
property was checked to fail when `confirmed_at` is taken after the fetch, and
when the staleness check is removed), R-readonly (random statements, including
`ATTACH`, `PRAGMA` and `VACUUM INTO`, change neither the file nor the store),
R-bounds (more cells and bytes than the limits) and R-switch.
`test/replica_test.exs` covers the tail and the zombie of a fenced owner. In
one VM a second instance cannot forward to the owner, so those tests tell the
side that answered from the error of the owner path; the owner fallback itself
is exercised by `test/replica_cluster_test.exs`.

**Telemetry.** `[:alambique, :replica, :read]`, `:build`, `:refresh` and
`:stop` (telemetry.md).

**Limit: a replica is built in full and can be older than the owner.** A
replica downloads the whole database before it serves a read, so the first
bounded reads of a node go to the owner, and a large cell costs a full restore
and a copy on every node that reads it (the lazy restore of §16 is not used). A
caller that alternates between the owner and a replica can see time go back by
up to the bound, and `{:bounded, 0}` is not accepted. A zombie file at the
head's next TXID is fetched again at every poll until the new owner overwrites
it. Moving a cell to another location (`Alambique.relocate/4`) does not carry
its replica switch: the cleanup of the old location deletes it, and replicas
stay off at the new location. Use `consistency: :strong` (or `verify_lease:
true`) for reads that must not go back in time, keep replicas for cells whose
size fits `max_bytes`, and call `Alambique.replicas/3` again after moving a
cell.

## 22. Node registry

Nodes of one deployment need to find each other, and the one thing they all
share is the store. With the `registry` option each node writes a small object
under `<prefix>/nodes/` and reads the others', so a node that starts learns
who is there without a seed list, a DNS name or a service of its own.
`Alambique.peers/1` returns them. The registry is discovery only: it decides
nothing about who owns a cell (that is the lease and its fencing, §5), and
Alambique never calls `Node.connect/1` with what it reads. The application
decides what `advertise` means and whether to connect.

**Key and entry.** One object per node id, `<prefix>/nodes/<node_id>`, in the
first location of the placement by id (§18). It sits beside `cells/`, never
under it, so no listing of the library reaches it: the listings of
one cell (`Replication.Objects`: retention, compaction, cleanup and replicas,
under `<prefix>/cells/<cell>/`), the cell listing of cross-cell queries
(`<prefix>/cells/`), the schema index that scrub and the sweeper read
(`<prefix>/schema/`), the feed (`<prefix>/feed/<shard>/`), the trash
(`<prefix>/trash/`) and the moved markers (`<prefix>/moved/`) all have a
longer prefix. No code lists `<prefix>/` itself; `test/nodes_test.exs` checks that the key is outside each
of those prefixes. The JSON body:

| Field | Meaning |
| --- | --- |
| `node_id`, `incarnation` | The instance: the option `node_id` and the random id drawn at each start (§5). |
| `advertise` | An opaque string for the application, such as `"10.0.0.7:4369"`. |
| `meta` | A JSON map for the application. |
| `started_at_ms` | When this registration was written. |
| `expires_at_ms` | When the entry stops being live, by the writer's clock. |
| `seq`, `token` | `seq` grows with every write, so two writes never have the same bytes (S3 etags are content hashes). `token` identifies one registration. |
| `released` | Set by a node that leaves. |

**Rules** (`Alambique.Nodes`, one conditional write each, built on the same
helpers as the lease: `Alambique.Conditional`).

- `register` creates the key with `If-None-Match`. An existing entry is taken
  over with `If-Match` on its etag when it is released, when it has expired
  (`now > expires_at_ms + skew_margin_ms` by the reader's clock, as for a
  lease), or when it is another incarnation of the same node id: a restart
  that is faster than the TTL. `takeover: :free` skips the last case.
- `renew` rewrites the entry with `If-Match`. If the key now holds another
  write it answers `{:error, :lost}`; a failed write whose answer was lost is
  read back and adopted.
- `deregister` writes `released: true` with `If-Match`, so the node leaves
  `list` at once. It is not a delete: the store has no conditional delete, and
  an unconditional one could remove the entry of a node that took the id in
  between.
- `list` pages through `<prefix>/nodes/`, reads each entry and hides the
  released and expired ones. An object that does not decode counts as a
  released entry.
- `sweep` deletes released and expired entries, reading each again before the
  delete. Because the delete is unconditional, a node that registers in the
  instant between that read and the delete loses its entry; its next `renew`
  answers `:lost` and it registers again. A live entry is never selected.

`ttl_ms` must be greater than twice `skew_margin_ms`, as for the lease, and each
request is bounded by `ttl_ms - skew_margin_ms`.

**The process.** `Alambique.NodeRegistry` is the last child of the instance
tree, like the compactor: a crash restarts nothing else, and it stops first,
so a stopping node leaves the registry before its cells hibernate. It
registers at start, renews every `renew_ms` and lists the peers every
`refresh_ms`; `Alambique.peers/1` reads an ETS table the process owns, so it
sends no message and never waits for the store. A drain (`Alambique.drain/2`)
deregisters at once, and the node stays out while it drains; `join/1` puts it
back within `renew_ms`. A stop deregisters in `terminate/2`, bounded by one
request. A store failure in any round is logged and emitted as
`[:alambique, :registry, :error]`; the instance keeps running and the last
peers stay readable. Requests go through the breaker of the store like any
other (§3).

**Losing the entry.** If `renew` finds another write (someone swept the key, or
another instance with the same node id registered), the node registers again
at once with `takeover: :free` and the same incarnation: it is the same
instance, so its cells and leases are unchanged. A live entry of another
incarnation is not taken back: two live instances with one node id would
otherwise take the key from each other at every round. The node waits, quietly,
until that entry is released or expires.

**Node id.** The option `node_id`, by default `Atom.to_string(node())`. It
must be unique among the nodes that should see each other, and stable across a
restart if the new run should replace the old entry at once.

**Trust.** Whoever can write under `<prefix>/nodes/` decides which addresses
the nodes of the deployment find, and so which hosts an application connects
to if it follows `advertise`. Give the credentials that can write there only
to the nodes, as for `cells/` (§3.1). The registry carries no secrets of its
own; keep them out of `meta`.

**Telemetry.** `[:alambique, :nodes, :registered | :renewed | :expired | :lost]`
and `[:alambique, :registry, :peers_changed | :error]` (telemetry.md).

**Verification.** `test/nodes_properties_test.exs` checks, with StreamData over
random schedules and a manual clock, that an expired entry leaves the listing,
that one node id is never live under two incarnations, and that `deregister`
removes an entry from the listing at once. `test/node_registry_test.exs` runs
the process: start, renew, loss and re-registration, drain, stop, store
failure, and `peers_changed` between two instances on one store.
