# How to load-test one node as a service

**The question:** how many commits, awake cells, wakes and cross-cell queries
can one Alambique node carry in a container with a fixed CPU and memory, and
what does each unit cost?

This guide stands alone. It is for contributors and for anyone sizing a node.
For a memory estimate without running anything, read
[how-to-size-a-node.md](how-to-size-a-node.md).

## What it measures

One node, as a service, in a container started with `docker run --cpus
--memory` (swap equal to memory). Two scenarios:

| Scenario | CPU | Memory |
| --- | --- | --- |
| A | 1 | 512 MB |
| B | 2 | 2 GB |

| Measure | Ramp | A step is bad when | Cost reported per unit |
| --- | --- | --- | --- |
| `commits` | commits per second over 64 cells, variants `small` (120-byte row) and `large` (75 KB blob) | p99 ack over 250 ms, or any error | CPU per 1000 commits/s, store bytes and trips per commit |
| `cells` | cells awake, variants default `cache_kib` (512) and `cache256` | process memory at 90% of the limit | MB per awake cell (slope), fixed MB (intercept) |
| `wakes` | wakes per second of distinct sleeping cells | p95 wake (first read) over 500 ms, or any error | CPU and store trips per wake |
| `query` | `Alambique.Query.all/5` per second over 20 of 200 cells | p99 over 1 s, or any error | CPU per query, dirty IO pool use |

Every step also fails when process memory reaches the limit.

How it is built, and why:

- **Store.** SeaweedFS 4.48 in its own container with `--tmpfs /data`, on a
  free port, outside the service limit. The service adds **25 ms to every store
  request** (`store.latency_ms`) with a delaying store around
  `Alambique.Store.S3`. A sleep costs no CPU, needs no root (`tc netem` does)
  and is exact; the real request still crosses the loopback. The wrapper also
  counts requests and bytes, which are the store cost per operation.
- **Service.** A `mix release` of the repository (`load/lib`, built with
  `MIX_ENV=load`) in an image built at the versions pinned in `mise.toml`. It
  runs with `+Mdai max +MMmcs 0`. The BEAM sees the container's CPUs
  (`schedulers_online` is 1 in scenario A).
- **Driver.** A separate BEAM on the host, outside the limits, that calls the
  service over distribution (`--network host`, a random cookie per case). Its
  CPU is not counted. The service builds the payloads, so the driver sends a
  cell id, not 75 KB.
- **Memory.** Process memory is the container's `memory.current` minus the
  `shmem` of `memory.stat`. The service's data directory is a tmpfs, whose
  pages are shared memory and counted apart. Because the kernel still charges
  them to the container, the cgroup limit is the scenario limit plus the tmpfs
  (`scenarios.<s>.data_tmpfs`, default 1g); the criteria use the scenario
  limit. CPU is the delta of `usage_usec` in `cpu.stat`.
- **Open loop.** Operations are due at fixed times (Poisson arrivals by
  default, or `fixed`). Latency is measured from the due time, so a slow
  service cannot hide its queue. At most 5 000 operations run at once; a due
  operation over the cap is dropped.

## A step, a ramp, and what the verdicts mean

A ramp runs fixed steps (180 s) of increasing level until one is bad, then
refines by bisection between the last good and the first bad level (2 steps),
then repeats the last good level once to confirm it. `cells` does not refine or
repeat: the cells stay awake, and the answer is the memory fit. These are the
defaults of `[measures.<m>.steps]` in the round file (see "The round file").

A step ends in one of:

- **good**, **bad** (a criterion broke), or
- **invalid**: the step does not say whether the service carries the level, so
  it is not a failure. The ramp stops. The reasons: fewer than 95% of the
  planned operations completed (`delivery`); no operation was planned
  (`no_operations`: a step with no operations is never good); the harness ran
  out of room (`data_full`, `store_full`); a preparation failed
  (`prepare_failed`); the service did not cool down (`no_cooldown`); the
  host was not quiet (`host_busy`); the service container is gone
  (`service_down`); the step ran past `round.step_timeout_s` (`timeout`).
- **repeat**: the host was busy, or the service did not cool down (below). The
  step runs again, up to 5 attempts (`judging.attempts`), then is
  invalid. The attempts that ran stay on disk. A repeat redoes the whole work
  of the step and measures it. For `cells`, the cells the disturbed attempt
  woke are put to sleep first, so the repeat wakes them again and the step
  does not pass with no operations.

Order of decision: why the step ended (the harness's own stops are invalid; a
stop on memory is bad even at the first second, and only a container that is
gone is `service_down`), then host busy, swap, memory, no operations, store
full, delivery, errors, `busy`, latency.

A step is also marked **suspect** (not invalid, the verdict stands) when the
service's CPU was throttled for more than 10% of the step
(`judging.suspect_throttle.max_pct`): the quota cut it as much as the
load did. Each step's JSON has the `throttled_usec` delta in `cgroup` and
`suspect`; `summary.md` lists the suspect steps.

**Busy refusals.** When `sql: [max_concurrent_statements:]` slots are taken,
the library answers a read with `{:error, :busy}` by design. The driver counts
those apart from other errors (`delivery.busy` and `delivery.busy_ratio` in the
step JSON, a `busy %` column in the summary, for every measure). A step is bad
with reason `busy` only when they are more than 1%
(`judging.busy.max_pct`) of the completed operations; other errors
keep their rule (`judging.errors.max_pct`).

**Cooldown.** Before every step, and before every repeat, the driver waits
until the service has recovered from the previous step: no operation of the
driver still running on the service, and (except for `cells`, whose awake cells
keep memory by design) process memory below 80% of the scenario limit
(`judging.cooldown.max_mem_pct`). It waits up to 300 s
(`judging.cooldown.max_wait_s`); if the service has not cooled down the
attempt is repeated like a busy host, and after the attempts the step is
invalid with `no_cooldown`. Without it a step that follows an overloaded one
starts on the backlog and memory of the other. `cooldown_s` in each step's JSON
is the time waited (the attempts included). A step that stops on memory is bad
with reason `memory`; it is `service_down` only when the container is gone.

**Room of the harness.** Two spaces can fill without the service being at
fault; both end the ramp with an invalid step.

- The service's data tmpfs (`scenarios.<s>.data_tmpfs`). The `wakes` ramp keeps
  every cell it woke, asleep, so its data grows with the level. Before it
  prepares a level it estimates the data: the cells it still has to create
  times the bytes one asleep cell took on the tmpfs (measured after the
  previous level; 160 000 bytes before any, `workload.cell_bytes`). If
  the data in use plus the estimate is over 90% of the tmpfs
  (`judging.data_full.estimate_pct`), the level is invalid `data_full`
  and is not run: this is a limit of the harness, not of the service. A
  preparation that fails is invalid `data_full` when the service's shared memory
  is at 95% of the tmpfs or more (`judging.data_full.full_pct`), else
  `prepare_failed`; the first error is in the step's `extra.prepare_error`.
  The summary is always written. Use a shorter `measures.wakes.steps.duration_s`
  (fewer fresh cells per level) or a larger `scenarios.<s>.data_tmpfs`.
- The store. The service never deletes during a case (the deletion grace is 24
  hours, the library default, and it is not changed because that would change
  the cost measured), so the store only grows. Every second the driver reads the
  store container's shared memory (its tmpfs) or, with
  `store.backing = "disk"`, the free space of the store directory's file
  system (`df`). At 95% of `store.tmpfs_size`
  (`judging.store_full.max_pct`), or with less than 1 GiB free on disk
  (`judging.store_full.min_free_mb`), the step stops and is invalid
  `store_full` (not bad), the ramp stops, and the driver says to run the case
  again with `store.backing = "disk"`. Errors that say "no space" are
  treated the same. So is a store HTTP 5xx in a step whose store tmpfs reached
  85% of `store.tmpfs_size` (`judging.store_full.error_pct`): a
  store that nearly full answers 500 `InternalError` without saying why, and
  the step would otherwise be judged `bad (errors)`; on disk, the same applies
  with less than 1 GiB free. The step JSON has `store` (`used_mb_end`, `used_mb_max`,
  and `free_mb_min` on disk; on disk, used is the fall of free space since the
  case started) and the summary a `store MB` column.

**Swap guard.** The service container runs with `--memory-swap` equal to
`--memory`, so it cannot swap. The store container has no memory limit, and the
driver is a process on the host. On a host with swap (a zram swap, for
example) idle processes drift into swap, so the host's total is not a criterion
by itself; what matters is whether the measured parts are swapped. Every second
the driver reads the host's swap in use (`SwapTotal - SwapFree`), its own
`VmSwap`, and `memory.swap.current` of the store and the service cgroups (null
when a file is missing). A step is repeated after the quiet wait, like a busy
host, up to the attempts, and is then invalid with reason `swap`, when:

- the driver's `VmSwap` at its maximum during the step is above
  `judging.driver_swap.max_mb` (default 16, absolute);
- the store's swap grew during the step (maximum minus its value at the start)
  by more than `judging.store_swap.max_growth_mb` (default 0);
- the host's swap in use at its maximum during the step is above
  `judging.host_swap.max_mb` (default 512, absolute).

A value exactly at its limit is valid. Any swap of the service makes the step
invalid with `swap` at once, with no repeat. Each step's JSON has a `swap`
object: for `host_mb`, `driver_mb`, `store_mb` and `service_mb`, the `start`,
`max`, `end` and `growth` (max minus start), in MB. The summary table has the
host swap maximum of each step.

**Host guard.** Net load is the host's 1-minute load average minus the same
average of the tasks the test keeps runnable (state R or D in the two
containers and the driver). The kernel averages that count every 5 s with a
60 s decay; the driver applies the same decay to its own count, sampled every
5 s. A step whose net load at any sample is above 6
(`judging.host_load.max`) is repeated. `MemAvailable` below 6 GiB
(`judging.host_memory.min_available_gib`) stops the run cleanly: the containers are
removed and the results so far stay. A case also waits up to 10 minutes before
it starts for load average at most 8 and that much free memory.

**Waiting for a quiet host.** Before every step, and before every repeat, the
driver waits until the net load is at most the limit in every sample of a
settle window (30 s, so 6 samples, `judging.host_load.quiet_window_s`). One quiet sample
is not enough: the 1-minute average lags. The service stays up and idle
meanwhile, so the net load is close to the plain 1-minute load average. The
driver prints one line when it starts waiting and one when it goes on. If the
host is not quiet after 30 minutes (`judging.host_load.quiet_max_wait_s`), the
step is invalid with `host_busy` and the ramp stops; that is not a failure of
the service. Low memory while waiting stops the run as above. Each step's JSON
has `quiet_wait_s`, the seconds waited for it (the settle window included, the
earlier attempts included), and the summary adds the total of the case.

## Run it

Prerequisites: Docker, `mise` with the pinned tools, `python3` 3.11 or newer
(for the round file), a quiet host with 6 GiB free. A full run is long; see
the estimate below.

On a fresh clone nothing else is needed: the first case fetches the Mix
dependencies and compiles the driver (`MIX_ENV=test`) before it starts any
container, which takes a few minutes with the Rust toolchain.

The load test has one entry, and a **round file** says everything a round does:
which cases run and in what order, the CPU and memory of each scenario, where
the store lives, the steps of each ramp, the limit of every criterion, and where
the results go. Changing a round is changing a file, never the code.

1. Copy the example, which lists every key at its default with a comment, and
   keep the keys you change (a missing key keeps its default):

   ```sh
   cp load/rounds/example.toml load/rounds/mine.toml
   ```

2. Check the file and read the plan. Nothing starts: no container, no build.
   An invalid file is refused with every problem, each naming its key:

   ```sh
   mise run load -- --config load/rounds/mine.toml --dry-run
   ```

   ```text
   round mine: 2 cases, results in results/load/mine
   skip-done on
   timeouts: 1800 s a step, 20400 s a case (computed), round none
   1. A/commits/small: 1 CPU, 512 MB, data tmpfs 1024 MB; store on tmpfs 4g, +25 ms
      case limit 20400 s; steps bisect [50, 100, 200, 400, 800, 1600, 3200] then 2 refinements, 1 confirmation, 180 s each, at most 10
      judged: latency, memory, errors, busy, delivery, store_full, data_full, driver_swap, store_swap, host_swap, host_load, host_memory, host_start, cooldown, suspect_throttle
   2. A/wakes: 1 CPU, 512 MB, data tmpfs 1024 MB; store on tmpfs 4g, +25 ms
   ...
   at most 1.0 h of steps, without waits and preparation
   ```

3. Run it (this also builds the image, `mise run load:image`, which is quick
   when its layers are cached):

   ```sh
   mise run load -- --config load/rounds/mine.toml
   ```

4. Read the results under `results/load/<name>/` (below).

The round file is the only argument that selects work: the old positional form
(`mise run load -- A commits`) is gone. `load/rounds/example.toml` is the
defaults, so `mise run load -- --config load/rounds/example.toml` runs scenario
A with every measure, as `mise run load -- A all` did.

To smoke-test without a long run, shorten the steps in a copy of the file:

```toml
[round]
cases = ["A/commits/small"]
[measures.commits.steps]
kind = "ladder"
values = [10]
duration_s = 15
refine = 0
```

The harness starts only containers it names itself, removes them by the
recorded id when it ends, fails or is interrupted, and never kills by name.

## The round file

`load/rounds/example.toml` documents every key. The tables:

| Table | What it says |
| --- | --- |
| `[round]` | `cases`: the ordered list `"scenario/measure[/variant]"` to run (a measure without a variant runs all of its variants); when empty, `scenarios` x `measures` run, as before. `step_timeout_s` and `timeout_s`: time limits (below). |
| `[output]` | `dir` and `name` of the results directory. |
| `[host]` | `ssh`, `repo`, `results`, `timeout_s` for `--remote`; `cgroup_root`. |
| `[scenarios.A]`, `[scenarios.B]` | `cpus`, `memory` and `data_tmpfs` of the service container. |
| `[store]` | `backing` (`tmpfs` or `disk`), `tmpfs_size`, `dir`, `latency_ms` added to every store request, `image`. |
| `[service]` | `image`, `erl_aflags`, `wal_checkpoint_bytes`, `idle_timeout_ms`, `max_cells`, `cache_kib`, `opts`. |
| `[generator]` | `arrivals`, `max_outstanding`, `drain_s`, `timeout_s` of one operation. |
| `[report]` | `fit_min_r2`, `fit_min_span` of the memory slope. |
| `[measures.<m>]` | `variants`; `steps` (the plan); `workload` (what the measure does); `store`, `service` and `judging` overrides for this measure; `variant.<v>.steps`, `.store`, `.service` overrides for one variant. |
| `[judging.<criterion>]` | each criterion has `enabled` and its limit. |

**Step plan.** `kind = "bisect"` grows from `start` by `factor` up to `max`
(each level above the last, rounded to a whole number), then after the first
bad step bisects `refine` times between the last good and the bad level, then
repeats the last good level once when `confirm` is true. `kind = "ladder"` runs
the growing `values` instead, with the same refinement and confirmation.
`duration_s` is the seconds of load of a step. `commits`, `wakes` and `query`
default to bisecting from 50, 5 and 1 (the `large` variant of `commits` from
5) by 2 up to 3200, 320 and 64 (320 for `large`); `cells` goes from 500 to
32 000 with no refinement and no confirmation.

**Overrides by measure and variant.** The `store`, `service` and `judging`
tables of the top level apply to every case. `[measures.<m>.store]` (and
`.service`, `.judging.<criterion>`) change keys for that measure only, and
`[measures.<m>.variant.<v>.store]` (and `.service`, `.steps`) for one variant,
and enter the hash of those cases only. The defaults use this: `cells` keeps
its awake cells for the whole ramp (`service.idle_timeout_ms`, `max_cells`), judges
memory at 90% and has no latency criterion; `wakes` lets cells sleep after 5 s
and judges the p95; `cache256` is a `service.cache_kib` of 256. A `cells`
or `wakes` variant can be added with any lowercase name (declare each name
in `variants` and give it a table, which may be empty):

```toml
[measures.cells]
variants = ["default", "cache1024"]
[measures.cells.variant.cache1024.service]
cache_kib = 1024
```

**Judging.** A criterion with `enabled = false` is not judged. Each limit is
exclusive. The table below says what each does; "BAD" ends the ramp with a
verdict on the level, "INVALID" means the harness could not tell, "REPEAT"
runs the step again (see the next section).

| Criterion | Limit keys | Effect |
| --- | --- | --- |
| `latency` | `percentile`, `max_ms` | BAD above it (per measure: p99 250 ms commits, p95 500 ms wakes, p99 1000 ms query, off for cells) |
| `memory` | `max_pct` | BAD at this share of the scenario memory (100; 90 for cells) |
| `errors`, `busy` | `max_pct` | BAD above this share of the completed operations (0 and 1) |
| `delivery` | `min_ratio` | INVALID below this share of the planned operations completed (0.95) |
| `store_full` | `max_pct`, `error_pct`, `min_free_mb` | INVALID: store tmpfs share, share at which a store 5xx counts as full, free MB on disk |
| `data_full` | `estimate_pct`, `full_pct` | INVALID (wakes): the data tmpfs |
| `driver_swap`, `store_swap`, `host_swap` | `max_mb`, `max_growth_mb`, `max_mb` | REPEAT, then INVALID |
| `host_load` | `max`, `quiet_window_s`, `quiet_max_wait_s` | REPEAT above it; wait for quiet before each step |
| `host_memory` | `min_available_gib` | the round stops cleanly below it |
| `host_start` | `max_load`, `max_wait_s` | a case waits for this load average before it starts |
| `cooldown` | `max_mem_pct`, `max_wait_s` | wait for the service to cool down before each step |
| `suspect_throttle` | `max_pct` | marks the step suspect |

`[judging] attempts` is how many times a disturbed step runs before it is
invalid. The service's own swap (any amount makes the step invalid) is not a
setting: the container cannot swap.

**Time limits.** `round.step_timeout_s` (default 1800) bounds one step's work,
its preparation and its measurement; a step that runs longer is stopped and is
INVALID with reason `timeout`, and the ramp ends there as for any invalid
step. The default is above the longest step of the default ramps: the largest
`cells` step fills 32 000 cells at 100 a second (320 s) and holds 60 s, and
`wakes` prepares up to 57 600 cells before its 180 s. The waits for a quiet
host and for the cooldown have their own limits and are not counted.
Every case's driver runs under `timeout --kill-after=30`. `round.case_timeout_s`
is its bound; 0, the default, computes it from the plan of the case: the most
steps it can run (levels, refinements, confirmation) times `step_timeout_s`,
plus `judging.host_start.max_wait_s` and 1800 s of preparation. `--dry-run`
shows it (for the default `commits` ramp, 10 x 1800 + 600 + 1800 = 20 400 s).
`round.timeout_s` bounds the whole round (0, the default, sets none); when
set, a case gets the smaller of its bound and the time left. A case stopped by
either bound has its containers removed by the usual cleanup, and the round
ends with status 6.

## Environment overrides

An `ALAMBIQUE_LOAD_*` variable overrides one key for a single run, and **wins
over the file**: `ALAMBIQUE_LOAD_STEP_S=15 mise run load -- --config ...` runs
15 s steps whatever the file says. A variable that sets a key of `steps`,
`store`, `service` or `judging` sets it for every measure and variant, so it
wins over a measure's own value too. It is checked like the file: a value the
key does not take is refused by name. The variables in effect are printed in
the command that every summary records (`Reproduce:`), so the run can be
repeated, and they enter the hash of the steps.

The file is the source of truth; use a variable for a one-off change, and put
a setting that stays in the file. Percentages are percentages (`MAX_BUSY_PCT=1`
is 1%), sizes `DATA_MB` and `STORE_MB` are megabytes.

The variables, and the key each one overrides:

| Variable | Overrides | Default |
| --- | --- | --- |
| `ALAMBIQUE_LOAD_STEP_S` | `measures.<m>.steps.duration_s` | `180.0` |
| `ALAMBIQUE_LOAD_REFINE` | `measures.<m>.steps.refine` | `2` |
| `ALAMBIQUE_LOAD_WAKES_STEP_S` | `measures.wakes.steps.duration_s` | `180.0` |
| `ALAMBIQUE_LOAD_ARRIVALS` | `generator.arrivals` | `poisson` |
| `ALAMBIQUE_LOAD_MAX_OUTSTANDING` | `generator.max_outstanding` | `5000` |
| `ALAMBIQUE_LOAD_DRAIN_S` | `generator.drain_s` | `20.0` |
| `ALAMBIQUE_LOAD_OP_TIMEOUT_S` | `generator.timeout_s` | `15.0` |
| `ALAMBIQUE_LOAD_FIT_MIN_R2` | `report.fit_min_r2` | `0.8` |
| `ALAMBIQUE_LOAD_FIT_MIN_SPAN` | `report.fit_min_span` | `2.0` |
| `ALAMBIQUE_LOAD_ATTEMPTS` | `judging.attempts` | `5` |
| `ALAMBIQUE_LOAD_DELIVERY_MIN` | `judging.delivery.min_ratio` | `0.95` |
| `ALAMBIQUE_LOAD_MAX_ERROR_PCT` | `judging.errors.max_pct` | `0.0` |
| `ALAMBIQUE_LOAD_MAX_BUSY_PCT` | `judging.busy.max_pct` | `1.0` |
| `ALAMBIQUE_LOAD_COOLDOWN_MEM_PCT` | `judging.cooldown.max_mem_pct` | `80.0` |
| `ALAMBIQUE_LOAD_COOLDOWN_MAX_S` | `judging.cooldown.max_wait_s` | `300.0` |
| `ALAMBIQUE_LOAD_SUSPECT_THROTTLE_PCT` | `judging.suspect_throttle.max_pct` | `10.0` |
| `ALAMBIQUE_LOAD_MAX_NET_LOAD` | `judging.host_load.max` | `6.0` |
| `ALAMBIQUE_LOAD_SETTLE_S` | `judging.host_load.quiet_window_s` | `30.0` |
| `ALAMBIQUE_LOAD_WAIT_QUIET_STEP_S` | `judging.host_load.quiet_max_wait_s` | `1800.0` |
| `ALAMBIQUE_LOAD_MAX_DRIVER_SWAP_MB` | `judging.driver_swap.max_mb` | `16.0` |
| `ALAMBIQUE_LOAD_MAX_STORE_SWAP_MB` | `judging.store_swap.max_growth_mb` | `0.0` |
| `ALAMBIQUE_LOAD_MAX_HOST_SWAP_MB` | `judging.host_swap.max_mb` | `512.0` |
| `ALAMBIQUE_LOAD_MIN_AVAILABLE_GIB` | `judging.host_memory.min_available_gib` | `6.0` |
| `ALAMBIQUE_LOAD_START_LOAD` | `judging.host_start.max_load` | `8.0` |
| `ALAMBIQUE_LOAD_WAIT_QUIET_S` | `judging.host_start.max_wait_s` | `600.0` |
| `ALAMBIQUE_LOAD_STORE_FULL_PCT` | `judging.store_full.max_pct` | `95.0` |
| `ALAMBIQUE_LOAD_STORE_ERROR_PCT` | `judging.store_full.error_pct` | `85.0` |
| `ALAMBIQUE_LOAD_STORE_MIN_FREE_MB` | `judging.store_full.min_free_mb` | `1024.0` |
| `ALAMBIQUE_LOAD_DATA_ESTIMATE_PCT` | `judging.data_full.estimate_pct` | `90.0` |
| `ALAMBIQUE_LOAD_DATA_FULL_PCT` | `judging.data_full.full_pct` | `95.0` |
| `ALAMBIQUE_LOAD_CELLS_MEM_PCT` | `measures.cells.judging.memory.max_pct` | `90.0` |
| `ALAMBIQUE_LOAD_COMMITS_P99_MS` | `measures.commits.judging.latency.max_ms` | `250.0` |
| `ALAMBIQUE_LOAD_WAKES_P95_MS` | `measures.wakes.judging.latency.max_ms` | `500.0` |
| `ALAMBIQUE_LOAD_QUERY_P99_MS` | `measures.query.judging.latency.max_ms` | `1000.0` |
| `ALAMBIQUE_LOAD_STORE_LATENCY_MS` | `store.latency_ms` | `25.0` |
| `ALAMBIQUE_LOAD_STORE_ON` | `store.backing` | `tmpfs` |
| `ALAMBIQUE_LOAD_STORE_DIR` | `store.dir` | library default |
| `ALAMBIQUE_LOAD_STORE_IMAGE` | `store.image` | `chrislusf/seaweedfs:4.48` |
| `ALAMBIQUE_LOAD_IMAGE` | `service.image` | `alambique-load:local` |
| `ALAMBIQUE_LOAD_ERL_AFLAGS` | `service.erl_aflags` | `+Mdai max +MMmcs 0` |
| `ALAMBIQUE_LOAD_WAL_CHECKPOINT_BYTES` | `service.wal_checkpoint_bytes` | `8388608` |
| `ALAMBIQUE_LOAD_IDLE_TIMEOUT_MS` | `service.idle_timeout_ms` | `0` |
| `ALAMBIQUE_LOAD_MAX_CELLS` | `service.max_cells` | `0` |
| `ALAMBIQUE_LOAD_CACHE_KIB` | `service.cache_kib` | `0` |
| `ALAMBIQUE_LOAD_OPTS` | `service.opts` | library default |
| `ALAMBIQUE_LOAD_COMMIT_CELLS` | `measures.commits.workload.cells` | `64` |
| `ALAMBIQUE_LOAD_COMMIT_ROWS` | `measures.commits.workload.rows` | `8` |
| `ALAMBIQUE_LOAD_CELLS_FILL_RATE` | `measures.cells.workload.fill_rate` | `100` |
| `ALAMBIQUE_LOAD_HOLD_S` | `measures.cells.workload.hold_s` | `60.0` |
| `ALAMBIQUE_LOAD_WAKE_MODE` | `measures.wakes.workload.mode` | `cold` |
| `ALAMBIQUE_LOAD_TAIL_S` | `measures.wakes.workload.tail_s` | `60.0` |
| `ALAMBIQUE_LOAD_CELL_BYTES` | `measures.wakes.workload.cell_bytes` | `160000` |
| `ALAMBIQUE_LOAD_QUERY_POPULATION` | `measures.query.workload.population` | `200` |
| `ALAMBIQUE_LOAD_QUERY_CELLS` | `measures.query.workload.cells_per_query` | `20` |
| `ALAMBIQUE_LOAD_CELL_ROWS` | `measures.wakes.workload.cell_rows` and `measures.query.workload.cell_rows` | `20` |
| `ALAMBIQUE_LOAD_ROW_BYTES` | `measures.wakes.workload.row_bytes` and `measures.query.workload.row_bytes` | `1500` |
| `ALAMBIQUE_LOAD_POPULATE_CONCURRENCY` | `measures.wakes.workload.populate_concurrency` and `measures.query.workload.populate_concurrency` | `64` |
| `ALAMBIQUE_LOAD_CGROUP_ROOT` | `host.cgroup_root` | `/sys/fs/cgroup/system.slice` |
| `ALAMBIQUE_LOAD_DATA_MB` | `scenarios.A.data_tmpfs` and `scenarios.B.data_tmpfs`, in MB | `1g` (A and B) |
| `ALAMBIQUE_LOAD_STORE_MB` | `store.tmpfs_size`, in MB | `4g` |
| `ALAMBIQUE_LOAD_LEVELS` | `measures.<m>.steps.kind = "ladder"` and `.values`, every measure | per measure |

`ALAMBIQUE_LOAD_NODE`, `_SERVICE_ID`, `_STORE_ID`, `_STORE_PATH` and
`_STORE_FSTYPE`, and the `ALAMBIQUE_LOAD_*` entries of the service container's
environment, are the wiring between `scripts/load-case` and the processes it
starts. They are set by the script, from the round file; do not set them.

## Rerun, skip done steps

Every step writes its own result when it ends, so a round that stopped (a host
shutdown, a timeout, an invalid step) loses only the step that was running.
Run the same command again: a step that already has a result is **not run
again**. It is read, its verdict feeds the ramp, and the ramp goes on from
where it stopped, because the bisection depends on the verdicts only.

```sh
mise run load -- --config load/rounds/mine.toml                  # skips done steps
mise run load -- --config load/rounds/mine.toml --no-skip-done   # runs every step again
```

`--skip-done` is the default. There is no service and no automatic resume:
you run the command again.

- A step is **done** when its result exists, was made with the same settings
  and its verdict is `good` or `bad`. An **invalid** step is never done: it
  runs again (the invalid verdict says the harness could not tell).
- The settings are the `config_hash` stored in each step: a SHA-256 of the
  resolved settings of that scenario, measure and variant (CPUs, memory, store,
  service, generator, step plan, workload, every criterion, and the
  `ALAMBIQUE_LOAD_*` overrides in effect), without `output.*` and `host.*`.
  Changing the plan of one measure changes the hash of its cases only.
- A results directory with a good or bad step made with **other settings** is
  refused before anything starts, with the file and both hashes. Use
  `--no-skip-done` to run it again over them (the old step files stay until
  the new ones replace them), or give the round another `output.name`.
- When every step of a case is done, the case starts no container: the driver
  writes its summary again from the results and the round goes on.
- A rerun needs a stable directory: set `output.name` (or `output.dir`). With
  neither, each run gets a time-stamped directory and skips nothing.
- `cells` keeps its awake cells in the service, which a rerun starts anew: the
  first step of `cells` that runs after skipped ones wakes all of its cells,
  from zero, in that one step.

## Results

```text
results/load/<name>/
  config.json                    the resolved config and the cases
  <scenario>/<measure>[/<variant>]/
    case.json                    the settings of the case, with its hash
    step-<level>.json            one per step, written whole or not at all
    step-<level>-2.json          the confirmation of that level
    step-<level>-attempt<N>.json an attempt the host disturbed
    summary.json, summary.md     the table of the case
    started-containers.txt       the containers the case started
```

For example `results/load/o022vm/A/commits/large/step-80.json` and
`results/load/o022vm/A/wakes/summary.md`. A step file holds the delivery
counts, latency percentiles, host readings, CPU, memory (max and mean), the
store counters, the native counters, the cost per operation and its
`config_hash`. `summary.md` ends with the command that reproduces the case.

## Dry run, remote host

`--dry-run` prints the plan and starts nothing: the cases in order, the
levels of each ramp, the criteria on and off, how many steps are already done,
and the longest the steps can take without waits and preparation.

With `[host] ssh` and `repo` in the file (and an `output.name`, so the results
can be found), the round can run on another machine, for example the VM that
has the load host's resources. Nothing runs there without a flag:

```sh
mise run load -- --config load/rounds/o022vm.toml --remote   # copy the file, start the round there, detached
mise run load -- --config load/rounds/o022vm.toml --fetch    # copy the results back to output.dir
```

`--remote` copies the file to `<results>/round.toml` on the host, starts
`scripts/load` there under `timeout --kill-after=60 <host.timeout_s>`, with its
log and PID in `<results>/load.log` and `load.pid`, and returns; it refuses to
start a second round while that PID is alive. `--fetch` copies the results
directory back (without the data of a store on disk), and says whether the
round is still running. Both use plain `ssh` and `tar`.

Without `host.ssh` the round runs where `mise run load` is called.

## Store on disk

By default the store's data is on a tmpfs of `store.tmpfs_size`. Use
`backing = "disk"` when that tmpfs would not fit in the host's memory, or when
a case filled it (see Troubleshooting), and read its result as "store on
disk". It can be for one measure or variant only:

```toml
[measures.commits.variant.large.store]
backing = "disk"
```

The store then writes to a fresh directory, bind mounted at `/data`:
`store.dir`, or `store/` in the case's results directory. The script creates
it (it fails if the directory exists) and writes its path to `store-dir.txt`.
`tmpfs_size` is not used on disk; the store is bounded by the disk, and the
criterion is `judging.store_full.min_free_mb`.

SeaweedFS runs as root, so its files may not be removable by you. At the end
the script removes the store container, empties the directory with a
throwaway container of the same SeaweedFS image (`find /d -mindepth 1
-delete`), and then removes the empty directory itself. Nothing else is
deleted and no `sudo` is used. If that fails, the script says so and leaves
the directory.

Disk is slower and more variable than a tmpfs, and the page cache competes
with the service for memory, so compare disk results with tmpfs results
knowingly. Each step's JSON has `store_on`, `summary.json` has `store_on` and
`store_fstype` (from `findmnt -no FSTYPE -T`), and when the store was on disk
the heading of `summary.md` says "store on disk (<fstype>)".

## A small VM

`load/rounds/o022vm.toml` is the round of order 022 on a VM with 4 vCPUs and
5.9 GB of RAM: the cases still to run, in order (`A/commits/small`,
`A/commits/large`, `B/commits/large`, `A/cells`, `B/cells`, `A/wakes`,
`B/wakes`), the settings that were approved for the VM, and the two `large`
cases with the store on disk:

```toml
[judging.host_memory]
min_available_gib = 1.0     # the default, 6, is above the VM's memory

[scenarios.A]
data_tmpfs = "768m"         # B the same

[store]
tmpfs_size = "1536m"
```

The VM has 11.8 GB of zram swap. The service container keeps `--memory-swap`
equal to `--memory`, so it never swaps. Idle processes of the VM drift into
swap, so the host's swap only counts above 512 MB; a step in which the driver
or the store goes to swap is repeated and then invalid (see the swap guard
above).
The driver runs on the same VM, so it shares the VM's CPUs with the service
and the store. Its CPU is not counted, but it can take CPU from them; the host
guard still repeats a step when the host is busy. If the store's tmpfs fills,
put the case's store on disk.

## What is configurable and what is not

Every number that decides what a round does or judges is a key of the round
file, with today's value as its default. What stays in the code is not a
setting of the round:

- the sampling and polling intervals (the samplers read once a second, the
  host every 5 s with the kernel's 60 s decay, the cooldown and quiet waits
  poll every second);
- the load itself: the payloads (a 120-byte row, a 75 KB blob, the SQL of each
  operation), the pool of 16 payloads, the chunks of 500 cells a populate call takes;
- the timeouts of the harness's own calls to the service (60 s, 120 s, 600 s),
  the 240 attempts to connect to the node and to find the store's bucket;
- the S3 bucket and keys of the load store (`alambique-load`, a fixed key
  pair for a store that is thrown away), the data directory `/data/cells`;
- that the service container never swaps, and that a step with no operations
  is never good;
- the order in which the criteria are judged (see above).

## What the numbers include

- A wake is the first read of a cell that is asleep (hibernated, its process
  stopped, its data written to the store). Each step wakes cells no earlier
  step woke: the population is `rate x step seconds` cells at the largest level
  so far, created and put to sleep before the step, outside its measurement
  (the data tmpfs holds all of them, see "Room of the harness"). In the default
  `cold` mode the local file of every cell of the step is removed first
  (`Ops.drop_local`), so the wake restores the cell from the store. Observed in
  the first step of `A wakes` in run o022b (937 wakes, cells of 20 rows of 1500
  bytes): 14.0 store trips per wake (1 list, 4 gets, 3 range reads, 2 heads and
  4 puts, the puts being the cell going back to sleep) and 149 KB read. With
  `workload.mode = "local"`
  the local files stay, and a wake reopens them and only checks the store. A
  wake is not a move of the cell to another node.
- The cost of a wake includes putting the cell back to sleep: the window ends
  when the woken cells sleep again.
- Store trips and bytes are everything the service sent in the window,
  background work included (compaction, the change feed), divided by the
  operations that succeeded.
- CPU per operation in milliseconds is the same number as cores per 1000
  operations per second.
- A cross-cell query is warm: one pass over the population fills the replica
  cache first.
- Memory per unit is a least-squares line of process memory against the level
  over the good steps (for `cells`, against the cells awake). The summary
  prints its R². It does not print the slope when R² is below 0.8, when the
  largest level is less than twice the smallest, or with fewer than three good
  steps; it says why instead.

## Estimate of a full run

Steps are 180 s plus the preparation each needs. One ramp is about 7 ramp
steps, 2 refinement steps and 1 confirmation, so 10 steps, about 30 to 40
minutes. Counting the variants, one scenario is `commits` 2 ramps, `cells` 2
ramps (about 7 steps of up to 90 s fill and 60 s hold, 15 minutes each),
`wakes` and `query` 1 each. That is about 3 hours per scenario, 3 to 4 with the
preparation (creating the cells of `wakes` and `query`), and 6 to 8 hours for
both. Ramps that stop early are shorter. Every step also waits at least the
settle window (30 s, 5 to 6 minutes per ramp). A busy host makes the run longer:
each wait lasts until the host is quiet, up to 30 minutes per step, and each
repeat waits again. This is an estimate. The round `o022vm` (9 cases, most of them stopped early) took 2 hours on the reference VM; see "Results on the reference VM".

## Results on the reference VM (order 022)

Two sets of numbers, both from the same VM. The round `o022vm` (2026-11-17) is
the official one. The earlier runs (2026-11-12) were cut by host shutdowns and
include the `query` measure, which `o022vm` did not run.

A level is **confirmed** when a repeat of it was also good, **not repeated**
when it was good once, and **not measured** when no step reached it.

### Setup

- A VM with 4 vCPU and 5.9 GB of RAM, 11.8 GB of zram swap, a 100 GB virtual
  disk (rotational, btrfs), cgroup v2 and Docker 29.7.2.
- The driver runs on the same VM, outside the measured container, and **shares
  the 4 vCPUs** with the service and the store. The numbers include that
  contention.
- The store is seaweedfs with +25 ms per request (`store.latency_ms`, the
  default), on a tmpfs, or on the VM's disk in the rows marked "store on disk".
  Those rows measure the VM's virtual disk, not the library.
- Settings that differ from the defaults of `load/rounds/example.toml`, in
  `o022vm.toml`: `judging.host_memory.min_available_gib = 1.0` (the default, 6,
  is above the VM's 5.9 GB), `scenarios.A.data_tmpfs` and
  `scenarios.B.data_tmpfs` = `"768m"`, `store.tmpfs_size = "1536m"` (the
  defaults, 1 GB and 4 GB, do not fit), and `store.backing = "disk"` for
  `commits` `large` (its `tmpfs_size = "12288m"` is not used on disk). No other key was changed; the first earlier run used
  `min_available_gib = 0.5`.
- Scenario A is 1 CPU and 512 MB, scenario B is 2 CPU and 2 GB. The service
  container's memory limit is the scenario's plus `data_tmpfs`, with swap equal to
  memory, and the memory criterion is `memory.current` minus the tmpfs.
- The swap of the service, the driver and the store was 0 in every step of the
  earlier runs. The host's swap was 270 to 291 MB in the first one (recorded
  only; the limit is 512) and 0 in the others.

### Round o022vm (2026-11-17)

How it ran:

- `mise run load -- --config load/rounds/o022vm.toml` on the VM, from commit
  0b57032, with no `ALAMBIQUE_LOAD_*` override. It ran from 22:17 to 00:18.
- 9 cases: `commits` small in A, `commits` large in A and B, `cells`
  (`default` and `cache256`) in A and B, `wakes` in A and B. The two `large`
  cases had the store on the VM's disk; the others had it on a tmpfs.
- Every case waited for a quiet host (88 to 208 s in all per case).

Capacity. "CPU" is milliseconds of CPU per operation, the same as cores per
1000 operations per second, taken at the last good level. "Memory" is the
process memory without the tmpfs, as a fit over the good steps when the
summary gives one.

| measure | A (1 CPU, 512 MB) | B (2 CPU, 2 GB) |
| --- | --- | --- |
| small commits/s, store on tmpfs | 300 good, its repeat bad; 200 was clean (p99 45 ms). CPU 3.9 ms at 50 to 200 (3.2 ms at 300). No memory fit (R² 0.79) | not run |
| 75 KB commits/s, store on disk | 5 good (p99 172 ms), its repeat bad (p99 609 ms). CPU 22.8 ms, 237 KB put per commit. No memory fit | 7 good (p99 82 ms), its repeat bad (p99 741 ms). CPU 26.8 ms, 263 KB put per commit. No memory fit |
| awake cells, `default` | 1000, not repeated; 2000 bad on memory. CPU 31.3 ms. No memory fit (2 good steps) | 4000, not repeated; 8000 bad on memory. CPU 45.3 ms. 127 MB + 0.235 MB per awake cell (R² 1.0) |
| awake cells, `cache256` | 1000, not repeated; 2000 bad on memory. CPU 31.1 ms. No memory fit | 4000, not repeated; 8000 bad on memory. CPU 45.5 ms. 129 MB + 0.234 MB per awake cell (R² 1.0) |
| cold wakes/s (restore from the store) | 20, not repeated (p95 177 ms, p99 184 ms); 40 invalid (`data_full`). CPU 17.2 ms. 129 MB + 3.045 MB per wake/s (R² 1.0) | 20, not repeated (p95 173 ms, p99 175 ms); 40 invalid (`data_full`). CPU 23.1 ms. 144 MB + 2.498 MB per wake/s (R² 0.96) |

Details that the table leaves out:

- Commits: the first step that broke the p99 limit was 400/s for small in A
  (p99 481 ms), 10/s for large in A (p99 299 ms) and 10/s for large in B
  (p99 288 ms). A large commit costs about 5 store trips.
- Cells: the 2000-cell step in A delivered 18.0% (`default`) and 26.8%
  (`cache256`) of its operations. The 8000-cell step in B delivered 97.4% and
  98.6%. The 256 KiB cache did not change the slope or the level that
  carried.
- Wakes: a wake costs 12 to 15 store trips and reads 144 KB.

### What these numbers do not say

- No last good level of a commits case is confirmed. Small commits in A and
  both `large` cases passed a level once and failed its repeat. Read
  them as the first good level, not as a confirmed ceiling.
- Cells and wakes were not repeated: the next step ended the case.
- The steps of small commits in A at 400/s and above, and of `cells` in A, were
  marked **suspect**: the service's CPU was throttled for more than 10% of
  them. For `cells` in A that is all three steps. Read those rows as limited
  by the CPU quota of the scenario, not as a clean measure of the library.
- The wakes ceiling is the harness, not the service: the data tmpfs (768 MB)
  would fill at 40/s, so the step was `invalid (data_full)`, not bad. The
  service may carry more.
- On disk the p99 rises while the rest stays flat. In A, large commits show
  p50 34 to 35 ms, p95 41 to 49 ms and CPU 21 to 23 ms per commit in every
  step, and a p99 of 172, 299, 2370, 545 and 609 ms. In B, p50 is 34 ms, p95 37
  to 38 ms and the p99 is 49, 288, 82, 289 and 741 ms. This is observed. The
  inference is that the delay comes from the virtual disk, not from the
  library, because the CPU per commit and the median do not move. The run did
  not isolate the disk, so it is not proved.
- `query` was not run in this round: its numbers are the earlier ones below.

### Earlier runs (2026-11-12)

The first full set of numbers, on the same VM. Runs were cut by host
shutdowns, so some cases are unconfirmed. The settings are those of "Setup",
with the first run at `min_available_gib = 0.5`. Compare `o022vm` first; these
rows remain for `query` and for the small commits in B.

#### Capacity

Cost per unit: "CPU" is milliseconds of CPU per operation, the same as cores
per 1000 operations per second; "memory" is the process memory without the
tmpfs.

| measure | A (1 CPU, 512 MB) | B (2 CPU, 2 GB) | cost per unit |
| --- | --- | --- | --- |
| small commits/s, store on tmpfs | 400 good but CPU-throttled, not confirmed; **200 clean** (p99 46 ms) | **800 good**, not repeated (p99 185 ms); the next level, 1600, hit `store_full` | A 2.5 to 3.9 ms CPU per commit, memory 116 MB + 0.248 MB per commit/s; B 2.5 to 5.2 ms |
| small commits/s, store on disk | not measured: the case was cut before it ended | **300 confirmed** (p99 196 to 197 ms) | B 4.5 ms CPU per commit, memory 108 MB + 0.418 MB per commit/s (R² 0.96) |
| 75 KB commits/s, store on tmpfs | 10 good, not repeated | 10 good, not repeated | 21 to 26 ms CPU per commit, about 236 KB sent per commit |
| 75 KB commits/s, store on disk | 7 good, its repeat bad | 5 bad (p99 319 ms), one step | same |
| awake cells | **1000** (2000 exceeds the memory criterion) | **8000** (4000 with a 256 KiB cache), not repeated | about 0.21 MB per awake cell, 140 MB fixed (B); 27 to 44 ms CPU per operation |
| cold wakes/s (restore from the store) | **20**, not repeated (p99 156 ms) | **20**, not repeated (p99 147 ms) | 15 to 23 ms CPU per wake, 3.1 to 3.3 MB per wake/s |
| `query` over 20 cells, queries/s (with `max_statement_share`) | **4 confirmed** (p99 184 ms, 0.9% `:busy`) | 8 good (p99 106 ms), its repeat bad with 1.5% `:busy`; **4 clean** | 63 to 71 ms CPU per query, about 40 store requests per query |

Stop criteria are the defaults: p99 above 250 ms, memory above its fraction
of the limit, errors or `:busy` above 1%, and the harness invalidations
(`data_full`, `store_full`, `host_busy`, `no_cooldown`).

#### What these numbers do not say

- Small commits in A have no clean confirmation: the repeat of 400/s ran with
  the store at 86% and failed on memory. Small commits in B at 800/s were not
  repeated, and the gap between 800 (tmpfs) and 300 (disk) is the disk's
  latency.
- 75 KB commits on tmpfs ended on a full store (HTTP 500 at 93% use) before
  they were confirmed. On disk the p99 passes 250 ms already at 5 to 10/s, so
  that row measures the virtual disk. Ten commits/s on tmpfs is the best
  number there is, without a repeat.
- Cells and wakes: none of the good levels was repeated, because the next level
  ended the case. The wakes ceiling is the harness's data tmpfs (768 MB; 40/s
  was `data_full`), not the service.
- Runs were made before a store HTTP 5xx near the limit counted as
  `store_full` (see "Room of the harness"), which is why those steps show as
  `bad (errors)` in the first run.

## Troubleshooting

- `host not quiet`: the case waited 10 minutes for the load average. Wait, or
  raise `judging.host_start.max_load` knowingly.
- A step invalid with `host_busy`: the host stayed above the net load limit for
  the whole wait (`judging.host_load.quiet_max_wait_s`) or every attempt was
  disturbed. Run again when the other work is done, or raise
  `judging.host_load.quiet_max_wait_s` or `judging.attempts`.
- Exit status 2 (invalid), 3 (host low on memory), 4 (service down), 5 (host
  not quiet), 6 (a case ran past its bound or the round ran out of time). For 4, read the container's log;
  a case prints it when the service died.
- The file is refused: read the problems, one line each, with its key. Check a
  file with `--dry-run` before a long run.
- `made with other settings`: the results directory has steps made with
  another configuration (the settings of a case enter its hash, the variables
  `ALAMBIQUE_LOAD_*` too). Run with `--no-skip-done`, or set another
  `output.name`.
- A step invalid with `timeout`: it ran past `round.step_timeout_s`. A step
  that hangs is a defect to look at; a legitimately long one (a large
  population to create) needs a larger limit.
- A step repeated or invalid with `{:swap, :driver, ...}`, `{:swap, :store,
  ...}` or `{:swap, :host, ...}`: the host pushed the driver or the store to
  swap, or its swap in use is high. Free memory on the host, or lower `store.tmpfs_size` / `scenarios.<s>.data_tmpfs`. With
  `{:swap, :service, ...}` the service container is not limited as
  intended; check its `memory.swap.max`.
- A step invalid with `data_full`: the service's data tmpfs is, or would be,
  full. See "Room of the harness": use a shorter `measures.wakes.steps.duration_s`
  or a larger `scenarios.<s>.data_tmpfs`. With `prepare_failed`, read
  `extra.prepare_error` in the step JSON.
- A step invalid with `store_full`, and "the store is out of space" in the
  summary: the store's tmpfs reached 95% (or the disk has less than 1 GiB
  free), or the operation errors said "no space", "volume full" or "quota", or
  the store answered HTTP 5xx with its tmpfs at 85% or more
  (`error_samples` and `store` of the step JSON). Run the case again with
  `store.backing = "disk"`, or raise `store.tmpfs_size`. A full
  store can also show only as timeouts; then check `docker logs` of the store
  container (the id is in `started-containers.txt`).
- A step invalid with `no_cooldown`: the service stayed above 80% of its memory
  limit, or kept operations running, for 300 s after the previous step, in every
  attempt. The previous step overloaded it; the level is past what it carries.
- A step bad with `busy`: the library refused more than 1% of the reads with
  `{:error, :busy}` because its statement slots were taken
  (`sql: [max_concurrent_statements:]`). That is the limit of the service at
  that level, not an error of the harness.

Next: [how-to-size-a-node.md](how-to-size-a-node.md) turns memory per cell into
node sizes; [contributing.md](contributing.md) lists the other benchmarks.
