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
  request** (`ALAMBIQUE_LOAD_STORE_LATENCY_MS`) with a delaying store around
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
  (`ALAMBIQUE_LOAD_DATA_MB`, default 1024); the criteria use the scenario
  limit. CPU is the delta of `usage_usec` in `cpu.stat`.
- **Open loop.** Operations are due at fixed times (Poisson arrivals by
  default, or `fixed`). Latency is measured from the due time, so a slow
  service cannot hide its queue. At most 5 000 operations run at once; a due
  operation over the cap is dropped.

## A step, a ramp, and what the verdicts mean

A ramp runs fixed steps (180 s) of increasing level until one is bad, then
refines by bisection between the last good and the first bad level (2 steps),
then repeats the last good level once to confirm it. `cells` does not refine or
repeat: the cells stay awake, and the answer is the memory fit.

A step ends in one of:

- **good**, **bad** (a criterion broke), or
- **invalid**: the step does not say whether the service carries the level, so
  it is not a failure. The ramp stops. The reasons: fewer than 95% of the
  planned operations completed (`delivery`); no operation was planned
  (`no_operations`: a step with no operations is never good); the harness ran
  out of room (`data_full`, `store_full`); a preparation failed
  (`prepare_failed`); the service did not cool down (`no_cooldown`); the
  host was not quiet (`host_busy`); the service container is gone
  (`service_down`).
- **repeat**: the host was busy, or the service did not cool down (below). The
  step runs again, up to 5 attempts (`ALAMBIQUE_LOAD_ATTEMPTS`), then is
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
(`ALAMBIQUE_LOAD_SUSPECT_THROTTLE_FRACTION`): the quota cut it as much as the
load did. Each step's JSON has the `throttled_usec` delta in `cgroup` and
`suspect`; `summary.md` lists the suspect steps.

**Busy refusals.** When `sql: [max_concurrent_statements:]` slots are taken,
the library answers a read with `{:error, :busy}` by design. The driver counts
those apart from other errors (`delivery.busy` and `delivery.busy_ratio` in the
step JSON, a `busy %` column in the summary, for every measure). A step is bad
with reason `busy` only when they are more than 1%
(`ALAMBIQUE_LOAD_MAX_BUSY_RATIO`) of the completed operations; other errors
keep their rule (`ALAMBIQUE_LOAD_MAX_ERROR_RATIO`).

**Cooldown.** Before every step, and before every repeat, the driver waits
until the service has recovered from the previous step: no operation of the
driver still running on the service, and (except for `cells`, whose awake cells
keep memory by design) process memory below 80% of the scenario limit
(`ALAMBIQUE_LOAD_COOLDOWN_MEM_FRACTION`). It waits up to 300 s
(`ALAMBIQUE_LOAD_COOLDOWN_MAX_S`); if the service has not cooled down the
attempt is repeated like a busy host, and after the attempts the step is
invalid with `no_cooldown`. Without it a step that follows an overloaded one
starts on the backlog and memory of the other. `cooldown_s` in each step's JSON
is the time waited (the attempts included). A step that stops on memory is bad
with reason `memory`; it is `service_down` only when the container is gone.

**Room of the harness.** Two spaces can fill without the service being at
fault; both end the ramp with an invalid step.

- The service's data tmpfs (`ALAMBIQUE_LOAD_DATA_MB`). The `wakes` ramp keeps
  every cell it woke, asleep, so its data grows with the level. Before it
  prepares a level it estimates the data: the cells it still has to create
  times the bytes one asleep cell took on the tmpfs (measured after the
  previous level; 160 000 bytes before any, `ALAMBIQUE_LOAD_CELL_BYTES`). If
  the data in use plus the estimate is over 90% of the tmpfs
  (`ALAMBIQUE_LOAD_DATA_ESTIMATE_FRACTION`), the level is invalid `data_full`
  and is not run: this is a limit of the harness, not of the service. A
  preparation that fails is invalid `data_full` when the service's shared memory
  is at 95% of the tmpfs or more (`ALAMBIQUE_LOAD_DATA_FULL_FRACTION`), else
  `prepare_failed`; the first error is in the step's `extra.prepare_error`.
  The summary is always written. Use a shorter `ALAMBIQUE_LOAD_WAKES_STEP_S`
  (fewer fresh cells per level) or a larger `ALAMBIQUE_LOAD_DATA_MB`.
- The store. The service never deletes during a case (the deletion grace is 24
  hours, the library default, and it is not changed because that would change
  the cost measured), so the store only grows. Every second the driver reads the
  store container's shared memory (its tmpfs) or, with
  `ALAMBIQUE_LOAD_STORE_ON=disk`, the free space of the store directory's file
  system (`df`). At 95% of `ALAMBIQUE_LOAD_STORE_MB`
  (`ALAMBIQUE_LOAD_STORE_FULL_FRACTION`), or with less than 1 GiB free on disk
  (`ALAMBIQUE_LOAD_STORE_MIN_FREE_MB`), the step stops and is invalid
  `store_full` (not bad), the ramp stops, and the driver says to run the case
  again with `ALAMBIQUE_LOAD_STORE_ON=disk`. Errors that say "no space" are
  treated the same. So is a store HTTP 5xx in a step whose store tmpfs reached
  85% of `ALAMBIQUE_LOAD_STORE_MB` (`ALAMBIQUE_LOAD_STORE_ERROR_FRACTION`): a
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
  `ALAMBIQUE_LOAD_MAX_DRIVER_SWAP_MB` (default 16, absolute);
- the store's swap grew during the step (maximum minus its value at the start)
  by more than `ALAMBIQUE_LOAD_MAX_STORE_SWAP_MB` (default 0);
- the host's swap in use at its maximum during the step is above
  `ALAMBIQUE_LOAD_MAX_HOST_SWAP_MB` (default 512, absolute).

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
(`ALAMBIQUE_LOAD_MAX_NET_LOAD`) is repeated. `MemAvailable` below 6 GiB
(`ALAMBIQUE_LOAD_MIN_AVAILABLE_GIB`) stops the run cleanly: the containers are
removed and the results so far stay. A case also waits up to 10 minutes before
it starts for load average at most 8 and that much free memory.

**Waiting for a quiet host.** Before every step, and before every repeat, the
driver waits until the net load is at most the limit in every sample of a
settle window (30 s, so 6 samples, `ALAMBIQUE_LOAD_SETTLE_S`). One quiet sample
is not enough: the 1-minute average lags. The service stays up and idle
meanwhile, so the net load is close to the plain 1-minute load average. The
driver prints one line when it starts waiting and one when it goes on. If the
host is not quiet after 30 minutes (`ALAMBIQUE_LOAD_WAIT_QUIET_STEP_S`), the
step is invalid with `host_busy` and the ramp stops; that is not a failure of
the service. Low memory while waiting stops the run as above. Each step's JSON
has `quiet_wait_s`, the seconds waited for it (the settle window included, the
earlier attempts included), and the summary adds the total of the case.

## Run it

Prerequisites: Docker, `mise` with the pinned tools, a quiet host with 6 GiB
free. A full run is long; see the estimate below.

On a fresh clone nothing else is needed: the first case fetches the Mix
dependencies and compiles the driver (`MIX_ENV=test`) before it starts any
container, which takes a few minutes with the Rust toolchain.

1. Build the image (the first build takes about 3 minutes and the image is
   about 250 MB; later builds reuse the cached layers):

   ```sh
   mise run load:image
   ```

2. Run a scenario and a measure (this also builds the image):

   ```sh
   mise run load -- A commits      # both variants
   mise run load -- A cells
   mise run load -- B wakes
   mise run load -- all all        # everything, both scenarios
   ```

3. Read the results under `results/load/<run id>/<scenario>-<measure>-<variant>/`:
   one `step-NNN-<phase>-<level>.json` per step (written whole or not at all),
   `summary.json` and `summary.md`, the table of the case. A step file holds
   the delivery counts, latency percentiles, host readings, CPU, memory (max
   and mean), the store counters, the native counters and the cost per
   operation. `started-containers.txt` lists the containers the case started.

To smoke-test without a long run, shorten the steps and set the levels:

```sh
ALAMBIQUE_LOAD_STEP_S=15 ALAMBIQUE_LOAD_LEVELS=10 ALAMBIQUE_LOAD_REFINE=0 \
  mise run load -- A commits
```

The harness starts only containers it names itself, removes them by the
recorded id when it ends, fails or is interrupted, and never kills by name.

## Settings

All are environment variables; set them before `mise run load`.

| Variable | Default | Meaning |
| --- | --- | --- |
| `ALAMBIQUE_LOAD_STEP_S` | 180 | seconds per step |
| `ALAMBIQUE_LOAD_LEVELS` | per measure (below) | comma list of levels |
| `ALAMBIQUE_LOAD_REFINE` | 2 | bisection steps |
| `ALAMBIQUE_LOAD_VARIANT` | all of the measure | run one variant: `small`, `large`, `default`, `cache256`, `cache<N>` |
| `ALAMBIQUE_LOAD_ARRIVALS` | `poisson` | or `fixed` |
| `ALAMBIQUE_LOAD_MAX_OUTSTANDING` | 5000 | operations in flight at most |
| `ALAMBIQUE_LOAD_DRAIN_S` | 20 | wait for stragglers after the last due time |
| `ALAMBIQUE_LOAD_OP_TIMEOUT_MS` | 15000 | per operation |
| `ALAMBIQUE_LOAD_DELIVERY_MIN` | 0.95 | delivery gate |
| `ALAMBIQUE_LOAD_MAX_ERROR_RATIO` | 0 | errors allowed, as a share of completed |
| `ALAMBIQUE_LOAD_MAX_BUSY_RATIO` | 0.01 | `{:error, :busy}` refusals allowed, as a share of completed; above it the step is bad `busy` |
| `ALAMBIQUE_LOAD_COOLDOWN_MEM_FRACTION` | 0.8 | share of the scenario's memory limit the service must be below before a step (not for `cells`) |
| `ALAMBIQUE_LOAD_COOLDOWN_MAX_S` | 300 | longest wait for the service to cool down; then the attempt is repeated, finally `no_cooldown` |
| `ALAMBIQUE_LOAD_SUSPECT_THROTTLE_FRACTION` | 0.1 | share of the step the CPU may be throttled before the step is marked `suspect` |
| `ALAMBIQUE_LOAD_FIT_MIN_R2`, `_FIT_MIN_SPAN` | 0.8, 2 | the memory slope is shown only with R² at least this and the largest level at least this times the smallest (and three good steps) |
| `ALAMBIQUE_LOAD_MAX_NET_LOAD` | 6 | host net load limit |
| `ALAMBIQUE_LOAD_SETTLE_S` | 30 | seconds of quiet readings before each step (one reading per 5 s) |
| `ALAMBIQUE_LOAD_WAIT_QUIET_STEP_S` | 1800 | longest wait for a quiet host before a step; then the step is invalid (`host_busy`) |
| `ALAMBIQUE_LOAD_ATTEMPTS` | 5 | times a step the host disturbed runs before it is invalid |
| `ALAMBIQUE_LOAD_MAX_DRIVER_SWAP_MB` | 16 | the driver's `VmSwap` allowed (max during a step); above it the step is repeated, then `swap` |
| `ALAMBIQUE_LOAD_MAX_STORE_SWAP_MB` | 0 | growth of the store's swap allowed within a step (max minus value at its start) |
| `ALAMBIQUE_LOAD_MAX_HOST_SWAP_MB` | 512 | the host's swap in use allowed (max during a step) |
| `ALAMBIQUE_LOAD_MIN_AVAILABLE_GIB` | 6 | clean stop below this |
| `ALAMBIQUE_LOAD_START_LOAD` | 8 | load average at most, to start a case |
| `ALAMBIQUE_LOAD_WAIT_QUIET_S` | 600 | how long a case waits to start (a step waits by `_WAIT_QUIET_STEP_S`) |
| `ALAMBIQUE_LOAD_STORE_LATENCY_MS` | 25 | added to every store request |
| `ALAMBIQUE_LOAD_DATA_MB` / `_STORE_MB` | 1024 / 4096 | tmpfs of the service data and of the store (`_STORE_MB` is ignored with `_STORE_ON=disk`) |
| `ALAMBIQUE_LOAD_STORE_FULL_FRACTION`, `_STORE_MIN_FREE_MB` | 0.95, 1024 | the store is full at this share of `_STORE_MB` (tmpfs), or with less than this free (disk) |
| `ALAMBIQUE_LOAD_STORE_ERROR_FRACTION` | 0.85 | a step with store HTTP 5xx errors, whose store tmpfs reached this share of `_STORE_MB`, is invalid `store_full` |
| `ALAMBIQUE_LOAD_DATA_ESTIMATE_FRACTION`, `_DATA_FULL_FRACTION` | 0.9, 0.95 | `wakes`: a level whose estimated data goes over the first share of `_DATA_MB` is not run (`data_full`); a failed preparation with the service's shared memory over the second is `data_full` |
| `ALAMBIQUE_LOAD_CELL_BYTES` | 160000 | `wakes`: bytes one asleep cell is estimated to take on the data tmpfs before one has been measured |
| `ALAMBIQUE_LOAD_STORE_ON` | `tmpfs` | `tmpfs` or `disk`: where the store keeps its data (below) |
| `ALAMBIQUE_LOAD_STORE_DIR` | `<case results dir>/store` | with `disk`, the directory the case creates for the store; it must not exist |
| `ALAMBIQUE_LOAD_CACHE_KIB` | library default (512) | `awake.cache_kib` (set by `cache<N>`) |
| `ALAMBIQUE_LOAD_IDLE_TIMEOUT_MS` | library default; 24 h for `cells`, 5 s for `wakes` | cell idle timeout |
| `ALAMBIQUE_LOAD_MAX_CELLS` | library default; 10 000 000 for `cells` | `awake.max_cells` |
| `ALAMBIQUE_LOAD_WAL_CHECKPOINT_BYTES` | 8388608 | `wal.checkpoint_bytes` (the library's 64 MiB times 64 cells would fill the tmpfs) |
| `ALAMBIQUE_LOAD_OPTS` | none | Elixir keyword list merged over the instance options |
| `ALAMBIQUE_LOAD_ERL_AFLAGS` | `+Mdai max +MMmcs 0` | VM flags of the service |
| `ALAMBIQUE_LOAD_COMMIT_CELLS`, `_COMMIT_ROWS` | 64, 8 | cells written, rows per cell |
| `ALAMBIQUE_LOAD_CELLS_FILL_RATE`, `_HOLD_S`, `_CELLS_MEM_FRACTION` | 100, 60, 0.9 | cells per second woken, hold for memory to settle, criterion |
| `ALAMBIQUE_LOAD_WAKE_MODE`, `_TAIL_S` | `cold`, 60 | `cold` removes the local file first (restore from the store), `local` keeps it; wait for the cells to sleep again |
| `ALAMBIQUE_LOAD_WAKES_STEP_S` | the global step | seconds per step of `wakes` only; a shorter step wakes fewer fresh cells per level |
| `ALAMBIQUE_LOAD_QUERY_POPULATION`, `_QUERY_CELLS` | 200, 20 | cells in the population, cells per query |
| `ALAMBIQUE_LOAD_CELL_ROWS`, `_ROW_BYTES` | 20, 1500 | data of the cells of `wakes` and `query` |
| `ALAMBIQUE_LOAD_COMMITS_P99_MS`, `_WAKES_P95_MS`, `_QUERY_P99_MS` | 250, 500, 1000 | latency criteria |

Default levels: commits small 50 to 3200 (doubling), large 5 to 320; cells 500
to 32 000; wakes 5 to 320; query 1 to 64.

## Store on disk

By default the store's data is on a tmpfs of `ALAMBIQUE_LOAD_STORE_MB`. Use
`ALAMBIQUE_LOAD_STORE_ON=disk` when that tmpfs would not fit in the host's
memory, or when a case filled it (see Troubleshooting): re-run that case with
the store on disk and read its result as "store on disk". The store then
writes to a fresh directory, bind mounted at `/data`: `ALAMBIQUE_LOAD_STORE_DIR`,
or `store/` in the case's results directory. The script creates it (it fails
if the directory exists) and writes its path to `store-dir.txt`.

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

For a VM with 4 vCPUs and 5.9 GB of RAM the settings that were approved are:

```sh
ALAMBIQUE_LOAD_MIN_AVAILABLE_GIB=0.5 ALAMBIQUE_LOAD_STORE_MB=1536 \
  ALAMBIQUE_LOAD_DATA_MB=768 mise run load -- A commits
```

The VM has 11.8 GB of zram swap. The service container keeps `--memory-swap`
equal to `--memory`, so it never swaps. Idle processes of the VM drift into
swap, so the host's swap only counts above 512 MB; a step in which the driver
or the store goes to swap is repeated and then invalid (see the swap guard
above).
The driver runs on the same VM, so it shares the VM's CPUs with the service
and the store. Its CPU is not counted, but it can take CPU from them; the host
guard still repeats a step when the host is busy. If the store's tmpfs fills,
re-run the case with `ALAMBIQUE_LOAD_STORE_ON=disk`.

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
  `ALAMBIQUE_LOAD_WAKE_MODE=local`
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
repeat waits again. This is an estimate: no full ramp has been run.

## Results on the reference VM (order 022)

These are the first full set of numbers. Runs were cut by host shutdowns, so
some cases are unconfirmed: a level is **confirmed** when a repeat of it was
also good, **not repeated** when it was good once, and **not measured** when no
step reached it.

### Setup

- A VM with 4 vCPU and 5.9 GB of RAM, 11.8 GB of zram swap, a 100 GB virtual
  disk (rotational, btrfs), cgroup v2 and Docker 29.7.2.
- The driver runs on the same VM, outside the measured container, and **shares
  the 4 vCPUs** with the service and the store. The numbers include that
  contention.
- The store is seaweedfs with +25 ms per request (`ALAMBIQUE_LOAD_STORE_LATENCY_MS`),
  on a tmpfs, or on the VM's disk in the rows marked "store on disk". Those
  rows measure the VM's virtual disk, not the library.
- Settings changed, and why: `ALAMBIQUE_LOAD_MIN_AVAILABLE_GIB` 1.0 (the
  default, 6, is above the VM's 5.9 GB; the first run used 0.5),
  `ALAMBIQUE_LOAD_STORE_MB=1536` and `ALAMBIQUE_LOAD_DATA_MB=768` (the default
  tmpfs of 4096 and 1024 MB do not fit). No other limit was changed.
- Scenario A is 1 CPU and 512 MB, scenario B is 2 CPU and 2 GB. The service
  container's memory limit is the scenario's plus `DATA_MB`, with swap equal to
  memory, and the memory criterion is `memory.current` minus the tmpfs.
- The swap of the service, the driver and the store was 0 in every step. The
  host's swap was 270 to 291 MB in the first run (recorded only; the limit is
  512) and 0 in the others.

### Capacity

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

### What these numbers do not say

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
  raise `ALAMBIQUE_LOAD_START_LOAD` knowingly.
- A step invalid with `host_busy`: the host stayed above the net load limit for
  the whole wait (`ALAMBIQUE_LOAD_WAIT_QUIET_STEP_S`) or every attempt was
  disturbed. Run again when the other work is done, or raise
  `ALAMBIQUE_LOAD_WAIT_QUIET_STEP_S` or `ALAMBIQUE_LOAD_ATTEMPTS`.
- Exit status 2 (invalid), 3 (host low on memory), 4 (service down). For 4,
  read the container's log; a case prints it when the service died.
- A step repeated or invalid with `{:swap, :driver, ...}`, `{:swap, :store,
  ...}` or `{:swap, :host, ...}`: the host pushed the driver or the store to
  swap, or its swap in use is high. Free memory on the host, or lower `ALAMBIQUE_LOAD_STORE_MB` / `ALAMBIQUE_LOAD_DATA_MB`. With
  `{:swap, :service, ...}` the service container is not limited as
  intended; check its `memory.swap.max`.
- A step invalid with `data_full`: the service's data tmpfs is, or would be,
  full. See "Room of the harness": use a shorter `ALAMBIQUE_LOAD_WAKES_STEP_S`
  or a larger `ALAMBIQUE_LOAD_DATA_MB`. With `prepare_failed`, read
  `extra.prepare_error` in the step JSON.
- A step invalid with `store_full`, and "the store is out of space" in the
  summary: the store's tmpfs reached 95% (or the disk has less than 1 GiB
  free), or the operation errors said "no space", "volume full" or "quota", or
  the store answered HTTP 5xx with its tmpfs at 85% or more
  (`error_samples` and `store` of the step JSON). Run the case again with
  `ALAMBIQUE_LOAD_STORE_ON=disk`, or raise `ALAMBIQUE_LOAD_STORE_MB`. A full
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
