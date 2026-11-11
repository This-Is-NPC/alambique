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
- **invalid**: fewer than 95% of the planned operations completed. It is not a
  failure; it says the load was not delivered. The ramp stops.
- **repeat**: the host was busy (below). The step runs again, up to 5 attempts
  (`ALAMBIQUE_LOAD_ATTEMPTS`), then is invalid. The attempts stay on disk.

Order of decision: host busy, memory, delivery, errors, latency.

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
| `ALAMBIQUE_LOAD_MAX_NET_LOAD` | 6 | host net load limit |
| `ALAMBIQUE_LOAD_SETTLE_S` | 30 | seconds of quiet readings before each step (one reading per 5 s) |
| `ALAMBIQUE_LOAD_WAIT_QUIET_STEP_S` | 1800 | longest wait for a quiet host before a step; then the step is invalid (`host_busy`) |
| `ALAMBIQUE_LOAD_ATTEMPTS` | 5 | times a step the host disturbed runs before it is invalid |
| `ALAMBIQUE_LOAD_MIN_AVAILABLE_GIB` | 6 | clean stop below this |
| `ALAMBIQUE_LOAD_START_LOAD` | 8 | load average at most, to start a case |
| `ALAMBIQUE_LOAD_WAIT_QUIET_S` | 600 | how long a case waits to start (a step waits by `_WAIT_QUIET_STEP_S`) |
| `ALAMBIQUE_LOAD_STORE_LATENCY_MS` | 25 | added to every store request |
| `ALAMBIQUE_LOAD_DATA_MB` / `_STORE_MB` | 1024 / 4096 | tmpfs of the service data and of the store (`_STORE_MB` is ignored with `_STORE_ON=disk`) |
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

The driver runs on the same VM, so it shares the VM's CPUs with the service
and the store. Its CPU is not counted, but it can take CPU from them; the host
guard still repeats a step when the host is busy. If the store's tmpfs fills,
re-run the case with `ALAMBIQUE_LOAD_STORE_ON=disk`.

## What the numbers include

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
  over the steps that ran (for `cells`, against the cells awake).

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

## Results

<!-- The tables of the full run go here. -->

Not run yet.

## Troubleshooting

- `host not quiet`: the case waited 10 minutes for the load average. Wait, or
  raise `ALAMBIQUE_LOAD_START_LOAD` knowingly.
- A step invalid with `host_busy`: the host stayed above the net load limit for
  the whole wait (`ALAMBIQUE_LOAD_WAIT_QUIET_STEP_S`) or every attempt was
  disturbed. Run again when the other work is done, or raise
  `ALAMBIQUE_LOAD_WAIT_QUIET_STEP_S` or `ALAMBIQUE_LOAD_ATTEMPTS`.
- Exit status 2 (invalid), 3 (host low on memory), 4 (service down). For 4,
  read the container's log; a case prints it when the service died.
- A tmpfs full error in the service log: raise `ALAMBIQUE_LOAD_DATA_MB`.
- A step bad with reason `:store_full`, and "the store is out of space" in
  the summary: the store's tmpfs filled. The driver tells it from other errors
  by words such as "no space", "volume full" or "quota" in the operation
  errors (`error_samples` of the step JSON). A full store can also show only as
  timeouts or a lower delivery; then check `docker logs` of the store container
  (the id is in `started-containers.txt`) for "no space" or "volume full". Run
  the case again with `ALAMBIQUE_LOAD_STORE_ON=disk`, or raise
  `ALAMBIQUE_LOAD_STORE_MB`.

Next: [how-to-size-a-node.md](how-to-size-a-node.md) turns memory per cell into
node sizes; [contributing.md](contributing.md) lists the other benchmarks.
