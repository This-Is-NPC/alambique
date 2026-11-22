# How to load-test one cell

**The question:** how do I run the load test of one Alambique cell from a round
file, and how do I read what one cell costs from its results?

This guide stands alone. For the cost of one cell, see
[cost-of-a-cell.md](cost-of-a-cell.md).

## Prerequisites

- Docker, and `mise` with the tools pinned in `mise.toml`.
- `python3` 3.11 or newer (the round file is read with `tomllib`).
- At least 6 GiB of available memory on the host, the default of
  `judging.host_memory.min_available_gib`. Below it, the round stops cleanly.

The first run builds the service image (`mise run load:image`) and fetches the
Mix dependencies, so it takes longer than the next runs.

## What it measures

The service, one Alambique instance, runs in a Docker container with a CPU and memory limit.
SeaweedFS runs as the store in its own container, outside that limit. The
driver runs on the host, sends the load, and raises the level step by step until
a step is bad.

| Measure | Load raised | Variants | Latency limit (default) | Cost it reports |
| --- | --- | --- | --- | --- |
| `commits` | commits per second over 64 cells | `small` (120-byte row), `large` (75 KB blob) | p99 ack, 250 ms | CPU per commit, store trips and bytes per commit |
| `cells` | cells awake | `default`, `cache256` | none; memory is judged | memory per awake cell |
| `wakes` | wakes per second of sleeping cells | none | p95 of the first read, 500 ms | CPU and store trips per wake |
| `query` | queries per second over 20 of 200 cells | none | p99, 1000 ms | CPU and store trips per query |

The container limits come from the scenarios of the round file, one table
`[scenarios.<name>]` each. The schema has two, with these defaults:

| Table | CPUs | Memory | Data tmpfs |
| --- | --- | --- | --- |
| `[scenarios.A]` | 1.0 | `512m` | `1g` |
| `[scenarios.B]` | 2.0 | `2g` | `1g` |

The memory limit does not include the data tmpfs. The memory criterion counts
the process memory of the service, without the tmpfs.

## Write a round file

A round file says what a round does: which cases run, their limits, the store,
the steps, the criteria, and where the results go. It is the only argument that
selects work.

1. Copy the example. It lists every key at its default:

   ```sh
   cp load/rounds/example.toml load/rounds/mine.toml
   ```

   Round files other than the example are ignored by git, so `mine.toml` stays
   on your machine.

2. Keep the keys you change and delete the rest. A missing key keeps its
   default. The keys you will most likely change:

   | Key | Meaning |
   | --- | --- |
   | `[round] cases` | the cases to run, in order, as `"<scenario>/commits/small"`. Empty runs every scenario and measure. |
   | `[round] step_timeout_s`, `timeout_s`, `case_timeout_s` | time limits: a step (1800 s), the round (none), a case (computed) |
   | `[output] name` | results go to `results/load/<name>`. Reruns and `--fetch` need it. |
   | `[scenarios.A] cpus, memory, data_tmpfs` | the limits of the service container |
   | `[store] backing, tmpfs_size, latency_ms` | the store's data on `tmpfs` (default) or `disk`, the size of that tmpfs, and the delay per store request (25 ms) |
   | `[measures.<m>.steps]` | the step plan: `kind`, `start`, `factor`, `max`, `values`, `duration_s`, `refine`, `confirm` |
   | `[judging.<criterion>]` | a criterion and its limit, or `enabled = false` |
   | `[host]` | the machine of a remote run |

3. For a short check that the run works, add this to the file. It is one
   15-second step, not a measurement:

   ```toml
   [round]
   cases = ["A/commits/small"]

   [measures.commits.steps]
   kind = "ladder"
   values = [10]
   duration_s = 15
   refine = 0
   confirm = false
   ```

To store the data of a case on disk instead of a tmpfs, set the store of that
measure or variant:

```toml
[measures.commits.variant.large.store]
backing = "disk"
```

The store writes to a new directory (`store.dir`, or `store/` in the case's
results). The script creates it and refuses an existing one. At the end it
empties that directory with a throwaway container of the same image, then
removes it.

## Dry run

`--dry-run` checks the file and prints the plan. It builds nothing and starts
no container.

```sh
mise run load -- --config load/rounds/mine.toml --dry-run
```

The output has a header line with the round name and results directory, a
`skip-done` line, the timeouts, and then, for each case, its limits, its steps
and the criteria it judges. The last line bounds the time of the steps only:

```text
round <name>: <n> cases, results in <dir>
...
at most <h> h of steps, without waits and preparation
```

Check the case order, the levels of each ramp, and the criteria marked `off`.
Waits for a quiet host and the preparation of cells take longer than that bound.

An invalid file is refused, and every problem is listed with its key.

## Run

```sh
mise run load -- --config load/rounds/mine.toml
```

The command builds the service image first, prints `results in <dir>`, and runs
the cases in order. Each step prints one line,
`<measure> <level> <unit>: <verdict> <reason>`. A step that is already done
prints `<measure> <level>: done, <verdict> <reason>` instead. Each case ends
with its summary.

| Option | Effect |
| --- | --- |
| `--dry-run` | print the plan and start nothing |
| `--no-skip-done` | run every step again ([Rerun](#rerun-skip-done-steps)) |
| `--output-dir DIR` | write the results to DIR, over `output.dir` |
| `--remote`, `--fetch` | run on the `[host]`, or copy its results back ([Remote run](#remote-run)) |

Each step writes its own file when it ends, so a stopped run keeps every step
that finished:

```text
results/load/<name>/
  config.json                      the resolved config and the cases
  <scenario>/<measure>[/<variant>]/
    case.json                      the settings of the case and their hash
    step-<level>.json              one step
    step-<level>-2.json            the confirmation of that level
    step-<level>-attempt<N>.json   an attempt that was repeated
    summary.json, summary.md       the table of the case
    started-containers.txt         the containers the case started
```

Exit status of `mise run load`:

| Status | Meaning |
| --- | --- |
| 0 | every case ran. Invalid steps are in the summary. |
| 1 | the file was refused, or a case failed (such as `5`, host not quiet); the other cases ran |
| 3 | the host ran low on memory; the run stopped, partial results stay |
| 6 | a timeout of a case or of the round; the run stopped, partial results stay |

## Rerun, skip done steps

A step with a result of the same settings and a verdict of `good` or `bad` is
**done**. A rerun reads it instead of running it again, and the ramp goes on
from where it stopped. An **invalid** step is never done: it runs again.

```sh
mise run load -- --config load/rounds/mine.toml                  # skips done steps (default)
mise run load -- --config load/rounds/mine.toml --no-skip-done   # runs every step again
```

- Set `output.name` (or `output.dir`). Without one, each run gets a new
  directory named by the time stamp, and skips nothing.
- Each step stores a hash of its case's resolved settings. If the directory
  holds a good or bad step made with other settings, the run is refused before
  it starts, and the message names the file and both hashes. Use
  `--no-skip-done`, or another `output.name`.
- A case whose steps are all done starts no container. Its summary is written
  again from the results.
- `cells` keeps its awake cells in the service, which a restart empties. The
  first `cells` step that runs after skipped ones wakes all its cells.

## Remote run

A round can run on another machine. Nothing runs there unless you pass a flag.
Set the machine in the round file. `host.repo` and `output.name` are required
when `host.ssh` is set:

```toml
[output]
name = "mine"

[host]
ssh = "user@example-host"   # ssh target (a placeholder)
repo = "alambique"          # the clone on the host
```

```sh
mise run load -- --config load/rounds/mine.toml --remote   # copy the file, start the round there, return
mise run load -- --config load/rounds/mine.toml --fetch    # copy the results back, and say whether the round runs
```

- `--remote` copies the file to the host and starts the run detached, under
  `timeout` (`host.timeout_s`, 172800 s by default). It prints the commands to
  follow it, and refuses to start while a round runs there.
- `--fetch` copies `results/load/<name>` (or `host.results`) to the local
  results directory. It skips the directories named `store`. It prints whether
  the round is `running` or `stopped`. There is no separate status command.
- To follow the log, run the `tail -f` command that `--remote` prints.

## How a step is judged

A ramp runs the levels of its plan in order until a step is bad. It then
bisects between the last good level and the bad one, `refine` times, and
repeats the last good level once when `confirm` is true. `cells` does not
bisect or repeat: its cells stay awake, and its answer is the memory fit.

| Verdict | Meaning | Next |
| --- | --- | --- |
| `good` | every criterion held | the ramp goes on |
| `bad` | a criterion broke; the reason says which | the ramp stops |
| `invalid` | the harness could not tell whether the service carries the level; not a failure | the ramp stops |
| `repeat` | the host was busy, swapped, or the service did not cool down | the step runs again, up to `judging.attempts` (5), then is invalid |

The criteria, with their defaults:

| Criterion | Default | Verdict when broken |
| --- | --- | --- |
| `latency` | p99 250 ms (`commits`), p95 500 ms (`wakes`), p99 1000 ms (`query`) | bad |
| `memory` | process memory at 100% of the limit (90% for `cells`) | bad |
| `errors` | 0% of the completed operations failed | bad |
| `busy` | 1% of the completed operations refused with `{:error, :busy}` | bad |
| `delivery` | 95% of the planned operations completed | invalid |
| `store_full` | 95% of the store's tmpfs, or under 1 GiB free on disk | invalid |
| `data_full` | 90% of the data tmpfs, estimated before a `wakes` level | invalid |
| `driver_swap`, `store_swap`, `host_swap` | driver 16 MB, store growth 0 MB, host 512 MB | repeat, then invalid |
| `host_load` | net load (host load average minus the test's own runnable tasks) 6 | repeat |
| `host_memory` | 6 GiB available | the round stops cleanly |
| `host_start` | load average 8, waited for at most 600 s before a case | the case does not start |
| `cooldown` | service memory under 80% of its limit before each step, waited for at most 300 s | repeat, then invalid |
| `suspect_throttle` | CPU throttled for more than 10% of a step | marks the step `suspect`; the verdict stands |

Other reasons of an invalid step: `no_operations`, `prepare_failed`,
`no_cooldown`, `host_busy`, `service_down`, `timeout` (the step ran past
`round.step_timeout_s`). Any swap of the service is invalid at once, with no
repeat.

## The summary

`summary.md` has a heading per case: the scenario, measure, variant, CPUs and
memory limit, and `store on disk` when the store is on disk. Then comes one
row per step.

| Column | Meaning |
| --- | --- |
| `level` | the level of the step, in the unit of the measure |
| `step` | sequence number and phase (`confirm` for a repeat) |
| `verdict` | the verdict, its reason, and `, suspect` when the CPU was throttled |
| `delivered` | completed operations over planned operations |
| `p50`, `p95`, `p99` | latency, in ms |
| `CPU ms/op` | CPU time of the service per successful operation |
| `mem MB` | mean process memory of the service |
| `swap MB` | host swap at its maximum |
| `trips/op` | store requests per successful operation |
| `put KB/op`, `got KB/op` | bytes the service sent to and read from the store, per successful operation |
| `busy %` | operations refused with `:busy` |
| `store MB` | store data at the end of the step |
| `awake` | (`cells` only) cells awake |
| `dirty IO util`, `stmts max` | (`query` only) disk utilisation of dirty pages, and the most statements at once |

Below the table, the summary has these lines. Each appears only when it applies:

- `Stopped by`: the verdict that ended the ramp, or that no step was bad.
- `Last good level`: the highest good level, whether a repeat confirmed it, and
  its `CPU ms per operation`. A level good once and not repeated is a first good
  level, not a ceiling.
- `Process memory`: a fixed part in MB and a part per unit, from a least-squares
  line over the good steps, with its R². The unit is the measure's unit: awake
  cells for `cells`, and commits/s, wakes/s or queries/s for the others. The
  line is left out, and the summary says why, when R² is below 0.8, when the
  largest level is under twice the smallest, or with fewer than three good steps.
- `The store's tmpfs is full` or `The disk of the store is nearly full`: the
  store reached its limit. See [Troubleshooting](#troubleshooting).
- `Suspect steps`: steps with CPU throttled for more than 10% of their time.
- `Waited ... s in all for the host to be quiet`: the time the run waited.
- `Reproduce:`: the command of the run and any `ALAMBIQUE_LOAD_*` variables.

### Reading the per-operation costs

- **CPU per operation.** Read `CPU ms/op` in a `good` row, or `CPU ms per
  operation` on the `Last good level` line. To turn it into cores at a rate,
  multiply the rate (operations per second) by the cost and divide by 1000.
  At 1000 operations per second, the cores equal the cost in ms.
- **Memory per cell or per rate.** The slope of `Process memory` is the MB added
  per unit. For `cells` it is the memory per awake cell. For `commits`, `wakes`
  or `query` it is the memory per unit of rate. The fixed part is the memory at
  zero.
- **Store trips and bytes.** `trips/op`, `put KB/op` and `got KB/op` count
  everything the service sent to the store in the window, background work
  included, divided by the successful operations.
- **A wake** is the first read of a sleeping cell. In the default `cold` mode the
  local file is removed first, so the wake restores the cell from the store. Its
  cost includes putting the cell back to sleep.
- **A cross-cell query** is warm: a pass over the population fills the cache first.

Read the cost from the rows that are `good`. A `bad` or `invalid` row shows the
service at a level it did not carry.

## Overrides

An `ALAMBIQUE_LOAD_*` variable overrides one key for one run, and wins over the
file:

```sh
ALAMBIQUE_LOAD_STEP_S=15 mise run load -- --config load/rounds/mine.toml
```

Use a variable for a one-off change. Keep a setting you want to keep in the
file. Percentages are percentages (`ALAMBIQUE_LOAD_MAX_BUSY_PCT=1` is 1%), and
`DATA_MB` and `STORE_MB` are megabytes. Each run prints its variables in the
`Reproduce:` line, and they change the hash of the steps.

| Variable | Overrides |
| --- | --- |
| `ALAMBIQUE_LOAD_STEP_S` | `measures.<m>.steps.duration_s`, every measure |
| `ALAMBIQUE_LOAD_LEVELS` | the values of every ladder, for example `10,20,40` |
| `ALAMBIQUE_LOAD_STORE_ON` | `store.backing` |
| `ALAMBIQUE_LOAD_STORE_MB` | `store.tmpfs_size`, in MB |
| `ALAMBIQUE_LOAD_DATA_MB` | `scenarios.<s>.data_tmpfs`, in MB, for A and B |
| `ALAMBIQUE_LOAD_MIN_AVAILABLE_GIB` | `judging.host_memory.min_available_gib` |

The full list is `ENV_KEYS` in `scripts/load_config.py`. The
`ALAMBIQUE_LOAD_NODE`, `_SERVICE_ID`, `_STORE_ID`, `_STORE_PATH` and
`_STORE_FSTYPE` variables are set by `scripts/load-case`; do not set them.

## Troubleshooting

- **Status 5, `host not quiet`.** The load average stayed above
  `judging.host_start.max_load` for 10 minutes. Let the other work finish, and
  run again.
- **`host_busy`.** The net load stayed above its limit for the whole wait
  (30 minutes), or every attempt was disturbed. Run again when the machine is
  idle.
- **`made with other settings`.** The directory holds steps made with other
  settings. Run with `--no-skip-done`, or set another `output.name`.
- **`timeout`.** A step ran past `round.step_timeout_s`. A legitimately long
  step, such as a large population to create, needs a larger limit. A step that
  hangs is a defect.
- **`swap`.** The driver, the store or the host swapped. Free memory on the
  host, or lower `store.tmpfs_size` or the data tmpfs. Service swap
  (`{:swap, :service, ...}`) means the container's `memory.swap.max` is not
  equal to its memory.
- **`data_full`.** The data tmpfs is, or would be, full. Use a shorter
  `measures.wakes.steps.duration_s` or a larger `scenarios.<s>.data_tmpfs`. For
  `prepare_failed`, read `extra.prepare_error` in the step file.
- **`store_full`.** The store reached its limit, or answered HTTP 5xx near it.
  Run the case with `store.backing = "disk"`, or raise `store.tmpfs_size`.
  Timeouts alone can also mean a full store: check `docker logs` of the store
  container, whose id is in `started-containers.txt`.
- **`no_cooldown`.** The service stayed above 80% of its memory, or kept
  operations running, for 300 s after the previous step. The level is past what
  the service carries.
- **`busy`.** More than 1% of the reads were refused because the statement
  slots were taken (`sql: [max_concurrent_statements:]`). That is the service's
  limit at that level.
- **The file is refused.** Read the problems, one per line, with their keys.
  Check the file with `--dry-run` before a long run.
- **Containers left after a failed run.** Their ids are in each case's
  `started-containers.txt`. Remove only those, with `docker rm -f <id>`.

Next: [cost-of-a-cell.md](cost-of-a-cell.md) gives the cost of one cell in the
current version. [contributing.md](contributing.md) lists the other benchmarks.
