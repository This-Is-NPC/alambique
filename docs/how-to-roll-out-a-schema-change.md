# How do I roll out a schema change?

**The question:** how do I change the schema of every cell while old and new
code run side by side, and know when it is safe to continue?

This guide stands alone. Background is in
[design.md](design.md), section 17.

## Prerequisites

- An instance with `migrations:` (a list of `{version, sql | fun}` with
  versions 1, 2, 3, ... or a module that exports `migrations/0`). The highest
  version is the instance's *schema version* N. For one list per kind of cell,
  see "Migrations chosen per cell" below.
- The deploy check and the sweeper read the schema index, which every wake
  writes. A cell that last woke before you upgraded to a release with the
  index has no entry until it wakes once; wake those cells (or run the old
  release's traffic) before relying on the check.

## The rule

During a rollout, code for N-1 and code for N serve the same cells. Code for N
migrates a cell to N when it wakes it. Code for N-1 then finds a cell on N and
serves it without migrating. It does so for cells up to `migration_window`
versions ahead (default 1) and refuses cells further ahead with
`{:error, {:schema_too_new, version}}`.

So migration N must be *expand-only*: code for N-1 must keep working on the
migrated cell. Add tables and nullable columns; do not drop, rename or
tighten anything. Alambique cannot check this, it is your responsibility.

## Steps

1. **Expand.** Add migration N with only additive changes (and data backfills
   that code for N-1 can ignore). Code that reads the new column must handle
   `NULL`, because code for N-1 keeps inserting rows without it.

2. **Check the deploy.** Before rolling out, run the check against the same
   placement configuration production uses:

   ```sh
   mix alambique.check_deploy --migrations MyApp.Migrations
   ```

   ```text
   Code at schema version 3 reads cells from version 2.
     location default: versions 1, 2  <- too old
   Deploy refused: cells in default (version 1) are older than version 2. Migrate them with an earlier release's sweeper or Alambique.sweep/2, then run the check again.
   ```

   The exit status is 1. Without the migrations module on the command line,
   set `config :alambique, :ops, migrations: MyApp.Migrations`. The store and
   placement come from the same `:ops` config or the store flags as the other
   `mix alambique.*` tasks. A different configuration lists other stores and
   passes for the wrong reason. From a release console or a deploy script
   call `Alambique.check_deploy(instance_or_options)`.

   | Exit status | Meaning |
   | --- | --- |
   | 0 | every location holds only versions the code can read |
   | 1 | the deploy is refused: a cell is older than version N minus the window |
   | 2 | the check could not run: bad options, or a location could not be listed |

3. **Drain the old versions.** Turn on the sweeper in the release that is
   running now (or in the new one), so idle cells migrate in the background:

   ```elixir
   {Alambique, name: MyApp.Cells, ..., sweeper: [interval_ms: 60_000, rate: 20]}
   ```

   Or run one pass by hand with `Alambique.sweep(MyApp.Cells)`:

   ```text
   iex> Alambique.sweep(:r3)
   {:ok, %{errors: 0, stale: 0, skipped: 0, cells: 5, migrated: 5}}
   ```

   The sweeper wakes each cell through the normal API, so leases, fencing and
   the change feed apply, and hibernates it again. It skips frozen cells
   (`skipped`), leaves cells that were awake awake, and removes index entries
   that a crash or a relocation left behind (`stale`). Run it on one or a few
   nodes: every node with the option sweeps the whole index.

4. **Check again** and roll out. The same command now exits 0:

   ```text
   Code at schema version 3 reads cells from version 2.
     location default: versions 3
   Deploy allowed: every live cell is at version 2 or later.
   ```

5. **Contract.** Once no code for N-1 runs, ship the release that removes what
   N-1 used, as migration N+1 in the next rollout. That release is itself
   expand-only toward the one after it.

## Many DDL statements in one migration

A SQL migration (a string) runs each statement through `Alambique.Tx`, and
every `CREATE`, `DROP` or `ALTER` regenerates the change-feed triggers. A
migration that creates a whole schema is faster as a function that runs one
`Alambique.Tx.script/2`, which regenerates them once (about four times faster
for 4 tables and 7 indexes, design section 13). Check its result, because
the value of a migration function is ignored:

```elixir
{1,
 fn tx ->
   :ok =
     Alambique.Tx.script(tx, """
     CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);
     CREATE INDEX notes_body ON notes (body);
     """)
 end}
```

A script holds schema statements only. Put seed rows or backfills in
`Tx.execute/3` calls of the same function, after the script.

## A long migration

Each statement of a migration and each `Tx.script/2` has a deadline,
`sql: [script_timeout_ms:]` (default 60 000). A statement that runs longer,
such as an index over a very large table, is interrupted, the migration rolls
back and the wake fails with `{:error, {:migration, version, :statement_timeout}}`.
Raise `script_timeout_ms` for such a rollout; callers' statements keep their
own, shorter `statement_timeout_ms`.

## Migrations chosen per cell

If groups of cells run different code, give each group its own list. Set
`migrations: {MyApp.Migrations, :for_cell}`. At every wake the cell calls
`MyApp.Migrations.for_cell(cell_id)`, which returns the list for that cell
(the same rules as above; `[]` means no migrations). N is then the highest
version of that cell's list, and the rule above applies to each cell with its
own N. The list can change while the instance runs: a deploy of one group
takes effect at the next wake of its cells.

A function that raises, or returns an invalid list, fails the wake with
`{:error, {:bad_migrations, message}}`, and the cell is not touched.

### Extra arguments

`migrations: {MyApp.Migrations, :for_cell, [extra]}` calls
`MyApp.Migrations.for_cell(cell_id, extra)`: the function needs arity
`1 + length(extra_args)`, which is checked when the instance starts.
`{module, function}` is the same as `{module, function, []}`. Use it when
instances in one VM must choose different lists, for example by passing the
name of the instance. `mix alambique.check_deploy --migrations MODULE:FUNCTION`
cannot pass extra arguments: put the three-element tuple in
`config :alambique, :ops, migrations: ...` and run the task without the
option.

### Migrating a cell that is awake

A grown list takes effect when a cell wakes, and a busy cell may stay awake
for days. To migrate it now without hibernating it:

```elixir
{:ok, 3} = Alambique.migrate(MyApp.Cells, "tenant-7")
```

The call returns the version of the cell afterwards. Writes it already
accepted are uploaded first, writes that arrive during the call wait and are
served after it, and the migrations are uploaded as one batch. A cell nobody
owns answers `{:ok, :asleep}`: it migrates at its next wake, or pass
`wake: true` to wake it now. A migration that fails answers
`{:error, {:migration, version, reason}}` and the cell keeps serving at the
last version that applied; `{:schema_too_new, v}` and `{:bad_migrations,
message}` also leave it serving. Migrations hold the cell while they run, so
pass a `:timeout` that fits the longest one.

Steps 2 to 4 change as follows:

- The check has no single version. Run it with
  `mix alambique.check_deploy --migrations MyApp.Migrations:for_cell`, or from
  code with `migrations: {MyApp.Migrations, :for_cell}`. It lists every cell
  and calls the function for each, so it takes longer the more cells exist.
  The report names the cells below `max(N - window, 1)` of their own list:

  ```text
  Migrations are chosen per cell; each cell is compared with its own version.
    location default: versions 1, 3  <- too old: a.1
  Deploy refused: cells in default (a.1) are older than their schema version minus the window. Migrate them with an earlier release's sweeper or Alambique.sweep/2, then run the check again.
  ```

- The sweeper lists every version and calls the function for each listed cell
  in every round. It wakes only the cells below their own N, and `cells` in
  its summary counts only those (and the cells whose function failed).
- Cells at version 0 are in the index but the sweeper and the check skip them.
  They migrate when they next wake.

## Troubleshooting

- *`{:schema_too_new, v}` from calls.* The node runs code more than
  `migration_window` versions behind the cell. Roll the node forward, or raise
  `migration_window` if the older code really tolerates it.
- *The check keeps failing after the sweep.* Look at the sweeper's telemetry
  (`[:alambique, :sweeper, :cell]` with `result: :error`); a failing migration
  or an unreachable store leaves the cell listed. Frozen cells stay on their
  version until unfrozen.
- *`{:bad_migrations, message}` from calls.* The function for that cell
  raised or returned a list that is not contiguous from 1. The message names
  the function and the cell.
- *Exit status 2.* Read the message: missing `--migrations`, a store that
  cannot be reached, or a placement that fails.

## Next steps

- [telemetry.md](telemetry.md) lists the sweeper and index events.
- [how-to-operate-a-cell.md](how-to-operate-a-cell.md) covers freezing and
  moving cells.
