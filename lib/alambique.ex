defmodule Alambique do
  @moduledoc """
  SQLite cells on demand, replicated to S3-compatible storage.

  A cell is one SQLite database identified by a cell id (by default
  `[A-Za-z0-9_.-]`, at most 128 bytes, but not `.` or `..`; the placement can change the rule). It is awake on one node while it is used and asleep
  otherwise. Any node that runs the instance can serve any cell: calls are
  routed to the node that owns it, and the cell is woken when nobody does.

      children = [
        {Alambique,
         name: MyApp.Cells,
         store: {Alambique.Store.S3, s3_config},
         prefix: "myapp",
         data_dir: "/var/lib/myapp/cells",
         migrations: [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]}
      ]

      {:ok, _} = Alambique.execute(MyApp.Cells, "org-1", "INSERT INTO notes (body) VALUES (?)", ["hi"])
      {:ok, %{rows: rows}} = Alambique.query(MyApp.Cells, "org-1", "SELECT body FROM notes")

  A write returns after its transaction is stored durably, together with the
  other transactions that committed while the previous upload was running
  (group commit). A read returns what the cell has committed locally,
  including a write whose upload has not finished.

  ## Options

    * `:name` - atom that names the instance (default `Alambique`). Nodes that
      serve the same cells use the same name and placement.
    * `:store` - `{module, config}` implementing `Alambique.Store`, with
      `:prefix`: the shorthand for a single location,
      `placement: {Alambique.Placement.Single, store: store, prefix: prefix}`.
    * `:placement` - `{module, opts}` implementing `Alambique.Placement`: where
      each cell lives and how ids look (design §18). Give this or `:store`.
    * `:data_dir` - directory for the local copies of cells (required). It must
      be on a case-sensitive file system and belong to this instance alone.
      The start takes an exclusive lock on `<data_dir>/lock` and returns
      `{:error, {:data_dir_in_use, path}}` when another live instance, in this
      VM or another OS process, holds it, and `{:error,
      {:data_dir_case_insensitive, path}}` on a file system that ignores case.
      The library creates it and the directories of the cells with mode 0700.
    * `:prefix` - key prefix in the store with `:store` (default `"alambique"`).
    * `:node_id` - names the node in leases and in the owner shown by
      `lease/3` (default `Atom.to_string(node())`). Fencing does not depend on
      it being unique: every start of an instance also draws a random
      incarnation, and a lease of the same id and another incarnation belongs
      to another instance (design §5). Keep it unique to read the owners; a
      node that crashes and restarts waits `:lease_ttl_ms` plus
      `:skew_margin_ms` for its old leases.
    * `:join` - whether the node joins the members at start (default `true`).
      With `false` it serves the calls made on it, but other nodes do not choose
      it as an owner or a preferred node until `join/1` (design §8).
    * `:clock` - `{module, config}` implementing `Alambique.Clock`
      (default `{Alambique.Clock.System, nil}`).
    * `:lease_ttl_ms` - lease duration (default 30 000). The cell renews it at a
      third of that, give or take `:lease_renew_jitter`.
    * `:lease_renew_jitter` - fraction of that interval, 0 to 0.5, by which each
      renewal is moved at random so that cells woken together do not renew
      together (default 0.1).
    * `:skew_margin_ms` - tolerated clock skew between nodes (default 2 000).
      `:lease_ttl_ms` must be more than twice as large.
    * `:idle_timeout_ms` - time without calls before a cell hibernates
      (default 300 000). An awake cell renews its lease three times per
      `:lease_ttl_ms`, so a cell used once costs about 30 renewals (one store
      request each) before it sleeps; lower this for hosts that touch many
      cells once. It is part of the memory budget of a node: an awake cell
      keeps its page cache until it sleeps (see `:awake`, and
      `docs/how-to-size-a-node.md`).
    * `:op_timeout_ms` - time budget of one store operation such as an upload or
      a restore, capped by the lease (default 10 000). Every attempt of a store
      call runs under this deadline and is killed when it passes, so a store
      that never answers cannot hold a cell longer. A store with its own client
      timeout (`Alambique.Store.S3` `:receive_timeout`) should keep it below this.
    * `:pipeline_depth` - how many batches a cell may have uploading at once,
      1 to 16 (default 8; 1 uploads one batch at a time). Above 1 a cell
      encodes the next batch while the previous ones upload and answers
      callers in TXID order; the depth only applies while every member of the
      instance runs this release (design §6).
    * `:call_timeout_ms` - default timeout of the calls below (default 15 000).
    * `:hint_limit` - size of the owner-hint cache (default 10 000).
    * `:migrations` - schema migrations of the cells, a list, a module,
      `{module, function}` or `{module, function, extra_args}`, see below.
    * `:migration_window` - how many versions ahead of the code's version a
      cell may be and still be served, and how many versions behind it the
      deploy check lets live cells be (default 1).
    * `:moved_grace_ms` - how long after a cell moved to another location its
      objects stay at the old one before they are deleted, so readers that
      started before the move finish (default 600 000). Design §18.
    * `:deletion_grace_ms` - how long what retention and the cleanup of a moved
      cell remove stays in the store before the reaper deletes it, so it can
      still be restored or kept (default 86 400 000; 0 deletes at once). Design
      §9.1.
    * `:trash` - the reaper that does the deleting: a keyword list with
      `:reaper` (run it on this node, default `true`), `:interval_ms` (time
      between rounds, default 300 000), `:page_size` (manifests listed per
      request, default 100), `:concurrency` (objects deleted at once, default
      8), `:rate` (objects per second, default 200), `:timeout_ms` (per
      manifest, default 120 000) and `:batch_keys` (keys per manifest, default
      1 000). See `Alambique.Trash`.
    * `:scrub` - background check that restores sampled cells and verifies them
      (default: off, see `Alambique.Scrub`). A keyword list turns it on and sets
      `:interval_ms` (default 3 600 000), `:cells` (sampled per round, default
      10), `:concurrency` (default 1), `:rate` (cells per second, default 1),
      `:timeout_ms` (per restore, default 120 000), `:check`
      (`:integrity_check` or `:quick_check`, default `:integrity_check`),
      `:attempts` (default 2), `:page_size` (default 20) and `:prefix` (default
      `""`). Design §6.1.
    * `:sweeper` - background migration of idle cells (default: off). A keyword
      list turns it on and sets `:interval_ms` (time between rounds, default
      60 000), `:concurrency` (cells woken at once, default 2), `:rate` (cells
      per second, default 20), `:page_size` (cells listed per request, default
      100) and `:max_backoff_ms` (longest wait after failing rounds, default
      600 000).
    * `:local_cache` - bound on the local files of hibernated cells (default:
      on). `false` turns it off; a keyword list sets `:max_bytes` (the total size
      of the cell directories in `:data_dir` above which the files of the cells
      used longest ago are removed, default 21 474 836 480, 20 GiB) and `:interval_ms` (time between
      rounds, default 60 000). A cell with a process keeps its files; an evicted
      cell restores from the store at its next wake. Design §7.1.
    * `:compaction` - background compaction (default: on). `false` turns it
      off; a keyword list sets `:interval_ms` (tick, default 5 000),
      `:concurrency` (cells compacted at once, default 2), `:queue_limit`
      (cells waiting, default 10 000), `:max_age_ms` (a written cell is looked
      at after this long even if little was written, default 300 000),
      `:retry_ms` (after a failed pass, default 30 000), `:moved_scan_ms`
      (time between the scans of the store for cleanups of moved cells that
      wait, default 60 000), `:moved_scan_page` (markers listed per request,
      one request per tick, default 100), `:batch_factor`
      (ready cells taken per tick as a multiple of `:concurrency`, default 4),
      `:hold_bytes` (memory per instance for the files cells just replicated,
      so a pass of the owning node does not read them back from the store,
      default 33 554 432, 32 MiB; 0 turns holding off), `:hold_object_bytes`
      (larger files are not held, default 1 048 576) and the options of
      `Alambique.Compaction.run/3`, among them `:fanout`, `:max_level` (default
      2), `:snapshot_ratio` (default 2.0: a snapshot is taken when the bytes
      written since the last one reach this many times its size),
      `:snapshot_interval_ms` (default seven days, the limit when little is
      written), `:snapshot_keep_ms` (default three days) and
      `:delete_superseded` (default `true`: a merged file is deleted once a
      file of the next level replaces it). Design §9,
      "Held bytes"; sizes in `docs/how-to-size-storage.md`.
    * `:retention` - what compaction deletes (default: on, with the defaults of
      `Alambique.Retention.run/3`). `false` keeps everything; a keyword list
      sets its options. It runs inside the compactor, so it does nothing when
      `:compaction` is `false`.
    * `:lazy_restore` - waking a cell without a reusable local file serves it
      before the file is downloaded (design §16). `false` turns it off; a
      keyword list sets `:min_bytes` (a cell whose files are smaller is
      downloaded at once, default 4 194 304; 0 makes every restore lazy),
      `:concurrency` (background reads at once, default 4), `:batch_bytes`
      (size of one background read, default 4 194 304) and `:miss_pages` (pages
      read with a page a query is waiting for, default 16).
    * `:drain` - how `drain/2` and the stop of the instance hibernate the cells
      (design §8): a keyword list with `:concurrency` (cells hibernating at
      once, default 64), `:timeout_ms` (per cell, default 15 000), `:deadline_ms`
      (the whole drain, default 60 000), `:rounds`
      (passes over the cells still awake, default 3), `:page_size` (cells listed
      per request, default 1 000) and `:shutdown_ms` (time the supervisor gives
      the drain when the instance stops, default 75 000).
    * `:list_cells` - defaults of `list_cells/2`: a keyword list with
      `:page_size` (ids per page, default 100), `:max_limit` (largest `:limit`
      a call may ask for, default 1 000) and `:timeout_ms` (time budget of one
      call, default five times `:op_timeout_ms`).
    * `:routing` - how a call reaches the owner (design §8): `:hops` (forwarding
      hops, default 2), `:attempts` (tries on one node, default 3) and
      `:slack_ms` (extra wait for a remote node's own timeout, default 1 000).
    * `:wake` - `:fence_attempts` (tries when a writer took the fence TXID,
      default 5) and `:failure_backoff_ms` (how long a wake that failed for a
      reason a retry repeats, such as an unreadable restore, is remembered, and
      calls for the cell get the same error without a new restore; default
      5 000).
    * `:wal` - `:checkpoint_bytes` (size of the `-wal` file of an awake cell
      above which it moves the WAL into the database file once its uploads are
      acknowledged, default 67 108 864, 64 MiB; design §6) and
      `:checkpoint_deadline_ms` (how long `checkpoint/3` waits for the uploads
      before it answers `{:error, :busy}`, default 10 000), `:max_bytes` (budget
      for the sum of the `-wal` files of the awake cells of the node, default
      `nil`, none: when the sum reaches it, the cell with the largest WAL
      checkpoints as `checkpoint/3` does, then the next one, until the sum is
      under it; design §6) and `:budget_interval_ms` (how often the sum is
      checked, default 1 000).
    * `:awake` - limits that protect a node (design §7): `:max_cells` (cells
      with a process on this node, default 20 000; a call that would wake one
      more hibernates the least recently used cell that has no caller waiting,
      and gives `{:error, :overloaded}` if there is none), `:max_wakes` (cells
      waking at once, default 256; a wake above it waits in a FIFO queue of
      `:max_wake_queue` places, default four times `:max_wakes`, for up to
      `:wake_queue_ms`, default 500 and never past the call timeout, and then
      gives `{:error, :overloaded}`, as a full queue does) and
      `:max_pending` (calls waiting on one cell, queued or for an upload,
      default 10 000; more give `{:error, :busy}`, never passed; a call whose
      caller gave up before the cell reached it is not run). The memory of the awake
      cells is bounded by `:cache_kib` (page cache of each cell's SQLite
      connection, `PRAGMA cache_size`, default 512 KiB; SQLite's own default
      is 2 000) and by `:max_bytes` (default `nil`, no budget). With a
      budget, every awake cell reports what its connection holds (page cache,
      schema and prepared statements) after each call and message; a call
      that would wake a cell while the total is at or over `:max_bytes`
      hibernates the least recently used cells that have no caller waiting
      until it is under, and gives `{:error, :overloaded}` if none is left to
      hibernate. The cell that wakes may take the total past the budget as its
      cache fills; the next wake makes room.
    * `:relocate` - `:fence_attempts` (default 5), `:timeout_ms` (default
      60 000; the `:timeout` of `relocate/4` overrides it) and `:poll_ms` (wait
      between tries for a hibernating owner, default 100).
    * `:rewind` - for `rewind/4`: `:fence_attempts` (tries when the chain moved
      before the rewind landed, default 5), `:timeout_ms` (default 60 000),
      `:poll_ms` (default 100) and `:max_bytes` (default 268 435 456, 256 MiB;
      `nil` for no limit): a rewind of a larger database is refused, because
      its peak memory is about five times the database (200 MB gave about
      1 GB, design §20).
    * `:schema` - `:check_page` (cells listed per request by the deploy check,
      default 1 000) and `:listing_timeout_ms` (deadline of a schema index
      listing in the sweeper, the scrub and the deploy check, default five
      times `:op_timeout_ms`).
    * `:restore` - `:edge_bytes` (bytes read from each end of an LTX file to
      find its header, trailer and page index, default 65 536),
      `:fetch_concurrency` (requests a restore makes at once to read ahead of
      its walk, default 16) and `:prefetch_bytes` (most bytes of whole files a
      full restore reads ahead of its walk, default 67 108 864).
    * `:hydrator` - `:poll_ms` (time between background hydration passes,
      default 100).
    * `:sql` - limits on the statements (design §7): `:statement_timeout_ms`
      (the statements of callers, `query/5`, `execute/5` and the statements of
      `transaction/4`; a statement that runs longer is interrupted, default
      `:op_timeout_ms`, 10 000, so a caller with the default
      `:call_timeout_ms` sees the error and not a timeout; a call can lower
      it), `:script_timeout_ms` (migrations, `Alambique.Tx.script/2` and the
      library's own statements, default 60 000; a migration that exceeds it
      fails the wake with `{:error, {:migration, version, :statement_timeout}}`),
      `:max_result_rows` (default 100 000) and
      `:max_result_bytes` (text and blobs by length, other values as 8 bytes;
      default 67 108 864, 64 MiB). A statement over a limit gives
      `{:error, :statement_timeout}` or `{:error, :result_too_large}`.
      `:max_concurrent_statements` is the most reads (owner, replica and
      cross-cell) that run on the node at once, so that commits keep dirty IO
      schedulers (default the dirty IO schedulers minus two, at least 1); a
      read over it gets `{:error, :busy}` at once. The count is VM-wide,
      shared by every instance in the VM, and the limit is per instance: an
      instance refuses its reads when the shared count reaches its own limit,
      so instances in one VM should use the same value.
    * `:query` - defaults of `Alambique.Query`: `:cache_bytes` (default 1 GiB),
      `:concurrency` (cells read at once, default 16: the wait for the store
      dominates a cell, so the default does not follow the CPUs),
      `:max_statement_share` (share of `:max_concurrent_statements` one query
      may hold at once, above 0 up to 1, default 0.5: two queries fit
      together and slots remain for cells),
      `:timeout_ms` (per cell, default 30 000), `:page_size` (ids per listing
      request, default 1 000), `:stale_tmp_seconds` (age at which a
      temporary replica file is removed, default 600), `:restore_attempts`
      (restores of a replica when compaction or retention change the files
      being read, default 3) and `:reflink` (refresh a cached replica from a
      copy-on-write clone where the filesystem has them, default true; `false`
      always copies the file).
    * `:quarantine` - freezing a cell that crashes the VM (design §10; default:
      on). `false` turns it off; a keyword list sets `:threshold` (crashes that
      freeze a cell, default 3), `:window_ms` (only crashes this recent count,
      default 3 600 000), `:startup_timeout_ms` (longest the start of the
      instance waits to record the crashes of the previous VM, default 5 000),
      `:retry_ms` (time between tries for those it could not record, default
      30 000) and `:concurrency` (cells settled at start, default 8). See
      `crashes/2`.
    * `:replicas` - read replicas of cells this node does not own (design §21;
      default: off, and then reads never look for the switch of a cell).
      `false` keeps them off; a keyword list turns them on and sets
      `:max_cells` (replicas on the node, default 32; at the limit the one read
      longest ago makes way if nothing read it for `:marker_ttl_ms`, else the
      read goes to the owner), `:max_bytes` (the files of all replicas, default
      1 073 741 824; a replica that would not fit is not built), `:cache_kib`
      (SQLite page cache of a replica, default 4 096), `:poll_ms` (time between
      refreshes of a replica that is being read, default 250), `:poll_max_ms`
      (longest time between refreshes of a replica nobody reads: the interval
      grows to a quarter of the idle time up to this value, default 5 000),
      `:idle_ms` (a replica nobody read for this long
      stops and removes its file, default 600 000), `:marker_ttl_ms` (how long
      the node trusts the switch of `replicas/3`, default 5 000), `:on_stale`
      (`:refresh` waits for a refresh, `:owner` sends the read to the owner,
      default `:refresh`), `:refresh_timeout_ms` (longest a read waits for a
      refresh before it goes to the owner, default 2 000) and `:drift_ppm`
      (clock drift between two readings of the monotonic clock that the bound
      allows for, default 200). See `query/5` and
      `docs/how-to-read-replicas.md`.
    * `:after_ack` - `fun(cell, txid)` called in the cell process after each
      acknowledged batch, including the one that carries the migrations, and
      not after fences. A failure is logged and ignored.
    * `:feed` - options of the change feed, see below. `false` captures and
      publishes nothing, for a service with no consumer.

  ## Migrations

  `:migrations` is a list of `{version, migration}` with versions that are
  unique, positive and contiguous from 1, a module that exports
  `migrations/0` returning such a list, or `{module, function}` or
  `{module, function, extra_args}` to choose the list per cell (see below). A
  migration is a SQL script (several statements allowed) or a function of one
  `Alambique.Tx`. The highest version
  is the instance's schema version N. When a cell wakes, the migrations above
  its `PRAGMA user_version` run in version order, each in its own transaction
  that also sets `user_version`, and are uploaded as one batch before the cell
  serves. A failing migration aborts the wake; the migrations before it in
  the same wake are uploaded first, so the cell stays durable at the last
  version that applied.

  With `{module, function, extra_args}` (`extra_args` is a list; `{module,
  function}` means `[]`), every wake calls `apply(module, function, [cell_id |
  extra_args])`, which must return such a list (`[]` for none). The function
  must be exported with arity `1 + length(extra_args)`; the extra arguments
  let instances that share a VM pass their own name or settings. N is then the highest version of
  that cell's list, and the list may change between wakes. A raise, throw or
  exit, or an invalid list, fails the wake with
  `{:error, {:bad_migrations, message}}` and nothing is migrated. The index
  holds every cell that has woken, `sweep/2` calls the function for each
  listed cell at version 1 or later, and `check_deploy/2` for each live cell. A cell that is awake when
  the list grows migrates with `migrate/3`.

  A cell on a version above N is served as it is, without migrating, up to
  `:migration_window` (default 1) versions ahead; a cell further ahead fails
  the wake with `{:error, {:schema_too_new, version}}`. This lets code for N-1
  keep serving cells that code for N has migrated during a rollout, so
  migration N must only add (new tables, new nullable columns), and the
  contract step ships with a later release. Each cell that has woken has an
  entry in an index of schema versions in the store, which `check_deploy/2`,
  the sweeper and `list_cells/2` read. See `docs/how-to-roll-out-a-schema-change.md`.

  ## Change feed

  Every row change that a write makes is published as a feed object, so other
  systems can follow cells without waking them. Read it with
  `Alambique.Feed`; the design is in `docs/design.md`, section 13. Rows that
  migrations write are published too; DDL and tables dropped by `DROP TABLE` are not.
  `feed: false` turns the feed off: no table gets triggers, the cell keeps no
  feed rows and publishes no object. Each row change is otherwise stored three
  times in the first hours (in the table, in the feed row inside the cell's
  file and in the feed object); `docs/how-to-size-storage.md` has the numbers.
  `:feed` takes:

    * `:window_ms` - a batch is published under its own time only within this
      time of it (default 10 000), and later under the time of its
      publication.
    * `:retention_ms` - delete feed objects older than this (default
      604 800 000, seven days; `nil` keeps them for ever). Every node of the
      instance prunes.
    * `:prune_interval_ms` - time between prunes (default 3 600 000).
    * `:publish_retry_ms` - wait before publishing a batch again after a
      failure (default 1 000). It doubles after each failure up to
      `:publish_retry_max_ms`. Publication runs in a process of its own and
      never delays a commit.
    * `:publish_retry_max_ms` - longest wait between attempts (default
      30 000, at least `:publish_retry_ms`).
    * `:max_backlog_batches` - batches the cell hands to its publisher at a
      time (default 256); the rest waits in the cell's file.
    * `:object_batches` - batches the publisher puts in one feed object when
      several wait (default 1, at most `:max_backlog_batches`). See
      `docs/how-to-consume-the-change-feed.md`.
    * `:linger_ms` - how long the publisher waits for more batches while a
      group is shorter than `:object_batches` (default 0: it never waits).
      A record is visible at most this much later; keep it well under
      `:window_ms`.
    * `:stop_publish_ms` - how long a cell that stops with its lease still
      valid waits for its publisher to put the batches it holds, ignoring
      `:linger_ms` (default 5 000; 0 turns it off). What is not published in
      time stays in the cell's file for the next owner.
    * `:shards` - prefixes the feed of each location is spread over (default
      16, at most 1 000). It is fixed for as long as the feed exists: consumers
      must use the same value, and a cell changes shard when it changes.
    * `:internal_prefixes` - prefixes of the host's own tables (a job queue, for
      example) whose changes stay out of the feed (default `[]`). A table whose
      name starts with one of them gets no triggers, like the library's
      `_alambique_` tables and SQLite's `sqlite_` tables, which are always
      internal. The match is on the exact, case-sensitive name. Queries,
      migrations and `Alambique.Query` still see these tables. Use the same
      value on every node: a node with another value regenerates the
      triggers when it wakes or changes the schema of a cell.
    * `:tables` - a map of table name to what the feed captures of it (default
      `%{}`): `true` the whole row, `false` nothing, `[omit: columns]` every
      column but those, `[only: columns]` the key and those columns. The
      columns of the key are always captured. An omitted column is never
      written to the cell's feed table and is absent from the record's
      `values`. An `UPDATE` that changes only omitted columns leaves no
      record. Names the table lacks are ignored. Use the same value on every
      node, as for `:internal_prefixes`.
    * `:max_object_bytes` - the most an object may inflate to when it
      is read (default 268 435 456); a larger one is rejected.
    * `:other_tables` - what the feed captures of a table `:tables` does not
      name: `:capture` (default) the whole row, `:skip` nothing, which makes
      `:tables` an allow list.

  ## Calls

  Every call accepts the option `:timeout` in milliseconds. The timeout
  applies to each hop. Calls return `{:error, reason}` when the cell cannot
  serve, for example `{:error, {:unavailable, owner, expires_at_ms}}` while a
  lease held by a node that is not a member has not expired (see `lease/3`
  for what it promises), or `{:error, :timeout}` when the outcome of a write
  is unknown.

  A full local disk is `{:error, :disk_full}`, from a statement, a transaction,
  a migration, a wake or a read replica alike (design §6.2). The call did not
  run and is safe to repeat once space is free; the cell keeps serving and
  holds everything it acknowledged.

  A cell that stops while a write's batch is in flight (`:lease_lost`,
  `:fenced`, `:lease_expired`, a failed upload) answers that write
  `{:error, :outcome_unknown}`: the batch may be in the store, so do not repeat
  the write unless it is idempotent. The refusals `{:error, :fenced}`,
  `{:error, :lease_lost}` and `{:error, :lease_expired}` on a write mean it did
  not run, and it is safe to repeat. Reads get the reason either way.

  A call can run on another node. If the connection to that node fails after
  the call was sent, a write may have run: `execute/5` and `transaction/4`
  answer `{:error, :outcome_unknown}`, as for a timeout, and never repeat it.
  A call counts as sent once it was issued to a node that was connected (or
  that connected on demand); one that was not sent is routed again. A
  connection that fails between that check and the send is reported as sent.
  Reads, `touch/3`, `hibernate/3` and `migrate/3` are repeated in both
  cases. With
  `idempotent: true` a write is repeated too, so it may run more than once:
  use it for statements that give the same result when repeated.

  Functions given to `transaction/4` may run on another node: their module
  must be loaded on every node of the cluster, as in a release.
  """

  alias Alambique.{Config, Drain, Lease, Membership, Placement, Replica, Router, Schema, Tx}
  alias Alambique.Replication.Retry

  @type instance :: atom
  @type cell :: String.t()
  @type result :: %{columns: [String.t()], rows: [[term]]}

  @doc "Child spec for `{Alambique, opts}`. Raises `ArgumentError` for invalid options."
  @spec child_spec(keyword) :: Supervisor.child_spec()
  def child_spec(opts) do
    config = Config.build(opts)

    %{
      id: {__MODULE__, config.name},
      start: {Alambique.Instance, :start_link, [config]},
      type: :supervisor
    }
  end

  @doc """
  Runs a read statement on the cell and returns its columns and rows.

  Parameters are bound as in `Alambique.Tx`. The statement may only read: one
  that would write (DML, DDL, `ATTACH`, `VACUUM`, a transaction or savepoint
  statement, a `PRAGMA` that sets a value or checkpoints) gives
  `{:error, :read_only}`. Only the `PRAGMA`s that describe the schema
  (`table_info(t)`, `index_list(t)`, `foreign_key_list(t)` and so on) and
  those without an argument are accepted.

  Other errors of the statement: `{:error, :statement_timeout}` when it ran
  longer than the `:sql` option `:statement_timeout_ms` (it is interrupted
  and the cell keeps serving), `{:error, :result_too_large}` when its result
  has more than `:max_result_rows` rows or `:max_result_bytes` bytes,
  `{:error, {:bad_params, message}}` for a wrong number or type of
  parameters, `{:error, :disk_full}` when the disk has no room for what the
  statement needs (temporary files of a sort, for example), and
  `{:error, message}` with SQLite's message for the rest.

  Options: `:timeout`, `:statement_timeout_ms`, `:verify_lease` (default
  `false`) and `:consistency`. `:statement_timeout_ms` is a positive integer
  that lowers the deadline of this statement; it never raises it above the
  `:sql` option of the same name (a value above it is the `:sql` one).
  Anything else is `{:error, :bad_option}`. It applies to the owner's read, not
  to `consistency: {:bounded, ms}`, whose deadline is `:timeout`.

  `consistency: :strong` (the default) is the read described below, answered
  by the owner. `consistency: {:bounded, ms}` (a positive integer) lets a node
  that does not own the cell answer from a read replica (design §21): the
  result contains every write whose caller was told it was done more than `ms`
  milliseconds before the read started. It is served from the replica when the
  node has one for the cell, `replicas/3` is on for it and the replica is
  fresh enough; otherwise by the owner, so the call works on any node. A node that owns the cell answers
  from the cell as for `:strong`. `{:bounded, ms}` with `verify_lease: true`,
  or any other value, gives `{:error, :bad_option}`. A caller that alternates
  between the owner and a replica can see time go back by up to `ms`, and
  `{:bounded, 0}` is not accepted: the bound is a positive number of
  milliseconds. See `docs/how-to-read-replicas.md`.

  A plain read is answered from the cell process as long as it believes it
  owns the cell. After `take_over/4` (or a partition) the old owner can still
  answer a read that misses writes the new owner acknowledged, until it learns
  of the new epoch: see "Stale reads" in design §5 for the bound. With
  `verify_lease: true` the cell reads the lease object from the store after it
  ran the statement (one `GET`, in the cell process, per read or per batch of
  reads waiting behind a write) and answers only if the stored lease is still
  its own acquisition. Otherwise it stops serving without answering and the
  router sends the call to the new owner, so the read sees every write
  acknowledged before the call was issued. If the lease cannot be read the
  answer is `{:error, {:lease_unreadable, reason}}` and the cell keeps serving.
  """
  @spec query(instance, cell, String.t(), list, keyword) :: {:ok, result} | {:error, term}
  def query(instance, cell, sql, params \\ [], opts \\ [])
      when is_binary(sql) and is_list(params) do
    case {consistency(opts), statement_timeout(opts)} do
      {_, :error} ->
        {:error, :bad_option}

      {:strong, limit} ->
        request =
          if opts[:verify_lease] == true,
            do: {:checked_query, sql, params, limit},
            else: {:query, sql, params, limit}

        Router.call(instance, cell, request, opts[:timeout])

      {{:bounded, ms}, _} ->
        Alambique.Replica.Read.query(
          Config.fetch!(instance),
          cell,
          sql,
          params,
          ms,
          opts[:timeout]
        )

      {:error, _} ->
        {:error, :bad_option}
    end
  end

  # The per-call `:statement_timeout_ms`: `nil` when absent.
  defp statement_timeout(opts) do
    case Keyword.fetch(opts, :statement_timeout_ms) do
      :error -> nil
      {:ok, ms} when is_integer(ms) and ms > 0 -> ms
      {:ok, _} -> :error
    end
  end

  defp consistency(opts) do
    case {Keyword.get(opts, :consistency, :strong), opts[:verify_lease]} do
      {:strong, _} -> :strong
      {{:bounded, ms}, verify} when is_integer(ms) and ms > 0 and verify != true -> {:bounded, ms}
      _ -> :error
    end
  end

  @doc """
  Runs one statement as a write and returns after it is durable. Rows come
  back for statements with a `RETURNING` clause.

  `ATTACH` and `DETACH` are refused on a cell connection. The errors of
  `query/5` for a statement that runs too long, returns too much or has bad
  parameters apply here too (`{:error, :statement_timeout}`,
  `{:error, :result_too_large}` for the `RETURNING` rows,
  `{:error, {:bad_params, message}}`); the write then rolls back and the cell
  keeps serving. So does `{:error, :disk_full}`, when the disk has no room for
  the write.

  Options: `:timeout`, `:statement_timeout_ms` (as in `query/5`),
  `:idempotent` (default `false`, see "Calls") and `:isolated` (default
  `false`, see `transaction/4`).
  """
  @spec execute(instance, cell, String.t(), list, keyword) :: {:ok, result} | {:error, term}
  def execute(instance, cell, sql, params \\ [], opts \\ [])
      when is_binary(sql) and is_list(params),
      do: write(instance, cell, &{:execute, sql, params, &1}, opts)

  @doc """
  Runs `fun` inside one SQLite transaction on the cell and returns
  `{:ok, value}` after the transaction is durable.

  `fun` receives an `Alambique.Tx` and runs in the cell process, on the node
  that owns the cell, so it should be quick and must not call back into the
  same cell. It rolls back, and the call returns `{:error, reason}`, when it
  raises, throws, or calls `Alambique.Tx.rollback/2`. A statement that
  `Alambique.Tx.execute/3` or `query/3` runs is bound by the `:sql` limits;
  one that is interrupted at its deadline returns
  `{:error, :statement_timeout}` and SQLite rolls the transaction back, so the
  function should return the error (the call then answers with it).

  Options: `:timeout`, `:statement_timeout_ms` (as in `query/5`, for each
  statement of `fun`), `:idempotent` (default `false`, see "Calls") and
  `:isolated` (default `false`). An isolated transaction owns its batch: the
  commits still waiting for their upload are uploaded first, and its own
  batch is uploaded before the cell runs another call. Inside it,
  `Alambique.Tx.before/1` gives the TXID of the state just before it, which
  `position/3` also gives from outside. It costs one extra upload.
  """
  @spec transaction(instance, cell, (Tx.t() -> term), keyword) :: {:ok, term} | {:error, term}
  def transaction(instance, cell, fun, opts \\ []) when is_function(fun, 1),
    do: write(instance, cell, &{:transaction, fun, &1}, opts)

  # Writes always go to the owner: `consistency:` belongs to `query/5`.
  defp write(instance, cell, request, opts) do
    limit = statement_timeout(opts)

    if Keyword.has_key?(opts, :consistency) or limit == :error do
      {:error, :bad_option}
    else
      request = request.(limit)
      request = if opts[:isolated] == true, do: {:isolated, request}, else: request
      Router.call(instance, cell, request, opts[:timeout], opts[:idempotent] == true)
    end
  end

  @doc """
  Where the cell's replicated chain ends: `{:ok, %{txid: txid, epoch: epoch,
  data: data, awake: awake}}` (design §13).

  An awake cell uploads the commits that are waiting first, then answers, so
  the answer is a cut: every write committed before the call is at or below
  `txid`, every write committed after is above. `epoch` is that of the newest
  file. `data` is the data TXID: the TXID of the newest commit that changed
  data (a write, a migration or a rewind), which a wake does not move
  although it takes a TXID (design §6). No data was committed after TXID `t`
  exactly when `data <= t`. An asleep cell is not woken: the answer is read from the store
  (`awake: false`, `txid: 0` for a cell without data). The call is routed like
  any other. Options: `:timeout`.
  """
  @spec position(instance, cell, keyword) ::
          {:ok,
           %{
             txid: non_neg_integer,
             epoch: non_neg_integer,
             data: non_neg_integer,
             awake: boolean
           }}
          | {:error, term}
  def position(instance, cell, opts \\ []),
    do: Router.call(instance, cell, :position, opts[:timeout])

  @doc """
  Replaces the state of the cell with its state at `target`: `{:txid, n}` or
  `{:timestamp, ms}` (design §20). Everything committed after the target is
  undone, and writes made after the call returns are kept.

  The cell is fenced first: its owner hibernates (or its lease expires), and
  until the rewind ends calls for it get `{:error, {:unavailable, "rewind",
  _}}`. The restored database then becomes the cell's state at a new TXID, in
  one conditional upload, so a crash leaves the old state or the new one and
  no reader sees anything between. The cell is not woken; its next call does.
  A frozen cell can be rewound and stays frozen. The TXIDs of the undone
  history stay in the store while retention keeps them, so a rewind can itself
  be undone with a target before it.

  Options:

    * `:keep` - `:internal` (default) carries the tables named by the
      instance's `feed: [internal_prefixes: ...]` from the current state into
      the rewound one, with their indexes, triggers and views, so the host's
      own bookkeeping does not go back. `:none` rewinds every table.
    * `:schema` - `:same` (default) refuses a target whose `PRAGMA
      user_version` differs from the current one. `:migrate` accepts it and
      the next wake migrates the cell forward again; it is refused together
      with carried tables, which are not migrated.
    * `:if_head` - the head TXID (see `position/3`) the caller expects.
      Another head gives `{:error, {:head_moved, head}}` before the lease is
      taken and again before the upload. Pass the value of `position/3`
      before the first try to repeat a call whose answer was lost. `{txid,
      :data}` instead refuses with `{:error, {:data_written, data_txid}}` when
      data was committed after `txid` (the `data` of `position/3` is greater),
      and ignores wakes, which commit no data: record `position/3` and pass
      its `{txid, :data}` to undo only if nothing was written since.
    * `:timeout` - overall deadline in milliseconds (default
      `rewind: [timeout_ms:]`, 60 000).

  The change feed gets an `Alambique.Feed.Rewind` record (design §13).

  Returns `{:ok, %{txid: new_txid, from: old_head, to: restored_txid, epoch:
  epoch, schema_version: version, kept: [table]}}`. Errors:
  `{:behind_retention, oldest_txid}` (restore cannot reach the target),
  `{:target_not_behind, head}` (the target is the head or ahead),
  `{:head_moved, head}`, `{:data_written, data_txid}`, `:bad_if_head`,
  `{:schema_differs, target_version, current_version}`,
  `:keep_needs_same_schema`, `{:keep_unsupported, table}` (a virtual table),
  `{:too_large, bytes, max_bytes}` (the target or the current state is larger
  than `rewind: [max_bytes:]`), `{:moved, location}`, `{:owned, owner, expires_at_ms}` (another operation
  holds the cell), `:lease_lost`, `{:fence, :txid_taken}` (the chain kept moving
  for `rewind: [fence_attempts:]` tries), `:outcome_unknown` (the upload may
  have landed: read `position/3`, or repeat with `:if_head`), `{:capture, reason}`
  (the segment could not be encoded here, nothing was sent), `{:attach, reason}`
  (the file of the current state was gone), `:no_history`,
  `:bad_target` and store errors. It writes about the size of the database
  and needs room in `:data_dir` for two copies of it.
  """
  @spec rewind(instance, cell, {:txid, pos_integer} | {:timestamp, integer}, keyword) ::
          {:ok, map} | {:error, term}
  def rewind(instance, cell, target, opts \\ []),
    do: Alambique.Rewind.run(Config.fetch!(instance), cell, target, opts)

  @doc """
  Checks that the code of an instance can read every live cell (design §17).

  It lists the schema index of every location of the placement, which costs a
  few requests per location whatever the number of cells, and compares the
  oldest live version with the oldest version the code accepts: its schema
  version minus `:migration_window` (at least 1). `instance` is the name of a
  running instance, or the keyword list of its options (`:placement` or
  `:store`, `:migrations`, `:migration_window`), which lets a deploy script
  check before the new code runs. Run it against the same placement
  configuration as production: another one lists other stores.

  Returns `{:ok, report}` when the code can read every cell,
  `{:error, {:too_old, report}}` when some location holds a cell older than
  that, and `{:error, reason}` when a location cannot be listed. The report is
  `%{code_version, oldest_readable, locations: [%{location, versions,
  too_old}]}`. With per-cell migrations (`{module, function[, extra_args]}`) the check lists
  every cell of every version and calls the function for each, comparing the
  version of the cell with `max(N - window, 1)` of its own list; the report has
  `code_version: nil` and `oldest_readable: nil`, and each location also has
  `too_old_cells`, the ids of the cells that are too old. Option: `:timeout_ms`
  (of each listing request).
  """
  @spec check_deploy(instance | keyword, keyword) ::
          {:ok, Alambique.Schema.report()} | {:error, term}
  def check_deploy(instance_or_options, opts \\ [])

  def check_deploy(instance, opts) when is_atom(instance),
    do: Alambique.Schema.check(Config.fetch!(instance), opts)

  def check_deploy(options, opts) when is_list(options) do
    options
    |> Keyword.put_new(:data_dir, System.tmp_dir!())
    |> Config.build()
    |> Alambique.Schema.check(opts)
  end

  @doc """
  Runs one pass of the sweeper (design §17) now: wakes and hibernates the
  cells that the schema index lists below the instance's schema version (with
  per-cell migrations: below their own, found by calling the function for each
  listed cell), so they migrate. Frozen cells are skipped and cells that were awake stay awake.
  The instance's `:sweeper` option runs the same pass in the background.

  Returns `{:ok, %{cells, migrated, stale, skipped, errors}}`. Options:
  `:concurrency`, `:rate`, `:page_size` and `:timeout_ms`, overriding the
  instance's `:sweeper` settings.
  """
  @spec sweep(instance, keyword) :: {:ok, Alambique.Schema.Sweeper.summary()} | {:error, term}
  def sweep(instance, opts \\ []),
    do: Alambique.Schema.Sweeper.run(Config.fetch!(instance), opts)

  @doc """
  Runs one scrub round (design §6.1) now: restores sampled cells without
  waking them and checks their checksums and SQLite's integrity. Works whether
  or not the instance's `:scrub` option is set; options override its settings
  (see `Alambique.Scrub`).

  Returns `{:ok, %{cells, ok, empty, failed, failures}}`.
  """
  @spec scrub(instance, keyword) :: {:ok, Alambique.Scrub.summary()} | {:error, term}
  def scrub(instance, opts \\ []), do: Alambique.Scrub.run(Config.fetch!(instance), opts)

  @doc """
  Puts the cell to sleep: uploads what is pending, checkpoints, releases the
  lease and stops its process. `:ok` also when the cell was not awake.

  When the store refuses the release, the cell is asleep and its data is
  durable, but the lease stays until it expires: the answer is
  `{:error, {:release_failed, reason}}`, so a caller that waits for another
  node to take the cell knows it must wait for the expiry.
  """
  @spec hibernate(instance, cell, keyword) :: :ok | {:error, term}
  def hibernate(instance, cell, opts \\ []),
    do: Router.call(instance, cell, :hibernate, opts[:timeout])

  @doc """
  Moves the WAL of an awake cell into its database file now, without
  hibernating it (design §6): the cell encodes what is pending, waits until
  the store has acknowledged every upload, then runs `wal_checkpoint(TRUNCATE)`,
  so the `-wal` file ends empty. It never truncates a frame that is not
  replicated. The cell answers no other call meanwhile.

  Returns `:ok`; `{:error, :busy}` when the uploads were not acknowledged
  within the deadline or SQLite could not finish (the cell keeps serving, the
  WAL stays, and the call can be repeated); or `:asleep` when nobody owns the
  cell: it is not woken, and its hibernation left no WAL.

  Options: `:deadline_ms` (default `:wal` `:checkpoint_deadline_ms`) and
  `:timeout` (milliseconds, default `:call_timeout_ms`, or the deadline plus
  `:routing` `:slack_ms` when that is longer).
  """
  @spec checkpoint(instance, cell, keyword) :: :ok | :asleep | {:error, term}
  def checkpoint(instance, cell, opts \\ []) do
    [deadline_ms: deadline, timeout: timeout] =
      opts |> Keyword.validate!(deadline_ms: nil, timeout: nil) |> Enum.sort()

    config = Config.fetch!(instance)
    deadline = deadline || config.wal.checkpoint_deadline_ms

    unless is_integer(deadline) and deadline > 0,
      do:
        raise(ArgumentError, ":deadline_ms must be a positive integer, got: #{inspect(deadline)}")

    timeout = timeout || max(config.call_timeout_ms, deadline + config.routing.slack_ms)
    Router.call(instance, cell, {:checkpoint, deadline}, timeout)
  end

  @doc """
  Migrates an awake cell now, instead of at its next wake. It runs the same
  step as a wake with the list chosen now (see "Migrations"): writes
  acknowledged or queued before the call are uploaded first, the pending
  migrations run each in its own transaction and are uploaded as one batch,
  and the cell keeps serving without hibernating. Calls that arrive meanwhile
  wait, so a long migration holds the cell: size `:timeout`.

  Returns `{:ok, version}`, the `PRAGMA user_version` of the cell afterwards
  (the version it already had when nothing was pending). A cell nobody owns is
  not woken by default and answers `{:ok, :asleep}`: it migrates at its next
  wake. With `wake: true` it is woken, which migrates it, and the answer is
  its version.

  Errors: `{:error, {:bad_migrations, message}}` when the function that
  chooses the list fails, `{:error, {:schema_too_new, version}}` when the cell
  is further ahead than the window allows, and `{:error, {:migration,
  version, reason}}` when a migration fails. They leave the cell serving. A
  failing migration rolls back; the ones of the same call before it stay
  applied and are uploaded, so the cell is at the version before the failing
  one. A failed upload stops the cell, as a failed commit does, and the call
  gives the reason. A cell woken with `wake: true` gives the errors of a wake.

  Options: `:wake` (default `false`) and `:timeout` (milliseconds, default
  `:call_timeout_ms`).
  """
  @spec migrate(instance, cell, keyword) :: {:ok, non_neg_integer | :asleep} | {:error, term}
  def migrate(instance, cell, opts \\ []) do
    [timeout: timeout, wake: wake] =
      opts |> Keyword.validate!(timeout: nil, wake: false) |> Enum.sort()

    unless is_boolean(wake),
      do: raise(ArgumentError, ":wake must be a boolean, got: #{inspect(wake)}")

    Router.call(instance, cell, {:migrate, wake}, timeout)
  end

  @doc """
  Takes the cell over on this node now, without waiting for its lease to
  expire (design §5). Use it when a failure detector or the platform suspects
  or knows that the owner is gone. `reason` is any term; it goes to the
  telemetry event `[:alambique, :lease, :take_over]` (as `cause`) and to a log
  line, never to the store.

  The takeover raises the epoch of the lease with a conditional write, then
  wakes the cell here as any wake does: restore, fence at the next TXID,
  migrations. It returns `{:ok, node()}` after the fence is stored; a cell
  already awake on this node answers at once. From then on every upload of
  the old owner collides with the fence and its cell stops with `:fenced`, so
  no write it acknowledged is lost and none is applied twice, also when the
  suspicion was wrong and the old owner is alive. The old owner stops
  answering reads within a bounded time ("Stale reads" in design §5); use
  `query/5` with `verify_lease: true` for reads that must not be stale.

  If the previous owner's node still runs the instance, it is told at once and
  its cell checks the lease immediately. That only shortens the stale-read
  window.

  Options: `:timeout` (milliseconds, default `:call_timeout_ms`) and `:epoch`,
  the epoch of the lease the suspicion is about (`lease/3` and
  `mix alambique.inspect` show it): if the cell has a newer epoch, someone
  else took it meanwhile and the call gives `{:error, {:epoch_changed,
  epoch}}` without changing anything.

  Errors are those of a wake, and also `{:error, {:contended, owner, epoch}}`
  when the lease changed between its read and its write (the caller may try
  again), `{:error, {:moved, location}}` and `{:error, :draining}` while this
  node drains.
  """
  @spec take_over(instance, cell, term, keyword) :: {:ok, node} | {:error, term}
  def take_over(instance, cell, reason, opts \\ []),
    do: Router.take_over(instance, cell, reason, opts)

  @doc """
  Takes this node out of service gracefully (design §8). It leaves the group
  of members first, so other nodes stop choosing it and the calls they
  forward here are routed again; from then on this node refuses to wake cells
  (`{:error, :draining}`, or the call goes to another member). Then it
  hibernates every cell with a process here, a few at a time: pending writes
  are uploaded and the leases released, so other nodes serve the cells at once
  instead of after the lease expires.

  Returns `{:ok, %{hibernated: n, errors: [{cell, reason}]}}`. A cell that
  could not be hibernated stays in `errors` (its lease then expires as after a
  crash), and so does a cell whose lease could not be released, with
  `{:release_failed, reason}`: it is asleep, but other nodes wait for the lease
  to expire. `hibernated` counts only the cells that were released.

  The drain ends at the deadline: cells that were not started are not
  touched, and every cell still awake is reported as `{cell, :deadline}`. They
  are neither killed nor hibernated by force: the node keeps serving them, and
  `drain/2` again, or stopping the instance, hibernates them. Another node
  takes them when their leases expire.

  Stopping the instance runs the same drain. Repeating the call is safe.
  Options: `:concurrency`, `:timeout` (per cell, in milliseconds) and
  `:deadline` (the whole drain, in milliseconds), overriding `:concurrency`,
  `:timeout_ms` and `:deadline_ms` of the instance's `:drain` option.
  `join/1` brings the node back into service, and `drain_status/1` tells how
  far a drain is. The telemetry events are in `docs/telemetry.md`.
  """
  @spec drain(instance, keyword) ::
          {:ok, %{hibernated: non_neg_integer, errors: [{cell, term}]}}
  def drain(instance, opts \\ []), do: Drain.run(Config.fetch!(instance), opts)

  @doc """
  How far the drain of this node is (design §8), readable while `drain/2`
  runs in another process. It answers:

    * `:draining` - the node left the members and refuses to wake cells
    * `:running` - a drain is in progress
    * `:started_at_ms` and `:finished_at_ms` - when the last drain began and
      ended, by the instance's clock; `nil` before it began or while it runs
    * `:cells_at_start` - cells with a process when it began
    * `:done` - cells hibernated with their lease released
    * `:errors` - `[{cell, reason}]` of the cells that failed so far
    * `:left` - cells with a process now

  A node that never drained answers `draining: false`, `running: false` and no
  times. The numbers belong to the last drain: a new one starts them again.
  """
  @spec drain_status(instance) :: %{
          draining: boolean,
          running: boolean,
          started_at_ms: integer | nil,
          finished_at_ms: integer | nil,
          cells_at_start: non_neg_integer,
          done: non_neg_integer,
          errors: [{cell, term}],
          left: non_neg_integer
        }
  def drain_status(instance), do: Drain.status(Config.fetch!(instance))

  @doc """
  Makes this node a member of the instance (design §8): other nodes can choose
  it as an owner or a preferred node from now on. Use it after starting with
  `join: false`, once everything the node needs to serve calls is running, or
  to bring a drained node back (`drain/2`). Returns `:ok` at once if the node
  is already a member.
  """
  @spec join(instance) :: :ok
  def join(instance), do: Membership.join(Config.fetch!(instance))

  @doc """
  Moves the cell to `node`: the current owner hibernates it and `node` wakes
  it. `node` must run the instance.
  """
  @spec move(instance, cell, node, keyword) :: {:ok, node} | {:error, term}
  def move(instance, cell, node, opts \\ []) when is_atom(node),
    do: Router.move(instance, cell, node, opts[:timeout])

  @doc """
  Moves the cell to another location of the placement (design §18). `to` is a
  `t:Alambique.Location.t/0` or the id of one of the placement's locations.

  The cell is hibernated, fenced at its location, snapshotted at the
  destination, and the old location is marked as moved before the placement
  learns of the new one, so only one location accepts its writes at any
  moment. The cell is unavailable for the short time between the mark and
  `c:Alambique.Placement.commit_move/4`. A failure or crash leaves the cell at
  its old location, or finished-to-be: call `relocate/4` again with the same
  destination until it returns `{:ok, location}`. Calls to the cell during the
  move get `{:error, {:unavailable, "relocate", _}}`.

  The last step removes what the cell left at the old location once
  `:moved_grace_ms` passed since the move (see `cleanup_moved/4`); the lease
  there stays, marked as moved. A cell can move back to a location it left
  once that cleanup is over, even while the data it marked is still there for
  `:deletion_grace_ms`: the new snapshot has a TXID above every object left
  there, so it covers them, and the reaper deletes the old ones later. Until
  a cleanup finished the answer is `{:error, :destination_not_cleaned}` (the
  call queues the cleanup in the compactor; try again after the grace); until
  it settled,
  `{:error, {:destination_settling, ready_at_ms}}` (wait until `ready_at_ms`
  on the instance's clock, without cleaning again, which would restart the
  wait). `{:error, :destination_not_empty}` means data at a destination with
  no moved lease, and `{:error, {:destination_ahead, txid}}` a file beyond
  the restored state, which the move refuses instead of colliding. Options:
  `:timeout` in milliseconds (default 60 000).
  """
  @spec relocate(instance, cell, Alambique.Location.t() | String.t(), keyword) ::
          {:ok, Alambique.Location.t()} | {:error, term}
  def relocate(instance, cell, to, opts \\ []),
    do: Alambique.Relocate.run(Config.fetch!(instance), cell, to, opts)

  @doc """
  Removes what `cell` left at the location `from` (a
  `t:Alambique.Location.t/0` or its id) after a move (design §18): segments and
  snapshots are marked for deletion (`:deletion_grace_ms`), the fixed keys
  (`frozen`, `trashed`, `crashes`) and the schema index entry go at once. The
  lease stays, marked as moved, because nodes with an old view of the placement
  rely on it to refuse the cell. `relocate/4` and the compactor (when enabled)
  call this after the grace period, and the compactor finds cleanups left
  pending by a restart in the store; call it to run a cleanup early with a
  smaller `:grace_ms`, or with compaction off. Pending marked
  data does not block a move back: the returning snapshot covers it and the
  reaper deletes it later.

  Returns `{:ok, %{result: :cleaned, objects: n}}` (marked and deleted
  objects), or `{:ok, %{result: :waiting, ready_at_ms: ms}}` while the move
  grace runs. Repeating it is safe. It refuses with `{:error,
  :live_location}` for the location the placement gives the cell, and
  `{:error, :not_moved}` for one whose lease does not say the cell moved.
  `{:error, :contended}` means another cleanup started meanwhile. Options:
  `:grace_ms` (default: the instance's `:moved_grace_ms`).
  """
  @spec cleanup_moved(instance, cell, Alambique.Location.t() | String.t(), keyword) ::
          {:ok, map} | {:error, term}
  def cleanup_moved(instance, cell, from, opts \\ []),
    do: Alambique.Relocate.Cleanup.run(Config.fetch!(instance), cell, from, opts)

  @doc """
  Freezes the cell: it hibernates now and refuses to wake until `unfreeze/3`
  (design §10). A cell that cannot be reached to hibernate stays frozen and
  hibernates at its next lease renewal; the call then returns
  `{:error, {:hibernate, reason}}`.
  """
  @spec freeze(instance, cell, keyword) :: :ok | {:error, term}
  def freeze(instance, cell, opts \\ []) do
    config = Config.fetch!(instance)

    with {:ok, ctx} <- repl_ctx(config, cell),
         :ok <- Alambique.Freeze.freeze(ctx, cell) do
      case hibernate(instance, cell, opts) do
        :ok -> :ok
        {:error, reason} -> {:error, {:hibernate, reason}}
      end
    end
  end

  @doc """
  Lets a frozen cell wake again and forgets its recorded crashes, so a cell
  frozen by the quarantine starts counting from zero. `:ok` also when it was
  not frozen.
  """
  @spec unfreeze(instance, cell) :: :ok | {:error, term}
  def unfreeze(instance, cell) do
    config = Config.fetch!(instance)

    with {:ok, ctx} <- repl_ctx(config, cell), do: Alambique.Freeze.unfreeze(ctx, cell)
  end

  @doc "Whether the cell is frozen."
  @spec frozen?(instance, cell) :: {:ok, boolean} | {:error, term}
  def frozen?(instance, cell) do
    config = Config.fetch!(instance)

    with {:ok, ctx} <- repl_ctx(config, cell), do: Alambique.Freeze.frozen?(ctx, cell)
  end

  @doc """
  The crashes of the VM recorded for the cell (design §10): `{:ok, nil}` when
  there are none, else `{:ok, %{total, recent}}`. `total` counts all of them
  since the counter was last reset by `unfreeze/2`; `recent` lists the newest
  ones that were inside the `:quarantine` window when the last was recorded,
  as `%{id, at_ms, node, kinds}`. A cell is frozen when `recent` reaches the
  threshold.
  """
  @spec crashes(instance, cell) :: {:ok, Alambique.Quarantine.counter() | nil} | {:error, term}
  def crashes(instance, cell) do
    config = Config.fetch!(instance)

    with {:ok, ctx} <- repl_ctx(config, cell), do: Alambique.Quarantine.read(ctx, cell)
  end

  @doc """
  Turns read replicas on or off for `cell` (design §21). The switch is an
  object in the store, so every node sees it within the instance's
  `replicas: [marker_ttl_ms:]`. With `true`, nodes that do not own the cell
  start keeping a copy of it when they serve a read with `consistency:
  {:bounded, ms}` (see `query/5`); with `false`, their replicas stop and
  remove their files within `marker_ttl_ms` plus `poll_ms`. Works whether or
  not this instance has the `:replicas` option, and does not wake the cell.
  """
  @spec replicas(instance, cell, boolean) :: :ok | {:error, term}
  def replicas(instance, cell, enabled) when is_boolean(enabled) do
    config = Config.fetch!(instance)

    with {:ok, location} <- Alambique.Placement.locate(config, cell, fresh: true),
         ctx = Config.repl_ctx(config, location),
         :ok <-
           if(enabled,
             do: Replica.Marker.enable(ctx, cell),
             else: Replica.Marker.disable(ctx, cell)
           ) do
      if config.replicas, do: Replica.Marker.remember(config, location, cell, enabled)
      :ok
    end
  end

  @doc "Whether read replicas are on for `cell` (one `HEAD`; the cell is not woken)."
  @spec replicas(instance, cell) :: {:ok, boolean} | {:error, term}
  def replicas(instance, cell) do
    config = Config.fetch!(instance)

    with {:ok, ctx} <- repl_ctx(config, cell),
         do: Replica.Marker.enabled?(ctx, cell, Retry.deadline_in(ctx.timeout_ms))
  end

  defp repl_ctx(config, cell) do
    with {:ok, location} <- Alambique.Placement.locate(config, cell, fresh: true),
         do: {:ok, Config.repl_ctx(config, location)}
  end

  @doc """
  The node that owns the cell now, `:none` when it is asleep. An owner that
  is not a member (gone, or running the instance without a link to this node)
  and whose lease has not expired gives `{:error, {:unavailable, owner,
  expires_at_ms}}`, as in `lease/3`.

  With `hint: true` the answer may come from the owner-hint cache: a local
  process answers `{:ok, node()}`, then a hint whose node still runs the
  instance is returned without a store request. A hint to a node that is gone
  is dropped and the lease is read, and a lease that names another node is
  stored as the hint. A hinted answer can be stale, for example after the cell
  moved; a routed call corrects the hint when it forwards. Default: `false`,
  which always reads the lease.

  With `peers: true` the answer comes from the processes and never from the
  store: a local process answers `{:ok, node()}`, otherwise the other members
  of the instance are asked in parallel whether they hold the cell, and the
  one that does is the answer. `:none` means no member holds it awake. The
  cell is never woken. A cell that is waking can be reported `:none` until its
  process is registered, and a member that does not answer within
  `call_timeout_ms` counts as not holding it, so `:none` can also hide an
  owner that is unreachable or not a member; the default reads the lease and
  does not have these gaps. `peers: true` wins over `hint: true`.
  """
  @spec owner(instance, cell, keyword) :: {:ok, node} | :none | {:error, term}
  def owner(instance, cell, opts \\ []),
    do: Router.owner(instance, cell, owner_source(opts))

  defp owner_source(opts) do
    cond do
      opts[:peers] == true -> :peers
      opts[:hint] == true -> :hint
      true -> :lease
    end
  end

  @typedoc """
  The public view of a lease (`lease/3`).

  `owner` is the node that holds it when that node still runs the instance,
  `nil` otherwise. `owner_id` is the raw owner id stored in the lease, which
  a takeover caller needs while its detector still knows the failed node.
  A cell that never had a lease answers epoch 0, `owner: nil`, `owner_id: nil`,
  `expires_at_ms: nil`, `released: true` and no moved fields.
  """
  @type lease :: %{
          owner: node | nil,
          owner_id: String.t() | nil,
          epoch: non_neg_integer,
          expires_at_ms: integer | nil,
          released: boolean,
          moved_to: String.t() | nil,
          moved_from: String.t() | nil
        }

  @doc """
  Reads the lease of `cell` without waking it: owner, epoch, expiry, whether
  it is released, and the moved markers (design §5, §18). Use the epoch with
  `take_over/4` when a failure detector suspects the owner.

  Returns `{:ok, lease}` with `t:lease/0`; a cell that never had a lease gives
  epoch 0 and `released: true`. A moved lease keeps its epoch and reports
  `moved_to`; a released lease after hibernation reports `released: true`.
  This is a store read only: it does not wake, freeze or lease the cell, so a
  frozen cell still returns its lease. The location is resolved like
  `owner/3` (through the placement cache). Invalid ids and placement errors
  are returned as they are; a guarded store failure is
  `{:error, {:store_crashed, kind, reason}}` and a call over `:timeout` is
  `{:error, :timeout}`.

  Options: `:timeout` in milliseconds (default: the instance's
  `:call_timeout_ms`).

  ## The lease of a node that is not a member

  A call for a cell whose lease is held, and not expired, by a holder that is
  not a member of this instance's group answers `{:error, {:unavailable,
  owner, expires_at_ms}}` at once, without waking the cell or waiting. That
  is the case of a node that runs the instance without a cluster link to this
  one, of a node that is gone, and of the pseudo-holders `"relocate"` and
  `"rewind"`. `owner` is the holder's `node_id` (a string, the `owner_id` of
  this function) and `expires_at_ms` is the expiry stored in the lease, by the
  clock of the instance that wrote it (the system clock by default).

  The promise is only this: the cell is not served by this node until the
  holder releases the lease or `expires_at_ms + skew_margin_ms` has passed by
  this node's clock, whichever comes first. A holder that releases earlier
  makes the next call succeed at once, so `expires_at_ms` is an upper bound of
  the wait, not a prediction. After a graceful drain it is nearly always
  early. To give a client a `Retry-After`, call this function on the refusal
  and use `max(0, expires_at_ms + skew_margin_ms - now)` when `released` is
  `false`, and `0` when it is `true`.

  The call is one store read of the lease object and nothing else: the
  location comes from the placement cache and nothing is written. In the
  in-memory store it takes about 11 µs; with 25 ms of latency on every
  store request it takes 26 ms, the time of one request
  (`mise run bench:lease`, design §8). A caller may run it on every refusal.
  """
  @spec lease(instance, cell, keyword) :: {:ok, lease} | {:error, term}
  def lease(instance, cell, opts \\ []) do
    opts = Keyword.validate!(opts, timeout: nil)
    config = Config.fetch!(instance)
    timeout = opts[:timeout] || config.call_timeout_ms

    unless is_integer(timeout) and timeout > 0,
      do: raise(ArgumentError, ":timeout must be a positive integer or nil")

    task = Task.async(fn -> read_lease(config, cell) end)

    case Task.yield(task, timeout) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      {:exit, reason} -> {:error, {:lease_crashed, reason}}
      nil -> {:error, :timeout}
    end
  end

  defp read_lease(config, cell) do
    with :ok <- Placement.validate_id(config, cell),
         {:ok, location} <- Placement.locate(config, cell) do
      case Lease.read(Config.lease_ctx(config, location), cell) do
        :none -> {:ok, empty_lease()}
        {:ok, lease} -> {:ok, lease_view(config, lease)}
        {:error, _} = error -> error
      end
    end
  end

  defp empty_lease do
    %{
      owner: nil,
      owner_id: nil,
      epoch: 0,
      expires_at_ms: nil,
      released: true,
      moved_to: nil,
      moved_from: nil
    }
  end

  defp lease_view(config, lease) do
    owner =
      case Membership.node_of(config, lease.owner, lease.incarnation) do
        {:ok, node} -> node
        :error -> nil
      end

    %{
      owner: owner,
      owner_id: lease.owner,
      epoch: lease.epoch,
      expires_at_ms: lease.expires_at_ms,
      released: lease.released,
      moved_to: lease.moved_to,
      moved_from: lease.moved_from
    }
  end

  @doc """
  Wakes the cell where a call would and returns the node that serves it: the
  owner when the cell is awake, the preferred node otherwise (design §8).
  Takes the option `:timeout` and gives the errors of any call, for example
  `{:error, :frozen}`.
  """
  @spec touch(instance, cell, keyword) :: {:ok, node} | {:error, term}
  def touch(instance, cell, opts \\ []),
    do: Router.touch(instance, cell, opts[:timeout])

  @doc """
  How much work waits on a cell that has a process on this node, to tell a
  saturated cell from one that is only called often.

  Returns `{:ok, %{pending: n}}`, or `:none` when the cell has no process on
  this node. It never wakes a cell, never asks another node and never calls
  the cell process, so a cell stuck in a long transaction or with a long
  mailbox answers at once. It costs one registry lookup, one liveness
  check and two counter reads.

  `pending` is the number of calls and transactions the cell has not answered
  yet: the calls sent to it that it has not run, plus the callers it accepted
  whose answer waits for a commit (writes and reads behind them, including
  every batch in the commit pipeline, design §6). Other messages of the
  mailbox (a queued flush, an upload that landed, a timer, the exit message of
  a task) do not count. A call that parks may count twice for an instant. A cell that is waking or hibernating has a
  process, so it answers with the calls queued behind that work.
  """
  @spec load(instance, cell) :: {:ok, %{pending: non_neg_integer}} | :none
  def load(instance, cell) do
    case Alambique.Cell.load(Config.fetch!(instance), cell) do
      {:ok, pending} -> {:ok, %{pending: pending}}
      :none -> :none
    end
  end

  @doc """
  The cells that have a process on this node (awake, waking or hibernating),
  in id order, a page at a time.

  Returns `%{cells: ids, next: id | nil, count: n}`. `next` is the last id of
  the page when more cells follow, and passing it as `after:` gives the next
  page; it is `nil` on the last page. `count` is the number of cells on this
  node. Options: `:after` (a cell id, exclusive; default `nil`, the start),
  `:limit` (a positive integer, default 100) and `:details` (default `false`).
  Anything else raises `ArgumentError`.

  With `details: true` each entry of `cells` is a map instead of an id:

    * `:id` - the cell id.
    * `:last_used` - the time of the last call the cell served, in
      `System.monotonic_time(:millisecond)` of this node, so `now - last_used`
      is the idle time and the smallest value is the least recently used cell.
      The router reads the same value to hibernate the least recently used
      cell at `awake.max_cells`. It is monotonic, not wall-clock time, because
      it is only compared on this node and a clock step must not reorder it.
      `nil` while the cell is still waking.
    * `:wal_bytes` - the size of the cell's WAL file after its last
      acknowledged batch or checkpoint, 0 when there is none. It is not read
      from disk by this call, so a write still being uploaded is not in it,
      and it drops to 0 when the cell checkpoints (see `:wal`,
      `:checkpoint_bytes`).

  A page costs in proportion to its size, whatever the number of cells. The
  list follows the registry by a message, so a cell that has just woken or
  stopped may be missing or listed late; entries whose process is gone are
  skipped. Cells on other nodes are not listed: ask each node.
  """
  @spec cells(instance, keyword) :: %{
          cells: [cell] | [%{id: cell, last_used: integer | nil, wal_bytes: non_neg_integer}],
          next: cell | nil,
          count: non_neg_integer
        }
  def cells(instance, opts \\ []) do
    [after: after_id, details: details?, limit: limit] =
      opts |> Keyword.validate!(after: nil, details: false, limit: 100) |> Enum.sort()

    unless is_nil(after_id) or is_binary(after_id),
      do: raise(ArgumentError, ":after must be a cell id or nil, got: #{inspect(after_id)}")

    unless is_integer(limit) and limit > 0,
      do: raise(ArgumentError, ":limit must be a positive integer, got: #{inspect(limit)}")

    unless is_boolean(details?),
      do: raise(ArgumentError, ":details must be a boolean, got: #{inspect(details?)}")

    Alambique.Live.page(Config.fetch!(instance), after_id, limit, details?)
  end

  @doc """
  The cells of a location that are stored, awake or asleep, whose id starts
  with a prefix, in byte order and each once, a page at a time.

  Returns `{:ok, %{cells: ids, next: id | nil}}`. `next` is the last id of the
  page when more cells may follow, and passing it as `after:` gives the next
  page; it is `nil` on the last page. Options:

    * `:location` - a location id or an `Alambique.Location` (default: the
      only location of the placement). With several locations and none
      given, the result is `{:error, :location_required}`; an id the
      placement does not know gives `{:error, {:unknown_location, id}}`.
    * `:prefix` - only ids that start with it (default `""`, all).
    * `:after` - a cell id, exclusive (default `nil`, the start).
    * `:limit` - ids per page, from 1 to the instance's `list_cells:
      max_limit` (default: its `list_cells: page_size`).
    * `:timeout_ms` - time budget of the call (default: the instance's
      `list_cells: timeout_ms`).

  Anything else raises `ArgumentError`.

  The list reads the schema version index (design §17), which holds a cell
  from the first time it wakes, so a cell that has not woken since the index
  exists is missing until it does. A page costs two store requests per
  schema version that has cells plus one, each returning at most `limit + 1`
  keys, whatever the number of cells. A cell that has just migrated may
  briefly be listed while its old entry is removed; it is listed once. A cell
  that moved to another location is listed there.

  `cells/2` lists the cells that have a process on this node instead.
  """
  @spec list_cells(instance, keyword) ::
          {:ok, %{cells: [cell], next: cell | nil}} | {:error, term}
  def list_cells(instance, opts \\ []) do
    config = Config.fetch!(instance)
    settings = config.list_cells

    opts =
      Keyword.validate!(opts,
        location: nil,
        prefix: "",
        after: nil,
        limit: settings.page_size,
        timeout_ms: settings.timeout_ms
      )

    prefix = opts[:prefix]
    after_id = opts[:after]
    limit = opts[:limit]
    timeout = opts[:timeout_ms]

    unless is_binary(prefix),
      do: raise(ArgumentError, ":prefix must be a string, got: #{inspect(prefix)}")

    unless is_nil(after_id) or is_binary(after_id),
      do: raise(ArgumentError, ":after must be a cell id or nil, got: #{inspect(after_id)}")

    unless is_integer(limit) and limit in 1..settings.max_limit//1,
      do:
        raise(
          ArgumentError,
          ":limit must be an integer from 1 to #{settings.max_limit}, got: #{inspect(limit)}"
        )

    unless is_integer(timeout) and timeout > 0,
      do: raise(ArgumentError, ":timeout_ms must be a positive integer, got: #{inspect(timeout)}")

    with {:ok, location} <- Placement.listed_location(config, opts[:location]),
         {:ok, cells, next} <-
           Schema.cells(
             config,
             location,
             prefix,
             after_id,
             limit,
             Retry.deadline_in(timeout)
           ) do
      {:ok, %{cells: cells, next: next}}
    end
  end
end
