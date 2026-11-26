defmodule Alambique.Sqlite do
  @moduledoc """
  SQLite inside the `alambique_nif` library (design §2, §16).

  Cell databases are opened with the Alambique VFS (`vfs: :alambique`), which
  can serve pages that are not on disk yet (lazy restore). Calls that can
  block run on dirty schedulers.

  Parameters are bound as integers, floats, `nil`, binaries and iodata as
  text, other atoms as their name, and `{:blob, binary}` as blobs. Rows are
  lists of integers, floats, binaries (text and blobs) and `nil`. A failed
  call returns `{:error, message}` with SQLite's message, or
  `{:error, :not_authorized}` for a statement the connection refuses (below),
  or `{:error, :disk_full}` when SQLite reports `SQLITE_FULL` (the disk or the
  `max_page_count` limit has no room). Text with a NUL byte is refused with
  `{:error, :nul_in_statement}` before anything runs.

  A connection refuses `ATTACH`, `DETACH` and `VACUUM` unless it was opened
  with `attach: true`, and is opened defensive (no writes to the schema or to
  shadow tables), with untrusted schemas and extension loading off.

  ## Ownership

  The process that opens a connection is its opener and, at first, its user.
  Only the user runs statements and reads its state (`run/4`, `execute/3`,
  `script/2`, `changes/1`, `transaction_status/1`, `checkpoint/2`...); only
  the opener closes it, interrupts it, reads its `memory/1` and names another
  user with `assign/2`. A call from any other process is
  `{:error, :not_owner}`. A connection closes when its opener ends, even
  when another process still holds the reference.

  ## Gated connections

  A gated connection (`gated: true`) allows only reading outside the
  transaction of `begin_transaction/1`: any other statement, pragma that sets
  something, `ATTACH`, `DETACH`, `VACUUM`, `ANALYZE`, `REINDEX` and
  `PRAGMA optimize` included, is `{:error, :not_authorized}`. The handle
  `open/2` returns holds a secret that `close/1`, `checkpoint/1`,
  `begin_transaction/1`, `assign/2` and a call with `write: true` need; the
  reference of `handle.ref` has none, so whoever keeps only the reference can
  read and nothing else. `write: true` lets one statement call write outside
  a transaction.
  """

  alias Alambique.Replication.Native
  alias Alambique.Sqlite.Handle

  @typedoc """
  A connection: the reference `open/2` returns, or the handle of a gated
  connection (`gated: true`), which also holds its secret.
  """
  @type conn :: reference | Handle.t()
  @type row :: [integer | float | binary | nil]

  @doc """
  Opens `path`, a file name or a `file:` URI. Options: `:readonly` (default
  `false`: read-write, created when missing), `:vfs`, `:alambique` for the
  Alambique VFS (default: SQLite's), `:attach` (default `false`) to allow
  `ATTACH`, `DETACH` and `VACUUM`, and `:timeout_ms` (default 0, none), the
  deadline of every `run/4` and `execute/3` call that sets none. A statement
  over it gives `{:error, :statement_timeout}`, `:gated` (default `false`)
  for a gated connection, which `open/2` then returns as a handle, and
  `:locking` (`:normal`, the default, or `:exclusive`): an exclusive
  connection takes SQLite's lock on the file at its first access and keeps it
  until it closes, and uses no shared memory for the WAL, so another
  connection that honours SQLite's locks gets `{:error, :busy}`.

  A read-write connection on a database file that a cell holds open
  read-write is refused with `{:error, :held_by_cell}`, whether it goes
  through the file name, a hard link, SQLite's own VFS or a `vfs=` URI; so is
  an `ATTACH` of it. Reading (`readonly: true`) stays allowed. Another OS
  process, a raw write of the file and code that opened it before the cell
  woke are not prevented; the cell finds their writes (design §15).
  """
  @spec open(Path.t(), keyword) :: {:ok, conn} | {:error, term}
  def open(path, opts \\ []) do
    vfs = if Keyword.get(opts, :vfs) == :alambique, do: "alambique"

    Native.sqlite_open(
      path,
      Keyword.get(opts, :readonly, false),
      vfs,
      Keyword.get(opts, :attach, false),
      Keyword.get(opts, :timeout_ms, 0),
      Keyword.get(opts, :gated, false),
      Keyword.get(opts, :locking, :normal) == :exclusive
    )
    |> case do
      {:ok, ref, 0} -> {:ok, ref}
      {:ok, ref, key} -> {:ok, %Handle{ref: ref, key: key}}
      error -> error
    end
  end

  @doc false
  @spec ref(conn) :: reference
  def ref(%Handle{ref: ref}), do: ref
  def ref(ref), do: ref

  defp key(%Handle{key: key}), do: key
  defp key(_ref), do: 0

  # The secret of a call that may write outside a transaction.
  defp write_key(conn, opts), do: if(Keyword.get(opts, :write, false), do: key(conn), else: 0)

  @doc """
  Closes the connection: the opener only, with the secret of a gated one, and
  not while the transaction of `begin_transaction/1` is open
  (`{:error, :not_authorized}`).
  """
  @spec close(conn) :: :ok | {:error, term}
  def close(conn), do: Native.sqlite_close(ref(conn), key(conn))

  @doc """
  Names `pid` as the process that runs statements on `conn`. The opener only,
  with the secret of a gated connection. The opener keeps closing, interrupting
  and reading the memory of the connection.
  """
  @spec assign(conn, pid) :: :ok | {:error, term}
  def assign(conn, pid) when is_pid(pid), do: Native.sqlite_assign(ref(conn), key(conn), pid)

  @doc """
  Runs one or more statements without reading rows, with the deadline of the
  connection (`open/2`). `no_transaction_control: true` is for a user's text:
  `BEGIN`, `COMMIT`, `END` and `ROLLBACK` are refused as in `run/4`.
  `write: true` is for a call of the library that may write outside a
  transaction (see the module doc).
  """
  @spec execute(conn, iodata, keyword) :: :ok | {:error, term}
  def execute(conn, sql, opts \\ []) do
    Native.sqlite_execute(
      ref(conn),
      IO.iodata_to_binary(sql),
      Keyword.get(opts, :no_transaction_control, false),
      write_key(conn, opts)
    )
  end

  @doc """
  Prepares, binds, runs and finalizes one statement.

  Options bound the call (all off by default):

    * `:read_only` - refuse transaction and savepoint statements and the
      pragmas that set something; the error is `{:error, :not_authorized}`.
      Values over `:max_bytes` cannot be built.
    * `:timeout_ms` - stop the statement after this long with
      `{:error, :statement_timeout}`; the default is the deadline of the
      connection. A statement of a transaction that is
      stopped rolls the transaction back.
    * `:max_rows` and `:max_bytes` - `{:error, :result_too_large}` when the
      result has more rows, or more bytes of text, blobs and (8 each) other
      values.
    * `:no_transaction_control` - for a user's text: refuse `BEGIN`, `COMMIT`,
      `END` and `ROLLBACK` with `{:error, :not_authorized}`, so the text
      cannot end the transaction the library holds. Savepoints are allowed.
    * `:max_concurrent` - a user statement: `{:error, :busy}` at once when
      this many user statements already run on the node (design §7). The
      call is counted while it runs on the dirty scheduler, so a caller that
      dies mid-call cannot leak a slot. `user_statements/0` is the count.
    * `:write` - the statement may write outside a transaction on a gated
      connection (it needs the secret of the handle).
  """
  @spec run(conn, iodata, list, keyword) ::
          {:ok, %{columns: [String.t()], rows: [row]}} | {:error, term}
  def run(conn, sql, params, opts \\ []) do
    limits = {
      Keyword.get(opts, :read_only, false),
      Keyword.get(opts, :timeout_ms, 0),
      Keyword.get(opts, :max_rows, 0),
      Keyword.get(opts, :max_bytes, 0),
      Keyword.get(opts, :max_concurrent, 0),
      Keyword.get(opts, :no_transaction_control, false),
      write_key(conn, opts)
    }

    case Native.sqlite_run(ref(conn), IO.iodata_to_binary(sql), params || [], limits) do
      {:ok, columns, rows} -> {:ok, %{columns: columns, rows: rows}}
      other -> raise_argument_error(other)
    end
  end

  @typedoc """
  A batch of the rows `feed_rows/6` read: `{txid, ts, rows, last_seq, bytes,
  {first, partial, rewind}}`.
  """
  @type feed_batch ::
          {pos_integer, integer, pos_integer, integer, non_neg_integer,
           {boolean, boolean, boolean}}

  @typedoc "`{bound, resumed, room}` of `feed_rows/6`."
  @type feed_plan :: {pos_integer | nil, non_neg_integer | nil, non_neg_integer | nil}

  @doc """
  Runs `sql`, a `SELECT txid, ts, tbl, op, key, vals, seq` of the change
  feed's rows in order of `txid, seq`, and sends the text of the records of
  those rows to `pid` as `{:rows, token, [binary]}`, one binary per batch (the
  text `Alambique.Feed.Record` splices, built by the library and never
  decoded). Returns `{:ok, batches, more, cut}`: `more` when the `bound` on
  rows left some, `cut` when `room` ended the read: a batch that would begin
  after `room` others is not read (`nil`: no limit). `resumed` is the `n` of the
  next row when the first batch goes on one that an earlier call cut, `nil`
  when it starts. `{:error, :sender_gone}` when `pid` no longer exists,
  `{:error, :bad_table}` for a table name that is not UTF-8. Nothing is sent
  when no row was read. The process that runs statements on `conn` only.
  """
  @spec feed_rows(conn, iodata, list, feed_plan, pid, reference) ::
          {:ok, [feed_batch], boolean, boolean} | {:error, term}
  def feed_rows(conn, sql, params, {bound, resumed, room}, pid, token) do
    Native.sqlite_feed_rows(
      ref(conn),
      IO.iodata_to_binary(sql),
      params,
      bound,
      resumed,
      room,
      pid,
      token
    )
  end

  @doc "User statements running now on the node (`:max_concurrent` of `run/4`)."
  @spec user_statements() :: non_neg_integer
  def user_statements, do: Native.sqlite_user_statements()

  @doc """
  Makes the statement running on `conn` stop with `{:error, "interrupted"}`.
  The opener only. `close/1` does it too.
  """
  @spec interrupt(conn) :: :ok | {:error, term}
  def interrupt(conn), do: Native.sqlite_interrupt(ref(conn))

  @doc """
  Copies every WAL frame into the database file and truncates the WAL. It is
  the only way frames reach the file: connections never checkpoint when they
  close or after a commit, and the pragmas that would checkpoint, turn
  automatic checkpoints on or leave WAL mode are refused when a statement is
  prepared (design §6). `{:error, :busy}` when a reader kept it from
  finishing, and `{:error, :not_authorized}` while the transaction of
  `begin_transaction/1` is open or without the secret of a gated connection.
  On a cell's own connection it is `{:error, :foreign_write}` when the
  wal-index header is not what the connection's own writes left: the
  checkpoint does not run, or, when something wrote while it took the lock,
  has run and the file holds what the WAL does not.
  """
  @spec checkpoint(conn) :: :ok | {:error, term}
  def checkpoint(conn), do: Native.sqlite_checkpoint(ref(conn), key(conn))

  @doc "Rows changed by the latest statement that changed rows."
  @spec changes(conn) :: {:ok, non_neg_integer} | {:error, term}
  def changes(conn), do: Native.sqlite_changes(ref(conn))

  @doc "`{:ok, :transaction}` inside an open transaction, `{:ok, :idle}` otherwise."
  @spec transaction_status(conn) :: {:ok, :idle | :transaction} | {:error, term}
  def transaction_status(conn), do: Native.sqlite_transaction_status(ref(conn))

  @doc """
  Runs `PRAGMA query_only = OFF; BEGIN IMMEDIATE` and returns `{:ok, token}`.
  The connection is then armed until `finish_transaction/3` ends the
  transaction with that token: `run/4` and `execute/3` return
  `{:error, :transaction_ended}` without running a statement once SQLite has
  ended the transaction (a conflict or a trigger that rolls back, an
  interrupted statement, an I/O error), and no statement on the connection
  may begin, commit or roll back a transaction (`{:error, :not_authorized}`).
  Beginning again while armed is `{:error, :not_authorized}` too. Arming notes
  the size of the WAL for `wal_advanced?/1`. Only the holder of the token can
  end the transaction, so the function of a transaction, which gets the
  connection but not the token, cannot. On a gated connection it needs the
  secret. On a cell's own connection the header of the wal-index is compared
  with what the connection's own writes left once the write lock is held; when
  it differs the transaction is rolled back, nothing runs and the result is
  `{:error, :foreign_write}`.
  """
  @spec begin_transaction(conn) :: {:ok, non_neg_integer} | {:error, term}
  def begin_transaction(conn), do: Native.sqlite_begin_transaction(ref(conn), key(conn))

  @doc """
  Ends the transaction of `begin_transaction/1` with `COMMIT` or `ROLLBACK`,
  the only statement that may, and disarms the connection once it succeeded.
  A wrong `token` is `{:error, :not_authorized}`. `:commit` of a transaction
  that SQLite already ended is `{:error, :transaction_ended}`; `:rollback` of
  one is `:ok`. After a failed statement the connection stays armed.
  `:commit` then runs `PRAGMA query_only = ON` in the same call; when it
  fails the write is committed, the connection is disarmed and the result is
  `{:error, {:query_only, reason}}`.
  """
  @spec finish_transaction(conn, non_neg_integer, :commit | :rollback) :: :ok | {:error, term}
  def finish_transaction(conn, token, how) when how in [:commit, :rollback],
    do: Native.sqlite_finish_transaction(ref(conn), token, how == :commit)

  @doc """
  Whether a commit reached the WAL of `conn` since `begin_transaction/1`.
  After a transaction that failed it says that frames were committed that no
  write accounts for.
  """
  @spec wal_advanced?(conn) :: boolean | {:error, term}
  def wal_advanced?(conn), do: Native.sqlite_wal_advanced(ref(conn))

  @doc """
  Compares the wal-index header of a cell's connection with what its own
  commits and checkpoints left (design §15): `:none` when nothing else wrote,
  `:advanced` when something else committed (its frames follow in the WAL),
  `:restarted` when the WAL restarted or the header cannot be read, so frames
  may be gone from the WAL. A connection that is not a cell's own read-write
  one has nothing to compare: `:none`.
  """
  @spec foreign_write(conn) :: :none | :advanced | :restarted | {:error, term}
  def foreign_write(conn), do: Native.sqlite_foreign_write(ref(conn), key(conn), 0)

  @doc """
  `Alambique.Replication.Native.capture_incremental/8` of the files of a
  cell's own connection, reading the WAL up to the end its wal-index records:
  frames past it are no transaction (a commit frame whose write failed after
  it reached the file). The header is read in the same native call.
  """
  def capture_incremental(conn, db, wal, page_size, pos, sums, pre, txid, timestamp) do
    Native.sqlite_capture_incremental(
      ref(conn),
      key(conn),
      db,
      wal,
      page_size,
      pos,
      sums,
      pre,
      txid,
      timestamp
    )
  end

  @doc """
  `Alambique.Replication.Native.wal_pending/3` of the WAL of a cell's own
  connection, up to the end its wal-index records.
  """
  def wal_pending(conn, wal, page_size, pos),
    do: Native.sqlite_wal_pending(ref(conn), key(conn), wal, page_size, pos)

  @doc """
  `Alambique.Replication.Native.capture_full/6` of the database of `conn`, read
  through the file the connection holds: no other descriptor of the file is
  opened and closed, which would drop the locks SQLite holds on it.
  """
  @spec capture_full(conn, Path.t(), pos_integer, pos_integer, integer, term) ::
          Alambique.Replication.Native.captured() | Alambique.Replication.Native.error()
  def capture_full(conn, wal, page_size, txid, timestamp, limit),
    do: Native.sqlite_capture_full(ref(conn), key(conn), wal, page_size, txid, timestamp, limit)

  @doc """
  Takes the header that the latest `foreign_write/1` saw as accounted for,
  once the frames it found are replicated.
  """
  @spec accept_foreign_write(conn) :: :ok | {:error, term}
  def accept_foreign_write(conn), do: Native.sqlite_foreign_write(ref(conn), key(conn), 1)

  @doc """
  Takes the header as it is as the one `foreign_write/1` compares with. A cell
  does it once its connection is set up.
  """
  @spec baseline_wal(conn) :: :ok | {:error, term}
  def baseline_wal(conn), do: Native.sqlite_foreign_write(ref(conn), key(conn), 2)

  @doc """
  Runs a user's script of schema statements, each as SQLite delimits it.
  A statement that does not start with `CREATE`, `DROP` or `ALTER` is refused
  with `{:error, {:not_ddl, statement}}` before it runs, and nothing after it
  runs; `BEGIN`, `COMMIT`, `END` and `ROLLBACK` are refused as in `run/4`.
  Statements before a refused one have run: the caller undoes them.
  """
  @spec script(conn, iodata) :: :ok | {:error, term}
  def script(conn, sql), do: Native.sqlite_script(ref(conn), IO.iodata_to_binary(sql))

  @doc """
  Whether a statement starts with `CREATE`, `DROP` or `ALTER` once the blanks
  and comments that SQLite skips are skipped (a UTF-8 byte order mark is one
  of them). It is the check of `script/2`, so a statement is schema for the
  library when it is schema for the script. It does not call SQLite.
  """
  @spec ddl?(iodata) :: boolean
  def ddl?(sql), do: Native.sqlite_starts_ddl(IO.iodata_to_binary(sql))

  @doc """
  Bytes the connection holds, by purpose: `:cache_used` (page cache),
  `:schema_used`, `:stmt_used` (prepared statements) and `:lookaside_used`.
  The opener only. It never waits: while a statement of the connection runs
  it gives `{:error, :busy}`.
  """
  @spec memory(conn) :: {:ok, %{atom => non_neg_integer}} | {:error, :busy | term}
  def memory(conn), do: Native.sqlite_memory(ref(conn))

  defp raise_argument_error({:error, {:argument_error, message}}),
    do: raise(ArgumentError, message)

  defp raise_argument_error(other), do: other
end
