defmodule Alambique.Cell.Db do
  @moduledoc false
  # SQLite access of a cell: connection setup, statements and transactions.

  alias Alambique.{Fault, Sqlite, Tx}
  alias Alambique.Feed.Capture

  @active :alambique_active_tx

  @type result :: %{columns: [String.t()], rows: [[term]]}

  def active_key, do: @active

  @doc """
  Opens `path` in WAL mode (design §6; `Alambique.Sqlite` connections never
  checkpoint on their own).
  The connection is read-only (`query_only`) except inside `writable/2`, so
  only the cell's write path can change the file. Recursive triggers are on so that a `REPLACE` that removes a row fires the
  delete trigger of the change feed (design §13). `opts` are those of
  `Alambique.Sqlite.open/2` (`attach: true` for the rewind's reconcile;
  `timeout_ms:`, the deadline of the statements that set none, which the
  callers take from `script_timeout_ms/1`), and
  `cache_kib:`, the page cache of the connection in KiB (SQLite's default
  when absent): the cache is the largest part of an awake cell's memory
  (docs/cost-of-a-cell.md).
  """
  @spec open(Path.t(), keyword) :: {:ok, Sqlite.conn()} | {:error, term}
  def open(path, opts \\ []) do
    {cache_kib, opts} = Keyword.pop(opts, :cache_kib)

    with {:ok, conn} <- Sqlite.open(path, [vfs: :alambique] ++ opts),
         {:ok, %{rows: [["wal"]]}} <- Sqlite.run(conn, "PRAGMA journal_mode = WAL", []),
         :ok <- cache_size(conn, cache_kib),
         :ok <- Sqlite.execute(conn, "PRAGMA synchronous = NORMAL"),
         :ok <- Sqlite.execute(conn, "PRAGMA recursive_triggers = ON"),
         :ok <- Sqlite.execute(conn, "PRAGMA query_only = ON") do
      {:ok, conn}
    else
      {:error, _} = error -> error
      other -> {:error, {:open, other}}
    end
  end

  defp cache_size(_conn, nil), do: :ok
  defp cache_size(conn, kib), do: Sqlite.execute(conn, "PRAGMA cache_size = -#{kib}")

  @doc """
  Opens a replica file (design §14) for reading only. The file never changes
  while it is open, so it is opened `immutable`: no locks, and no `-wal` or
  `-shm` file even though the header says WAL. The connection is also
  `query_only`.
  """
  @spec open_read_only(Path.t(), keyword) :: {:ok, Sqlite.conn()} | {:error, term}
  def open_read_only(path, opts \\ []) do
    uri = "file:" <> URI.encode(path, &(URI.char_unreserved?(&1) or &1 == ?/)) <> "?immutable=1"

    with {:ok, conn} <- Sqlite.open(uri, [readonly: true] ++ opts),
         :ok <- Sqlite.execute(conn, "PRAGMA query_only = ON") do
      {:ok, conn}
    end
  end

  @doc """
  Opens a read-only connection next to the cell's own on `path`, for the feed
  sender (`feed: [hand_over: :sender]`): WAL readers never block the writer,
  see committed rows only, and hold no lock between statements. The page
  cache is `cache_kib`.
  """
  @spec open_reader(Path.t(), pos_integer) :: {:ok, Sqlite.conn()} | {:error, term}
  def open_reader(path, cache_kib) do
    with {:ok, conn} <- Sqlite.open(path, readonly: true, vfs: :alambique) do
      case cache_size(conn, cache_kib) do
        :ok ->
          {:ok, conn}

        {:error, _} = error ->
          Sqlite.close(conn)
          error
      end
    end
  end

  @spec close(Sqlite.conn()) :: :ok
  def close(conn), do: Sqlite.close(conn)

  @doc """
  Statement limits from the `:sql` option of the instance: the options of
  `Alambique.Sqlite.run/4` for a statement run for a caller. `ms` is the
  caller's own deadline; it never exceeds `statement_timeout_ms`.
  `max_concurrent` is the node-wide limit on user statements, the limit of reads.
  """
  @spec limits(map, pos_integer | nil) :: keyword
  def limits(%{sql: sql}, ms \\ nil) do
    [
      timeout_ms: min(ms || sql.statement_timeout_ms, sql.statement_timeout_ms),
      max_rows: sql.max_result_rows,
      max_bytes: sql.max_result_bytes,
      max_concurrent: sql.max_concurrent_statements
    ]
  end

  @doc """
  `limits/2` for the statements of a write transaction. They are counted, so
  reads see them, but only a full pool refuses one: a write is not refused
  because reads hold the slots (design §7).
  """
  @spec write_limits(map, pos_integer | nil) :: keyword
  def write_limits(config, ms \\ nil),
    do:
      Keyword.put(limits(config, ms), :max_concurrent, :erlang.system_info(:dirty_io_schedulers))

  @doc """
  The deadline of the statements of migrations, `Alambique.Tx.script/2` and
  the library itself (the `:sql` option `:script_timeout_ms`): the options of
  `Alambique.Sqlite.run/4` for a migration statement.
  """
  @spec migration_limits(map) :: keyword
  def migration_limits(%{sql: sql}), do: [timeout_ms: sql.script_timeout_ms]

  @doc "The deadline for `Alambique.Sqlite.open/2` (`:timeout_ms`)."
  @spec script_timeout_ms(map) :: pos_integer
  def script_timeout_ms(%{sql: sql}), do: sql.script_timeout_ms

  @doc """
  Runs a statement for a caller that may only read. Anything that would write
  or change the session gives `{:error, :read_only}`: `query_only` refuses the
  writes, and the authorizer of the native library (design §5) refuses
  `ATTACH`, transaction statements and the pragmas that set a value.

  `limits` are the options of `Alambique.Sqlite.run/4`; a statement over the
  deadline gives `{:error, :statement_timeout}`, a result over a cap
  `{:error, :result_too_large}`.
  """
  @spec read(Sqlite.conn(), String.t(), list, keyword) :: {:ok, result} | {:error, term}
  def read(conn, sql, params, limits \\ []) do
    case run(conn, sql, params, [read_only: true] ++ limits) do
      {:error, "attempt to write a readonly database"} -> {:error, :read_only}
      {:error, :not_authorized} -> {:error, :read_only}
      other -> other
    end
  end

  @doc """
  Like `read/4`, with the deadline `ms` milliseconds from now, which gives
  `{:error, :timeout}` instead.
  """
  @spec read_within(Sqlite.conn(), String.t(), list, pos_integer, keyword) ::
          {:ok, result} | {:error, term}
  def read_within(conn, sql, params, ms, limits) do
    case read(conn, sql, params, Keyword.put(limits, :timeout_ms, ms)) do
      {:error, :statement_timeout} -> {:error, :timeout}
      other -> other
    end
  end

  @doc "Runs `fun` with the connection writable."
  @spec writable(Sqlite.conn(), (-> result)) :: result when result: var
  def writable(conn, fun) do
    :ok = Sqlite.execute(conn, "PRAGMA query_only = OFF")

    try do
      fun.()
    after
      Sqlite.execute(conn, "PRAGMA query_only = ON")
    end
  end

  @doc """
  Runs a script with the connection writable, in one call on the dirty IO
  pool: `sql` between the two pragmas. A script that fails still leaves the
  connection read-only.
  """
  @spec writable_script(Sqlite.conn(), String.t()) :: :ok | {:error, term}
  def writable_script(conn, sql) do
    case Sqlite.execute(conn, "PRAGMA query_only = OFF; #{sql}; PRAGMA query_only = ON") do
      :ok ->
        :ok

      {:error, _} = error ->
        read_only(conn)
        error
    end
  end

  @doc """
  Runs one statement with `params`. A wrong number or type of parameters is
  `{:error, {:bad_params, message}}`.
  """
  @spec run(Sqlite.conn(), String.t(), list, keyword) :: {:ok, result} | {:error, term}
  def run(conn, sql, params, limits \\ []) do
    Sqlite.run(conn, sql, params, limits)
  rescue
    error in ArgumentError -> {:error, {:bad_params, error.message}}
  end

  @doc """
  Runs `fun` with a handle inside `BEGIN IMMEDIATE` ... `COMMIT`.

  Returns `{:ok, value}` after the local commit, or `{:error, reason}` after
  a rollback (a raise, a `Tx.rollback/2`, or a failed commit). When the commit
  ran and `PRAGMA query_only = ON` after it failed the reason is
  `{:query_only, reason}`; when the rollback failed it is
  `{:rollback_failed, reason}`: the cell stops in both cases. While `fun`
  runs, a statement on the connection returns `{:error, :transaction_ended}`
  without running once SQLite has ended the transaction itself (a conflict or
  a trigger that rolls back, an interrupt, an I/O error), so no later
  statement commits on its own; a function that returns after that gets
  `{:error, :transaction_ended}` too.

  With `capture: false` the transaction does not keep the change-feed triggers
  up to date when it changes the schema (migrations and the feed's own steps).
  `select:` is the feed selection (`feed` of the instance configuration) that decides which tables and columns the triggers capture.
  `isolated: true` says that the transaction owns its batch (`Alambique.Tx.before/1`).
  `limits:` are the options of `Alambique.Sqlite.run/4` for the statements
  the function runs through its handle. Those statements cannot run `BEGIN`,
  `COMMIT`, `END` or `ROLLBACK` (`{:error, :not_authorized}`), nor can any
  other statement on the connection while `fun` runs.
  """
  @spec transaction(Sqlite.conn(), pos_integer, (Tx.t() -> term), keyword) ::
          {:ok, term} | {:error, term}
  def transaction(conn, txid, fun, opts \\ []) do
    # Each statement is one call on the dirty IO pool, so the pragma shares
    # a call with the BEGIN and with the COMMIT.
    case Sqlite.execute(conn, "PRAGMA query_only = OFF; BEGIN IMMEDIATE") do
      :ok ->
        # From here a statement on the connection does not run once SQLite
        # has ended the transaction by itself (design §15).
        :ok = Sqlite.expect_transaction(conn, true)
        transact(conn, txid, fun, opts)

      {:error, _} = error ->
        read_only(conn)
        error
    end
  end

  defp read_only(conn), do: Sqlite.execute(conn, "PRAGMA query_only = ON")

  defp transact(conn, txid, fun, opts) do
    tx = %Tx{
      conn: conn,
      txid: txid,
      ref: make_ref(),
      capture: Keyword.get(opts, :capture, true),
      select: Keyword.get(opts, :select, Capture.everything()),
      isolated: Keyword.get(opts, :isolated, false),
      limits: Keyword.get(opts, :limits, [])
    }

    fun |> invoke(tx) |> finish(conn)
  end

  defp invoke(fun, tx) do
    Process.put(@active, tx.ref)

    try do
      {:commit, fun.(tx)}
    rescue
      exception -> {:abort, {:raised, exception, __STACKTRACE__}}
    catch
      :throw, {:alambique_rollback, reason} -> {:abort, reason}
      kind, reason -> {:abort, {kind, reason}}
    after
      Process.delete(@active)
    end
  end

  defp finish({:commit, value}, conn) do
    with :ok <- check_open(conn),
         :ok <- Sqlite.expect_transaction(conn, false) do
      case Sqlite.execute(conn, "COMMIT; PRAGMA query_only = ON") do
        :ok -> {:ok, value}
        {:error, reason} -> failed_commit(conn, reason)
      end
    else
      {:error, reason} -> abort(conn, reason)
    end
  end

  defp finish({:abort, reason}, conn), do: abort(conn, reason)

  # The commit ran and the pragma after it did not: the frames are in the WAL
  # and the connection may write outside a transaction. The cell sees frames
  # no write carries and stops.
  defp failed_commit(conn, reason) do
    if Sqlite.transaction_status(conn) == {:ok, :idle} and Sqlite.wal_advanced?(conn),
      do: {:error, {:query_only, reason}},
      else: abort(conn, reason)
  end

  defp abort(conn, reason) do
    case rollback(conn) do
      :ok -> {:error, reason}
      {:error, failure} -> {:error, {:rollback_failed, failure}}
    end
  end

  # The function ended the transaction itself, for example with COMMIT.
  defp check_open(conn) do
    case Sqlite.transaction_status(conn) do
      {:ok, :transaction} -> :ok
      _ -> {:error, :transaction_ended}
    end
  end

  # A failed `ROLLBACK` leaves the transaction open: the cell stops.
  defp rollback(conn) do
    :ok = Sqlite.expect_transaction(conn, false)
    Fault.point(:db_before_rollback, conn)

    result =
      if Sqlite.transaction_status(conn) == {:ok, :transaction},
        do: Sqlite.execute(conn, "ROLLBACK"),
        else: :ok

    read_only(conn)
    result
  end

  @doc """
  Runs a multi-statement script such as a migration. `opts` are those of
  `Alambique.Sqlite.execute/3`.
  """
  @spec script(Sqlite.conn(), String.t(), keyword) :: :ok | {:error, term}
  def script(conn, sql, opts \\ []), do: Sqlite.execute(conn, sql, opts)

  @doc "Current `PRAGMA user_version`."
  @spec user_version(Sqlite.conn()) :: {:ok, non_neg_integer} | {:error, term}
  def user_version(conn) do
    case Sqlite.run(conn, "PRAGMA user_version", []) do
      {:ok, %{rows: [[version]]}} -> {:ok, version}
      {:error, _} = error -> error
    end
  end
end
