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
  """

  alias Alambique.Replication.Native

  @type conn :: reference
  @type row :: [integer | float | binary | nil]

  @doc """
  Opens `path`, a file name or a `file:` URI. Options: `:readonly` (default
  `false`: read-write, created when missing), `:vfs`, `:alambique` for the
  Alambique VFS (default: SQLite's), `:attach` (default `false`) to allow
  `ATTACH`, `DETACH` and `VACUUM`, and `:timeout_ms` (default 0, none), the
  deadline of every `run/4` and `execute/2` call that sets none. A statement
  over it gives `{:error, :statement_timeout}`.
  """
  @spec open(Path.t(), keyword) :: {:ok, conn} | {:error, term}
  def open(path, opts \\ []) do
    vfs = if Keyword.get(opts, :vfs) == :alambique, do: "alambique"

    Native.sqlite_open(
      path,
      Keyword.get(opts, :readonly, false),
      vfs,
      Keyword.get(opts, :attach, false),
      Keyword.get(opts, :timeout_ms, 0)
    )
  end

  @doc "Closes the connection. "
  @spec close(conn) :: :ok
  def close(conn), do: Native.sqlite_close(conn)

  @doc """
  Runs one or more statements without reading rows, with the deadline of the
  connection (`open/2`). `no_transaction_control: true` is for a user's text:
  `BEGIN`, `COMMIT`, `END` and `ROLLBACK` are refused as in `run/4`.
  """
  @spec execute(conn, iodata, keyword) :: :ok | {:error, term}
  def execute(conn, sql, opts \\ []) do
    Native.sqlite_execute(
      conn,
      IO.iodata_to_binary(sql),
      Keyword.get(opts, :no_transaction_control, false)
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
      Keyword.get(opts, :no_transaction_control, false)
    }

    case Native.sqlite_run(conn, IO.iodata_to_binary(sql), params || [], limits) do
      {:ok, columns, rows} -> {:ok, %{columns: columns, rows: rows}}
      other -> raise_argument_error(other)
    end
  end

  @doc "User statements running now on the node (`:max_concurrent` of `run/4`)."
  @spec user_statements() :: non_neg_integer
  def user_statements, do: Native.sqlite_user_statements()

  @doc """
  Makes the statement running on `conn` stop with `{:error, "interrupted"}`.
  Callable from any process. `close/1` does it too.
  """
  @spec interrupt(conn) :: :ok
  def interrupt(conn), do: Native.sqlite_interrupt(conn)

  @doc """
  Copies every WAL frame into the database file and truncates the WAL. It is
  the only way frames reach the file: connections never checkpoint when they
  close or after a commit, and the pragmas that would checkpoint, turn
  automatic checkpoints on or leave WAL mode are refused when a statement is
  prepared (design §6). `{:error, :busy}` when a reader kept it from
  finishing.
  """
  @spec checkpoint(conn) :: :ok | {:error, term}
  def checkpoint(conn), do: Native.sqlite_checkpoint(conn)

  @doc "Rows changed by the latest statement that changed rows."
  @spec changes(conn) :: {:ok, non_neg_integer} | {:error, term}
  def changes(conn), do: Native.sqlite_changes(conn)

  @doc "`{:ok, :transaction}` inside an open transaction, `{:ok, :idle}` otherwise."
  @spec transaction_status(conn) :: {:ok, :idle | :transaction} | {:error, term}
  def transaction_status(conn), do: Native.sqlite_transaction_status(conn)

  @doc """
  Says that a transaction of the caller is open on `conn` (`true`) or is not
  (`false`). While it is, `run/4` and `execute/3` return
  `{:error, :transaction_ended}` without running a statement once SQLite has
  ended the transaction (a conflict or a trigger that rolls back, an
  interrupted statement, an I/O error). Turn it off before the final `COMMIT`
  or `ROLLBACK`.
  """
  @spec expect_transaction(conn, boolean) :: :ok | {:error, term}
  def expect_transaction(conn, on), do: Native.sqlite_expect_transaction(conn, on)

  @doc """
  Runs a user's script of schema statements, each as SQLite delimits it.
  A statement that does not start with `CREATE`, `DROP` or `ALTER` is refused
  with `{:error, {:not_ddl, statement}}` before it runs, and nothing after it
  runs; `BEGIN`, `COMMIT`, `END` and `ROLLBACK` are refused as in `run/4`.
  Statements before a refused one have run: the caller undoes them.
  """
  @spec script(conn, iodata) :: :ok | {:error, term}
  def script(conn, sql), do: Native.sqlite_script(conn, IO.iodata_to_binary(sql))

  @doc """
  Bytes the connection holds, by purpose: `:cache_used` (page cache),
  `:schema_used`, `:stmt_used` (prepared statements) and `:lookaside_used`.
  """
  @spec memory(conn) :: {:ok, %{atom => non_neg_integer}} | {:error, term}
  def memory(conn), do: Native.sqlite_memory(conn)

  defp raise_argument_error({:error, {:argument_error, message}}),
    do: raise(ArgumentError, message)

  defp raise_argument_error(other), do: other
end
