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
  `{:error, :not_authorized}` for a statement the connection refuses (below).

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
  connection (`open/2`).
  """
  @spec execute(conn, iodata) :: :ok | {:error, term}
  def execute(conn, sql), do: Native.sqlite_execute(conn, IO.iodata_to_binary(sql))

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
  """
  @spec run(conn, iodata, list, keyword) ::
          {:ok, %{columns: [String.t()], rows: [row]}} | {:error, term}
  def run(conn, sql, params, opts \\ []) do
    limits = {
      Keyword.get(opts, :read_only, false),
      Keyword.get(opts, :timeout_ms, 0),
      Keyword.get(opts, :max_rows, 0),
      Keyword.get(opts, :max_bytes, 0)
    }

    case Native.sqlite_run(conn, IO.iodata_to_binary(sql), params || [], limits) do
      {:ok, columns, rows} -> {:ok, %{columns: columns, rows: rows}}
      other -> raise_argument_error(other)
    end
  end

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
  Bytes the connection holds, by purpose: `:cache_used` (page cache),
  `:schema_used`, `:stmt_used` (prepared statements) and `:lookaside_used`.
  """
  @spec memory(conn) :: {:ok, %{atom => non_neg_integer}} | {:error, term}
  def memory(conn), do: Native.sqlite_memory(conn)

  defp raise_argument_error({:error, {:argument_error, message}}),
    do: raise(ArgumentError, message)

  defp raise_argument_error(other), do: other
end
