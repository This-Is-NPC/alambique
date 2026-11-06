defmodule Alambique.Tx do
  @moduledoc """
  The connection handle that `Alambique.transaction/3` passes to its function.

  A handle is valid only while the function runs, and only in the process
  that runs it (the cell process, on the node that owns the cell). Using it
  anywhere else returns `{:error, :inactive_transaction}`.

  Parameters are bound as in `Alambique.Sqlite.bind/2`: integers, floats,
  `nil`, binaries as text and `{:blob, binary}` as blobs. Results are
  `%{columns: [String.t()], rows: [[term]]}`. `script/2` creates a schema with
  one change-capture cycle.
  """

  alias Alambique.Cell.Db
  alias Alambique.Feed.Capture

  alias Alambique.Cell.Sql

  @enforce_keys [:conn, :txid, :ref]
  defstruct [:conn, :txid, :ref, capture: true, select: nil, isolated: false, limits: []]

  @opaque t :: %__MODULE__{
            conn: term,
            txid: pos_integer,
            ref: reference,
            capture: boolean,
            select: Capture.select(),
            isolated: boolean,
            limits: keyword
          }

  @doc "Runs a statement and returns its rows."
  @spec query(t, String.t(), list) :: {:ok, Db.result()} | {:error, term}
  def query(%__MODULE__{} = tx, sql, params \\ []), do: guarded(tx, sql, params)

  @doc "Runs a statement for its effect. Rows are returned for `RETURNING` clauses."
  @spec execute(t, String.t(), list) :: {:ok, Db.result()} | {:error, term}
  def execute(%__MODULE__{} = tx, sql, params \\ []), do: guarded(tx, sql, params)

  @doc """
  Runs a script of schema statements (`CREATE`, `DROP`, `ALTER`) separated by
  semicolons, with one change-capture cycle for the whole script instead of
  one per statement. The triggers of new tables are installed before it
  returns, so later writes in the same transaction are captured.

  The script is atomic: if a statement fails, none of its effects remain and
  the error is returned. A script with any other statement, such as an
  `INSERT`, is refused with `{:error, {:not_ddl, statement}}` before it runs,
  because rows written while the triggers are dropped would escape the feed.
  Use `execute/3` for those. Returns `:ok`.
  """
  @spec script(t, String.t()) :: :ok | {:error, term}
  def script(%__MODULE__{conn: conn, capture: capture, select: select} = tx, sql) do
    with :ok <- active(tx),
         :ok <- ddl_only(Sql.split(sql)) do
      if capture,
        do: Capture.around_ddl(conn, select, fn -> atomic(conn, sql) end),
        else: atomic(conn, sql)
    end
  end

  defp ddl_only(statements) do
    case Enum.find(statements, &(not Capture.ddl?(&1))) do
      nil -> :ok
      statement -> {:error, {:not_ddl, statement}}
    end
  end

  # A savepoint makes the script all or nothing inside the transaction. Each
  # of its statements is a call of its own, so the caller's text (an open
  # comment, say) cannot swallow them.
  defp atomic(conn, sql) do
    :ok = Db.script(conn, "SAVEPOINT alambique_script")

    case Db.script(conn, sql) do
      :ok ->
        :ok = Db.script(conn, "RELEASE alambique_script")

      {:error, _} = error ->
        :ok = Db.script(conn, "ROLLBACK TO alambique_script")
        :ok = Db.script(conn, "RELEASE alambique_script")
        error
    end
  end

  @doc """
  TXID of the replicated batch this transaction will join (design §13). Every
  transaction that commits before the next upload starts shares it.
  """
  @spec txid(t) :: pos_integer
  def txid(%__MODULE__{txid: txid}), do: txid

  @doc """
  TXID of the last batch before this transaction's, for a transaction run
  with `isolated: true` (see `Alambique.transaction/4`): the pending commits
  were uploaded first and this transaction owns the next batch, so a restore
  of this TXID holds exactly the writes committed before it.

  Any other transaction shares its batch with earlier commits that no upload
  separates from it, and gets `{:error, :not_isolated}`.
  """
  @spec before(t) :: {:ok, non_neg_integer} | {:error, :not_isolated}
  def before(%__MODULE__{isolated: true, txid: txid}), do: {:ok, txid - 1}
  def before(%__MODULE__{}), do: {:error, :not_isolated}

  @doc "Aborts the transaction: it rolls back and the caller gets `{:error, reason}`."
  @spec rollback(t, term) :: no_return
  def rollback(%__MODULE__{}, reason), do: throw({:alambique_rollback, reason})

  defp active(%__MODULE__{ref: ref}) do
    if Process.get(Db.active_key()) == ref, do: :ok, else: {:error, :inactive_transaction}
  end

  defp guarded(%__MODULE__{conn: conn, capture: capture, select: select} = tx, sql, params) do
    with :ok <- active(tx) do
      if capture and Capture.ddl?(sql),
        do: Capture.around_ddl(conn, select, fn -> Db.run(conn, sql, params, tx.limits) end),
        else: Db.run(conn, sql, params, tx.limits)
    end
  end
end
