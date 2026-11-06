defmodule Alambique.Rewind.Reconcile do
  @moduledoc false
  # Turns the restored target database into the state a rewind installs
  # (design §20): the host tables are carried over from the current state, and
  # the feed gets the marker of the rewind. Everything happens in one
  # transaction on the target file; nothing else is changed.

  alias Alambique.Cell.Db
  alias Alambique.Feed.{Capture, Rewind}
  alias Alambique.{Sqlite, Tx}

  @builtin ["_alambique_", "sqlite_"]

  @type opts :: %{
          keep: [String.t()],
          select: Alambique.Feed.Capture.select(),
          capture: boolean,
          txid: pos_integer,
          ts: integer,
          marker: %{from: non_neg_integer, to: non_neg_integer, target: tuple}
        }

  @doc """
  `conn` is the connection to the target file, `current` the path of the
  restored current state. `keep` lists the prefixes of the tables to carry
  over (empty for none). Returns the names of the carried tables and the
  number of pages of the reconciled database.
  """
  @spec run(Sqlite.conn(), Path.t(), opts) ::
          {:ok, %{kept: [String.t()], pages: pos_integer}} | {:error, term}
  def run(conn, current, opts) do
    Db.writable(conn, fn ->
      # Read-only and by URI: attaching a path that is gone must fail, where a
      # plain `ATTACH` would create an empty database and the host tables would
      # be carried over empty.
      case Sqlite.execute(conn, "ATTACH DATABASE #{literal(uri(current))} AS cur") do
        :ok ->
          try do
            with {:ok, kept} <-
                   Db.transaction(conn, opts.txid, &transact(&1, opts), capture: false),
                 {:ok, %{rows: [[pages]]}} <- Db.run(conn, "PRAGMA page_count", []) do
              {:ok, %{kept: kept, pages: pages}}
            end
          after
            Sqlite.execute(conn, "DETACH DATABASE cur")
          end

        {:error, reason} ->
          {:error, {:attach, reason}}
      end
    end)
  end

  defp transact(tx, opts) do
    kept = if opts.keep == [], do: [], else: carry(tx, opts.keep)
    marker = Rewind.encode(Map.put(opts.marker, :kept, kept))

    if opts.capture,
      do: :ok = Capture.mark_rewind(tx.conn, opts.select, opts.txid, opts.ts, marker)

    kept
  end

  # Replaces every host table of the target by the one of the current state.
  defp carry(tx, prefixes) do
    tables = query(tx, "SELECT name, sql FROM cur.sqlite_schema WHERE type = 'table'")
    kept = for [name, sql] <- tables, kept?(name, prefixes), do: {name, sql}

    for {name, sql} <- kept,
        Regex.match?(~r/\A\s*create\s+virtual/i, sql),
        do: Tx.rollback(tx, {:keep_unsupported, name})

    drop_existing(tx, prefixes)
    Enum.each(kept, fn {name, sql} -> execute(tx, sql, [], name) end)
    Enum.each(kept, fn {name, _} -> copy(tx, name) end)
    sequences(tx, Enum.map(kept, &elem(&1, 0)))
    dependents(tx, prefixes)
    Enum.map(kept, &elem(&1, 0)) |> Enum.sort()
  end

  # Indexes, triggers and views of the carried tables; the objects of the
  # library are never carried, whatever table they sit on.
  defp dependents(tx, prefixes) do
    rows =
      query(
        tx,
        "SELECT name, tbl_name, sql FROM cur.sqlite_schema " <>
          "WHERE type IN ('index', 'trigger', 'view') AND sql IS NOT NULL ORDER BY rowid"
      )

    for [name, table, sql] <- rows,
        kept?(table, prefixes),
        not String.starts_with?(name, @builtin),
        do: execute(tx, sql, [], name)
  end

  defp drop_existing(tx, prefixes) do
    for [type, name] <-
          query(tx, "SELECT type, name FROM main.sqlite_schema WHERE type IN ('table', 'view')"),
        kept?(name, prefixes) do
      execute(tx, "DROP #{String.upcase(type)} IF EXISTS main.#{ident(name)}", [], name)
    end
  end

  defp copy(tx, table) do
    columns =
      query(tx, "SELECT name FROM pragma_table_xinfo(?, 'cur') WHERE hidden = 0", [table])
      |> Enum.map(fn [name] -> ident(name) end)

    [[rowid?]] =
      query(tx, "SELECT NOT wr FROM pragma_table_list WHERE schema = 'cur' AND name = ?", [table])

    names = Enum.join(if(rowid? == 1, do: ["rowid" | columns], else: columns), ", ")

    execute(
      tx,
      "INSERT INTO main.#{ident(table)} (#{names}) SELECT #{names} FROM cur.#{ident(table)}",
      [],
      table
    )
  end

  defp sequences(_tx, []), do: :ok

  defp sequences(tx, tables) do
    # Only a file with an AUTOINCREMENT table has the sequence table.
    if query(tx, "SELECT 1 FROM cur.sqlite_schema WHERE name = 'sqlite_sequence'") != [] and
         query(tx, "SELECT 1 FROM main.sqlite_schema WHERE name = 'sqlite_sequence'") != [] do
      marks = Enum.map_join(tables, ", ", fn _ -> "?" end)

      execute(
        tx,
        "DELETE FROM main.sqlite_sequence WHERE name IN (#{marks})",
        tables,
        "sqlite_sequence"
      )

      execute(
        tx,
        "INSERT INTO main.sqlite_sequence SELECT name, seq FROM cur.sqlite_sequence WHERE name IN (#{marks})",
        tables,
        "sqlite_sequence"
      )
    end

    :ok
  end

  @doc "Whether a table is carried for the prefixes of `keep`."
  @spec kept?(String.t(), [String.t()]) :: boolean
  def kept?(name, prefixes),
    do: String.starts_with?(name, prefixes) and not String.starts_with?(name, @builtin)

  defp query(tx, sql, params \\ []) do
    case Tx.query(tx, sql, params) do
      {:ok, %{rows: rows}} -> rows
      {:error, reason} -> Tx.rollback(tx, {:reconcile, reason})
    end
  end

  defp execute(tx, sql, params, object) do
    case Tx.execute(tx, sql, params) do
      {:ok, _} -> :ok
      {:error, reason} -> Tx.rollback(tx, {:reconcile, object, reason})
    end
  end

  defp uri(path),
    do: "file:" <> URI.encode(path, &(URI.char_unreserved?(&1) or &1 == ?/)) <> "?mode=ro"

  defp ident(name), do: ~s("#{String.replace(name, ~s("), ~s(""))}")
  defp literal(text), do: "'#{String.replace(text, "'", "''")}'"
end
