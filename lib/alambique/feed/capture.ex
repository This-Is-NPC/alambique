defmodule Alambique.Feed.Capture do
  @moduledoc false
  # Change capture inside a cell's SQLite file (design §13).
  #
  # Every user table has three triggers that insert one row per change into the
  # internal table `_alambique_feed`, in the same transaction as the change.
  # The rows are born pending (a negative `txid`); the cell stamps them with
  # the batch TXID and time just before the batch is uploaded, so the triggers
  # need no per-transaction state. The triggers are part of the file and are
  # generated again whenever the schema changes: after migrations, and around
  # DDL.

  alias Alambique.Cell.Db

  @feed "_alambique_feed"
  @state "_alambique_feed_state"
  @trigger_prefix "_alambique_feed_t_"
  # Changes when the trigger SQL changes, so the triggers of existing files regenerate.
  @format 1

  # A pending row has a negative TXID and a time of 2^40, which stamping
  # replaces with the batch's own values. SQLite stores an integer in the
  # fewest bytes its value needs, and a record that keeps its size is
  # overwritten in place: only the page of its header is written, not the
  # overflow pages of a large value. So the pending TXID is of the same width
  # as the TXID the next batch will take (the saved `_alambique_feed_pending`),
  # and a time in milliseconds is always of six bytes, like 2^40.
  @pending "_alambique_feed_pending"
  @pending_time 1_099_511_627_776

  # Possessive and atomic, so that a statement of many comments cannot
  # backtrack exponentially.
  @ddl ~r/\A(?:\s++|--[^\n]*+(?:\n|\z)|\/\*(?>.*?\*\/))*+(?:create|drop|alter)\b/is

  @doc "Whether `sql` is a statement that can change the schema."
  @spec ddl?(String.t()) :: boolean
  def ddl?(sql), do: Regex.match?(@ddl, sql)

  @typedoc """
  What the feed captures: the `feed` map of the instance configuration. A table
  is captured whole (`:capture`), not at all (`:skip`), without some columns
  (`{:omit, columns}`) or as its key plus some columns (`{:only, columns}`).
  `tables` says it for the tables by name and `other_tables` for the rest.
  """
  @type select :: %{
          required(:internal_prefixes) => [String.t()],
          required(:tables) => %{String.t() => :capture | :skip | {:omit | :only, [String.t()]}},
          required(:other_tables) => :capture | :skip,
          optional(atom) => term
        }

  @everything %{internal_prefixes: [], tables: %{}, other_tables: :capture}

  @doc "The selection that captures every table whole."
  @spec everything() :: select
  def everything, do: @everything

  @doc """
  Whether a table is never captured: it belongs to the library or to SQLite,
  or its name starts with one of the host's `prefixes` (`feed: [internal_prefixes: ...]`).
  """
  @spec internal?(String.t(), [String.t()]) :: boolean
  def internal?(name, prefixes \\ []),
    do: String.starts_with?(name, ["_alambique_", "sqlite_" | prefixes])

  # Every function below keeps the number of statements small: each one is a
  # call into SQLite on a dirty scheduler, and waking thousands of cells at
  # once makes those calls the cost of a wake.

  # Triggers

  @doc """
  Whether the triggers match the schema: the fingerprint saved with the
  triggers is the one of the captured tables. A file that never had triggers
  and has no captured table matches. The fingerprint covers what is captured
  of each table, so changing the selection regenerates the triggers.
  """
  @spec current?(term, select) :: boolean
  def current?(conn, select) do
    case {user_tables(conn, select), saved_fingerprint(conn)} do
      {[], nil} -> true
      {tables, saved} -> saved == fingerprint(tables)
    end
  end

  @doc "Whether the internal tables exist."
  @spec tables?(term) :: boolean
  def tables?(conn) do
    {:ok, %{rows: [[count]]}} =
      Db.run(
        conn,
        "SELECT count(*) FROM main.sqlite_schema WHERE type = 'table' AND name IN (?, ?, ?)",
        [@feed, @state, @pending]
      )

    count == 3
  end

  @doc "Drops the triggers and forgets the fingerprint. Run it in a transaction."
  @spec uninstall(term) :: :ok
  def uninstall(conn), do: Db.script(conn, drops(conn))

  # DROP statements for the triggers, and the reset of the fingerprint when
  # the state table exists.
  defp drops(conn) do
    {:ok, %{rows: rows}} =
      Db.run(
        conn,
        """
        SELECT type, name FROM main.sqlite_schema
        WHERE (type = 'trigger' AND name LIKE ? ESCAPE '\\') OR (type = 'table' AND name = ?)
        """,
        [String.replace(@trigger_prefix, "_", "\\_") <> "%", @state]
      )

    Enum.map_join(rows, "\n", fn
      ["trigger", name] -> "DROP TRIGGER main.#{ident(name)};"
      ["table", _] -> "UPDATE #{@state} SET schema = '';"
    end)
  end

  @doc """
  Creates the internal tables if needed and generates the triggers of every
  user table afresh. Run it in a transaction.
  """
  @spec install(term, select) :: :ok
  def install(conn, select) do
    started = System.monotonic_time()
    tables = user_tables(conn, select)
    keys = keys(conn)

    triggers =
      for {table, _sql, spec} <- tables, do: triggers(table, Map.fetch!(keys, table), spec)

    :ok =
      Db.script(conn, [
        tables_sql(),
        drops(conn),
        triggers,
        """
        INSERT INTO #{@state} (id, published_txid, last_time, schema) VALUES (1, 0, 0, '#{fingerprint(tables)}')
        ON CONFLICT (id) DO UPDATE SET schema = excluded.schema;
        """
      ])

    Alambique.Telemetry.span_stop([:feed, :install], started, %{tables: length(tables)}, %{})
  end

  defp tables_sql do
    """
    CREATE TABLE IF NOT EXISTS #{@feed} (
      seq INTEGER PRIMARY KEY,
      txid INTEGER NOT NULL DEFAULT 0,
      ts INTEGER NOT NULL DEFAULT 0,
      tbl TEXT NOT NULL,
      op TEXT NOT NULL,
      key TEXT NOT NULL,
      vals TEXT
    );
    CREATE INDEX IF NOT EXISTS #{@feed}_txid ON #{@feed} (txid);
    CREATE TABLE IF NOT EXISTS #{@state} (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      published_txid INTEGER NOT NULL,
      last_time INTEGER NOT NULL,
      schema TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS #{@pending} (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      txid INTEGER NOT NULL
    );
    INSERT OR IGNORE INTO #{@pending} (id, txid) VALUES (1, #{placeholder(2)});
    """
  end

  @doc """
  Runs `fun`, a statement that may change the schema, with the triggers
  dropped (SQLite refuses to drop a column that a trigger names), then
  generates them again for the new schema. Run it in a transaction.
  """
  @spec around_ddl(term, select, (-> result)) :: result when result: var
  def around_ddl(conn, select, fun) do
    :ok = uninstall(conn)

    try do
      fun.()
    after
      if not current?(conn, select), do: :ok = install(conn, select)
    end
  end

  defp saved_fingerprint(conn) do
    case Db.run(conn, "SELECT schema FROM #{@state}", []) do
      {:ok, %{rows: [[fingerprint]]}} -> fingerprint
      _ -> nil
    end
  end

  # The definitions of the user tables decide the triggers.
  defp fingerprint(tables) do
    :crypto.hash(:sha256, :erlang.term_to_binary({@format, tables}))
    |> Base.encode16(case: :lower)
  end

  # `{name, create statement, what is captured}` of every captured table, by
  # name. Virtual tables and their shadow tables are not listed by
  # `table_list` as tables.
  defp user_tables(conn, select) do
    {:ok, %{rows: rows}} =
      Db.run(
        conn,
        """
        SELECT l.name, s.sql FROM pragma_table_list l JOIN main.sqlite_schema s ON s.name = l.name
        WHERE l.schema = 'main' AND l.type = 'table' AND s.type = 'table' ORDER BY l.name
        """,
        []
      )

    for [name, sql] <- rows,
        not internal?(name, select.internal_prefixes),
        spec = Map.get(select.tables, name, select.other_tables),
        spec != :skip,
        do: {name, sql, spec}
  end

  # Names of the columns and the key of every user table, in one query: the
  # primary key columns in key order, or the rowid when there are none.
  defp keys(conn) do
    {:ok, %{rows: rows}} =
      Db.run(
        conn,
        """
        SELECT l.name, p.name, p.pk FROM pragma_table_list l JOIN pragma_table_info(l.name) p
        WHERE l.schema = 'main' AND l.type = 'table' ORDER BY l.name, p.cid
        """,
        []
      )

    rows
    |> Enum.group_by(&hd/1, fn [_, column, pk] -> {column, pk} end)
    |> Map.new(fn {table, columns} ->
      pk = for({column, pos} <- Enum.sort_by(columns, &elem(&1, 1)), pos > 0, do: column)

      {table,
       %{columns: Enum.map(columns, &elem(&1, 0)), key: if(pk == [], do: :rowid, else: pk)}}
    end)
  end

  defp triggers(table, %{columns: columns, key: key}, spec) do
    base = @trigger_prefix <> table
    insert = "INSERT INTO #{@feed} (txid, ts, tbl, op, key, vals)"
    pending = "(SELECT txid FROM #{@pending}), #{@pending_time}"
    name = literal(table)
    changed = key_changed(key)
    captured = captured(columns, key, spec)
    new = "#{key_json(key, "new")}, #{values_json(captured, "new")}"

    [
      {"_i", "INSERT", "", "#{insert} VALUES (#{pending}, #{name}, 'insert', #{new});"},
      {"_u", "UPDATE", update_when(spec, captured, key),
       """
       #{insert} SELECT #{pending}, #{name}, 'delete', #{key_json(key, "old")}, NULL WHERE #{changed};
       #{insert} VALUES (#{pending}, #{name}, CASE WHEN #{changed} THEN 'insert' ELSE 'update' END, #{new});
       """},
      {"_d", "DELETE", "",
       "#{insert} VALUES (#{pending}, #{name}, 'delete', #{key_json(key, "old")}, NULL);"}
    ]
    |> Enum.map(fn {suffix, event, condition, body} ->
      "CREATE TRIGGER #{ident(base <> suffix)} AFTER #{event} ON #{ident(table)}#{condition} BEGIN #{body} END;\n"
    end)
  end

  # The key columns are always captured, with the columns the selection keeps,
  # in declaration order. A column the table does not have is ignored.
  defp captured(columns, _key, :capture), do: columns

  defp captured(columns, key, {kind, names}) do
    keep = if key == :rowid, do: [], else: key

    Enum.filter(columns, fn column ->
      listed? = column in names
      column in keep or if(kind == :omit, do: not listed?, else: listed?)
    end)
  end

  # A table that leaves columns out skips an update that changes only those:
  # the record would repeat the last one.
  defp update_when(:capture, _captured, _key), do: ""

  defp update_when(_spec, captured, key) do
    changes = Enum.map(captured, &"old.#{ident(&1)} IS NOT new.#{ident(&1)}")
    changes = if key == :rowid, do: ["old.rowid IS NOT new.rowid" | changes], else: changes
    " WHEN " <> Enum.join(changes, " OR ")
  end

  defp key_json(:rowid, ref), do: "json_object('rowid', #{ref}.rowid)"

  defp key_json(columns, ref),
    do: "json_object(#{Enum.map_join(columns, ", ", &"#{literal(&1)}, #{ref}.#{ident(&1)}")})"

  defp key_changed(:rowid), do: "old.rowid IS NOT new.rowid"

  defp key_changed(columns),
    do: Enum.map_join(columns, " OR ", &"old.#{ident(&1)} IS NOT new.#{ident(&1)}")

  # json_object refuses blobs, so they travel as {"$blob": "<hex>"}.
  defp values_json(columns, ref) do
    pairs =
      Enum.map_join(columns, ", ", fn column ->
        value = "#{ref}.#{ident(column)}"

        "#{literal(column)}, CASE WHEN typeof(#{value}) = 'blob' THEN json_object('$blob', lower(hex(#{value}))) ELSE #{value} END"
      end)

    "json_object(#{pairs})"
  end

  defp ident(name), do: ~s("#{String.replace(name, ~s("), ~s(""))}")
  defp literal(text), do: "'#{String.replace(text, "'", "''")}'"

  # Rows

  # Statements on the internal tables fail with "no such table" in a file that
  # has no user table yet; that means there is nothing to read or stamp.
  defp rows(conn, sql, params) do
    case Db.run(conn, sql, params) do
      {:ok, %{rows: rows}} -> rows
      {:error, "no such table" <> _} -> []
    end
  end

  @doc """
  Captured rows of stamped batches after `txid` up to `upto`, in commit order:
  `[txid, ts, table, op, key, values]`.
  """
  @spec rows_after(term, non_neg_integer, non_neg_integer) :: [list]
  def rows_after(conn, txid, upto) do
    rows(
      conn,
      "SELECT txid, ts, tbl, op, key, vals FROM #{@feed} WHERE txid > ? AND txid <= ? ORDER BY txid, seq",
      [txid, upto]
    )
  end

  @doc "Latest time any batch of this cell took, from the rows left and the saved state."
  @spec last_time(term) :: non_neg_integer
  def last_time(conn) do
    sql =
      "SELECT coalesce(max(t), 0) FROM (SELECT last_time AS t FROM #{@state} UNION ALL SELECT max(ts) FROM #{@feed})"

    case rows(conn, sql, []) do
      [[time]] -> time
      [] -> 0
    end
  end

  @doc """
  Gives the rows written since the last stamp the TXID and the time of the
  batch they will be uploaded in, and saves the pending TXID of the next
  batch.
  """
  @spec stamp(term, pos_integer, integer) :: :ok
  def stamp(conn, txid, ts) do
    # All integers; the script is one dirty IO call (`Db.writable_script/2`).
    # The last statement writes only when the width of the TXIDs changes.
    next = placeholder(txid + 1)

    case Db.writable_script(
           conn,
           """
           UPDATE #{@feed} SET txid = #{txid}, ts = #{ts} WHERE txid < 0;
           UPDATE #{@pending} SET txid = #{next} WHERE txid <> #{next}
           """
         ) do
      :ok -> :ok
      {:error, "no such table" <> _} -> :ok
    end
  end

  # A negative integer that takes as many bytes as `txid` does.
  defp placeholder(txid) when txid <= 127, do: -2
  defp placeholder(txid) when txid <= 32_767, do: -129
  defp placeholder(txid) when txid <= 8_388_607, do: -32_769
  defp placeholder(txid) when txid <= 2_147_483_647, do: -8_388_609
  defp placeholder(_txid), do: -2_147_483_649

  @doc """
  Adds the marker of a rewind (design §20) as the only row of batch `txid`,
  created with the internal tables if the file has none yet, and raises the
  saved time to `ts`. `json` is the marker (`Alambique.Feed.Rewind.encode/1`).
  Run it in a transaction.
  """
  @spec mark_rewind(term, select, pos_integer, integer, String.t()) :: :ok
  def mark_rewind(conn, select, txid, ts, json) do
    unless tables?(conn), do: :ok = install(conn, select)

    {:ok, _} =
      Db.run(
        conn,
        "INSERT INTO #{@feed} (txid, ts, tbl, op, key, vals) VALUES (?, ?, '', 'rewind', '{}', ?)",
        [txid, ts, json]
      )

    :ok = Db.script(conn, raise_floor(ts))
  end

  @doc "Deletes the rows of stamped batches up to `txid` and remembers the time, in the caller's transaction."
  @spec cleanup(term, non_neg_integer, integer) :: :ok
  def cleanup(conn, txid, time) do
    :ok =
      Db.script(conn, """
      DELETE FROM #{@feed} WHERE txid > 0 AND txid <= #{txid};
      #{raise_floor(time)}
      """)
  end

  # The latest time is saved in a row of the feed table itself (seq 0, TXID 0,
  # so no batch reads it): the delete above already writes that table's page,
  # and a row in another table would write one more page on every commit.
  # Files of the first release saved it in `_alambique_feed_state.last_time`,
  # which `last_time/1` still reads.
  defp raise_floor(time) do
    """
    INSERT INTO #{@feed} (seq, txid, ts, tbl, op, key) VALUES (0, 0, #{time}, '', 'floor', '{}')
    ON CONFLICT (seq) DO UPDATE SET ts = max(ts, excluded.ts);
    """
  end
end
