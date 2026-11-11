defmodule Alambique.Scrub do
  @moduledoc """
  Background check of the replicas (design §6.1).

  A round samples cells of the placement's locations, restores each one into
  a temporary directory without waking it (no lease, nothing is written to
  the store) and checks that

    1. the restore succeeded, which includes the checksums of every file and
       of the chain,
    2. the checksum of the restored file equals the checksum the chain ends
       with, and
    3. SQLite's `PRAGMA integrity_check` (or `quick_check`) answers `ok`.

  A cell that fails is reported, never frozen. The failure is logged as an
  error and emitted as `[:alambique, :scrub, :cell]`.

  The instance option `:scrub` (default `false`) turns it on with a keyword
  list:

    * `:interval_ms` - time between rounds (default 3 600 000)
    * `:cells` - cells sampled per round (default 10)
    * `:concurrency` - cells restored at once (default 1)
    * `:rate` - cells started per second at most (default 1)
    * `:timeout_ms` - time budget of one restore, and of its check (default
      120 000)
    * `:check` - `:integrity_check` (default) or `:quick_check`
    * `:attempts` - restores of a cell before it counts as failed (default
      2); a restore that races with a deletion fails once and succeeds again
    * `:page_size` - ids read per draw, one of them is scrubbed (default 20)
    * `:prefix` - sample only cells whose id starts with it (default `""`)

  ## Sampling

  Each location has a cursor that walks through its ids across rounds (kept
  in an ETS table of the instance; it starts at a random id). A draw
  reads the `:page_size` ids after the cursor, scrubs one of them at random
  and moves the cursor to the last id of the page; past the last id the walk
  wraps to the start. A round makes `:cells` draws at random locations, so it
  costs `:cells` listings and never lists a location. Every cell is drawn with
  the same chance per pass over the ids, `1 / :page_size`; a larger
  `:page_size` scrubs fewer cells per pass. A restart of the node starts a new
  walk at a random id. `:prefix` restricts the walk to one part of the ids.
  The cells come from the schema index (design §17), so a cell that never
  woke is not sampled.
  """

  require Logger

  alias Alambique.{Config, Placement, Schema, Telemetry}
  alias Alambique.Cell.Db
  alias Alambique.Replication
  alias Alambique.Replication.{Native, Retry}
  alias Alambique.Sqlite

  @defaults [
    interval_ms: 3_600_000,
    cells: 10,
    concurrency: 1,
    rate: 1,
    timeout_ms: 120_000,
    page_size: 20,
    attempts: 2,
    check: :integrity_check,
    prefix: ""
  ]

  @alphabet ~c"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-"

  @type failure :: %{cell: String.t(), location: String.t(), reason: term}
  @type summary :: %{
          cells: non_neg_integer,
          ok: non_neg_integer,
          empty: non_neg_integer,
          failed: non_neg_integer,
          failures: [failure]
        }

  @doc "Defaults of the `:scrub` option."
  @spec defaults() :: keyword
  def defaults, do: @defaults

  @doc false
  def children(config) do
    # The cursors live in a table owned by the instance, so they go with it.
    cursors = %{
      id: Alambique.Scrub.Cursors,
      start:
        {Agent, :start_link,
         [fn -> :ets.new(table(config), [:named_table, :public, :set]) && :ok end]}
    }

    if config.scrub do
      [
        cursors,
        %{
          id: Alambique.Scrub,
          start:
            {Alambique.Periodic, :start_link,
             ["scrub", config.scrub.interval_ms, fn -> run(config, []) end]}
        }
      ]
    else
      [cursors]
    end
  end

  defp table(config), do: Module.concat(config.name, ScrubCursors)

  @doc """
  One scrub round now. Options override the instance's `:scrub` settings
  (all of the list above except `:interval_ms`).

  Returns `{:ok, %{cells, ok, empty, failed, failures}}`: cells sampled, found
  sound, found without data, and failed with the reason of each.
  """
  @spec run(Config.t(), keyword) :: {:ok, summary} | {:error, term}
  def run(config, opts \\ []) do
    started = System.monotonic_time()
    settings = settings(config, opts)

    result =
      with {:ok, locations} <- Placement.locations(config),
           {:ok, samples} <- sample(config, settings, locations) do
        {:ok, check_all(config, settings, samples)}
      end

    summary =
      case result do
        {:ok, summary} -> summary
        {:error, _} -> %{cells: 0, ok: 0, empty: 0, failed: 0}
      end

    Telemetry.span_stop(
      [:scrub, :round],
      started,
      Map.take(summary, [:cells, :ok, :empty, :failed]),
      %{instance: config.name, result: if(match?({:ok, _}, result), do: :ok, else: :error)}
    )

    result
  end

  defp settings(config, opts) do
    keys = Keyword.keys(@defaults) -- [:interval_ms]

    (config.scrub || Map.new(@defaults))
    |> Map.merge(Map.new(Keyword.take(opts, keys)))
  end

  # Sampling

  defp sample(_config, _settings, []), do: {:ok, []}

  defp sample(config, settings, locations) do
    cursors = Map.new(locations, &{&1.id, saved_cursor(config, &1, settings)})

    result =
      Enum.reduce_while(1..settings.cells, {:ok, MapSet.new(), cursors}, fn _,
                                                                            {:ok, acc, cursors} ->
        location = Enum.random(locations)

        case draw(config, settings, location, cursors[location.id]) do
          {:ok, nil, cursor} ->
            {:cont, {:ok, acc, Map.put(cursors, location.id, cursor)}}

          {:ok, cell, cursor} ->
            {:cont,
             {:ok, MapSet.put(acc, {location, cell}), Map.put(cursors, location.id, cursor)}}

          {:error, _} = error ->
            {:halt, error}
        end
      end)

    with {:ok, samples, cursors} <- result do
      for location <- locations, do: save_cursor(config, location, cursors[location.id])
      {:ok, MapSet.to_list(samples)}
    end
  end

  # One page after the cursor, then one of its cells; the page's last id is
  # the next cursor and the end of the ids wraps to the start.
  defp draw(config, settings, location, cursor) do
    case page(config, settings, location, cursor) do
      {:ok, [], _} when cursor != nil -> draw(config, settings, location, nil)
      {:ok, [], _} -> {:ok, nil, nil}
      {:ok, cells, next} -> {:ok, Enum.random(cells), next}
      {:error, _} = error -> error
    end
  end

  defp page(config, settings, location, cursor) do
    Schema.cells(
      config,
      location,
      settings.prefix,
      cursor,
      settings.page_size,
      Retry.deadline_in(config.schema.listing_timeout_ms)
    )
  end

  # Where the walk through a location's ids went on from; random at first.
  defp saved_cursor(config, location, settings) do
    case :ets.lookup(table(config), location.id) do
      [{_, cursor}] -> cursor
      [] -> random_cursor(settings.prefix)
    end
  end

  defp save_cursor(config, location, cursor),
    do: :ets.insert(table(config), {location.id, cursor})

  defp random_cursor(prefix) do
    length = :rand.uniform(4)
    prefix <> List.to_string(for _ <- 1..length, do: Enum.random(@alphabet))
  end

  # Checking

  defp check_all(config, settings, samples) do
    empty = %{cells: 0, ok: 0, empty: 0, failed: 0, failures: []}

    samples
    |> Enum.chunk_every(settings.concurrency)
    |> Enum.reduce(empty, fn chunk, acc ->
      started = System.monotonic_time(:millisecond)

      results =
        chunk
        |> Task.async_stream(
          fn {location, cell} -> scrub_cell(config, settings, location, cell) end,
          max_concurrency: settings.concurrency,
          timeout: :infinity
        )
        |> Enum.zip(chunk)
        |> Enum.map(fn
          {{:ok, result}, _} -> result
          {{:exit, reason}, {location, cell}} -> {location.id, cell, {:error, {:exit, reason}}}
        end)

      pace(settings.rate, length(chunk), started)
      Enum.reduce(results, acc, &count/2)
    end)
  end

  defp count({location, cell, result}, acc) do
    acc = %{acc | cells: acc.cells + 1}

    case result do
      {:ok, :empty} -> %{acc | empty: acc.empty + 1}
      {:ok, _txid} -> %{acc | ok: acc.ok + 1}
      {:error, reason} -> failed(acc, location, cell, reason)
    end
  end

  defp failed(acc, location, cell, reason) do
    %{
      acc
      | failed: acc.failed + 1,
        failures: [%{cell: cell, location: location, reason: reason} | acc.failures]
    }
  end

  defp pace(rate, count, started) do
    wait = div(count * 1000, rate) - (System.monotonic_time(:millisecond) - started)
    if wait > 0, do: Process.sleep(wait)
  end

  defp scrub_cell(config, settings, location, cell) do
    started = System.monotonic_time()
    result = attempts(config, settings, location, cell, settings.attempts)

    {measurements, meta} =
      case result do
        {:ok, :empty} -> {%{}, %{result: :empty}}
        {:ok, txid} -> {%{txid: txid}, %{result: :ok}}
        {:error, reason} -> {%{}, %{result: :error, reason: reason}}
      end

    Telemetry.span_stop(
      [:scrub, :cell],
      started,
      measurements,
      Map.merge(meta, %{cell: cell, location: location.id})
    )

    with {:error, reason} <- result do
      Logger.error(
        "Alambique scrub: replica of #{cell} at #{location.id} is damaged: #{inspect(reason)}"
      )
    end

    {location.id, cell, result}
  end

  defp attempts(config, settings, location, cell, left) do
    case check(config, settings, location, cell) do
      {:error, _} when left > 1 -> attempts(config, settings, location, cell, left - 1)
      other -> other
    end
  end

  # Restores into a directory of its own, removed in every case.
  defp check(config, settings, location, cell) do
    dir = Alambique.Cell.Local.work_dir(config, :scrub)
    path = Path.join(dir, "cell.db")
    ctx = %{Config.repl_ctx(config, location) | timeout_ms: settings.timeout_ms}

    try do
      case Replication.restore(ctx, cell, path, :latest) do
        {:ok, %{txid: 0}} -> {:ok, :empty}
        {:ok, result} -> verify(path, result, settings.check, settings.timeout_ms)
        {:error, reason} -> {:error, {:restore, reason}}
      end
    after
      File.rm_rf(dir)
    end
  end

  @doc false
  def verify(path, result, check, timeout_ms) do
    with :ok <- file_checksum(path, result),
         :ok <- integrity(path, check, timeout_ms) do
      {:ok, result.txid}
    end
  end

  defp file_checksum(path, result) do
    case Native.file_state(path, result.page_size) do
      {:ok, _sums, checksum, _pages} when checksum == result.checksum -> :ok
      {:ok, _sums, checksum, _pages} -> {:error, {:checksum_mismatch, result.checksum, checksum}}
      {:error, reason} -> {:error, {:checksum, reason}}
    end
  end

  defp integrity(path, check, timeout_ms) do
    with {:ok, conn} <- Db.open_read_only(path, timeout_ms: timeout_ms) do
      try do
        case Sqlite.run(conn, "PRAGMA #{check}", []) do
          {:ok, %{rows: [["ok"]]}} -> :ok
          {:ok, %{rows: rows}} -> {:error, {check, Enum.map(rows, &hd/1)}}
          {:error, reason} -> {:error, {check, reason}}
        end
      after
        Db.close(conn)
      end
    else
      {:error, reason} -> {:error, {:open, reason}}
    end
  end
end
