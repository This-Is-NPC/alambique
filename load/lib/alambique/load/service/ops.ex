defmodule Alambique.Load.Service.Ops do
  @moduledoc false
  # What the driver calls on the service node (over distribution). Payloads are
  # built here from pools made at start, so that the driver sends a cell id and
  # not 75 KB, and the service does not spend CPU making them.

  alias Alambique.Load.Service.{DelayStore, Probe}

  @instance :load
  @upsert "INSERT OR REPLACE INTO notes (id, body) VALUES (?, ?)"
  @open "SELECT id, length(body) FROM notes ORDER BY id DESC LIMIT 20"
  @count "SELECT count(*) FROM notes"
  @pool 16

  @doc "The migrations of every cell the load test makes."
  def migrations, do: [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body BLOB NOT NULL)"}]

  @doc "Builds the pools of payloads; called once at start."
  def init_pools do
    :persistent_term.put({__MODULE__, :small}, pool(120))
    :persistent_term.put({__MODULE__, :large}, pool(75_000))
  end

  defp pool(bytes), do: List.to_tuple(for i <- 1..@pool, do: Alambique.BenchText.body(i, bytes))

  defp body(kind, seq) do
    pool = :persistent_term.get({__MODULE__, kind})
    elem(pool, rem(seq, @pool))
  end

  def ping, do: :pong

  @doc "A write of one row (`:small`, 120 bytes) or of a 75 KB blob (`:large`) into one of `rows` rows."
  def commit(cell, kind, seq, rows) do
    value = if kind == :large, do: {:blob, body(:large, seq)}, else: body(:small, seq)
    outcome(Alambique.execute(@instance, cell, @upsert, [rem(seq, rows) + 1, value]))
  end

  @doc "Wakes the cell if asleep and writes one small row."
  def touch(cell), do: commit(cell, :small, 0, 1)

  @doc "A read that opens the cell as a mailbox would; it wakes the cell if asleep."
  def wake(cell) do
    outcome(Alambique.query(@instance, cell, @open))
  end

  @doc "A cross-cell query over `cells`, nothing woken."
  def cross_query(cells) do
    case Alambique.Query.all(@instance, cells, @count) do
      %{errors: []} -> :ok
      %{errors: [error | _]} -> {:error, error}
    end
  end

  @doc """
  Writes `rows` rows of `row_bytes` bytes to each of `cells` in one transaction
  and puts the cell to sleep. Returns the cells that failed, each with its
  error as text.
  """
  def populate(cells, rows, row_bytes, concurrency) do
    pool = List.to_tuple(for i <- 1..@pool, do: Alambique.BenchText.body(i, row_bytes))

    cells
    |> Task.async_stream(
      fn cell ->
        with {:ok, :ok} <-
               Alambique.transaction(@instance, cell, fn tx ->
                 for i <- 1..rows do
                   {:ok, _} =
                     Alambique.Tx.execute(tx, @upsert, [
                       i,
                       {:blob, elem(pool, rem(i + :erlang.phash2(cell), @pool))}
                     ])
                 end

                 :ok
               end),
             do: Alambique.hibernate(@instance, cell)
      end,
      max_concurrency: concurrency,
      timeout: :infinity
    )
    |> Enum.zip(cells)
    |> Enum.reject(fn {{:ok, result}, _cell} -> result == :ok end)
    |> Enum.map(fn {{:ok, result}, cell} -> {cell, inspect(result, limit: 10)} end)
  end

  @doc "Puts `cells` to sleep (an asleep cell answers `:ok`); returns how many failed."
  def sleep_cells(cells, concurrency) do
    cells
    |> Task.async_stream(&Alambique.hibernate(@instance, &1),
      max_concurrency: concurrency,
      timeout: :infinity
    )
    |> Enum.count(fn {:ok, result} -> result != :ok end)
  end

  @doc """
  Operations of the driver still running on this node: processes `:erpc` started
  for a call, not counting this one.
  """
  def outstanding do
    me = self()

    Enum.count(Process.list(), fn pid ->
      pid != me and Process.info(pid, :initial_call) == {:initial_call, {:erpc, :execute_call, 4}}
    end)
  end

  @doc "Removes the local files of `cells`, so that the next wake restores them from the store."
  def drop_local(cells) do
    dir = Alambique.Config.fetch!(@instance).data_dir
    for cell <- cells, do: File.rm_rf(Path.join(dir, cell <> ".cell"))
    :ok
  end

  @doc "Cells awake on the node."
  def awake, do: Alambique.cells(@instance, limit: 1).count

  @doc "Waits until no cell is awake, up to `timeout_ms`."
  def await_asleep(timeout_ms) do
    cond do
      awake() == 0 -> :ok
      timeout_ms <= 0 -> {:error, {:still_awake, awake()}}
      true -> Process.sleep(100) && await_asleep(timeout_ms - 100)
    end
  end

  @doc "The counters of the store and of the native library, and the VM."
  def snapshot do
    %{
      store: DelayStore.counts(:persistent_term.get({__MODULE__, :counters})),
      native: Alambique.NativeStats.read(),
      awake: awake(),
      beam_total_mb: :erlang.memory(:total) / 1_048_576
    }
  end

  @doc "The effective settings, for the step files."
  def settings do
    config = Alambique.Config.fetch!(@instance)

    %{
      schedulers_online: :erlang.system_info(:schedulers_online),
      dirty_io_schedulers: :erlang.system_info(:dirty_io_schedulers),
      otp: List.to_string(:erlang.system_info(:otp_release)),
      cache_kib: config.awake.cache_kib,
      idle_timeout_ms: config.idle_timeout_ms,
      max_cells: config.awake.max_cells,
      wal_checkpoint_bytes: config.wal.checkpoint_bytes,
      max_concurrent_statements: config.sql.max_concurrent_statements,
      query_concurrency: config.query.concurrency,
      store_latency_ms: :persistent_term.get({__MODULE__, :latency})
    }
  end

  defdelegate probe_begin, to: Probe, as: :begin
  defdelegate probe_finish, to: Probe, as: :finish

  defp outcome({:ok, _}), do: :ok
  defp outcome({:error, reason}), do: {:error, reason}
end
