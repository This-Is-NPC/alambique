defmodule Alambique.Cell.Hydration do
  @moduledoc false
  # The lazy phase of a cell's database file (design §16): the hydrator
  # process that fetches the pages a restore left in the store, from the wake
  # until the file is complete.
  #
  # A cell that ends while pages are missing discards the file. The next wake
  # starts a new lazy restore: no marker is written, so the half-filled file
  # is never taken for a complete one.

  alias Alambique.Inflight
  alias Alambique.Cell.State
  alias Alambique.Replication.{Hydrator, Lazy, Native, Replicator, Retry}

  # Options of `Replication.restore/5` for this cell.
  @spec restore_opts(State.t()) :: keyword
  def restore_opts(%State{config: %{lazy_restore: nil}}), do: []

  def restore_opts(%State{config: %{lazy_restore: lazy, op_timeout_ms: op}}) do
    # A statement waits a little longer than the hydrator's fetch, so that its
    # failure reaches the statement before the wait ends.
    [lazy: %{min_bytes: lazy.min_bytes, wait_ms: op + 1_000}]
  end

  @doc """
  Makes the lazy plan of a restore result and fetches page 1, which opening
  the file reads at once. Returns the `Lazy` for the replicator, or `nil` for
  a result that is a complete file. It needs no lease and no process of the
  cell, so a wake runs it beside the lease.
  """
  @spec prepare(State.t(), map) :: {:ok, Lazy.t() | nil} | {:error, {:lazy, term}}
  def prepare(state, %{lazy: _} = result) do
    config = state.config
    opts = config.lazy_restore

    lazy =
      Lazy.new(result, state.repl_ctx,
        miss_pages: opts.miss_pages,
        batch_bytes: opts.batch_bytes
      )

    # Page 1 is fetched while no hydrator is running: the open reads it and
    # blocks the dirty I/O scheduler that runs SQLite.
    case Lazy.fetch_page(lazy, 1, Retry.deadline_in(config.op_timeout_ms)) do
      :ok ->
        {:ok, lazy}

      {:error, reason} ->
        release(result, lazy)
        {:error, {:lazy, reason}}
    end
  end

  def prepare(_state, _result), do: {:ok, nil}

  @doc "Gives up a prepared result that no cell will use: its page map and its file."
  @spec release(map, Lazy.t() | nil) :: :ok
  def release(_result, nil), do: :ok

  def release(result, %Lazy{} = lazy) do
    Native.lazy_release(lazy.handle)
    for suffix <- ["", "-wal", "-shm"], do: File.rm(result.path <> suffix)
    :ok
  end

  @doc """
  Starts the hydrator of a prepared lazy plan, before the file is opened. A
  complete file (`nil`) needs none.
  """
  @spec start(State.t(), Lazy.t() | nil) :: State.t()
  def start(state, nil), do: state

  def start(state, %Lazy{} = lazy) do
    config = state.config
    opts = config.lazy_restore

    # One marker covers the whole hydration: the fetch tasks must not create
    # markers of their own, because a file operation needs a dirty I/O
    # scheduler and an open statement may be holding the only one while it
    # waits for one of those pages.
    marker = Inflight.open(config, state.id, :hydrator)
    :ok = Inflight.hold(marker)

    {:ok, pid} =
      Hydrator.start_link(lazy,
        config: config,
        concurrency: opts.concurrency,
        fetch_ms: config.op_timeout_ms
      )

    %{state | hydrator: pid, hydrator_marker: marker}
  end

  @doc "The hydrator finished: the file is complete."
  @spec done(State.t()) :: {:ok, State.t()} | {:error, term}
  def done(%State{repl: repl} = state) do
    close_marker(state)

    with {:ok, repl} <- Replicator.hydrated(repl),
         do: {:ok, %{state | repl: repl, hydrator: nil, hydrator_marker: nil}}
  end

  @doc """
  Whether the file is still being filled. A hydrator that just finished
  counts as done.
  """
  @spec hydrating?(State.t()) :: boolean
  def hydrating?(%State{hydrator: nil}), do: false

  def hydrating?(%State{hydrator: pid}) do
    receive do
      {:hydrated, ^pid} -> false
    after
      0 -> true
    end
  end

  @doc "Stops the hydrator and removes the incomplete file."
  @spec discard(State.t()) :: State.t()
  def discard(%State{hydrator: nil} = state), do: state

  def discard(%State{hydrator: pid, repl: repl, path: path} = state) do
    Hydrator.stop(pid)
    if repl && repl.lazy, do: Native.lazy_release(repl.lazy.handle)
    close_marker(state)
    for suffix <- ["", "-wal", "-shm"], do: File.rm(path <> suffix)
    %{state | hydrator: nil, hydrator_marker: nil}
  end

  defp close_marker(%State{config: config, hydrator_marker: marker}) do
    Inflight.close(config, marker)
  end
end
