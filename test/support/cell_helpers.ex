defmodule Alambique.CellHelpers do
  @moduledoc "Starts instances over a memory store and captures telemetry for cell tests."

  import ExUnit.Callbacks, only: [start_supervised!: 1, on_exit: 1]

  alias AlambiqueTestkit.MemoryStore

  @doc "A fresh memory store and a data directory removed when the test ends."
  def setup_store do
    pid = start_supervised!({MemoryStore, []})

    dir = Alambique.TestDirs.new("cells")
    on_exit(fn -> File.rm_rf(dir) end)
    config = MemoryStore.config(pid)
    %{store: {MemoryStore, config}, store_config: config, dir: dir}
  end

  @doc "Starts an instance with short timings; `opts` override the defaults. Returns its name."
  def start_instance(%{store: store, dir: dir}, opts \\ []) do
    name =
      Keyword.get_lazy(opts, :name, fn -> :"instance_#{System.unique_integer([:positive])}" end)

    defaults = [
      name: name,
      store: store,
      prefix: "cells-test",
      data_dir: Path.join(dir, Atom.to_string(name)),
      node_id: Atom.to_string(name),
      lease_ttl_ms: 3_000,
      skew_margin_ms: 500,
      idle_timeout_ms: 60_000,
      op_timeout_ms: 2_000,
      # Faults injected in tests last longer than a breaker would let requests
      # reach the store; the breaker has its own tests.
      breaker: [failures: 1_000_000]
    ]

    merged = Keyword.merge(defaults, opts)
    merged = if opts[:placement], do: Keyword.drop(merged, [:store, :prefix]), else: merged
    spec = Supervisor.child_spec({Alambique, merged}, id: name)
    start_supervised!(spec)
    name
  end

  @doc """
  Leaves the marker a VM crash would leave for `cell` in the data directory of
  an instance that is not running: busy, and nobody removes it.
  """
  def leave_busy_marker(data_dir, cell, kind \\ :cell) do
    config = %{quarantine: %{}, data_dir: data_dir, inflight: :no_tracker}
    marker = Alambique.Inflight.open(config, cell, kind)
    :ok = :file.pwrite(marker.fd, 0, <<1>>)
    :ok = :file.close(marker.fd)
  end

  @doc "The markers in the data directory: `%{flag: :busy | :idle, cell: id, kind: name}`."
  def markers(data_dir) do
    dir = Path.join(data_dir, "inflight")

    for name <- File.ls(dir) |> elem(1),
        {:ok, <<flag, json::binary>>} <- [File.read(Path.join(dir, name))],
        %{"cell" => cell, "kind" => kind} = Jason.decode!(json) do
      %{flag: if(flag == 1, do: :busy, else: :idle), cell: cell, kind: kind}
    end
  end

  @doc "Replication context of the location the instance gives `cell`."
  def repl_ctx(instance, cell) do
    config = Alambique.Config.fetch!(instance)
    {:ok, location} = Alambique.Placement.locate(config, cell)
    Alambique.Config.repl_ctx(config, location)
  end

  @doc """
  Messages waiting in the mailbox of a cell, not counting the reports of its
  feed publisher, which arrive at any time.
  """
  def queued(pid) do
    {:messages, messages} = Process.info(pid, :messages)
    Enum.count(messages, &(not match?({:feed_published, _, _, _, _}, &1)))
  end

  @doc "The replies of `callers` queries that start at the same moment."
  def burst_queries(inst, cell, callers, timeout) do
    test_pid = self()

    pids =
      for _ <- 1..callers do
        spawn_link(fn ->
          receive do
            :go ->
              reply = Alambique.query(inst, cell, "SELECT count(*) FROM t", [], timeout: timeout)
              send(test_pid, {self(), reply})
          end
        end)
      end

    Enum.each(pids, &send(&1, :go))

    for pid <- pids,
        do:
          (receive do
             {^pid, reply} -> reply
           end)
  end

  def unique_cell, do: "cell-#{System.unique_integer([:positive])}"

  @doc "Sends the given telemetry events to the calling process as `{:telemetry, event, measurements, meta}`."
  def capture(events) do
    id = {__MODULE__, make_ref()}
    :telemetry.attach_many(id, events, &__MODULE__.relay/4, self())

    on_exit(fn -> :telemetry.detach(id) end)
  end

  @doc false
  def relay(event, measurements, meta, test),
    do: send(test, {:telemetry, event, measurements, meta})

  @doc "Waits for a captured event of `event` for `cell` and returns its metadata."
  def await_event(event, cell, timeout \\ 5_000, match \\ fn _ -> true end) do
    receive do
      {:telemetry, ^event, m, %{cell: ^cell} = meta} ->
        if match.(meta), do: {m, meta}, else: await_event(event, cell, timeout, match)
    after
      timeout -> raise "no #{inspect(event)} event for #{cell}"
    end
  end

  @doc "All captured events of `event` for `cell` received so far."
  def drain(event, cell) do
    receive do
      {:telemetry, ^event, m, %{cell: ^cell} = meta} -> [{m, meta} | drain(event, cell)]
    after
      0 -> []
    end
  end

  @doc "Polls `fun` until it returns a truthy value, up to `timeout` milliseconds."
  def eventually(fun, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    poll(fun, deadline)
  end

  defp poll(fun, deadline) do
    case fun.() do
      falsy when falsy in [nil, false] ->
        if System.monotonic_time(:millisecond) > deadline do
          raise "condition not reached in time"
        else
          Process.sleep(10)
          poll(fun, deadline)
        end

      value ->
        value
    end
  end

  @doc "Paths of the files this VM holds open."
  def open_files do
    for fd <- File.ls!("/proc/self/fd"),
        {:ok, target} <- [File.read_link(Path.join("/proc/self/fd", fd))],
        do: target
  end

  @doc """
  Rows of `sql` on the state of `cell` restored from the store, which is what
  survives if every node dies now.
  """
  def durable_rows(%{store: store, dir: dir}, cell, sql, prefix \\ "cells-test") do
    ctx =
      Alambique.Replication.ctx(store: store, prefix: prefix, node: "verifier", timeout_ms: 5_000)

    path = Path.join(dir, "verify-#{System.unique_integer([:positive])}.db")
    {:ok, _} = Alambique.Replication.restore(ctx, cell, path, :latest)
    {:ok, conn} = Alambique.Sqlite.open(path, readonly: true)
    {:ok, %{rows: rows}} = Alambique.Sqlite.run(conn, sql, [])
    Alambique.Sqlite.close(conn)
    File.rm(path)
    rows
  end
end
