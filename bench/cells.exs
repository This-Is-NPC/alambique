# Thousands of cells on one node (design §12).
#
#   mise run bench:cells            # 5 000 cells
#   mise run bench:cells -- 20000   # another count
#
# Wakes N cells with one write each against the in-memory store, reports the
# cost while they are awake, then waits for them to hibernate and reports it
# again to show that idle cells cost nothing. A second table says where the
# memory is: the BEAM allocators, the store's ETS, SQLite and Rust (from the
# native counters) and what lies outside the allocators.

defmodule Bench do
  alias AlambiqueTestkit.MemoryStore

  @instance :bench_cells
  @idle_ms 20_000

  def run(count) do
    dir = Alambique.TestDirs.new("bench")
    {:ok, store} = MemoryStore.start_link()

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           name: @instance,
           store: {MemoryStore, MemoryStore.config(store)},
           data_dir: dir,
           migrations: [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}],
           idle_timeout_ms: @idle_ms,
           lease_ttl_ms: 60_000}
        ],
        strategy: :one_for_one
      )

    try do
      machine()
      baseline = snapshot("baseline", store)

      {cpu0, _} = :erlang.statistics(:runtime)
      {wake_us, :ok} = :timer.tc(fn -> wake(count) end)
      {cpu1, _} = :erlang.statistics(:runtime)
      awake = snapshot("awake", store)
      listing = listing(count)

      {idle_us, :ok} = :timer.tc(fn -> await_hibernation() end)
      asleep = snapshot("asleep", store)

      report(count, wake_us, cpu1 - cpu0, idle_us, [baseline, awake, asleep], listing)
      check(count, asleep, baseline)
    after
      Supervisor.stop(sup)
      GenServer.stop(store)
      File.rm_rf(dir)
    end
  end

  defp wake(count) do
    1..count
    |> Task.async_stream(
      fn i ->
        {:ok, _} =
          Alambique.execute(@instance, "cell-#{i}", "INSERT INTO notes (body) VALUES (?)", [
            "hello"
          ])
      end,
      max_concurrency: 256,
      ordered: false,
      timeout: :infinity
    )
    |> Stream.run()
  end

  defp await_hibernation do
    if Alambique.cells(@instance).count > 0 do
      Process.sleep(500)
      await_hibernation()
    else
      :ok
    end
  end

  # A page of 100 from the middle of the id range, and every page in turn:
  # a page costs its size, not the number of cells.
  defp listing(count) do
    ids = Alambique.cells(@instance, limit: count).cells
    middle = Enum.at(ids, div(count, 2))

    times =
      for _ <- 1..9 do
        {us, _} = :timer.tc(fn -> Alambique.cells(@instance, after: middle, limit: 100) end)
        us
      end

    {all_us, pages} = :timer.tc(fn -> pages(nil, 0) end)

    "cells/2:   page of 100 mid-range #{Enum.at(Enum.sort(times), 4)} us (median of 9), " <>
      "all #{pages} pages #{div(all_us, 1000)} ms"
  end

  defp pages(after_id, n) do
    case Alambique.cells(@instance, after: after_id) do
      %{next: nil} -> n + 1
      %{next: next} -> pages(next, n + 1)
    end
  end

  defp snapshot(label, store) do
    Enum.each(Process.list(), &:erlang.garbage_collect/1)
    fds = File.ls!("/proc/self/fd")

    cell_files =
      Enum.count(fds, fn fd ->
        case File.read_link("/proc/self/fd/" <> fd) do
          {:ok, target} -> String.contains?(target, "/cell.db")
          _ -> false
        end
      end)

    %{
      label: label,
      awake: Alambique.cells(@instance).count,
      processes: :erlang.system_info(:process_count),
      fds: length(fds),
      cell_files: cell_files,
      beam_mb: mb(:erlang.memory(:total)),
      rss_mb: rss_kb("VmRSS") / 1024,
      rss_anon_mb: rss_kb("RssAnon") / 1024,
      store_mb: store_bytes(store) / 1_048_576,
      erts_mb: mb(:recon_alloc.memory(:allocated)),
      erts_used_mb: mb(:recon_alloc.memory(:used)),
      ets_mb: mb(:erlang.memory(:ets)),
      native: native_mb(),
      hints: :ets.info(Module.concat(@instance, Hints), :size)
    }
  end

  defp native_mb do
    s = Alambique.Replication.Native.stats()

    %{
      sqlite: mb(s.sqlite_alloc_bytes - s.sqlite_free_bytes),
      rust: mb(s.rust_alloc_bytes - s.rust_free_bytes)
    }
  end

  defp mb(bytes), do: bytes / 1_048_576

  defp rss_kb(field) do
    "/proc/self/status"
    |> File.read!()
    |> then(&Regex.run(~r/#{field}:\s+(\d+) kB/, &1))
    |> then(fn [_, kb] -> String.to_integer(kb) end)
  end

  # Memory of the ETS tables that belong to the in-memory store process.
  defp store_bytes(store) do
    for t <- :ets.all(), :ets.info(t, :owner) == store, reduce: 0 do
      acc -> acc + :ets.info(t, :memory) * :erlang.system_info(:wordsize)
    end
  end

  defp machine do
    cpu =
      "/proc/cpuinfo"
      |> File.read!()
      |> then(&Regex.run(~r/model name\s*:\s*(.+)/, &1))
      |> then(fn [_, name] -> name end)

    IO.puts("""
    machine:   #{cpu}, #{System.schedulers_online()} schedulers
    runtime:   Erlang/OTP #{:erlang.system_info(:otp_release)}, Elixir #{System.version()}
    """)
  end

  defp report(count, wake_us, wake_cpu_ms, idle_us, snapshots, listing) do
    IO.puts("cells:     #{count} (one write each, 256 at a time, in-memory store)")

    IO.puts(
      "wake all:  #{Float.round(wake_us / 1_000_000, 2)} s, #{round(count / (wake_us / 1_000_000))} cells/s"
    )

    IO.puts("wake cpu:  #{Float.round(wake_cpu_ms / 1000, 2)} s in all threads")

    IO.puts(
      "hibernate: #{Float.round(idle_us / 1_000_000, 2)} s after the last call (idle timeout #{@idle_ms} ms)\n"
    )

    IO.puts(listing)
    IO.puts("")
    IO.puts("phase      awake cells  processes  open fds  cell.db fds  BEAM MB  RSS MB  hints")

    for s <- snapshots do
      IO.puts(
        :io_lib.format("~-10s ~11B  ~9B  ~8B  ~11B  ~7.1f  ~6.1f  ~5B", [
          s.label,
          s.awake,
          s.processes,
          s.fds,
          s.cell_files,
          s.beam_mb,
          s.rss_mb,
          s.hints
        ])
      )
    end

    IO.puts("""

    memory (MB): erts = held by the BEAM allocators (used = in blocks), store =
    ETS of the in-memory store, sqlite and rust = native bytes allocated minus
    freed, outside = anonymous RSS not held by erts
    """)

    IO.puts("phase      RSS anon  erts  erts used  store  sqlite  rust  outside")

    for s <- snapshots do
      n = s.native

      IO.puts(
        :io_lib.format("~-10s ~8.1f  ~4B  ~9B  ~5B  ~6.1f  ~4B  ~7B", [
          s.label,
          s.rss_anon_mb,
          round(s.erts_mb),
          round(s.erts_used_mb),
          round(s.store_mb),
          n.sqlite,
          round(n.rust),
          round(s.rss_anon_mb - s.erts_mb)
        ])
      )
    end
  end

  # Idle cells must not leave processes or files behind.
  defp check(_count, asleep, baseline) do
    ok? =
      asleep.awake == 0 and asleep.cell_files == 0 and asleep.processes <= baseline.processes + 5

    unless ok?, do: raise("idle cells still cost resources")
  end
end

count =
  case System.argv() do
    [n | _] -> String.to_integer(n)
    [] -> 5_000
  end

Alambique.TestDirs.sweep()

try do
  Bench.run(count)
after
  Alambique.TestDirs.clean()
end
