# Schema creation on a fresh cell (design §13).
#
#   mise run bench:schema           # 500 cells per mode
#   mise run bench:schema -- 2000   # another count
#
# Creates the same schema (4 tables and 7 indexes) in N fresh cells, one after
# another, in one transaction each, against the in-memory store: once with a
# `Tx.execute/3` per statement (a change-capture cycle each) and once with one
# `Tx.script/2`. Reports the time per cell and the cells per second. The
# modes run in turns, three rounds, so machine noise hits both.

defmodule Bench do
  alias Alambique.Tx
  alias AlambiqueTestkit.MemoryStore

  @rounds 3

  @statements [
    "CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_at INTEGER)",
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, account_id INTEGER NOT NULL, total INTEGER, state TEXT)",
    "CREATE TABLE items (id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL, sku TEXT, qty INTEGER)",
    "CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, payload TEXT, at INTEGER)",
    "CREATE INDEX accounts_name ON accounts (name)",
    "CREATE INDEX orders_account ON orders (account_id)",
    "CREATE INDEX orders_state ON orders (state)",
    "CREATE INDEX items_order ON items (order_id)",
    "CREATE INDEX items_sku ON items (sku)",
    "CREATE INDEX events_kind ON events (kind)",
    "CREATE INDEX events_at ON events (at)"
  ]

  def run(count) do
    dir = Alambique.TestDirs.new("bench")
    {:ok, store} = MemoryStore.start_link()

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           name: :bench_schema,
           store: {MemoryStore, MemoryStore.config(store)},
           data_dir: dir,
           idle_timeout_ms: 600_000,
           lease_ttl_ms: 60_000}
        ],
        strategy: :one_for_one
      )

    try do
      IO.puts("#{count} fresh cells per mode, #{length(@statements)} statements, #{@rounds} rounds")
      warm_up()

      for round <- 1..@rounds, mode <- [:execute, :script] do
        us = measure(mode, "#{round}-#{mode}", count)
        report(round, mode, count, us)
      end
    after
      Supervisor.stop(sup)
      GenServer.stop(store)
      File.rm_rf(dir)
    end
  end

  defp warm_up do
    for mode <- [:execute, :script], do: measure(mode, "warm-#{mode}", 20)
  end

  defp measure(mode, tag, count) do
    {us, :ok} =
      :timer.tc(fn ->
        for n <- 1..count do
          {:ok, :ok} = Alambique.transaction(:bench_schema, "#{tag}-#{n}", &create(mode, &1))
        end

        :ok
      end)

    us
  end

  defp create(:execute, tx) do
    for sql <- @statements, do: {:ok, _} = Tx.execute(tx, sql)
    :ok
  end

  defp create(:script, tx), do: Tx.script(tx, Enum.join(@statements, ";\n"))

  defp report(round, mode, count, us) do
    ms = us / 1000 / count
    per_second = round(count / (us / 1_000_000))

    IO.puts(
      "round #{round} #{String.pad_trailing(to_string(mode), 8)} #{Float.round(ms, 2)} ms per cell, #{per_second} cells/s"
    )
  end
end

count =
  case System.argv() do
    [] -> 500
    [n | _] -> String.to_integer(n)
  end

Bench.run(count)
