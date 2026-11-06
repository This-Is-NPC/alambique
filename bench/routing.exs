# Cost of the cluster view that every forwarded call reads (design §8, §15).
#
#   mise run bench:routing
#   mise run bench:routing -- 200000   # operations per repetition
#
# One node cannot fake other nodes, but the cost of the view grows with the
# number of members of the `:pg` groups, which local processes can fake. For
# 1, 100 and 1 000 members it times, per operation, the reads a forwarded call
# makes: `Membership.nodes/1`, `Membership.foreign_nodes/1` and
# `Membership.preferred/2`. Median of three repetitions.

defmodule Bench do
  alias AlambiqueTestkit.MemoryStore

  @reps 3

  def run(ops) do
    dir = Alambique.TestDirs.new("bench")
    {:ok, store} = MemoryStore.start_link()
    name = :bench_routing

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           name: name,
           store: {MemoryStore, MemoryStore.config(store)},
           prefix: "bench-routing",
           data_dir: dir,
           compaction: false,
           retention: false}
        ],
        strategy: :one_for_one
      )

    try do
      config = Alambique.Config.fetch!(name)
      IO.puts("operations per repetition: #{ops}, median µs per operation\n")
      IO.puts("members  nodes  foreign_nodes  preferred")

      for members <- [1, 100, 1000] do
        fakes = grow(config, members)
        Process.sleep(100)

        cols = [
          time(ops, fn -> Alambique.Membership.nodes(config) end),
          time(ops, fn -> Alambique.Membership.foreign_nodes(config) end),
          time(ops, fn -> Alambique.Membership.preferred(config, "cell") end)
        ]

        IO.puts(:io_lib.format("~7B  ~5.2f  ~13.2f  ~9.2f", [members | cols]))
        Enum.each(fakes, &Process.exit(&1, :kill))
      end
    after
      Supervisor.stop(sup)
      File.rm_rf(dir)
    end
  end

  # Fake members join the group of members.
  defp grow(config, members) do
    for _ <- 2..members//1 do
      pid = spawn(fn -> Process.sleep(:infinity) end)
      :ok = :pg.join(config.scope, :members, pid)
      pid
    end
  end

  defp time(ops, fun) do
    times =
      for _ <- 1..@reps do
        {us, _} = :timer.tc(fn -> for _ <- 1..ops, do: fun.() end)
        us / ops
      end

    times |> Enum.sort() |> Enum.at(1)
  end
end

ops =
  case System.argv() do
    [n] -> String.to_integer(n)
    _ -> 2_000
  end

Alambique.TestDirs.sweep()

try do
  Bench.run(ops)
after
  Alambique.TestDirs.clean()
end
