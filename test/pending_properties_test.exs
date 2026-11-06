defmodule Alambique.PendingPropertiesTest do
  @moduledoc """
  Admission against a model (design §7): a call is refused with `:busy` only
  when at least `awake.max_pending` other calls are really pending on the
  cell, and never because of messages that are not calls. A gated store holds
  the writes, so the callers stay pending; stray messages are injected into
  the cell between the calls.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias AlambiqueTestkit.GatedStore

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @insert "INSERT INTO t (n) VALUES (?)"

  setup do
    setup_store()
  end

  defp stray(pid, {:EXIT, reason}), do: send(pid, {:EXIT, self(), reason})
  defp stray(pid, other), do: send(pid, other)

  property "a call is busy only when max_pending other calls are pending", env do
    check all(
            max_pending <- integer(1..4),
            writers <- integer(0..6),
            strays <- list_of(member_of([{:EXIT, :normal}, :unknown, :late]), max_length: 6),
            max_runs: 20,
            max_shrinking_steps: 0
          ) do
      {:ok, gate} = GatedStore.start_link()
      store = GatedStore.wrap(gate, env.store)

      inst =
        start_instance(%{env | store: store},
          migrations: @schema,
          awake: [max_pending: max_pending],
          compaction: false
        )

      cell = unique_cell()
      {:ok, _} = Alambique.execute(inst, cell, @insert, [0])
      pid = Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)
      GatedStore.hold(gate, ~r{/0000/})

      # One writer at a time, so the model knows how many calls are pending.
      {tasks, pending} =
        Enum.reduce(1..writers//1, {[], 0}, fn n, {tasks, pending} ->
          Enum.each(strays, &stray(pid, &1))
          task = Task.async(fn -> Alambique.execute(inst, cell, @insert, [n]) end)

          if pending < max_pending do
            eventually(fn -> Alambique.load(inst, cell) == {:ok, %{pending: pending + 1}} end)
            # Past this call the cell has run the write that it counts.
            :sys.get_state(pid)
            {[task | tasks], pending + 1}
          else
            reply =
              Task.yield(task, 2_000) || {:stuck, Alambique.load(inst, cell), pending, strays}

            assert reply == {:ok, {:error, :busy}}
            {tasks, pending}
          end
        end)

      Enum.each(strays, &stray(pid, &1))
      probe = Alambique.query(inst, cell, "SELECT count(*) FROM t", [], timeout: 300)

      # A read waits for the held writes, if there are any, so it times out.
      expected =
        cond do
          pending >= max_pending -> {:error, :busy}
          pending > 0 -> {:error, :timeout}
          true -> {:ok, %{columns: ["count(*)"], rows: [[1]]}}
        end

      assert probe == expected

      GatedStore.open(gate)
      Enum.each(tasks, &assert({:ok, _} = Task.await(&1, 10_000)))
      GenServer.stop(gate)
    end
  end
end
