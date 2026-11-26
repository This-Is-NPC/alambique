defmodule Alambique.CellOptionsTest do
  @moduledoc "Options a cell sets for itself at wake (design §7)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.{Cell, Config, Sqlite}
  alias AlambiqueTestkit.MemoryStore

  @moduletag :capture_log

  defmodule Callbacks do
    @moduledoc false
    # The callback runs in a process of its own: the test sets the function it
    # calls through a persistent term.
    @key {__MODULE__, :fun}
    def set(fun), do: :persistent_term.put(@key, fun)
    def clear, do: :persistent_term.erase(@key)
    def options(cell), do: :persistent_term.get(@key).(cell)
    def options(cell, extra), do: :persistent_term.get(@key).({cell, extra})
  end

  setup do
    on_exit(&Callbacks.clear/0)
    setup_store()
  end

  defp start(ctx, opts \\ []) do
    start_instance(ctx, [cell_options: {Callbacks, :options}] ++ opts)
  end

  defp pid(inst, cell), do: Cell.whereis(Config.fetch!(inst), cell)

  defp wake(inst, cell), do: Alambique.query(inst, cell, "SELECT 1")

  # Reads the connection from the process of the cell without a call: a call
  # would restore the cache.
  defp cache_size(inst, cell) do
    in_cell(pid(inst, cell), fn state ->
      {:ok, %{rows: [[kib]]}} = Sqlite.run(state.conn, "PRAGMA cache_size", [])
      kib
    end)
  end

  defp cell_opts(inst, cell), do: in_cell(pid(inst, cell), & &1.cell_opts)

  defp free_slots(inst) do
    config = Config.fetch!(inst)
    config.awake.max_wakes - map_size(:sys.get_state(config.wakes).holders)
  end

  defp lease_keys(%{store_config: config}) do
    {:ok, keys, nil} = MemoryStore.list(config, "cells-test/", nil, 100_000)
    Enum.filter(keys, &String.contains?(&1, "lease"))
  end

  describe "an override" do
    test "sets the idle shrink of its cell, and the restore gives its cache back", ctx do
      inst = start(ctx, awake: [cache_kib: 512, idle_cache_kib: 64], idle_timeout_ms: 60_000)

      Callbacks.set(fn _ ->
        [awake: [cache_kib: 128, idle_cache_after_ms: 50, idle_cache_kib: 8]]
      end)

      cell = unique_cell()
      assert {:ok, _} = wake(inst, cell)

      # The node has no shrink and a cache of 512: the cell's own values.
      eventually(fn -> cache_size(inst, cell) == -8 end)
      assert {:ok, _} = wake(inst, cell)
      assert cache_size(inst, cell) == -128
    end

    test "with idle_cache_after_ms nil turns the node's shrink off for its cell", ctx do
      inst = start(ctx, awake: [idle_cache_after_ms: 50])

      Callbacks.set(fn
        "off" -> [awake: [idle_cache_after_ms: nil]]
        _ -> []
      end)

      assert {:ok, _} = wake(inst, "off")
      assert {:ok, _} = wake(inst, "node")
      assert in_cell(pid(inst, "off"), & &1.idle_cache) == nil
      assert is_reference(in_cell(pid(inst, "node"), & &1.idle_cache))
    end

    test "turns the shrink on for its cell where the node has it off", ctx do
      inst = start(ctx)

      Callbacks.set(fn
        "on" -> [awake: [idle_cache_after_ms: 50, idle_cache_kib: 8]]
        _ -> []
      end)

      assert {:ok, _} = wake(inst, "on")
      assert {:ok, _} = wake(inst, "node")
      assert in_cell(pid(inst, "node"), & &1.idle_cache) == nil
      eventually(fn -> cache_size(inst, "on") == -8 end)
    end

    test "sets the idle timeout of its cell", ctx do
      inst = start(ctx, idle_timeout_ms: 60_000)
      Callbacks.set(fn _ -> [idle_timeout_ms: 50] end)
      assert {:ok, _} = wake(inst, "short")
      eventually(fn -> pid(inst, "short") == nil end)
    end

    test "receives the cell and the extra arguments", ctx do
      inst = start(ctx, cell_options: {Callbacks, :options, [:extra]})
      test = self()

      Callbacks.set(fn arg ->
        send(test, {:called, arg})
        []
      end)

      assert {:ok, _} = wake(inst, "named")
      assert_receive {:called, {"named", :extra}}
    end

    test "is asked once per wake, not per call", ctx do
      inst = start(ctx)
      test = self()

      Callbacks.set(fn cell ->
        send(test, {:called, cell})
        []
      end)

      for _ <- 1..3, do: assert({:ok, _} = wake(inst, "once"))
      assert_receive {:called, "once"}
      refute_receive {:called, _}, 0
    end

    test "is not asked when the instance has no callback", ctx do
      inst = start_instance(ctx)
      assert {:ok, _} = wake(inst, "plain")
      assert cell_opts(inst, "plain") == %{}
    end
  end

  describe "the node limits" do
    setup ctx do
      inst =
        start(ctx,
          awake: [
            cache_kib: 256,
            cell_cache_kib_max: 256,
            cell_idle_timeout_ms_min: 1_500,
            cell_idle_timeout_ms_max: 90_000
          ]
        )

      capture([
        [:alambique, :cell, :options, :clamped],
        [:alambique, :cell, :options, :repaired]
      ])

      %{inst: inst}
    end

    test "clamp each field, with an event for each", %{inst: inst} do
      Callbacks.set(fn _ -> [idle_timeout_ms: 100_000, awake: [cache_kib: 1_024]] end)
      assert {:ok, _} = wake(inst, "big")
      assert cell_opts(inst, "big") == %{cache_kib: 256, idle_timeout_ms: 90_000}

      {m, _} =
        await_event(
          [:alambique, :cell, :options, :clamped],
          "big",
          5_000,
          &(&1.field == :cache_kib)
        )

      assert m == %{requested: 1_024, applied: 256}

      await_event(
        [:alambique, :cell, :options, :clamped],
        "big",
        5_000,
        &(&1.field == :idle_timeout_ms)
      )
    end

    test "raise a value below the minimum", %{inst: inst} do
      Callbacks.set(fn _ -> [idle_timeout_ms: 1_200] end)
      assert {:ok, _} = wake(inst, "low")
      assert cell_opts(inst, "low") == %{idle_timeout_ms: 1_500}
    end

    test "leave a value inside them alone and send no event", %{inst: inst} do
      Callbacks.set(fn _ -> [idle_timeout_ms: 2_000, awake: [cache_kib: 128]] end)
      assert {:ok, _} = wake(inst, "fine")
      assert cell_opts(inst, "fine") == %{cache_kib: 128, idle_timeout_ms: 2_000}
      assert drain([:alambique, :cell, :options, :clamped], "fine") == []
    end

    test "are clamped before the cross-field rules: a clamped timeout turns the shrink off",
         %{inst: inst} do
      # 1_200 is raised to 1_500, which is not above the shrink of 1_600.
      Callbacks.set(fn _ -> [idle_timeout_ms: 1_200, awake: [idle_cache_after_ms: 1_600]] end)
      assert {:ok, _} = wake(inst, "both")
      assert cell_opts(inst, "both") == %{idle_timeout_ms: 1_500, idle_cache_after_ms: nil}

      await_event(
        [:alambique, :cell, :options, :repaired],
        "both",
        5_000,
        &(&1.field == :idle_cache_after_ms)
      )

      assert in_cell(pid(inst, "both"), & &1.idle_cache) == nil
    end
  end

  describe "the cross-field repair" do
    setup ctx do
      inst = start(ctx, awake: [idle_cache_kib: 64])
      capture([[:alambique, :cell, :options, :repaired]])
      %{inst: inst}
    end

    test "drops an idle timeout that is not above the feed cleanup", %{inst: inst} do
      # feed.cleanup_after_ms is 1_000.
      Callbacks.set(fn _ -> [idle_timeout_ms: 1_000, awake: [cache_kib: 128]] end)
      assert {:ok, _} = wake(inst, "feed")
      assert cell_opts(inst, "feed") == %{cache_kib: 128}

      await_event(
        [:alambique, :cell, :options, :repaired],
        "feed",
        5_000,
        &(&1.field == :idle_timeout_ms)
      )
    end

    test "turns the shrink off when it is not below the idle timeout", %{inst: inst} do
      Callbacks.set(fn _ -> [idle_timeout_ms: 2_000, awake: [idle_cache_after_ms: 2_000]] end)
      assert {:ok, _} = wake(inst, "after")
      assert cell_opts(inst, "after") == %{idle_timeout_ms: 2_000, idle_cache_after_ms: nil}

      await_event(
        [:alambique, :cell, :options, :repaired],
        "after",
        5_000,
        &(&1.field == :idle_cache_after_ms)
      )
    end

    test "lowers idle_cache_kib to the node's when it is not below cache_kib", %{inst: inst} do
      Callbacks.set(fn _ ->
        [awake: [cache_kib: 100, idle_cache_after_ms: 5_000, idle_cache_kib: 100]]
      end)

      assert {:ok, _} = wake(inst, "kib")

      assert %{idle_cache_kib: 64, idle_cache_after_ms: 5_000, cache_kib: 100} =
               cell_opts(inst, "kib")

      await_event(
        [:alambique, :cell, :options, :repaired],
        "kib",
        5_000,
        &(&1.field == :idle_cache_kib)
      )
    end

    test "turns the shrink off when the node's idle_cache_kib is not below cache_kib either",
         %{inst: inst} do
      Callbacks.set(fn _ -> [awake: [cache_kib: 50, idle_cache_after_ms: 5_000]] end)
      assert {:ok, _} = wake(inst, "off")
      assert cell_opts(inst, "off") == %{cache_kib: 50, idle_cache_after_ms: nil}

      await_event(
        [:alambique, :cell, :options, :repaired],
        "off",
        5_000,
        &(&1.field == :idle_cache_after_ms)
      )
    end

    test "leaves a consistent override alone", %{inst: inst} do
      Callbacks.set(fn _ -> [awake: [cache_kib: 256, idle_cache_after_ms: 5_000]] end)
      assert {:ok, _} = wake(inst, "ok")
      assert cell_opts(inst, "ok") == %{cache_kib: 256, idle_cache_after_ms: 5_000}
      assert drain([:alambique, :cell, :options, :repaired], "ok") == []
    end
  end

  describe "an invalid override" do
    setup do
      capture([[:alambique, :cell, :options, :invalid], [:alambique, :cell, :wake]])
    end

    for {name, value} <- [
          {"a key that only the node may set", [awake: [max_cells: 1]]},
          {"a node-only top-level key", [lease_ttl_ms: 1_000]},
          {"an unknown key", [colour: :red]},
          {"a value the rule refuses", [awake: [cache_kib: -1]]},
          {"a repeated key", [idle_timeout_ms: 5_000, idle_timeout_ms: 6_000]},
          {"a value that is not a keyword", :oops},
          {"an awake that is not a keyword", [awake: 5]}
        ] do
      test "(#{name}) falls back to the node's options as a whole", ctx do
        inst = start(ctx)
        value = unquote(Macro.escape(value))
        # A valid key beside the invalid one: the override is dropped as a whole.
        Callbacks.set(fn _ ->
          if Keyword.keyword?(value), do: [idle_timeout_ms: 5_000] ++ value, else: value
        end)

        assert {:ok, _} = wake(inst, "bad")
        assert cell_opts(inst, "bad") == %{}
        {_, meta} = await_event([:alambique, :cell, :options, :invalid], "bad")
        assert meta.action == :fallback and is_binary(meta.reason)
        {_, wake} = await_event([:alambique, :cell, :wake], "bad")
        assert wake.options == :fallback
        assert free_slots(inst) == Config.fetch!(inst).awake.max_wakes
      end
    end

    test "is refused with {:cell_options, reason} under :refuse, writing no lease", ctx do
      inst = start(ctx, cell_options_invalid: :refuse)
      Callbacks.set(fn _ -> [awake: [cache_kib: -1]] end)

      assert {:error, {:cell_options, message}} = wake(inst, "refused")
      assert message =~ "cache_kib"
      assert pid(inst, "refused") == nil
      assert Alambique.owner(inst, "refused") == :none
      assert lease_keys(ctx) == []
      eventually(fn -> free_slots(inst) == Config.fetch!(inst).awake.max_wakes end)
      {_, meta} = await_event([:alambique, :cell, :wake], "refused")
      assert meta.result == :error and meta.options == :fallback
    end

    test "is accepted again once the callback returns a valid value, under :refuse", ctx do
      inst = start(ctx, cell_options_invalid: :refuse, wake: [failure_backoff_ms: 1])
      Callbacks.set(fn _ -> :oops end)
      assert {:error, {:cell_options, _}} = wake(inst, "again")
      Callbacks.set(fn _ -> [awake: [cache_kib: 128]] end)
      eventually(fn -> match?({:ok, _}, wake(inst, "again")) end)
      assert cell_opts(inst, "again") == %{cache_kib: 128}
    end
  end

  describe "a callback that does not return" do
    setup do
      capture([[:alambique, :cell, :options, :invalid], [:alambique, :cell, :wake]])
    end

    # Each ends the wake with the node's options, with the slot given back,
    # nothing left in the mailbox and the process of the callback dead.
    for kind <- [:hang, :raise, :exit, :throw, :wrong_shape, :late] do
      test "(#{kind}) falls back and leaves nothing behind", ctx do
        inst = start(ctx, cell_options_timeout_ms: 100)
        test = self()
        kind = unquote(kind)

        Callbacks.set(fn _ ->
          send(test, {:runner, self()})

          case kind do
            :hang ->
              Process.sleep(:infinity)

            :raise ->
              raise "boom"

            :exit ->
              exit(:gone)

            :throw ->
              throw(:nope)

            :wrong_shape ->
              {:ok, [awake: [cache_kib: 128]]}

            :late ->
              Process.sleep(400)
              [awake: [cache_kib: 128]]
          end
        end)

        assert {:ok, _} = wake(inst, "slow")
        assert_receive {:runner, runner}
        assert cell_opts(inst, "slow") == %{}
        {_, meta} = await_event([:alambique, :cell, :options, :invalid], "slow")
        assert meta.action == :fallback

        ref = Process.monitor(runner)
        assert_receive {:DOWN, ^ref, :process, ^runner, _}, 5_000
        assert free_slots(inst) == Config.fetch!(inst).awake.max_wakes
        assert queued(pid(inst, "slow")) == 0
      end
    end

    test "under :refuse a hang ends the wake with no lease and the slot back", ctx do
      inst = start(ctx, cell_options_timeout_ms: 100, cell_options_invalid: :refuse)
      Callbacks.set(fn _ -> Process.sleep(:infinity) end)

      assert {:error, {:cell_options, message}} = wake(inst, "hang")
      assert message =~ "timed out after 100 ms"
      assert lease_keys(ctx) == []
      assert Alambique.owner(inst, "hang") == :none
      eventually(fn -> free_slots(inst) == Config.fetch!(inst).awake.max_wakes end)
    end

    test "many hangs at once hold no slot past the timeout", ctx do
      inst = start(ctx, cell_options_timeout_ms: 100, awake: [max_wakes: 2])
      Callbacks.set(fn _ -> Process.sleep(:infinity) end)

      cells = for n <- 1..6, do: "storm-#{n}"
      tasks = for cell <- cells, do: Task.async(fn -> wake(inst, cell) end)
      assert Enum.all?(await_all(tasks), &match?({:ok, _}, &1))
      assert free_slots(inst) == 2
    end
  end

  describe "the wait for the callback" do
    test "runs outside the busy marker of the cell", ctx do
      inst = start(ctx)
      test = self()
      data_dir = Config.fetch!(inst).data_dir

      Callbacks.set(fn cell ->
        send(test, {:markers, cell, markers(data_dir)})
        []
      end)

      assert {:ok, _} = wake(inst, "marker")
      assert_receive {:markers, "marker", markers}
      own = Enum.filter(markers, &(&1.cell == "marker" and &1.kind == "cell"))
      assert [%{flag: :idle}] = own
    end
  end

  describe "the wake span" do
    setup do
      capture([[:alambique, :cell, :wake]])
    end

    test "says where the options came from and what the callback cost", ctx do
      inst = start(ctx, awake: [cell_cache_kib_max: 512])

      Callbacks.set(fn
        "plain" -> nil
        "empty" -> []
        _ -> [awake: [cache_kib: 1_024]]
      end)

      for cell <- ["plain", "empty", "set"], do: assert({:ok, _} = wake(inst, cell))

      {m, meta} = await_event([:alambique, :cell, :wake], "set")
      assert %{options: :override, clamped: [:cache_kib], repaired: []} = meta
      assert is_integer(m.options_time) and m.options_time >= 0

      assert {_, %{options: :node}} = await_event([:alambique, :cell, :wake], "plain")
      assert {_, %{options: :node}} = await_event([:alambique, :cell, :wake], "empty")
    end

    test "says :node when the instance has no callback", ctx do
      inst = start_instance(ctx)
      assert {:ok, _} = wake(inst, "none")

      assert {%{options_time: 0}, %{options: :node}} =
               await_event([:alambique, :cell, :wake], "none")
    end
  end
end
