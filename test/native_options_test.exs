defmodule Alambique.NativeOptionsTest do
  @moduledoc "The load data of the NIF: defaults, overrides and refusals."
  # The settings live in the application environment of the whole node.
  use ExUnit.Case, async: false

  alias Alambique.Replication.NativeOptions
  alias Alambique.Replication.Native

  setup do
    # The library loads, and reads the settings, at its first call: load it
    # before a test changes them.
    _ = Native.stats()
    saved = Application.get_env(:alambique, :page_arena)

    on_exit(fn ->
      if saved,
        do: Application.put_env(:alambique, :page_arena, saved),
        else: Application.delete_env(:alambique, :page_arena)
    end)
  end

  defp load(opts) do
    Application.put_env(:alambique, :page_arena, opts)
    NativeOptions.load_data()
  end

  test "the arena is on, with documented defaults" do
    assert load([]) == %{
             enabled: true,
             reserved_mib: 4096,
             extent_kib: 64,
             trim_after_kib: 1024,
             keep_kib: 4096,
             max_trim_kib: 4096,
             max_regions: 65_536,
             trim_idle_ms: 30_000
           }
  end

  test "settings override the defaults one by one" do
    data = load(enabled: true, extent_kib: 256, keep_kib: 0)
    assert %{enabled: true, extent_kib: 256, keep_kib: 0, reserved_mib: 4096} = data
  end

  test "settings that cannot work are refused when the library loads" do
    for {opts, message} <- [
          {[enabled: 1], ":enabled must be true or false"},
          {[reserved_mib: 63], ":reserved_mib must be from 64 up to 1048576"},
          {[reserved_mib: 1_048_577], ":reserved_mib must be from 64 up to 1048576"},
          {[extent_kib: 6], ":extent_kib must be a multiple of 4 up to 1024"},
          {[extent_kib: 2048], ":extent_kib must be a multiple of 4 up to 1024"},
          {[keep_kib: -1], ":keep_kib must be a non-negative integer"},
          {[max_trim_kib: 0], ":max_trim_kib must be a positive integer"},
          {[trim_idle_ms: 0], ":trim_idle_ms must be a positive integer"},
          {[max_regions: 0], ":max_regions must be a positive integer"},
          {[reserved: 1], "unknown :page_arena settings: [:reserved]"}
        ] do
      assert_raise ArgumentError, ~r/#{Regex.escape(message)}/, fn -> load(opts) end
    end
  end

  test "the library reports the settings it loaded with" do
    stats = Native.stats()
    expected = Application.get_env(:alambique, :page_arena)
    assert stats.page_arena_state == if(expected[:enabled], do: 1, else: 0)

    if stats.page_arena_state == 1 do
      assert stats.page_arena_extent_kib == (expected[:extent_kib] || 64)
      assert stats.page_arena_keep_kib == (expected[:keep_kib] || 4096)
    end
  end

  describe "a setting that arrives after the library loaded" do
    import ExUnit.CaptureLog
    import Alambique.CellHelpers, only: [setup_store: 0, start_instance: 2]

    setup do
      handler = {__MODULE__, make_ref()}
      test = self()

      :telemetry.attach(
        handler,
        [:alambique, :page_arena, :config_ignored],
        fn _event, measurements, meta, _ -> send(test, {:ignored, measurements, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)
      setup_store()
    end

    defp start_with(ctx, env) do
      current = Application.get_env(:alambique, :page_arena, [])
      Application.put_env(:alambique, :page_arena, Keyword.merge(current, env))
      capture_log(fn -> start_instance(ctx, []) end)
    end

    test "is warned about and reported with the keys that differ", ctx do
      log = start_with(ctx, extent_kib: 128, keep_kib: 1)

      if Native.stats().page_arena_state == 1 do
        assert log =~ "differs from what the library loaded with"
        assert log =~ "extent_kib"
        assert log =~ "config/config.exs"
        assert_receive {:ignored, %{keys: 2}, %{keys: keys, instance: _}}
        assert Enum.sort(keys) == [:extent_kib, :keep_kib]
      else
        # The library applied nothing but the switch: nothing to compare.
        refute log =~ "differs from what the library loaded with"
      end
    end

    test "an arena switched off or on after the load is reported as :enabled", ctx do
      on? = Native.stats().page_arena_state != 0
      log = start_with(ctx, enabled: not on?)

      assert log =~ ":enabled"
      assert_receive {:ignored, _, %{keys: [:enabled]}}
    end

    test "a setting the library refuses is reported", ctx do
      log = start_with(ctx, extent_kib: 6)

      assert log =~ ":invalid"
      assert_receive {:ignored, _, %{keys: [:invalid]}}
    end

    test "what the library loaded with is not reported", ctx do
      log = capture_log(fn -> start_instance(ctx, []) end)

      refute log =~ "differs from what the library loaded with"
      assert NativeOptions.differing() == []
      refute_received {:ignored, _, _}
    end
  end
end
