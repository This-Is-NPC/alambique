defmodule Alambique.Replication.NativeOptions do
  @moduledoc false
  # Load data of the NIF (`config :alambique, :page_arena, ...`). SQLite's
  # allocator is installed when the library loads, so these settings are read
  # then and cannot change while the node runs. `Native.stats/0` reports the
  # settings the library loaded with.
  #
  #   * `enabled` - page-cache blocks live in the page arena (default true).
  #   * `reserved_mib` - address space reserved, from 64 up to 1048576 (default 4096).
  #   * `extent_kib` - size of the extents a connection owns, a multiple of 4
  #     up to 1024 (default 64).
  #   * `trim_after_kib` - free memory that makes a trim worth running
  #     (default 1024).
  #   * `keep_kib` - free memory a trim leaves alone (default 4096).
  #   * `max_trim_kib` - most a trim releases at once (default 4096).
  #   * `max_regions` - connections that get extents of their own, at least 1
  #     (default 65536); the others, and a value of 1, share one region.
  #   * `trim_idle_ms` - how long the trim thread waits for a wake-up before it
  #     looks anyway (default 30000); the tests set it high to see the wake-ups.

  @defaults [
    enabled: true,
    reserved_mib: 4096,
    extent_kib: 64,
    trim_after_kib: 1024,
    keep_kib: 4096,
    max_trim_kib: 4096,
    max_regions: 65_536,
    trim_idle_ms: 30_000
  ]

  @spec load_data() :: map
  def load_data do
    # The environment of the application is readable once it is loaded, which
    # an embedded release may not have done yet when this module loads.
    _ = Application.load(:alambique)

    configured = Application.get_env(:alambique, :page_arena, [])
    unknown = Keyword.keys(configured) -- Keyword.keys(@defaults)

    if unknown != [],
      do: raise(ArgumentError, "unknown :page_arena settings: #{inspect(unknown)}")

    opts = Keyword.merge(@defaults, configured)
    validate!(opts)
    Map.new(opts)
  end

  defp validate!(opts) do
    unless is_boolean(opts[:enabled]),
      do: raise(ArgumentError, ":page_arena :enabled must be true or false")

    check!(opts, :reserved_mib, &(&1 >= 64 and &1 <= 1_048_576), "from 64 up to 1048576")

    check!(
      opts,
      :extent_kib,
      &(&1 > 0 and &1 <= 1024 and rem(&1, 4) == 0),
      "a multiple of 4 up to 1024"
    )

    for key <- [:trim_after_kib, :keep_kib],
        do: check!(opts, key, &(&1 >= 0), "a non-negative integer")

    check!(opts, :trim_idle_ms, &(&1 > 0), "a positive integer")
    check!(opts, :max_regions, &(&1 >= 1), "a positive integer")
    check!(opts, :max_trim_kib, &(&1 > 0), "a positive integer")
  end

  defp check!(opts, key, ok?, what) do
    value = opts[key]

    unless is_integer(value) and ok?.(value),
      do:
        raise(
          ArgumentError,
          ":page_arena #{inspect(key)} must be #{what}, got: #{inspect(value)}"
        )
  end
end
