defmodule Alambique.Replication.PageArena do
  @moduledoc false
  # Compares `config :alambique, :page_arena` with what the NIF loaded with.
  # Kept apart from `NativeOptions`, which the NIF module needs to load.

  alias Alambique.Replication.Native
  alias Alambique.Replication.NativeOptions

  require Logger

  # What the library applied, by the key that set it.
  @applied %{
    reserved_mib: :page_arena_reserved_mib,
    extent_kib: :page_arena_extent_kib,
    trim_after_kib: :page_arena_trim_after_kib,
    keep_kib: :page_arena_keep_kib,
    max_trim_kib: :page_arena_max_trim_kib,
    max_regions: :page_arena_max_regions,
    trim_idle_ms: :page_arena_trim_idle_ms
  }

  @doc """
  The keys of `config :alambique, :page_arena` that differ from what the
  library applied when it loaded (`[:invalid]` for a setting `NativeOptions.load_data/0`
  refuses). The library reads them once, so a setting that arrives later, as
  from `config/runtime.exs` of a release that is started, is ignored.
  """
  @spec differing() :: [atom]
  def differing do
    stats = Native.stats()
    wanted = NativeOptions.load_data()
    on? = stats.page_arena_state != 0

    cond do
      wanted.enabled != on? ->
        [:enabled]

      stats.page_arena_state == 1 ->
        for {key, stat} <- @applied, Map.fetch!(stats, stat) != wanted[key], do: key

      true ->
        []
    end
  rescue
    ArgumentError -> [:invalid]
  end

  @doc """
  Warns, and emits `[:alambique, :page_arena, :config_ignored]` with the keys
  in its metadata, when `differing/0` finds any. Called when an instance starts.
  """
  @spec check_applied(term) :: :ok
  def check_applied(instance) do
    case differing() do
      [] ->
        :ok

      keys ->
        Logger.warning(
          "Alambique #{inspect(instance)}: config :alambique, :page_arena differs from " <>
            "what the library loaded with (#{inspect(keys)}). It is read once, when the " <>
            "library loads: set it in config/config.exs (the sys.config of a release); " <>
            "config/runtime.exs is read after a release started with bin/<app> start has " <>
            "loaded it."
        )

        Alambique.Telemetry.emit([:page_arena, :config_ignored], %{keys: length(keys)}, %{
          instance: instance,
          keys: keys
        })
    end
  end
end
