defmodule Alambique.Cell.Options do
  @moduledoc false
  # The options of one cell, resolved once per wake and again at each refresh
  # (`Alambique.refresh_options/3`) from `:cell_options` (design §7). The
  # result is a map with only the keys the override set; a key that is present,
  # even with `nil`, replaces the node's value, and a key that is absent leaves
  # it. `State.cell_opt/2` reads it.
  #
  # Order: the callback (bounded) -> each key checked by the rule of
  # `Config.check_cell_option/2` -> each value clamped to the node limits ->
  # the cross-field rules repair what the merge breaks. An override that is
  # invalid as a whole falls back to the node's options, or refuses the wake
  # (`:cell_options_invalid`). At a refresh an invalid override changes
  # nothing: the cell keeps the options it has and the caller gets the reason.

  require Logger

  alias Alambique.{BoundedCall, Config, Telemetry}

  @awake_keys [
    :cache_kib,
    :idle_cache_after_ms,
    :idle_cache_kib,
    :idle_close_after_ms,
    :idle_gc,
    :track_bytes,
    :lookaside_bytes,
    :lookaside_count
  ]

  # The keyword lists nested in the override, and the keys each may set.
  @sections [
    awake: @awake_keys,
    sql: [:locking_mode],
    lazy_restore: [:mode],
    feed: [:cleanup_after_ms]
  ]

  @type info :: %{
          source: :node | :override | :fallback | :kept,
          clamped: [atom],
          repaired: [atom],
          time: integer
        }

  @spec resolve(Config.t(), String.t(), :wake | :refresh) ::
          {:ok, map, info} | {:error, String.t(), info}
  def resolve(config, id, phase \\ :wake)
  def resolve(%{cell_options: nil}, _id, _phase), do: {:ok, %{}, info(:node, [], [], 0)}

  def resolve(config, id, phase) do
    started = System.monotonic_time()
    called = call(config, id)
    time = System.monotonic_time() - started

    case with({:ok, value} <- called, do: validate(value)) do
      {:ok, override} when map_size(override) == 0 ->
        {:ok, %{}, info(:node, [], [], time)}

      {:ok, override} ->
        {clamped, override} = clamp(config, id, override)
        {repaired, override} = repair(config, id, override)
        {:ok, override, info(:override, clamped, repaired, time)}

      {:error, message} ->
        invalid(config, id, message, time, phase)
    end
  end

  @doc "The answer of a refresh whose resolution failed outside the callback."
  @spec failed(String.t()) :: {:error, String.t(), info}
  def failed(message), do: {:error, message, info(:kept, [], [], 0)}

  defp info(source, clamped, repaired, time),
    do: %{source: source, clamped: clamped, repaired: repaired, time: time}

  defp call(config, id) do
    {module, function, args} = config.cell_options
    timeout = config.cell_options_timeout_ms

    case BoundedCall.run(fn -> apply(module, function, [id | args]) end, timeout) do
      {:ok, value} ->
        {:ok, value}

      {:error, :timeout} ->
        {:error, "timed out after #{timeout} ms"}

      {:error, {:raised, kind, reason}} ->
        {:error, "failed: " <> Exception.format_banner(kind, reason)}
    end
  end

  # The shape of the instance options: `idle_timeout_ms:`, `awake: [...]` and
  # `lazy_restore: [mode: ...]` and `feed: [cleanup_after_ms:]`, the keys of
  # `Config.cell_option_keys/0` and no others.
  defp validate(nil), do: {:ok, %{}}

  defp validate(value) do
    with true <- Keyword.keyword?(value) or {:error, "must return a keyword list or nil"},
         {:ok, sections, top} <- split_sections(value),
         :ok <- known(top, [:idle_timeout_ms]),
         :ok <- known_sections(sections),
         flat = flatten(top, sections),
         true <- Keyword.keys(flat) == Enum.uniq(Keyword.keys(flat)) or {:error, "repeats a key"},
         :ok <- check(flat) do
      {:ok, Map.new(flat)}
    end
  end

  defp split_sections(value) do
    Enum.reduce_while(@sections, {:ok, %{}, value}, fn {key, _allowed}, {:ok, found, rest} ->
      {opts, rest} = Keyword.pop(rest, key, [])

      if Keyword.keyword?(opts),
        do: {:cont, {:ok, Map.put(found, key, opts), rest}},
        else: {:halt, {:error, ":#{key} must be a keyword list"}}
    end)
  end

  defp known_sections(sections) do
    Enum.find_value(@sections, :ok, fn {key, allowed} ->
      with :ok <- known(Map.fetch!(sections, key), allowed), do: nil
    end)
  end

  defp flatten(top, sections) do
    Keyword.take(top, [:idle_timeout_ms]) ++
      sections.awake ++
      sections.sql ++
      sections.feed ++ for({:mode, mode} <- sections.lazy_restore, do: {:lazy_restore_mode, mode})
  end

  defp known(opts, allowed) do
    case Keyword.keys(opts) -- allowed do
      [] -> :ok
      keys -> {:error, "#{inspect(keys)} cannot be set per cell"}
    end
  end

  defp check(flat) do
    Enum.find_value(flat, :ok, fn {key, value} ->
      case Config.check_cell_option(key, value) do
        :ok -> nil
        {:error, _} = error -> error
      end
    end)
  end

  # Each value inside the node limits.
  defp clamp(config, id, override) do
    awake = config.awake

    limits = [
      cache_kib: {nil, awake.cell_cache_kib_max},
      idle_timeout_ms: {awake.cell_idle_timeout_ms_min, awake.cell_idle_timeout_ms_max}
    ]

    Enum.reduce(limits, {[], override}, fn {key, {low, high}}, {clamped, acc} ->
      with {:ok, value} <- Map.fetch(acc, key),
           applied = value |> max(low || value) |> min(high || value),
           true <- applied != value do
        Telemetry.emit([:cell, :options, :clamped], %{requested: value, applied: applied}, %{
          cell: id,
          field: key
        })

        {[key | clamped], Map.put(acc, key, applied)}
      else
        _ -> {clamped, acc}
      end
    end)
  end

  # What the merge breaks is repaired with a fixed result, so that an
  # override never becomes invalid after the limits changed it.
  #
  # 0. `cleanup_after_ms` not below `idle_timeout_ms`: the node's value.
  # 1. `idle_timeout_ms` not above `cleanup_after_ms`: the node's value.
  # 2. `idle_cache_after_ms` not below `idle_timeout_ms`: the shrink is off.
  # 3. `idle_close_after_ms` not below `idle_timeout_ms`: the close is off.
  # 4. `idle_close_after_ms` with `locking_mode: :exclusive`: the close is
  #    off. The lock is the cell's own choice of safety, and the close would
  #    drop it; the close only saves memory, and a node value for either key
  #    would not clear the combination (the build refuses it for the node).
  # 5. `idle_cache_after_ms` not below `idle_close_after_ms`: the shrink is
  #    off, as the close comes first and frees the cache.
  # 6. `idle_cache_kib` not below `cache_kib`: the node's `idle_cache_kib`,
  #    or the shrink off when that is not below either.
  # 7. `locking_mode: :exclusive` where the node's feed hands over to the
  #    sender or a lazy restore is on: the node's value (the build refuses
  #    the combination for the node's own value).
  # 8. `lookaside_bytes` and `lookaside_count` that are not a pair SQLite can
  #    use (one alone, or too much memory): both keys are dropped, so the
  #    node's pair applies.
  defp repair(config, id, override) do
    {repaired, override} =
      Enum.reduce(repairs(), {[], override}, fn {key, rule}, {repaired, acc} ->
        repair_step({key, rule}, config, repaired, acc)
      end)

    for field <- repaired,
        do: Telemetry.emit([:cell, :options, :repaired], %{}, %{cell: id, field: field})

    {Enum.reverse(repaired), override}
  end

  # In order: the numbers of the list above.
  defp repairs do
    [
      {:cleanup_after_ms, &cleanup_not_below_idle/2},
      {:idle_timeout_ms, &idle_not_above_cleanup/2},
      {:idle_cache_after_ms, &shrink_not_below_idle/2},
      {:idle_close_after_ms, &close_not_below_idle/2},
      {:idle_close_after_ms, &close_not_exclusive/2},
      {:idle_cache_after_ms, &shrink_not_below_close/2},
      {:idle_cache_kib, &shrink_below_cache/2},
      {:lookaside_bytes, &lookaside_usable/2},
      {:locking_mode, &exclusive_allowed/2}
    ]
  end

  defp cleanup_not_below_idle(config, o) do
    if value(config, o, :cleanup_after_ms) >= value(config, o, :idle_timeout_ms), do: :drop
  end

  defp idle_not_above_cleanup(config, o),
    do: if(o[:idle_timeout_ms] <= value(config, o, :cleanup_after_ms), do: :drop)

  defp shrink_not_below_idle(config, o) do
    with after_ms when not is_nil(after_ms) <- value(config, o, :idle_cache_after_ms),
         true <- after_ms >= value(config, o, :idle_timeout_ms),
         do: {:put, nil},
         else: (_ -> nil)
  end

  defp close_not_below_idle(config, o) do
    with after_ms when not is_nil(after_ms) <- value(config, o, :idle_close_after_ms),
         true <- after_ms >= value(config, o, :idle_timeout_ms),
         do: {:put, nil},
         else: (_ -> nil)
  end

  defp close_not_exclusive(config, o) do
    with close when not is_nil(close) <- value(config, o, :idle_close_after_ms),
         :exclusive <- value(config, o, :locking_mode),
         do: {:put, nil},
         else: (_ -> nil)
  end

  defp shrink_not_below_close(config, o) do
    with close when not is_nil(close) <- value(config, o, :idle_close_after_ms),
         after_ms when not is_nil(after_ms) <- value(config, o, :idle_cache_after_ms),
         true <- after_ms >= close,
         do: {:put, nil},
         else: (_ -> nil)
  end

  defp shrink_below_cache(config, o) do
    with after_ms when not is_nil(after_ms) <- value(config, o, :idle_cache_after_ms),
         true <- value(config, o, :idle_cache_kib) >= value(config, o, :cache_kib) do
      node_kib = config.awake.idle_cache_kib
      if node_kib < value(config, o, :cache_kib), do: {:put, node_kib}, else: :off
    else
      _ -> nil
    end
  end

  defp lookaside_usable(config, o) do
    bytes = value(config, o, :lookaside_bytes)
    count = value(config, o, :lookaside_count)
    if Config.lookaside_error(bytes, count), do: :drop_lookaside
  end

  defp exclusive_allowed(config, o) do
    if o.locking_mode == :exclusive and
         (config.feed.hand_over == :sender or config.lazy_restore != nil),
       do: :drop
  end

  defp repair_step({key, rule}, config, repaired, acc) do
    case if(rule_applies?(key, acc), do: rule.(config, acc)) do
      nil -> {repaired, acc}
      :drop -> {[key | repaired], Map.delete(acc, key)}
      :drop_lookaside -> {[key | repaired], Map.drop(acc, [:lookaside_bytes, :lookaside_count])}
      {:put, v} -> {[key | repaired], Map.put(acc, key, v)}
      :off -> {[:idle_cache_after_ms | repaired], Map.put(acc, :idle_cache_after_ms, nil)}
    end
  end

  # Steps 1 and 7 read the key they may drop; the others read the merged values.
  defp rule_applies?(:idle_timeout_ms, override), do: Map.has_key?(override, :idle_timeout_ms)
  defp rule_applies?(:locking_mode, override), do: Map.has_key?(override, :locking_mode)
  defp rule_applies?(:cleanup_after_ms, override), do: Map.has_key?(override, :cleanup_after_ms)

  defp rule_applies?(:lookaside_bytes, override),
    do: Map.has_key?(override, :lookaside_bytes) or Map.has_key?(override, :lookaside_count)

  defp rule_applies?(_key, _override), do: true

  defp value(config, override, key) do
    case override do
      %{^key => value} -> value
      _ -> node_value(config, key)
    end
  end

  @doc false
  @spec node_value(Config.t(), atom) :: term
  def node_value(config, :idle_timeout_ms), do: config.idle_timeout_ms
  def node_value(config, :locking_mode), do: config.sql.locking_mode
  def node_value(config, :lazy_restore_mode), do: Config.lazy_mode(config, nil)
  def node_value(config, :cleanup_after_ms), do: config.feed.cleanup_after_ms
  def node_value(config, key), do: Map.fetch!(config.awake, key)

  defp invalid(config, id, message, time, phase) do
    action = if phase == :refresh, do: :keep, else: config.cell_options_invalid

    Telemetry.emit([:cell, :options, :invalid], %{}, %{
      cell: id,
      reason: message,
      action: action,
      phase: phase
    })

    warn(config, id, message, action)

    case action do
      :fallback -> {:ok, %{}, info(:fallback, [], [], time)}
      :refuse -> {:error, message, info(:fallback, [], [], time)}
      :keep -> {:error, message, info(:kept, [], [], time)}
    end
  end

  # At most one line every `breaker.log_interval_ms` for the instance: a
  # callback that fails fails for many cells.
  defp warn(config, id, message, action) do
    now = System.monotonic_time(:millisecond)

    last =
      case :ets.lookup(config.admission, :options_log) do
        [{_, at}] -> at
        [] -> nil
      end

    if is_nil(last) or now - last >= config.breaker.log_interval_ms do
      :ets.insert(config.admission, {:options_log, now})

      Logger.warning(
        "Alambique cell_options for #{id} is invalid (#{message}); " <>
          case action do
            :refuse -> "the wake is refused"
            :fallback -> "serving with the node options"
            :keep -> "keeping the options it has"
          end
      )
    end

    :ok
  end
end
