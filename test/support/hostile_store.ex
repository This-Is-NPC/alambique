defmodule AlambiqueTestkit.HostileStore do
  @moduledoc """
  A memory store whose requests fail, or whose ranged reads come back damaged,
  on command. The counters tell which requests were hit.

  `damage/3` damages the next `times` ranged reads: `{:flip, pct}` flips one
  byte at `pct` percent of the body, `:short` cuts it in half and `:shifted`
  returns the bytes that follow it.
  """

  @behaviour Alambique.Store

  alias AlambiqueTestkit.MemoryStore

  @ops [:get_range, :get, :head, :list, :put]
  @left 1
  @mask 2
  @delivered 3
  @damage_left 4
  @damage_kind 5
  @damaged 6
  @range_delivered 7
  @damage_at 8

  def config(inner), do: %{inner: inner, ctl: :atomics.new(8, [])}

  def fail(%{ctl: ctl}, ops, times) do
    :atomics.put(ctl, @mask, Enum.reduce(ops, 0, &(Bitwise.bsl(1, index(&1)) + &2)))
    :atomics.put(ctl, @left, if(times == :infinity, do: 1_000_000, else: times))
  end

  def heal(%{ctl: ctl}) do
    :atomics.put(ctl, @left, 0)
    :atomics.put(ctl, @damage_left, 0)
  end

  def damage(%{ctl: ctl}, kind, times) do
    :atomics.put(ctl, @damage_kind, index(kind))
    :atomics.put(ctl, @damage_at, with({:flip, at} <- kind, do: at, else: (_ -> 0)))
    :atomics.put(ctl, @damage_left, times)
  end

  def delivered(%{ctl: ctl}), do: :atomics.get(ctl, @delivered)
  def range_delivered(%{ctl: ctl}), do: :atomics.get(ctl, @range_delivered)
  def damaged(%{ctl: ctl}), do: :atomics.get(ctl, @damaged)

  defp index(op) when op in @ops, do: Enum.find_index(@ops, &(&1 == op))
  defp index({:flip, _}), do: 1
  defp index(:short), do: 2
  defp index(:shifted), do: 3

  defp hit?(%{ctl: ctl}, op) do
    if Bitwise.band(:atomics.get(ctl, @mask), Bitwise.bsl(1, index(op))) != 0 and
         take?(ctl, @left) do
      :atomics.add(ctl, @delivered, 1)
      if op == :get_range, do: :atomics.add(ctl, @range_delivered, 1)
      true
    else
      false
    end
  end

  # Takes one from a counter that is above zero.
  defp take?(ctl, at) do
    case :atomics.get(ctl, at) do
      0 ->
        false

      n ->
        if :atomics.compare_exchange(ctl, at, n, n - 1) == :ok, do: true, else: take?(ctl, at)
    end
  end

  defp faulty(config, op, fun) do
    if hit?(config, op), do: {:error, :injected}, else: fun.()
  end

  @impl true
  def location(%{inner: inner}), do: inner

  @impl true
  def put(%{inner: inner} = c, key, body, condition, meta),
    do: faulty(c, :put, fn -> MemoryStore.put(inner, key, body, condition, meta) end)

  @impl true
  def get(%{inner: inner} = c, key), do: faulty(c, :get, fn -> MemoryStore.get(inner, key) end)

  @impl true
  def head(%{inner: inner} = c, key), do: faulty(c, :head, fn -> MemoryStore.head(inner, key) end)

  @impl true
  def list(%{inner: inner} = c, prefix, start_after, limit),
    do: faulty(c, :list, fn -> MemoryStore.list(inner, prefix, start_after, limit) end)

  @impl true
  def delete(%{inner: inner}, key), do: MemoryStore.delete(inner, key)

  @impl true
  def get_range(%{inner: inner, ctl: ctl} = c, key, offset, length) do
    faulty(c, :get_range, fn ->
      with {:ok, part, size, meta} <- MemoryStore.get_range(inner, key, offset, length) do
        if part != "" and take?(ctl, @damage_left) do
          :atomics.add(ctl, @damaged, 1)

          {:ok,
           harm(
             :atomics.get(ctl, @damage_kind),
             part,
             inner,
             key,
             offset,
             length,
             :atomics.get(ctl, @damage_at)
           ), size, meta}
        else
          {:ok, part, size, meta}
        end
      end
    end)
  end

  defp harm(1, part, _inner, _key, _offset, _length, pct) do
    at = div((byte_size(part) - 1) * pct, 99)
    <<head::binary-size(^at), byte, tail::binary>> = part
    <<head::binary, Bitwise.bxor(byte, 0xFF), tail::binary>>
  end

  defp harm(2, part, _inner, _key, _offset, _length, _pct),
    do: binary_part(part, 0, div(byte_size(part), 2))

  # The bytes that follow: another page's frame in place of the requested one.
  defp harm(3, part, inner, key, offset, length, _pct) do
    case MemoryStore.get_range(inner, key, offset + length, byte_size(part)) do
      {:ok, other, _, _} when byte_size(other) == byte_size(part) -> other
      _ -> part
    end
  end
end
