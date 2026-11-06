defmodule AlambiqueTestkit.MemoryStore do
  @moduledoc """
  In-memory `Alambique.Store` with fault injection (design §11).

  One GenServer owns all objects, so conditional writes are atomic. The
  store config is `%{server: server}` where `server` is a pid, a registered
  name or `{name, node}`, so peer nodes share one store. Each request carries
  its caller's node, which faults can target.

  Faults are planned by the server but executed in the caller, so sleeping
  never blocks other callers.
  """
  @behaviour Alambique.Store

  use GenServer

  @ops [:put, :get, :get_range, :head, :list, :delete]

  @type server :: GenServer.server()
  @type config :: %{server: server}

  # Lifecycle

  @doc "Starts a store and returns it. Pass `name:` to register it."
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, :ok, opts)

  @doc "Store config for a started server."
  @spec config(server) :: config
  def config(server), do: %{server: server}

  # Faults

  @doc """
  Injects a fault and returns its id.

  Options:

    * `:kind` - ``:error`, `:timeout`, `:latency`, `:lost_ack`, `:raise`, `:exit` or
      `:throw` (required)
    * `:node` - caller node to affect, default any
    * `:key` - `Regex` the key must match, default any
    * `:ops` - list of operations (`:put`, `:get`, `:get_range`, `:head`,
      `:list`, `:delete`), default all. `:list` matches against its prefix
    * `:times` - number of requests to affect, default `:infinity`
    * `:reason` - error term for `:error` and `:lost_ack`, shown in the
      exception message of `:raise` and the exit reason of `:exit`
    * `:ms` - sleep for `:timeout` and `:latency`

  `:error` fails the request without applying it. `:timeout` sleeps `:ms`
  and returns `{:error, :timeout}` without applying it. `:latency` sleeps
  and then runs the request. `:lost_ack` applies a `put` or `delete` and
  then returns the error. `:raise` raises a `RuntimeError` in the caller and
  `:exit` makes it exit with `{:memory_store_fault, reason}`, both without
  applying the request, as a store module with a bug or a crashed client does.
  """
  @spec inject(config, keyword) :: reference
  def inject(%{server: server}, opts) do
    kind = Keyword.fetch!(opts, :kind)

    unless kind in [:error, :timeout, :latency, :lost_ack, :raise, :exit, :throw],
      do: raise(ArgumentError, "bad kind")

    fault = %{
      id: make_ref(),
      kind: kind,
      node: Keyword.get(opts, :node, :any),
      key: Keyword.get(opts, :key),
      ops: Keyword.get(opts, :ops, @ops),
      times: Keyword.get(opts, :times, :infinity),
      reason: Keyword.get(opts, :reason, default_reason(kind)),
      ms: Keyword.get(opts, :ms, 0)
    }

    GenServer.call(server, {:inject, fault})
  end

  @doc "All requests from `node` fail with `{:error, :partitioned}` until healed."
  @spec partition(config, node) :: reference
  def partition(config, node),
    do: inject(config, kind: :error, node: node, reason: :partitioned)

  @doc "Removes one fault by id."
  @spec remove(config, reference) :: :ok
  def remove(%{server: server}, id), do: GenServer.call(server, {:remove, id})

  @doc "Removes every fault."
  @spec clear(config) :: :ok
  def clear(%{server: server}), do: GenServer.call(server, :clear)

  @doc "Number of stored objects."
  @spec count(config) :: non_neg_integer
  def count(%{server: server}), do: GenServer.call(server, :count)

  defp default_reason(:lost_ack), do: :lost_ack
  defp default_reason(_), do: :injected

  # Store behaviour

  @impl Alambique.Store
  def put(config, key, body, condition, meta),
    do: run(config, :put, key, {:put, key, IO.iodata_to_binary(body), condition, meta})

  @impl Alambique.Store
  def get(config, key), do: run(config, :get, key, {:get, key})

  @impl Alambique.Store
  def get_range(config, key, offset, length),
    do: run(config, :get_range, key, {:get_range, key, offset, length})

  @impl Alambique.Store
  def head(config, key), do: run(config, :head, key, {:head, key})

  @impl Alambique.Store
  def list(config, prefix, start_after, limit),
    do: run(config, :list, prefix, {:list, prefix, start_after, limit})

  @impl Alambique.Store
  def delete(config, key), do: run(config, :delete, key, {:delete, key})

  defp run(%{server: server}, op, key, request) do
    effects = GenServer.call(server, {:plan, node(), op, key})

    Enum.reduce_while(effects, {:run, nil}, fn
      {:sleep, ms}, acc ->
        Process.sleep(ms)
        {:cont, acc}

      {:fail, reason}, _ ->
        {:halt, {:done, {:error, reason}}}

      {:fail_after, ms}, _ ->
        Process.sleep(ms)
        {:halt, {:done, {:error, :timeout}}}

      {:lost_ack, reason}, _ ->
        {:halt, {:lost, reason}}

      {:raise, reason}, _ ->
        raise "MemoryStore fault: #{inspect(reason)}"

      {:exit, reason}, _ ->
        exit({:memory_store_fault, reason})

      {:throw, reason}, _ ->
        throw({:memory_store_fault, reason})
    end)
    |> case do
      {:run, _} -> GenServer.call(server, {:apply, request})
      {:done, result} -> result
      {:lost, reason} -> lose_ack(server, request, reason)
    end
  end

  defp lose_ack(server, request, reason) do
    GenServer.call(server, {:apply, request})
    {:error, reason}
  end

  # Server

  @impl GenServer
  def init(:ok),
    do: {:ok, %{objects: :ets.new(:objects, [:ordered_set, :private]), counter: 0, faults: []}}

  @impl GenServer
  def handle_call({:inject, fault}, _from, state),
    do: {:reply, fault.id, %{state | faults: state.faults ++ [fault]}}

  def handle_call({:remove, id}, _from, state),
    do: {:reply, :ok, %{state | faults: Enum.reject(state.faults, &(&1.id == id))}}

  def handle_call(:clear, _from, state), do: {:reply, :ok, %{state | faults: []}}
  def handle_call(:count, _from, state), do: {:reply, :ets.info(state.objects, :size), state}

  def handle_call({:plan, caller, op, key}, _from, state) do
    {effects, faults} = plan(state.faults, caller, op, key)
    {:reply, effects, %{state | faults: faults}}
  end

  def handle_call({:apply, request}, _from, state) do
    {reply, state} = apply_request(request, state)
    {:reply, reply, state}
  end

  defp plan(faults, caller, op, key) do
    Enum.flat_map_reduce(faults, faults, fn fault, acc ->
      if matches?(fault, caller, op, key) do
        {[effect(fault)], consume(acc, fault)}
      else
        {[], acc}
      end
    end)
  end

  defp matches?(fault, caller, op, key) do
    fault.node in [:any, caller] and op in fault.ops and
      (fault.key == nil or Regex.match?(fault.key, key))
  end

  defp effect(%{kind: :error, reason: r}), do: {:fail, r}
  defp effect(%{kind: :timeout, ms: ms}), do: {:fail_after, ms}
  defp effect(%{kind: :latency, ms: ms}), do: {:sleep, ms}
  defp effect(%{kind: :lost_ack, reason: r}), do: {:lost_ack, r}
  defp effect(%{kind: :raise, reason: r}), do: {:raise, r}
  defp effect(%{kind: :exit, reason: r}), do: {:exit, r}
  defp effect(%{kind: :throw, reason: r}), do: {:throw, r}

  defp consume(faults, %{times: :infinity}), do: faults

  defp consume(faults, %{id: id, times: 1}), do: Enum.reject(faults, &(&1.id == id))

  defp consume(faults, %{id: id}),
    do: Enum.map(faults, &if(&1.id == id, do: %{&1 | times: &1.times - 1}, else: &1))

  defp apply_request({:put, key, body, condition, meta}, state) do
    current = lookup(state, key)

    case {condition, current} do
      {:if_none_match, {_, _, _}} ->
        {{:error, :precondition_failed}, state}

      {{:if_match, etag}, {_, current_etag, _}} when etag != current_etag ->
        {{:error, :precondition_failed}, state}

      {{:if_match, _}, nil} ->
        {{:error, :precondition_failed}, state}

      _ ->
        counter = state.counter + 1
        etag = "etag-" <> Integer.to_string(counter, 16)
        :ets.insert(state.objects, {key, body, etag, meta})
        {{:ok, etag}, %{state | counter: counter}}
    end
  end

  defp apply_request({:get, key}, state) do
    case lookup(state, key) do
      {body, etag, meta} -> {{:ok, body, etag, meta}, state}
      nil -> {{:error, :not_found}, state}
    end
  end

  defp apply_request({:get_range, key, offset, length}, state) do
    case lookup(state, key) do
      {body, _etag, meta} ->
        size = byte_size(body)

        part =
          if offset >= size, do: "", else: binary_part(body, offset, min(length, size - offset))

        {{:ok, part, size, meta}, state}

      nil ->
        {{:error, :not_found}, state}
    end
  end

  defp apply_request({:head, key}, state) do
    case lookup(state, key) do
      {_, etag, meta} -> {{:ok, etag, meta}, state}
      nil -> {{:error, :not_found}, state}
    end
  end

  # Objects are an ordered table, so a listing reads only its own range.
  defp apply_request({:list, prefix, start_after, limit}, state) do
    first =
      cond do
        start_after != nil -> :ets.next(state.objects, start_after)
        :ets.member(state.objects, prefix) -> prefix
        true -> :ets.next(state.objects, prefix)
      end

    keys = take_prefixed(state.objects, first, prefix, limit + 1, [])
    page = Enum.take(keys, limit)
    next = if length(keys) > limit, do: List.last(page)
    {{:ok, page, next}, state}
  end

  defp apply_request({:delete, key}, state) do
    :ets.delete(state.objects, key)
    {:ok, state}
  end

  defp lookup(state, key) do
    case :ets.lookup(state.objects, key) do
      [{_, body, etag, meta}] -> {body, etag, meta}
      [] -> nil
    end
  end

  defp take_prefixed(_table, :"$end_of_table", _prefix, _left, acc), do: Enum.reverse(acc)
  defp take_prefixed(_table, _key, _prefix, 0, acc), do: Enum.reverse(acc)

  defp take_prefixed(table, key, prefix, left, acc) do
    if String.starts_with?(key, prefix),
      do: take_prefixed(table, :ets.next(table, key), prefix, left - 1, [key | acc]),
      else: Enum.reverse(acc)
  end
end
