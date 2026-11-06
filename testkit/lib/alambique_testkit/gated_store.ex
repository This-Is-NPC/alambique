defmodule AlambiqueTestkit.GatedStore do
  @moduledoc """
  Wraps a store and holds individual puts until the test decides their fate,
  so a test controls the order in which uploads land (design §11).

  A put whose key matches a `hold/2` pattern blocks in the caller until
  `release/2`, `fail/2` or `lose_ack/2` answers it. Answers given before the
  put arrives wait for it, in order. Other requests pass through.

      gate = start_supervised!(GatedStore)
      store = GatedStore.wrap(gate, {MemoryStore, MemoryStore.config(server)})
      GatedStore.hold(gate, ~r{/0000/})
      GatedStore.await(gate, key)   # the put is waiting
      GatedStore.release(gate, key) # it lands
  """
  @behaviour Alambique.Store

  use GenServer

  # Gate

  @doc "Starts a gate."
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, :ok, opts)

  @doc "`{module, config}` of a store that gates puts of `inner` through `gate`."
  @spec wrap(GenServer.server(), {module, term}) :: {module, map}
  def wrap(gate, {_module, _config} = inner), do: {__MODULE__, %{inner: inner, gate: gate}}

  @doc "Holds every later put whose key matches `pattern`."
  @spec hold(GenServer.server(), Regex.t()) :: :ok
  def hold(gate, pattern), do: GenServer.call(gate, {:hold, pattern})

  @doc "Waits until a put of `key` is held; `:ok` or exits after `timeout`."
  @spec await(GenServer.server(), String.t(), timeout) :: :ok
  def await(gate, key, timeout \\ 5_000), do: GenServer.call(gate, {:await, key}, timeout)

  @doc "Keys with a put waiting, sorted."
  @spec held(GenServer.server()) :: [String.t()]
  def held(gate), do: GenServer.call(gate, :held)

  @doc "The next put of `key` is applied."
  @spec release(GenServer.server(), String.t()) :: :ok
  def release(gate, key), do: GenServer.call(gate, {:answer, key, :release})

  @doc "The next put of `key` fails and is not applied."
  @spec fail(GenServer.server(), String.t()) :: :ok
  def fail(gate, key), do: GenServer.call(gate, {:answer, key, :fail})

  @doc "The next put of `key` is applied and its caller gets an error."
  @spec lose_ack(GenServer.server(), String.t()) :: :ok
  def lose_ack(gate, key), do: GenServer.call(gate, {:answer, key, :lose_ack})

  @doc "Stops holding and applies every held put."
  @spec open(GenServer.server()) :: :ok
  def open(gate), do: GenServer.call(gate, :open)

  # Store behaviour

  @impl Alambique.Store
  def location(%{inner: inner}), do: Alambique.Store.location(inner)

  @impl Alambique.Store
  def put(%{inner: {m, c}, gate: gate}, key, body, condition, meta) do
    case GenServer.call(gate, {:put, key}, :infinity) do
      :release -> m.put(c, key, body, condition, meta)
      :fail -> {:error, :gated}
      :lose_ack -> with {:ok, _} <- m.put(c, key, body, condition, meta), do: {:error, :lost_ack}
    end
  end

  @impl Alambique.Store
  def get(%{inner: {m, c}}, key), do: m.get(c, key)

  @impl Alambique.Store
  def get_range(%{inner: {m, c}}, key, offset, length), do: m.get_range(c, key, offset, length)

  @impl Alambique.Store
  def head(%{inner: {m, c}}, key), do: m.head(c, key)

  @impl Alambique.Store
  def list(%{inner: {m, c}}, prefix, start_after, limit),
    do: m.list(c, prefix, start_after, limit)

  @impl Alambique.Store
  def delete(%{inner: {m, c}}, key), do: m.delete(c, key)

  # Server: `held` maps a key to the callers waiting, `answers` to the
  # answers given early, `watchers` to the `await/3` callers.

  @impl GenServer
  def init(:ok), do: {:ok, %{patterns: [], held: %{}, answers: %{}, watchers: %{}}}

  @impl GenServer
  def handle_call({:hold, pattern}, _from, state),
    do: {:reply, :ok, %{state | patterns: [pattern | state.patterns]}}

  def handle_call({:put, key}, from, state) do
    case pop(state.answers, key) do
      {answer, answers} ->
        {:reply, answer, %{state | answers: answers}}

      nil ->
        if Enum.any?(state.patterns, &Regex.match?(&1, key)) do
          state = update_in(state.held, &Map.update(&1, key, [from], fn q -> q ++ [from] end))
          {:noreply, wake(state, key)}
        else
          {:reply, :release, state}
        end
    end
  end

  def handle_call({:await, key}, from, state) do
    if Map.has_key?(state.held, key),
      do: {:reply, :ok, state},
      else:
        {:noreply,
         update_in(state.watchers, &Map.update(&1, key, [from], fn w -> [from | w] end))}
  end

  def handle_call({:answer, key, answer}, _from, state) do
    case pop(state.held, key) do
      {waiter, held} ->
        GenServer.reply(waiter, answer)
        {:reply, :ok, %{state | held: held}}

      nil ->
        {:reply, :ok,
         update_in(state.answers, &Map.update(&1, key, [answer], fn q -> q ++ [answer] end))}
    end
  end

  def handle_call(:held, _from, state),
    do: {:reply, state.held |> Map.keys() |> Enum.sort(), state}

  def handle_call(:open, _from, state) do
    for {_key, waiters} <- state.held, waiter <- waiters, do: GenServer.reply(waiter, :release)
    {:reply, :ok, %{state | patterns: [], held: %{}}}
  end

  defp wake(state, key) do
    {watchers, rest} = Map.pop(state.watchers, key, [])
    for watcher <- watchers, do: GenServer.reply(watcher, :ok)
    %{state | watchers: rest}
  end

  # The oldest entry of `key`, and the map without it.
  defp pop(map, key) do
    case Map.get(map, key) do
      nil -> nil
      [first] -> {first, Map.delete(map, key)}
      [first | rest] -> {first, Map.put(map, key, rest)}
    end
  end
end
