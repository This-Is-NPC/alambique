defmodule Alambique.StoreBreakerPropertiesTest do
  @moduledoc """
  `Alambique.Store.Breaker` against a model (design §3), over random runs of
  requests that start, finish and let the clock pass: while it is open no
  request reaches the store, once the cool-down has passed exactly one request
  (the probe) does until it finishes, and it closes only on the success of that
  probe.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Store.Breaker

  @failures 3
  @cooldown 100
  @settings %{failures: @failures, cooldown_ms: @cooldown, probe_timeout_ms: 1_000_000_000}

  defmodule Gate do
    @moduledoc false
    # A store whose every request waits for the test to say how it ends.
    @behaviour Alambique.Store

    defp ask(test) do
      send(test, {:reached, self()})

      receive do
        {:go, result} -> result
      end
    end

    @impl true
    def put(test, _key, _body, _condition, _meta), do: ask(test)
    @impl true
    def get(test, _key), do: ask(test)
    @impl true
    def head(test, _key), do: ask(test)
    @impl true
    def get_range(test, _key, _offset, _length), do: ask(test)
    @impl true
    def list(test, _prefix, _start_after, _limit), do: ask(test)
    @impl true
    def delete(test, _key), do: ask(test)
  end

  @results %{
    ok: {:ok, "", "e", %{}},
    fail: {:error, {:http, 500, ""}},
    timeout: {:error, :timeout},
    not_found: {:error, :not_found},
    precondition: {:error, :precondition_failed}
  }
  @failing [:fail, :timeout]

  defp step do
    one_of([
      tuple({constant(:start), member_of(Map.keys(@results))}),
      tuple({constant(:finish), integer(0..9)}),
      tuple({constant(:advance), integer(1..150)})
    ])
  end

  # The model: `state` is `:closed` or `{:open, until}`; once `now` passes
  # `until` the breaker is half-open and `probe` is the request that tests it.
  defp model, do: %{state: :closed, fails: 0, probe: nil, now: 0, pending: [], next: 0}

  defp admit(%{state: :closed}), do: :closed

  defp admit(%{state: {:open, until}, now: now, probe: nil}) when now >= until, do: :probe
  defp admit(_), do: :reject

  defp apply_step({:advance, ms}, m, _store), do: %{m | now: m.now + ms}

  defp apply_step({:start, outcome}, m, %{store: store, pids: pids}) do
    id = m.next
    {_, config} = store
    test = self()
    pid = spawn(fn -> send(test, {:done, self(), Breaker.get(config, "k")}) end)
    Agent.update(pids, &[pid | &1])

    expected = admit(m)

    reached =
      receive do
        {:reached, ^pid} -> true
        {:done, ^pid, {:error, :store_unavailable}} -> false
      after
        2_000 -> flunk("request #{id} neither reached the store nor was refused")
      end

    assert reached == (expected != :reject),
           "admission: model #{inspect(expected)}, store reached: #{reached}, model #{inspect(m)}"

    case expected do
      :reject -> %{m | next: id + 1}
      ticket -> add(%{m | next: id + 1}, {pid, ticket, outcome})
    end
  end

  defp apply_step({:finish, _}, %{pending: []} = m, _store), do: m

  defp apply_step({:finish, k}, m, _store) do
    {pid, ticket, outcome} = Enum.at(m.pending, rem(k, length(m.pending)))
    send(pid, {:go, @results[outcome]})
    assert_receive {:done, ^pid, result}, 2_000
    assert result == @results[outcome]
    finish(%{m | pending: List.keydelete(m.pending, pid, 0)}, ticket, outcome)
  end

  defp add(m, {_pid, :probe, _} = request),
    do: %{m | pending: m.pending ++ [request], probe: request}

  defp add(m, request), do: %{m | pending: m.pending ++ [request]}

  # A request admitted while closed counts only if the breaker is still
  # closed when it ends.
  defp finish(%{state: :closed} = m, :closed, outcome) do
    if outcome in @failing do
      if m.fails + 1 >= @failures,
        do: %{m | state: {:open, m.now + @cooldown}, fails: 0},
        else: %{m | fails: m.fails + 1}
    else
      %{m | fails: 0}
    end
  end

  defp finish(m, :closed, _outcome), do: m

  defp finish(m, :probe, outcome) do
    if outcome in @failing,
      do: %{m | state: {:open, m.now + @cooldown}, probe: nil},
      else: %{m | state: :closed, fails: 0, probe: nil}
  end

  property "requests reach the store as the model says, and the breaker closes only on a probe success" do
    check all(steps <- list_of(step(), max_length: 40), max_runs: 200) do
      {:ok, clock} = Agent.start_link(fn -> 0 end)
      {:ok, pids} = Agent.start_link(fn -> [] end)

      breaker =
        Breaker.new(@settings, %{instance: :property, location: "l"},
          now: fn -> Agent.get(clock, & &1) + 1 end,
          jitter: 0.0
        )

      store = {Breaker, %{store: {Gate, self()}, breaker: breaker}}

      try do
        Enum.reduce(steps, model(), fn step, m ->
          m = apply_step(step, m, %{store: store, pids: pids})
          Agent.update(clock, fn _ -> m.now end)

          assert Breaker.open?(breaker) == (m.state != :closed),
                 "after #{inspect(step)}: #{inspect(m)}"

          m
        end)
      after
        pids |> Agent.get(& &1) |> Enum.each(&Process.exit(&1, :kill))
        Agent.stop(clock)
        Agent.stop(pids)
      end
    end
  end
end
