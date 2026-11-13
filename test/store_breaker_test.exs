defmodule Alambique.StoreBreakerTest do
  @moduledoc """
  The circuit breaker of a store and the attempt limit of the retry loop
  (design §3): after enough failures a request does not reach the store, one
  probe tests it after the cool-down, and a request is attempted at most
  `max_attempts` times.
  """
  use ExUnit.Case, async: true

  import Alambique.CellHelpers,
    only: [capture: 1, eventually: 1, setup_store: 0, start_instance: 2, unique_cell: 0]

  alias Alambique.Replication
  alias Alambique.Replication.{Objects, Retry}
  alias Alambique.Store.Breaker
  alias AlambiqueTestkit.MemoryStore

  defmodule Stub do
    @moduledoc false
    # Counts its requests and answers what `reply` says.
    @behaviour Alambique.Store

    def new(reply), do: %{counter: :counters.new(1, []), reply: reply}
    def count(%{counter: counter}), do: :counters.get(counter, 1)

    defp answer(%{counter: counter, reply: reply}) do
      :counters.add(counter, 1, 1)
      reply.()
    end

    @impl true
    def put(c, _key, _body, _condition, _meta), do: answer(c)
    @impl true
    def get(c, _key), do: answer(c)
    @impl true
    def head(c, _key), do: answer(c)
    @impl true
    def get_range(c, _key, _offset, _length), do: answer(c)
    @impl true
    def list(c, _prefix, _start_after, _limit), do: answer(c)
    @impl true
    def delete(c, _key), do: answer(c)
  end

  @http_500 {:error, {:http, 500, ""}}
  @settings %{failures: 3, cooldown_ms: 1_000, probe_timeout_ms: 5_000}

  # A breaker over a stub, with a clock the test moves.
  defp breaker(reply, settings \\ %{}) do
    {:ok, clock} = Agent.start_link(fn -> 1_000 end)
    stub = Stub.new(reply)

    breaker =
      Breaker.new(Map.merge(@settings, settings), %{instance: :test, location: "loc"},
        now: fn -> Agent.get(clock, & &1) end,
        jitter: 0.0
      )

    %{
      stub: stub,
      breaker: breaker,
      store: {Breaker, %{store: {Stub, stub}, breaker: breaker}},
      advance: fn ms -> Agent.update(clock, &(&1 + ms)) end
    }
  end

  defp get(%{store: {m, c}}), do: m.get(c, "k")

  describe "the breaker" do
    test "opens after the configured failures in a row and then does not reach the store" do
      b = breaker(fn -> @http_500 end)

      for _ <- 1..3, do: assert(get(b) == @http_500)
      assert Stub.count(b.stub) == 3
      assert Breaker.open?(b.breaker)

      for _ <- 1..50, do: assert(get(b) == {:error, :store_unavailable})
      assert Stub.count(b.stub) == 3
    end

    test "answers fast while open" do
      b = breaker(fn -> Process.sleep(20) && @http_500 end, %{failures: 1})
      get(b)

      {us, {:error, :store_unavailable}} = :timer.tc(fn -> get(b) end)
      assert us < 5_000
    end

    test "counts 5xx, timeouts, transport errors and crashes, not 404, 412 or 4xx" do
      counted = [
        @http_500,
        {:error, {:http, 503, "slow down"}},
        {:error, :timeout},
        {:error, :econnrefused},
        {:error, {:store_crashed, :exit, :normal}}
      ]

      for reply <- counted do
        b = breaker(fn -> reply end, %{failures: 2})
        get(b)
        get(b)
        assert Breaker.open?(b.breaker), "#{inspect(reply)} must count"
      end

      ignored = [
        {:error, :not_found},
        {:error, :precondition_failed},
        {:error, {:http, 403, ""}},
        {:error, {:http, 404, ""}},
        {:ok, "body", "etag", %{}}
      ]

      for reply <- ignored do
        b = breaker(fn -> reply end, %{failures: 2})
        for _ <- 1..20, do: get(b)
        refute Breaker.open?(b.breaker), "#{inspect(reply)} must not count"
        assert Stub.count(b.stub) == 20
      end
    end

    test "a success resets the run of failures" do
      {:ok, replies} = Agent.start_link(fn -> [@http_500, @http_500, {:ok, "", "e", %{}}] end)

      reply = fn ->
        Agent.get_and_update(replies, fn
          [one | rest] -> {one, rest}
          [] -> {@http_500, []}
        end)
      end

      b = breaker(reply)
      for _ <- 1..5, do: get(b)
      refute Breaker.open?(b.breaker)
      get(b)
      assert Breaker.open?(b.breaker)
    end

    test "lets one probe through after the cool-down and closes on its success" do
      {:ok, store_up} = Agent.start_link(fn -> false end)

      b =
        breaker(fn ->
          if Agent.get(store_up, & &1), do: {:ok, "", "e", %{}}, else: @http_500
        end)

      for _ <- 1..3, do: get(b)
      assert get(b) == {:error, :store_unavailable}

      b.advance.(999)
      assert get(b) == {:error, :store_unavailable}
      assert Stub.count(b.stub) == 3

      Agent.update(store_up, fn _ -> true end)
      b.advance.(1)
      assert {:ok, _, _, _} = get(b)
      assert Stub.count(b.stub) == 4
      refute Breaker.open?(b.breaker)

      for _ <- 1..5, do: assert({:ok, _, _, _} = get(b))
      assert Stub.count(b.stub) == 9
    end

    test "a failed probe opens it again for another cool-down" do
      b = breaker(fn -> @http_500 end)
      for _ <- 1..3, do: get(b)

      b.advance.(1_000)
      assert get(b) == @http_500
      assert Stub.count(b.stub) == 4

      assert get(b) == {:error, :store_unavailable}
      b.advance.(999)
      assert get(b) == {:error, :store_unavailable}
      b.advance.(1)
      assert get(b) == @http_500
      assert Stub.count(b.stub) == 5
    end

    test "only one request is the probe while it is in flight" do
      test = self()

      b =
        breaker(fn ->
          send(test, {:reached, self()})

          receive do
            :go -> @http_500
          end
        end)

      # Three failures through blocked requests, released at once.
      for _ <- 1..3 do
        task = Task.async(fn -> get(b) end)
        assert_receive {:reached, pid}
        send(pid, :go)
        Task.await(task)
      end

      b.advance.(1_000)
      probe = Task.async(fn -> get(b) end)
      assert_receive {:reached, probe_pid}

      others = for _ <- 1..20, do: Task.async(fn -> get(b) end)
      assert Enum.all?(Task.await_many(others), &(&1 == {:error, :store_unavailable}))
      assert Stub.count(b.stub) == 4

      send(probe_pid, :go)
      assert Task.await(probe) == @http_500
    end

    test "a probe that never reports is given up after the probe timeout" do
      test = self()

      b =
        breaker(fn ->
          send(test, {:reached, self()})
          Process.sleep(:infinity)
        end)

      # Open it with three failures reported as the retry loop does.
      for _ <- 1..3, do: Breaker.timed_out(b.store)
      assert Breaker.open?(b.breaker)

      b.advance.(1_000)
      hung = spawn(fn -> get(b) end)
      assert_receive {:reached, ^hung}

      assert get(b) == {:error, :store_unavailable}
      b.advance.(5_000)
      second = spawn(fn -> get(b) end)
      assert_receive {:reached, ^second}
    end

    test "emits an event when it opens and when it closes" do
      capture([[:alambique, :store, :breaker, :open], [:alambique, :store, :breaker, :close]])

      {:ok, store_up} = Agent.start_link(fn -> false end)

      b =
        breaker(fn ->
          if Agent.get(store_up, & &1), do: {:ok, "", "e", %{}}, else: @http_500
        end)

      for _ <- 1..3, do: get(b)

      assert_receive {:telemetry, [:alambique, :store, :breaker, :open], %{cooldown_ms: 1_000},
                      %{instance: :test, location: "loc", reason: :failures}}

      b.advance.(1_000)
      get(b)

      assert_receive {:telemetry, [:alambique, :store, :breaker, :open], _,
                      %{reason: :probe_failed}}

      b.advance.(1_000)
      Agent.update(store_up, fn _ -> true end)
      get(b)

      assert_receive {:telemetry, [:alambique, :store, :breaker, :close], %{},
                      %{instance: :test, location: "loc"}}

      refute_receive {:telemetry, _, _, %{instance: :test}}, 20
    end

    test "classify names the breaker's answer through the steps that wrap it" do
      assert Breaker.classify({:restore, "key", :store_unavailable}) == :store_unavailable
      assert Breaker.classify({:lease, :store_unavailable}) == :store_unavailable
      assert Breaker.classify({:lease, :timeout}) == {:lease, :timeout}
      assert Breaker.classify(:timeout) == :timeout
    end
  end

  describe "the retry loop" do
    defp ctx(store, opts \\ []) do
      Replication.ctx(
        [store: store, prefix: "p", node: "n", backoff_ms: 1, max_backoff_ms: 2] ++ opts
      )
    end

    test "attempts a request exactly max_attempts times against a store that always fails" do
      stub = Stub.new(fn -> @http_500 end)

      for attempts <- [1, 2, 7] do
        before = Stub.count(stub)
        ctx = ctx({Stub, stub}, max_attempts: attempts)

        assert Objects.fetch(ctx, "k", Retry.deadline_in(30_000)) == @http_500
        assert Stub.count(stub) - before == attempts
      end
    end

    test "does not retry what the open breaker refused" do
      b = breaker(fn -> @http_500 end, %{failures: 1})
      get(b)
      ctx = ctx(b.store, max_attempts: 10)

      assert Objects.fetch(ctx, "k", Retry.deadline_in(30_000)) == {:error, :store_unavailable}
      assert Stub.count(b.stub) == 1
    end

    test "a request killed at its deadline counts as a failure of the store" do
      b = breaker(fn -> Process.sleep(:infinity) end, %{failures: 2})
      ctx = ctx(b.store)

      for _ <- 1..2,
          do: assert(Objects.fetch(ctx, "k", Retry.deadline_in(20)) == {:error, :timeout})

      assert Breaker.open?(b.breaker)
      assert Objects.fetch(ctx, "k", Retry.deadline_in(20)) == {:error, :store_unavailable}
      assert Stub.count(b.stub) == 2
    end

    test "a lease request killed at its timeout counts too" do
      b = breaker(fn -> Process.sleep(:infinity) end, %{failures: 1})

      assert Retry.once(b.store, 10, fn -> get(b) end) == {:error, :timeout}
      assert Breaker.open?(b.breaker)
    end
  end

  describe "an instance" do
    setup do
      env = setup_store()
      {:ok, env}
    end

    test "answers {:error, :store_unavailable} to a call that needs a failing store", ctx do
      seeder = start_instance(ctx, migrations: [{1, "CREATE TABLE t (n INTEGER)"}])
      cell = unique_cell()
      assert {:ok, _} = Alambique.execute(seeder, cell, "INSERT INTO t VALUES (1)", [])
      :ok = Alambique.hibernate(seeder, cell)

      reader =
        start_instance(ctx,
          migrations: [{1, "CREATE TABLE t (n INTEGER)"}],
          compaction: false,
          breaker: [failures: 2, cooldown_ms: 60_000],
          retry: [max_attempts: 1],
          wake: [backoff_ms: 1, backoff_max_ms: 1, backoff_jitter: 0.0]
        )

      MemoryStore.inject(ctx.store_config, kind: :error, reason: {:http, 503, ""})

      # Each call waits for the wake backoff of the one before to end, so it
      # reaches the store and the breaker.
      config = Alambique.Config.fetch!(reader)

      answers =
        for _ <- 1..4 do
          eventually(fn -> Alambique.Admission.store_failure(config, cell) == nil end)
          Alambique.execute(reader, cell, "INSERT INTO t VALUES (2)", [])
        end

      assert List.last(answers) == {:error, :store_unavailable}
    end

    test "no cell loses its lease to an outage that opens the breaker and ends in time", ctx do
      capture([[:alambique, :cell, :stop], [:alambique, :store, :breaker, :open]])
      schema = [{1, "CREATE TABLE t (n INTEGER)"}]

      # TTL 3 s, renewals about every second, margin 0.5 s.
      inst =
        start_instance(ctx,
          migrations: schema,
          compaction: false,
          breaker: [failures: 2, cooldown_ms: 8_000]
        )

      config = Alambique.Config.fetch!(inst)
      cells = for _ <- 1..5, do: unique_cell()

      for cell <- cells,
          do: assert({:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (1)", []))

      pids = for cell <- cells, do: Alambique.Cell.whereis(config, cell)

      fault =
        MemoryStore.inject(ctx.store_config,
          kind: :error,
          reason: {:http, 503, ""},
          times: :infinity
        )

      # Wakes that fail open the breaker.
      for _ <- 1..3, do: Alambique.execute(inst, unique_cell(), "INSERT INTO t VALUES (1)", [])
      assert_receive {:telemetry, [:alambique, :store, :breaker, :open], _, %{instance: ^inst}}

      Process.sleep(1_500)
      MemoryStore.remove(ctx.store_config, fault)

      # The breaker is still open; two more TTLs' worth of renewals must get through.
      Process.sleep(4_000)

      assert pids == for(cell <- cells, do: Alambique.Cell.whereis(config, cell))
      assert Enum.all?(pids, &Process.alive?/1)
      refute_received {:telemetry, [:alambique, :cell, :stop], _, _}
    end
  end
end
