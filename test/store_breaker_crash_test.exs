defmodule Alambique.StoreBreakerCrashTest do
  @moduledoc """
  A crash of the store module (design §3): it is told in `[:store, :failure]`
  and in the log, and it counts toward the breaker unless it was the node's
  own wait for a connection of a pool, which never left.
  """
  use ExUnit.Case, async: true

  @moduletag :capture_log

  import Alambique.CellHelpers, only: [capture: 1]
  import ExUnit.CaptureLog

  alias Alambique.Store.{Breaker, Guard}

  defmodule Crashing do
    @moduledoc false
    # A store whose every request runs the function of its config, in the caller.
    @behaviour Alambique.Store

    @impl true
    def get(fun, _key), do: fun.()
    @impl true
    def put(fun, _key, _body, _condition, _meta), do: fun.()
    @impl true
    def head(fun, _key), do: fun.()
    @impl true
    def get_range(fun, _key, _offset, _length), do: fun.()
    @impl true
    def list(fun, _prefix, _start_after, _limit), do: fun.()
    @impl true
    def delete(fun, _key), do: fun.()
  end

  @settings %{
    failures: 3,
    cooldown_ms: 60_000,
    probe_timeout_ms: 60_000,
    detail_limit: 80,
    log_interval_ms: 1_000
  }

  # A guarded crashing store behind a breaker with a clock the test moves. Its
  # location is unique, so the events and log lines of other tests are told apart.
  defp breaker(fun, settings \\ %{}) do
    {:ok, clock} = Agent.start_link(fn -> 1_000 end)
    location = "loc-#{System.unique_integer([:positive])}"

    breaker =
      Breaker.new(Map.merge(@settings, settings), %{instance: :test, location: location},
        now: fn -> Agent.get(clock, & &1) end,
        jitter: 0.0
      )

    %{
      breaker: breaker,
      location: location,
      store: {Breaker, %{store: {Guard, {Crashing, fun}}, breaker: breaker}},
      advance: fn ms -> Agent.update(clock, &(&1 + ms)) end
    }
  end

  defp get(%{store: {m, c}}), do: m.get(c, "k")

  defp failure(location) do
    assert_receive {:telemetry, [:alambique, :store, :failure], _, %{location: ^location} = meta}
    meta
  end

  describe "a crash that is the wait for a pool connection" do
    for cause <- [:timeout, :noproc] do
      test "#{cause} is local: it does not count and says why" do
        capture([[:alambique, :store, :failure]])
        b = breaker(fn -> exit({unquote(cause), {NimblePool, :checkout, [self()]}}) end)

        for _ <- 1..10, do: assert({:error, {:store_crashed, :exit, _}} = get(b))
        refute Breaker.open?(b.breaker)
        assert Breaker.status(b.breaker).failures == 0

        assert %{reason: :local, counted: false, kind: :exit, detail: detail} =
                 failure(b.location)

        assert detail == :"pool_checkout_#{unquote(cause)}"
      end
    end

    test "so is the error that Finch raises for it" do
      capture([[:alambique, :store, :failure]])

      error = %RuntimeError{
        message: "Finch was unable to provide a connection within the timeout"
      }

      b = breaker(fn -> raise error end)

      for _ <- 1..10, do: get(b)
      refute Breaker.open?(b.breaker)
      assert %{reason: :local, kind: :error, detail: :pool_checkout_timeout} = failure(b.location)
    end

    test "a request_crashed of the same shape is local too" do
      b =
        breaker(fn ->
          {:error, {:request_crashed, :exit, {:timeout, {NimblePool, :checkout, []}}}}
        end)

      for _ <- 1..10, do: get(b)
      refute Breaker.open?(b.breaker)
    end
  end

  describe "a crash of the store module" do
    for {kind, crash} <- [
          error: quote(do: raise("boom")),
          throw: quote(do: throw(:boom)),
          exit: quote(do: exit(:boom))
        ] do
      test "#{kind} counts and the event shows its cause" do
        capture([[:alambique, :store, :failure]])
        b = breaker(fn -> unquote(crash) end)

        for _ <- 1..3, do: get(b)
        assert Breaker.open?(b.breaker)

        assert %{reason: :crashed, counted: true, kind: kind, detail: detail} =
                 failure(b.location)

        assert kind == unquote(kind)
        assert detail =~ "boom"
      end
    end

    test "a crash of the client of a request that left is a transport error, with its cause" do
      capture([[:alambique, :store, :failure]])
      b = breaker(fn -> {:error, {:request_crashed, :error, %ArgumentError{message: "bad"}}} end)

      get(b)

      assert %{reason: :transport, counted: true, kind: :error, detail: detail} =
               failure(b.location)

      assert detail =~ "bad"
    end

    test "a failure that is not a crash has no kind and no detail" do
      capture([[:alambique, :store, :failure]])
      b = breaker(fn -> {:error, {:http, 500, ""}} end)

      get(b)
      assert %{reason: {:http, 500}, kind: nil, detail: nil} = failure(b.location)
    end

    test "the detail is cut to the configured limit" do
      capture([[:alambique, :store, :failure]])
      b = breaker(fn -> raise String.duplicate("x", 10_000) end, %{detail_limit: 50})

      get(b)
      assert %{detail: detail} = failure(b.location)
      assert is_binary(detail) and String.length(detail) <= 50
    end
  end

  describe "the log" do
    test "says a crash, at most once per interval for a store" do
      b = breaker(fn -> raise "boom" end, %{failures: 1_000})

      log =
        capture_log(fn ->
          for _ <- 1..20, do: get(b)
          b.advance.(999)
          get(b)
        end)

      assert count(log, b.location) == 1
      assert log =~ "boom"
      assert log =~ "counted: true"

      log =
        capture_log(fn ->
          b.advance.(1)
          get(b)
          get(b)
        end)

      assert count(log, b.location) == 1
    end

    test "says a crash that did not count too" do
      b = breaker(fn -> exit({:timeout, {NimblePool, :checkout, []}}) end)
      log = capture_log(fn -> get(b) end)
      assert log =~ "counted: false"
      assert log =~ "pool_checkout_timeout"
    end

    test "has nothing for a failure that is not a crash" do
      b = breaker(fn -> {:error, {:http, 500, ""}} end)
      assert capture_log(fn -> get(b) end) == ""
    end
  end

  defp count(log, location), do: log |> String.split(location) |> length() |> Kernel.-(1)
end
