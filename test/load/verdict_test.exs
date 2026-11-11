defmodule Alambique.Load.VerdictTest do
  use ExUnit.Case, async: true

  alias Alambique.Load.Verdict

  @limits %{
    delivery_min: 0.95,
    max_net_load: 6.0,
    max_driver_swap_mb: 16.0,
    max_store_swap_mb: 0.0,
    max_host_swap_mb: 512.0,
    max_error_ratio: 0.0,
    latency: {:p99, 250},
    mem_limit_mb: 512,
    mem_fraction: 1.0
  }

  @good %{
    planned: 1_000,
    completed: 1_000,
    errors: 0,
    latency_ms: %{p99: 100.0},
    net_load_max: 1.0,
    mem_mb_max: 200.0,
    aborted: nil
  }

  defp judge(changes, limits \\ %{}),
    do: Verdict.judge(Map.merge(@good, changes), Map.merge(@limits, limits))

  test "a step that delivers and meets the criterion is good" do
    assert judge(%{}) == :good
  end

  test "the latency limit is strict" do
    assert judge(%{latency_ms: %{p99: 250.0}}) == :good
    assert judge(%{latency_ms: %{p99: 250.1}}) == {:bad, {:latency, :p99, 250}}
  end

  test "the delivery gate is 95%, and below it the step is invalid, not bad" do
    assert judge(%{completed: 950}) == :good
    assert {:invalid, {:delivery, ratio}} = judge(%{completed: 949})
    assert_in_delta ratio, 0.949, 1.0e-9
    # Even when the latency of what did complete is far over the limit.
    assert {:invalid, _} = judge(%{completed: 500, latency_ms: %{p99: 9_000.0}})
  end

  test "a step with nothing planned delivers" do
    assert judge(%{planned: 0, completed: 0}) == :good
  end

  test "an error is bad unless the limit allows its share" do
    assert judge(%{errors: 1}) == {:bad, :errors}
    assert judge(%{errors: 10}, %{max_error_ratio: 0.01}) == :good
    assert judge(%{errors: 11}, %{max_error_ratio: 0.01}) == {:bad, :errors}
  end

  test "memory at its criterion is bad, also when the step was cut short" do
    assert judge(%{mem_mb_max: 460.8}, %{mem_fraction: 0.9}) == {:bad, :memory}
    assert judge(%{mem_mb_max: 460.7}, %{mem_fraction: 0.9}) == :good
    assert judge(%{mem_mb_max: 460.8}) == :good
    assert judge(%{mem_mb_max: 512.0}) == {:bad, :memory}
    assert judge(%{aborted: :memory, completed: 10}) == {:bad, :memory}
  end

  test "a busy host repeats the step before anything else is judged" do
    assert judge(%{net_load_max: 6.1}) == {:repeat, {:host_busy, 6.1}}
    assert judge(%{net_load_max: 6.0}) == :good
    assert {:repeat, _} = judge(%{net_load_max: 9.0, completed: 0, errors: 5, mem_mb_max: 600.0})
  end

  test "errors that say the store is full are named store_full" do
    assert judge(%{errors: 3, store_full: true}) == {:bad, :store_full}
    assert judge(%{errors: 3, store_full: false}) == {:bad, :errors}
    assert judge(%{errors: 0, store_full: true}) == :good
  end

  test "store_full? reads the error samples" do
    full = %{
      error: "{:error, {:s3, 500, \"write /data/1.dat: no space left on device\"}}",
      count: 2
    }

    other = %{error: "{:exit, :timeout}", count: 9}
    assert Verdict.store_full?([other, full])
    refute Verdict.store_full?([other])
    refute Verdict.store_full?([])
  end

  describe "swap" do
    defp swap(changes),
      do: %{
        swap:
          Map.merge(
            %{driver_max_mb: 0.0, host_max_mb: 0.0, store_growth_mb: 0.0, service_mb: 0.0},
            changes
          )
      }

    test "the driver's swap is judged by its absolute maximum; the limit itself is valid" do
      assert judge(swap(%{driver_max_mb: 16.0})) == :good
      assert judge(swap(%{driver_max_mb: 16.1})) == {:repeat, {:swap, :driver, 16.1}}

      assert judge(swap(%{driver_max_mb: 3.0}), %{max_driver_swap_mb: 2.0}) ==
               {:repeat, {:swap, :driver, 3.0}}
    end

    test "the store's swap is judged by its growth; the limit itself is valid" do
      assert judge(swap(%{store_growth_mb: 0.0})) == :good
      assert judge(swap(%{store_growth_mb: 0.1})) == {:repeat, {:swap, :store, 0.1}}
      assert judge(swap(%{store_growth_mb: 2.0}), %{max_store_swap_mb: 2.0}) == :good
    end

    test "the host's swap is judged by its absolute maximum; the limit itself is valid" do
      assert judge(swap(%{host_max_mb: 512.0})) == :good
      assert judge(swap(%{host_max_mb: 512.5})) == {:repeat, {:swap, :host, 512.5}}
    end

    test "unreadable swap is not a problem" do
      none = %{driver_max_mb: nil, host_max_mb: nil, store_growth_mb: nil, service_mb: nil}
      assert judge(swap(none)) == :good
    end

    test "any swap of the service is invalid, and is judged after the host load" do
      assert judge(swap(%{service_mb: 0.1})) == {:invalid, {:swap, :service, 0.1}}

      assert {:repeat, {:host_busy, _}} =
               judge(Map.merge(swap(%{driver_max_mb: 50.0}), %{net_load_max: 9.0}))
    end

    test "swap is judged before memory, delivery, errors and latency" do
      bad = %{mem_mb_max: 900.0, completed: 0, errors: 9, latency_ms: %{p99: 9_999.0}}
      assert {:repeat, {:swap, :store, _}} = judge(Map.merge(swap(%{store_growth_mb: 1.0}), bad))
    end
  end

  test "a measure without a latency criterion ignores latency" do
    assert judge(%{latency_ms: %{p99: 99_999.0}}, %{latency: nil}) == :good
  end

  test "the ramp sees good, bad and invalid" do
    assert Verdict.ramp_verdict(:good) == :good
    assert Verdict.ramp_verdict({:bad, :errors}) == :bad
    assert Verdict.ramp_verdict({:invalid, :service_down}) == :invalid
  end
end
