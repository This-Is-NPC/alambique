defmodule Alambique.Load.HostTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Load.Host

  test "reads the 1-minute load average" do
    assert Host.parse_loadavg("8.36 12.76 9.82 3/1234 56789\n") == 8.36
  end

  test "reads MemAvailable in GiB" do
    text = "MemTotal: 48000000 kB\nMemFree: 1 kB\nMemAvailable:    6291456 kB\nBuffers: 3 kB\n"
    assert Host.parse_mem_available_gib(text) == 6.0
    assert Host.low_memory?(5.99, 6)
    refute Host.low_memory?(6.0, 6)
  end

  test "reads the state of a task whose name has parentheses and spaces" do
    assert Host.task_state("1234 (beam.smp) R 1 1234 1234 0") == "R"
    assert Host.task_state("42 (a) b) D 1 2") == "D"
    assert Host.task_state("42 (x y) S 1 2") == "S"
    assert Host.runnable?("R") and Host.runnable?("D")
    refute Host.runnable?("S")
  end

  test "counts the runnable tasks of the processes it is given" do
    # This test's own VM has at least one thread running while it reads.
    assert Host.runnable_tasks([System.pid()]) >= 1
    assert Host.runnable_tasks(["0", "no-such-pid"]) == 0
  end

  test "our own average follows the kernel's: steady count converges to it" do
    ema = Enum.reduce(1..200, nil, fn _, acc -> Host.ema(acc, 4) end)
    assert_in_delta ema, 4.0, 1.0e-9
    # After 12 ticks (one minute) the old count weighs about 1/e.
    assert_in_delta Host.ema(10.0, 0)
                    |> then(&Enum.reduce(1..11, &1, fn _, a -> Host.ema(a, 0) end)),
                    10 * :math.exp(-1),
                    1.0e-9
  end

  test "net load is the load average without our own tasks, never below zero" do
    assert Host.net_load(9.5, 3.5) == 6.0
    assert Host.net_load(2.0, 3.5) == 0.0
  end

  property "net load never exceeds the load average" do
    check all(load <- float(min: 0.0, max: 64.0), own <- float(min: 0.0, max: 64.0)) do
      net = Host.net_load(load, own)
      assert net >= 0.0 and net <= load
    end
  end
end
