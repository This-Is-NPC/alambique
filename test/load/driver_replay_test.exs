defmodule Alambique.Load.DriverReplayTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Alambique.Load.{Config, Driver}

  @moduletag :tmp_dir

  # The driver in --replay mode starts no service: it reads the steps that are
  # done and says whether they cover the ramp. That is the skip-done logic of
  # a real run, which reads the same files before every step.

  defp case_file(dir, extra_args \\ []) do
    round = Path.join(dir, "round.toml")

    File.write!(round, """
    [round]
    cases = ["A/wakes"]
    [output]
    dir = "#{dir}/out"
    [measures.wakes.steps]
    kind = "ladder"
    values = [10, 20, 40]
    refine = 0
    confirm = false
    """)

    {_, 0} =
      System.cmd("python3", ["-I", "scripts/load_config.py", "--prepare", round | extra_args],
        stderr_to_stdout: true
      )

    path = Path.join(dir, "out/A/wakes/case.json")
    {path, path |> File.read!() |> Jason.decode!()}
  end

  defp step(cfg, name, fields) do
    step = %{
      schema: 1,
      seq: 1,
      phase: "ramp",
      level: 10,
      verdict: "good",
      reason: "",
      quiet_wait_s: 0.0,
      cooldown_s: 0.0,
      suspect: false,
      delivery: %{ratio: 1.0, busy_ratio: 0.0},
      latency_ms: %{p50: 5.0, p95: 9.0, p99: 12.0},
      cgroup: nil,
      cost: nil,
      store: nil,
      swap: nil,
      service: nil,
      extra: %{},
      config_hash: cfg["hash"]
    }

    File.write!(Path.join(cfg["dir"], name), Jason.encode!(Map.merge(step, Map.new(fields))))
  end

  defp replay(path),
    do: capture_io(fn -> send(self(), {:status, Driver.main(["--replay", path])}) end)

  defp status do
    assert_received {:status, status}
    status
  end

  test "steps that cover the ramp are read, not run: the summary is written and the status is 0",
       %{tmp_dir: dir} do
    {path, cfg} = case_file(dir)
    step(cfg, "step-10.json", level: 10)
    step(cfg, "step-20.json", level: 20, verdict: "bad", reason: ":errors")

    out = replay(path)
    assert status() == 0
    assert out =~ "wakes 10: done, good"
    assert out =~ "wakes 20: done, bad :errors"

    summary = cfg["dir"] |> Path.join("summary.json") |> File.read!() |> Jason.decode!()
    assert summary["outcome"]["last_good"] == 10 and summary["outcome"]["first_bad"] == 20
    assert summary["stopped_by"] == "bad: errors"
    assert summary["config_hash"] == cfg["hash"]
    assert File.exists?(Path.join(cfg["dir"], "summary.md"))
    refute File.exists?(Path.join(cfg["dir"], "step-40.json"))
  end

  test "a step left to run is status 10 and no summary", %{tmp_dir: dir} do
    {path, cfg} = case_file(dir)
    step(cfg, "step-10.json", level: 10)

    replay(path)
    assert status() == 10
    refute File.exists?(Path.join(cfg["dir"], "summary.json"))
  end

  test "a step of other settings, or an invalid one, is not done", %{tmp_dir: dir} do
    {path, cfg} = case_file(dir)
    step(cfg, "step-10.json", level: 10, config_hash: "other")
    replay(path)
    assert status() == 10

    step(cfg, "step-10.json", level: 10)
    step(cfg, "step-20.json", level: 20, verdict: "invalid", reason: ":timeout")
    replay(path)
    assert status() == 10
  end

  test "a run that does not skip done steps runs every step", %{tmp_dir: dir} do
    {path, cfg} = case_file(dir, ["--no-skip-done"])
    step(cfg, "step-10.json", level: 10)
    step(cfg, "step-20.json", level: 20)
    step(cfg, "step-40.json", level: 40)

    replay(path)
    assert status() == 10
  end

  describe "the limits of a case" do
    defp limits(dir, round_text, holds_memory? \\ false) do
      round = Path.join(dir, "round.toml")
      File.write!(round, round_text <> "\n[output]\ndir = \"#{dir}/out\"\n")

      {_, 0} =
        System.cmd("python3", ["-I", "scripts/load_config.py", "--prepare", round],
          stderr_to_stdout: true
        )

      path = dir |> Path.join("out") |> Path.join("**/case.json") |> Path.wildcard() |> hd()
      path |> File.read!() |> Jason.decode!() |> Config.limits(holds_memory?)
    end

    test "are the criteria of the file", %{tmp_dir: dir} do
      l =
        limits(dir, """
        [round]
        cases = ["A/commits/small"]
        [judging.busy]
        max_pct = 2
        [judging.latency]
        max_ms = 100
        [judging.host_load]
        max = 4
        """)

      assert l.max_busy_ratio == 0.02 and l.max_error_ratio == 0.0
      assert l.latency == {:p99, 100.0}
      assert l.max_net_load == 4.0 and l.mem_limit_mb == 512 and l.mem_fraction == 1.0
      assert l.store_mb == 4096 and l.store_full_fraction == 0.95 and l.attempts == 5
      assert l.cooldown_memory? and l.cooldown? and l.wait_quiet?
    end

    test "a criterion that is off has a limit nothing reaches", %{tmp_dir: dir} do
      l =
        limits(dir, """
        [round]
        cases = ["A/commits/small"]
        [judging.busy]
        enabled = false
        [judging.errors]
        enabled = false
        [judging.latency]
        enabled = false
        [judging.memory]
        enabled = false
        [judging.delivery]
        enabled = false
        [judging.host_load]
        enabled = false
        [judging.host_memory]
        enabled = false
        [judging.store_full]
        enabled = false
        [judging.cooldown]
        enabled = false
        """)

      assert l.latency == nil and l.delivery_min == 0.0 and l.min_available_gib == 0.0
      # errors and busy refusals can never exceed all the completed operations
      assert l.max_busy_ratio == 1.0 and l.max_error_ratio == 1.0
      assert l.mem_fraction > 1.0e9 and l.max_net_load > 1.0e9 and l.store_full_fraction > 1.0e9
      assert l.store_min_free_mb == 0.0
      refute l.cooldown? or l.wait_quiet?
    end

    test "cells: memory at 90% of the limit, no latency, memory kept between steps", %{
      tmp_dir: dir
    } do
      l = limits(dir, "[round]\ncases = [\"B/cells/default\"]\n", true)
      assert l.mem_fraction == 0.9 and l.mem_limit_mb == 2048 and l.latency == nil
      refute l.cooldown_memory?
    end
  end
end
