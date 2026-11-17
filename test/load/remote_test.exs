defmodule Alambique.Load.RemoteTest do
  use ExUnit.Case, async: true

  # scripts/load --remote and --fetch against a stand-in for ssh that runs the
  # command on this machine, in a directory that plays the host's clone of the
  # repository. The `scripts/load` of that clone only records how it was started.

  setup do
    base = Path.join(System.tmp_dir!(), "alambique-remote-#{System.unique_integer([:positive])}")
    repo = Path.join(base, "repo")
    bin = Path.join(base, "bin")
    File.mkdir_p!(Path.join(repo, "scripts"))
    File.mkdir_p!(bin)
    on_exit(fn -> File.rm_rf!(base) end)

    File.write!(Path.join(bin, "ssh"), "#!/usr/bin/env bash\nshift\nexec bash -c \"$1\"\n")
    File.chmod!(Path.join(bin, "ssh"), 0o755)

    File.write!(
      Path.join(repo, "scripts/load"),
      "#!/usr/bin/env bash\necho \"$@\" >\"$(dirname \"$0\")/../started-with.txt\"\n"
    )

    File.chmod!(Path.join(repo, "scripts/load"), 0o755)

    round = Path.join(base, "round.toml")

    File.write!(round, """
    [output]
    name = "remote-test"
    dir = "#{base}/local"
    [host]
    ssh = "builder"
    repo = "#{repo}"
    timeout_s = 600
    """)

    {:ok, base: base, repo: repo, bin: bin, round: round}
  end

  defp load(ctx, args) do
    path = ctx.bin <> ":" <> System.get_env("PATH")

    System.cmd(Path.expand("scripts/load"), ["--config", ctx.round | args],
      env: [{"PATH", path}],
      stderr_to_stdout: true
    )
  end

  test "--remote copies the file, starts the round detached under a timeout and records its pid",
       ctx do
    {out, 0} = load(ctx, ["--remote"])
    assert out =~ "started pid"
    assert out =~ "--fetch"

    results = Path.join(ctx.repo, "results/load/remote-test")
    assert File.read!(Path.join(results, "round.toml")) == File.read!(ctx.round)
    pid = results |> Path.join("load.pid") |> File.read!() |> String.trim()
    assert pid =~ ~r/^\d+$/

    started = Path.join(ctx.repo, "started-with.txt")
    wait_for(fn -> File.exists?(started) and File.read!(started) != "" end)

    assert File.read!(started) ==
             "--config results/load/remote-test/round.toml --output-dir results/load/remote-test\n"

    # nothing ran on this machine: the results directory of the round file is untouched
    refute File.exists?(Path.join(ctx.base, "local"))
  end

  test "--remote passes --no-skip-done on", ctx do
    {_, 0} = load(ctx, ["--remote", "--no-skip-done"])
    started = Path.join(ctx.repo, "started-with.txt")
    wait_for(fn -> File.exists?(started) and File.read!(started) != "" end)
    assert File.read!(started) =~ "--no-skip-done"
  end

  test "a round that is still running is not started twice", ctx do
    results = Path.join(ctx.repo, "results/load/remote-test")
    File.mkdir_p!(results)
    # a process of this test stands for the running round, and is stopped by its pid
    port = Port.open({:spawn_executable, System.find_executable("sleep")}, args: ["30"])
    {:os_pid, pid} = Port.info(port, :os_pid)
    File.write!(Path.join(results, "load.pid"), "#{pid}\n")

    try do
      {out, 1} = load(ctx, ["--remote"])
      assert out =~ "a round is already running there (pid #{pid})"
    after
      System.cmd("kill", [Integer.to_string(pid)])
    end
  end

  test "--fetch copies the results back and leaves a store directory", ctx do
    results = Path.join(ctx.repo, "results/load/remote-test")
    File.mkdir_p!(Path.join(results, "A/wakes"))
    File.mkdir_p!(Path.join(results, "A/commits/large/store"))
    File.write!(Path.join(results, "A/wakes/step-10.json"), "{}")
    File.write!(Path.join(results, "A/commits/large/store/data"), "x")

    {out, 0} = load(ctx, ["--fetch"])
    assert out =~ "copied to #{ctx.base}/local"

    assert File.exists?(Path.join(ctx.base, "local/A/wakes/step-10.json"))
    refute File.exists?(Path.join(ctx.base, "local/A/commits/large/store"))
  end

  test "--remote and --fetch need host.ssh, and one flag at a time", ctx do
    File.write!(ctx.round, "[output]\nname = \"x\"\n")
    {out, 1} = load(ctx, ["--remote"])
    assert out =~ "host.ssh: not set"

    {_, 2} = load(ctx, ["--remote", "--fetch"])
  end

  defp wait_for(fun, tries \\ 100) do
    cond do
      fun.() -> :ok
      tries == 0 -> flunk("the started round never wrote its record")
      true -> Process.sleep(50) && wait_for(fun, tries - 1)
    end
  end
end
