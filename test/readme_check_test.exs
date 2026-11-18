defmodule Alambique.ReadmeCheckTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  # scripts/readme-check: the guard against release-please rewriting the
  # README (a one-line start and end block, a 127.0.0.1 turned into 0.4.0.1).

  @script "scripts/readme-check"

  defp run(dir, content) do
    path = Path.join(dir, "README.md")
    File.write!(path, content)
    System.cmd("python3", ["-I", @script, path], stderr_to_stdout: true)
  end

  test "a file with the one-line marker passes", %{tmp_dir: dir} do
    assert {"", 0} =
             run(dir, "The current version is 0.12.0. <!-- x-release-please-version -->\n")
  end

  test "0.0.0.0 passes", %{tmp_dir: dir} do
    assert {"", 0} = run(dir, "Listen on 0.0.0.0 or 0.0.0.0.\n")
  end

  test "a start and an end marker on one line fail", %{tmp_dir: dir} do
    {out, 1} =
      run(
        dir,
        "ok\n<!-- x-release-please-start-version --> 0.1.0 <!-- x-release-please-end -->\n"
      )

    assert out =~ "README.md:2: release-please start and end markers on one line"
  end

  test "an address-like 0.x.y.z fails", %{tmp_dir: dir} do
    {out, 1} = run(dir, "ok\nbind 0.4.0.1:4000\n")
    assert out =~ "README.md:2: address-like version replace: 0.4.0.1"
  end

  test "a multi-line start block fails", %{tmp_dir: dir} do
    {out, 1} =
      run(
        dir,
        "<!-- x-release-please-start-version -->\nversion 0.1.0\n<!-- x-release-please-end -->\n"
      )

    assert out =~ "README.md:1: release-please start block"
  end

  test "the real README passes" do
    assert {"", 0} = System.cmd("python3", ["-I", @script], stderr_to_stdout: true)
  end
end
