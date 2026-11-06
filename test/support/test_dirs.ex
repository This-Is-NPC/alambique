defmodule Alambique.TestDirs do
  @moduledoc """
  Temporary directories of tests and benchmarks, all under one root per OS
  process (`<tmp>/alambique-test-<os pid>`). A run removes its root when it
  ends, and the roots of runs whose process is gone when it starts, so a run
  that was killed leaves nothing behind for long. The sweep also takes the
  directories of `AlambiqueTestkit.Cluster` (`alambique-cluster-<os pid>_<n>`).
  """

  @prefix "alambique-test-"
  @cluster "alambique-cluster-"

  @doc "A new empty directory under this run's root; `label` names it."
  @spec new(String.t()) :: Path.t()
  def new(label) do
    dir = Path.join(root(), "#{label}-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    dir
  end

  @doc "Removes this run's root."
  @spec clean() :: :ok
  def clean do
    File.rm_rf(root())
    :ok
  end

  @doc "Removes the directories of runs whose OS process no longer exists."
  @spec sweep() :: :ok
  def sweep do
    tmp = System.tmp_dir!()

    for name <- File.ls!(tmp), pid = owner(name), pid != nil, not alive?(pid) do
      File.rm_rf(Path.join(tmp, name))
    end

    :ok
  end

  defp owner(@prefix <> pid), do: pid
  defp owner(@cluster <> run), do: run |> String.split("_") |> hd()
  defp owner(_name), do: nil

  defp root, do: Path.join(System.tmp_dir!(), @prefix <> System.pid())

  defp alive?(pid) do
    match?({_, 0}, System.cmd("kill", ["-0", pid], stderr_to_stdout: true))
  end
end
