defmodule Alambique.Load.ReproduceCommandTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  # scripts/load runs in a copy of the repository layout whose scripts/load-case
  # only prints the command the case would record.
  defp reproduce(tmp_dir, env) do
    File.mkdir_p!(Path.join(tmp_dir, "scripts"))
    File.cp!("scripts/load", Path.join(tmp_dir, "scripts/load"))
    stub = Path.join(tmp_dir, "scripts/load-case")
    File.write!(stub, "#!/usr/bin/env bash\necho \"COMMAND: $ALAMBIQUE_LOAD_COMMAND\"\n")
    File.chmod!(stub, 0o755)

    {out, 0} = System.cmd(Path.join(tmp_dir, "scripts/load"), ["B", "commits"], env: env)
    out |> String.split("\n") |> Enum.filter(&String.starts_with?(&1, "COMMAND: "))
  end

  test "a variant the user exported appears once in the command", %{tmp_dir: tmp_dir} do
    env = [{"ALAMBIQUE_LOAD_VARIANT", "small"}, {"ALAMBIQUE_LOAD_CPUS", "2"}]

    assert reproduce(tmp_dir, env) ==
             [
               "COMMAND: ALAMBIQUE_LOAD_CPUS=2 ALAMBIQUE_LOAD_VARIANT=small mise run load -- B commits"
             ]
  end
end
