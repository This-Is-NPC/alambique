defmodule Alambique.TelemetryDocsTest do
  @moduledoc "docs/telemetry.md and the code agree on which events exist."
  use ExUnit.Case, async: true

  @doc_events ~r/`\[:alambique, ((?::\w+)(?:, :\w+)*)\]`/
  @call ~r/Telemetry\.(?:emit|span_stop)\(\s*\[((?::\w+)(?:, :\w+)*)\]/

  defp documented do
    "docs/telemetry.md"
    |> File.read!()
    |> then(&Regex.scan(@doc_events, &1))
    |> Enum.map(fn [_, names] -> names end)
    |> MapSet.new()
  end

  defp sources, do: Path.wildcard("lib/**/*.ex") |> Enum.map(&{&1, File.read!(&1)})

  test "every documented event is emitted somewhere" do
    for names <- documented() do
      [first | rest] = String.split(names, ", ")
      last = List.last(rest)

      assert Enum.any?(sources(), fn {_, src} ->
               String.contains?(src, "[#{first},") and Regex.match?(~r/#{last}\b/, src)
             end),
             "[:alambique, #{names}] is documented but no module emits it"
    end
  end

  test "every event the code emits is documented" do
    emitted =
      for {_, src} <- sources(),
          [_, names] <- Regex.scan(@call, src),
          into: MapSet.new(),
          do: names

    assert MapSet.size(emitted) > 10

    for names <- emitted do
      assert names in documented(),
             "[:alambique, #{names}] is emitted but not in docs/telemetry.md"
    end
  end
end
