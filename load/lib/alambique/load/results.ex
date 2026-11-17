defmodule Alambique.Load.Results do
  @moduledoc false
  # One JSON file per step, written whole or not at all.

  @doc """
  Writes `term` as JSON to `path`: encode first, write a temporary file in the
  same directory, flush it and rename it over `path`. A failure leaves the old
  file (or none) and no temporary file.
  """
  @spec write!(Path.t(), term) :: :ok
  def write!(path, term) do
    json = Jason.encode_to_iodata!(term, pretty: true)
    File.mkdir_p!(Path.dirname(path))
    tmp = "#{path}.tmp-#{System.unique_integer([:positive])}"

    try do
      File.open!(tmp, [:write, :binary], fn file ->
        IO.binwrite(file, json)
        :ok = :file.sync(file)
      end)

      File.rename!(tmp, path)
    after
      File.rm(tmp)
    end
  end

  @doc """
  The file name of a step: its level, and `-2` for the confirmation, which
  repeats the level of an earlier step.
  """
  @spec step_name(number, atom) :: String.t()
  def step_name(level, :confirm), do: "step-#{level}-2.json"
  def step_name(level, _phase), do: "step-#{level}.json"

  @doc """
  The step result at `path` when it counts as done: it exists, was made with
  the settings `hash` and is not invalid. Returns the step with its keys as
  atoms and `verdict_term` as the driver holds it.
  """
  @spec read_done(Path.t(), String.t()) :: {:ok, map} | :none
  def read_done(path, hash) do
    with {:ok, text} <- File.read(path),
         {:ok, %{config_hash: ^hash, verdict: verdict, phase: phase, reason: reason} = step}
         when verdict != "invalid" <- Jason.decode(text, keys: :atoms) do
      {:ok,
       step
       |> Map.put(:phase, String.to_existing_atom(phase))
       |> Map.put(:verdict_term, verdict_term(verdict, reason))}
    else
      _ -> :none
    end
  end

  @doc "The verdict of a stored step as the ramp and the summary hold it: its name and the reason `inspect`ed."
  @spec verdict_term(String.t(), String.t()) :: :good | {atom, term}
  def verdict_term("good", _reason), do: :good
  def verdict_term(name, reason), do: {String.to_existing_atom(name), reason_term(reason)}

  # Reasons are stored as `inspect/1` text; a literal term reads back, anything else stays text.
  defp reason_term(text) do
    with {:ok, quoted} <- Code.string_to_quoted(text),
         true <- Macro.quoted_literal?(quoted) do
      quoted |> Code.eval_quoted() |> elem(0)
    else
      _ -> text
    end
  end
end
