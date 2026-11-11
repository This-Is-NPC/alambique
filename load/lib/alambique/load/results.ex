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

  @doc "The file name of a step: sequence, phase and level, sortable."
  @spec step_name(pos_integer, atom, number) :: String.t()
  def step_name(seq, phase, level) do
    "step-#{String.pad_leading(Integer.to_string(seq), 3, "0")}-#{phase}-#{level}.json"
  end
end
