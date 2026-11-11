defmodule Alambique.Load.Env do
  @moduledoc false
  # Settings of the load test come from ALAMBIQUE_LOAD_* variables, each with
  # a default documented in docs/how-to-load-test.md.

  @spec get(String.t(), String.t() | nil) :: String.t() | nil
  def get(name, default \\ nil), do: System.get_env("ALAMBIQUE_LOAD_" <> name, default)

  @spec int(String.t(), integer) :: integer
  def int(name, default), do: name |> get() |> parse(default, &String.to_integer/1)

  @spec float(String.t(), number) :: float
  def float(name, default), do: name |> get() |> parse(default, &parse_float/1) |> Kernel.*(1.0)

  @doc "A comma separated list of numbers, or `default`."
  @spec numbers(String.t(), [number]) :: [number]
  def numbers(name, default) do
    parse(get(name), default, fn text ->
      text |> String.split([",", " "], trim: true) |> Enum.map(&parse_number/1)
    end)
  end

  defp parse(nil, default, _fun), do: default
  defp parse("", default, _fun), do: default
  defp parse(text, _default, fun), do: fun.(text)

  defp parse_float(text) do
    {value, ""} = Float.parse(text)
    value
  end

  defp parse_number(text) do
    case Integer.parse(text) do
      {value, ""} -> value
      _ -> parse_float(text)
    end
  end
end
