defmodule Alambique.Telemetry do
  @moduledoc false

  @doc "Emits `[:alambique | event]` with a `duration` measured from `started`."
  @spec span_stop([atom], integer, map, map) :: :ok
  def span_stop(event, started, measurements \\ %{}, meta) do
    :telemetry.execute(
      [:alambique | event],
      Map.put(measurements, :duration, System.monotonic_time() - started),
      meta
    )
  end

  @spec emit([atom], map, map) :: :ok
  def emit(event, measurements \\ %{}, meta),
    do: :telemetry.execute([:alambique | event], measurements, meta)
end
