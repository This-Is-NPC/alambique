defmodule Alambique.NativeStats do
  @moduledoc """
  Counters and gauges kept by the native library (design §2), read with one
  cheap call.

  `read/0` returns the current values. `emit/0` sends them as the
  `[:alambique, :native, :stats]` telemetry event, so it works as a
  `:telemetry_poller` measurement:

      {:telemetry_poller,
       measurements: [{Alambique.NativeStats, :emit, []}], period: :timer.seconds(10)}

  Every measurement is documented in `docs/telemetry.md`.
  """

  alias Alambique.Replication.Native
  alias Alambique.Telemetry

  @doc "The native counters and gauges as a map of integers."
  @spec read() :: %{atom => integer}
  def read, do: Native.stats()

  @doc "Emits `[:alambique, :native, :stats]` with the current values."
  @spec emit() :: :ok
  def emit, do: Telemetry.emit([:native, :stats], read(), %{})
end
