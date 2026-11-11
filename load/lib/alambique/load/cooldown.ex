defmodule Alambique.Load.Cooldown do
  @moduledoc false
  # Waits, before a step, until the service has recovered from the previous
  # one: no operation of it still running on the service and, unless the
  # measure holds memory by design (awake cells), process memory below
  # `max_mem_mb`. A step that starts on a loaded service measures the previous
  # step's backlog, not its own level.
  #
  # `opts`: `max_mem_mb` (nil to skip the memory check), `max_wait_s`.
  # `io`: `read` (`%{process_mb, outstanding}`, or nil when the service is
  # gone), `now` (ms), `sleep` (ms) and `log` (one line).

  @poll_ms 1_000

  @type result ::
          {:cooled, float}
          | {:no_cooldown, float, float}
          | {:service_down, float}

  @doc "Waits; returns `{:cooled, waited_s}`, `{:no_cooldown, waited_s, process_mb}` on timeout or `{:service_down, waited_s}`."
  @spec wait(map, map) :: result
  def wait(opts, io), do: loop(opts, io, io.now.(), false)

  defp loop(opts, io, started, announced?) do
    waited = (io.now.() - started) / 1000

    case io.read.() do
      nil ->
        {:service_down, waited}

      reading ->
        cond do
          cooled?(reading, opts) ->
            {:cooled, waited}

          waited >= opts.max_wait_s ->
            {:no_cooldown, waited, reading.process_mb}

          true ->
            unless announced? do
              io.log.(
                "service busy (#{round(reading.process_mb)} MB, #{reading.outstanding} operations running): waiting for it to cool down"
              )
            end

            io.sleep.(@poll_ms)
            loop(opts, io, started, true)
        end
    end
  end

  defp cooled?(reading, opts) do
    reading.outstanding == 0 and
      (opts.max_mem_mb == nil or reading.process_mb < opts.max_mem_mb)
  end
end
