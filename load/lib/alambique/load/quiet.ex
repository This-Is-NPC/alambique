defmodule Alambique.Load.Quiet do
  @moduledoc false
  # Waits until the host is quiet before a step: the net load (see
  # `Alambique.Load.Host`) at or below the limit in every reading of a settle
  # window. The 1-minute load average lags, so one quiet reading is not
  # enough. Only readings taken after the wait began count. The service stays
  # up and idle meanwhile; the caller decides what a timeout or low memory
  # means.
  #
  # `opts`: `max_net_load`, `min_available_gib`, `settle_s`, `max_wait_s`.
  # `io`: `read` (the latest host reading, or nil), `now` (ms since the epoch,
  # the clock of the readings' `at`), `sleep` (ms) and `log` (one line).

  alias Alambique.Load.Host

  @poll_ms 1_000

  @type result ::
          {:quiet, float}
          | {:busy, float, float}
          | {:low_memory, float}

  @doc "The readings of the settle window: one per host sample."
  @spec settle_readings(number) :: pos_integer
  def settle_readings(settle_s), do: max(ceil(settle_s / Host.tick_s()), 1)

  @doc "Waits; returns `{:quiet, waited_s}`, `{:busy, waited_s, last_net_load}` on timeout or `{:low_memory, waited_s}`."
  @spec wait(map, map) :: result
  def wait(opts, io) do
    started = io.now.()

    loop(opts, io, %{
      started: started,
      last_at: started,
      streak: 0,
      load: nil,
      waiting?: false,
      needed: settle_readings(opts.settle_s)
    })
  end

  defp loop(opts, io, state) do
    host = io.read.()

    if host && host.at > state.last_at do
      state = %{state | last_at: host.at, load: host.net_load}
      reading(opts, io, state, host)
    else
      pause(opts, io, state)
    end
  end

  defp reading(opts, io, state, host) do
    cond do
      Host.low_memory?(host.mem_available_gib, opts.min_available_gib) ->
        {:low_memory, waited(io, state)}

      host.net_load > opts.max_net_load ->
        state = announce_waiting(io, %{state | streak: 0})
        pause(opts, io, state)

      state.streak + 1 >= state.needed ->
        waited = waited(io, state)

        if state.waiting?,
          do:
            io.log.(
              "host quiet again (net load #{fmt(host.net_load)}), going on after #{fmt(waited)} s"
            )

        {:quiet, waited}

      true ->
        pause(opts, io, %{state | streak: state.streak + 1})
    end
  end

  defp pause(opts, io, state) do
    if waited(io, state) >= opts.max_wait_s do
      {:busy, waited(io, state), state.load || 0.0}
    else
      io.sleep.(@poll_ms)
      loop(opts, io, state)
    end
  end

  defp announce_waiting(io, %{waiting?: false} = state) do
    io.log.("host busy (net load #{fmt(state.load)}): waiting for it to calm down")
    %{state | waiting?: true}
  end

  defp announce_waiting(_io, state), do: state

  defp waited(io, state), do: (io.now.() - state.started) / 1000
  defp fmt(number), do: Float.round(number * 1.0, 1)
end
