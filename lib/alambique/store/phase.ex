defmodule Alambique.Store.Phase do
  @moduledoc false
  # Where the request of a store call is, so the circuit breaker can tell a
  # failure of the store from one that was born in the node (design §3).
  #
  # The breaker marks the process that makes a store call with the operation
  # and a phase. Finch emits, in that same process, an event before it waits
  # for a connection of the pool, when it has one and when it sends the request,
  # and a handler moves the phase along: `:queued` (waiting for a connection: the
  # request has not left), `:checked_out` (a connection is held, it may still be
  # opening) and `:sent`. A store that does not use Finch leaves `:unknown`,
  # which the breaker counts like a request that left.

  @key :alambique_store_call
  @events [[:finch, :queue, :start], [:finch, :queue, :stop], [:finch, :send, :start]]

  @type phase :: :unknown | :queued | :checked_out | :sent

  @doc "Starts following the Finch requests of the processes that call a store (idempotent)."
  @spec attach() :: :ok
  def attach do
    _ = :telemetry.attach_many(__MODULE__, @events, &__MODULE__.handle/4, nil)
    :ok
  end

  @doc "Marks this process as in a call of `operation`."
  @spec begin(atom) :: :ok
  def begin(operation) do
    Process.put(@key, {operation, :unknown})
    :ok
  end

  @doc "The `{operation, phase}` of this process, or `nil` outside a call."
  @spec current() :: {atom, phase} | nil
  def current, do: Process.get(@key)

  @doc "Leaves the call."
  @spec finish() :: :ok
  def finish do
    Process.delete(@key)
    :ok
  end

  @doc "The `{operation, phase}` of the process `pid`, or `nil` outside a call or when it is gone."
  @spec of(pid) :: {atom, phase} | nil
  def of(pid) do
    with {:dictionary, dictionary} <- Process.info(pid, :dictionary),
         {@key, state} <- List.keyfind(dictionary, @key, 0) do
      state
    else
      _ -> nil
    end
  end

  @doc false
  @spec handle(:telemetry.event_name(), term, term, term) :: :ok
  def handle([:finch, :queue, :start], _, _, _), do: move(:queued)
  def handle([:finch, :queue, :stop], _, _, _), do: move(:checked_out)
  def handle([:finch, :send, :start], _, _, _), do: move(:sent)

  defp move(phase) do
    case Process.get(@key) do
      {operation, _} -> Process.put(@key, {operation, phase})
      nil -> :ok
    end

    :ok
  end
end
