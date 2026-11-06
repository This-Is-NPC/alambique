defmodule AlambiqueTestkit.CrashPoints do
  @moduledoc """
  Arms the named points that library code reaches through
  `Alambique.Fault.point/2`. Armed state is per VM.

      CrashPoints.arm(:after_upload, :raise)
      CrashPoints.arm(:before_fence, :kill, times: 2, when: &match?(%{cell: "a"}, &1))
  """

  defmodule Crash do
    defexception [:point]
    @impl true
    def message(%{point: point}), do: "crash point #{inspect(point)}"
  end

  @key {__MODULE__, :armed}

  @type action :: :raise | :exit | :kill | (atom, term -> any)

  @doc """
  Arms `name`. Actions: `:raise` raises `Crash`, `:exit` exits with
  `{:crash_point, name}`, `:kill` kills the calling process, or a function
  called with the point name and context. Options: `:times` (default 1, or
  `:infinity`) and `:when`, a predicate on the context.
  """
  @spec arm(atom, action, keyword) :: :ok
  def arm(name, action \\ :raise, opts \\ []) do
    install()

    counter =
      case Keyword.get(opts, :times, 1) do
        :infinity ->
          :infinity

        times ->
          counter = :atomics.new(1, signed: true)
          :atomics.put(counter, 1, times)
          counter
      end

    entry = {action, counter, Keyword.get(opts, :when)}
    :persistent_term.put(@key, Map.put(armed(), name, entry))
  end

  @doc "Disarms one point."
  @spec disarm(atom) :: :ok
  def disarm(name), do: :persistent_term.put(@key, Map.delete(armed(), name))

  @doc "Disarms everything and uninstalls the handler."
  @spec reset() :: :ok
  def reset do
    Alambique.Fault.uninstall()
    :persistent_term.erase(@key)
    :ok
  end

  @doc "Installs the handler into `Alambique.Fault`."
  @spec install() :: :ok
  def install, do: Alambique.Fault.install(&__MODULE__.handle/2)

  @doc false
  def handle(name, context) do
    with {action, counter, guard} <- Map.get(armed(), name),
         true <- guard == nil or guard.(context),
         true <- counter == :infinity or :atomics.sub_get(counter, 1, 1) >= 0 do
      fire(action, name, context)
    else
      _ -> :ok
    end
  end

  defp fire(:raise, name, _), do: raise(Crash, point: name)
  defp fire(:exit, name, _), do: exit({:crash_point, name})
  defp fire(:kill, _, _), do: Process.exit(self(), :kill)
  defp fire(fun, name, context) when is_function(fun, 2), do: fun.(name, context)

  defp armed, do: :persistent_term.get(@key, %{})
end
