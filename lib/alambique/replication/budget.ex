defmodule Alambique.Replication.Budget do
  @moduledoc false
  # Bytes that one restore, or one lazy hydration, holds from the store at once
  # (design §6.3). A read is weighed before it is made (`weigh/2`): an object
  # larger than the cap weighs the cap, so it is read alone.

  @enforce_keys [:cap, :held]
  defstruct [:cap, :held]

  @type t :: %__MODULE__{cap: pos_integer, held: :atomics.atomics_ref()}

  @poll_ms 2

  @spec new(pos_integer) :: t
  def new(cap), do: %__MODULE__{cap: cap, held: :atomics.new(1, [])}

  @doc "What `bytes` take of the budget."
  @spec weigh(t, non_neg_integer) :: non_neg_integer
  def weigh(%__MODULE__{cap: cap}, bytes), do: min(bytes, cap)

  @doc "Takes `weight` if it fits under the cap now."
  @spec try_acquire(t, non_neg_integer) :: :ok | :full
  def try_acquire(%__MODULE__{cap: cap, held: held} = budget, weight) do
    now = :atomics.get(held, 1)

    cond do
      now + weight > cap -> :full
      :atomics.compare_exchange(held, 1, now, now + weight) == :ok -> :ok
      true -> try_acquire(budget, weight)
    end
  end

  @doc "Waits for room until `deadline` (monotonic ms) and takes `weight`."
  @spec acquire(t, non_neg_integer, integer) :: :ok | {:error, :timeout}
  def acquire(budget, weight, deadline) do
    case try_acquire(budget, weight) do
      :ok ->
        :ok

      :full ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :timeout}
        else
          Process.sleep(@poll_ms)
          acquire(budget, weight, deadline)
        end
    end
  end

  @doc "Takes `weight` whether or not it fits: for reads whose size is known only after they arrive."
  @spec force(t, non_neg_integer) :: :ok
  def force(%__MODULE__{held: held}, weight), do: :atomics.add(held, 1, weight)

  @spec release(t, non_neg_integer) :: :ok
  def release(%__MODULE__{held: held}, weight), do: :atomics.sub(held, 1, weight)
end
