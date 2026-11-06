defmodule Alambique.Exclusive do
  @moduledoc false
  # The lease that an operation which replaces a cell's state takes under its
  # own owner name (`Relocate`, `Rewind`; design §18, §20). A cell that is
  # awake keeps its lease until it hibernates, so the owner is asked to; a
  # dead owner's lease expires by itself. When this returns, no process serves
  # the cell any more.

  alias Alambique.{Config, Lease, Router}

  @spec take(Config.t(), String.t(), Lease.ctx(), String.t(), pos_integer, integer) ::
          {:ok, Lease.t()} | {:moved, String.t()} | {:error, term}
  def take(config, cell, ctx, owner, poll_ms, deadline) do
    case Lease.acquire(ctx, cell, owner) do
      {:ok, lease} ->
        {:ok, lease}

      {:error, {:moved, id}} ->
        {:moved, id}

      {:error, {:owned, held_by, _} = reason} ->
        cond do
          held_by == owner -> reclaim(config, cell, ctx, owner, poll_ms, deadline)
          past?(deadline) -> {:error, reason}
          true -> wait_for_owner(config, cell, ctx, owner, poll_ms, deadline)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # An operation that died keeps the lease for its ttl; releasing it on its
  # behalf is safe because what the operation does at its end is conditional
  # on the lease or on the store's chain.
  defp reclaim(config, cell, ctx, owner, poll_ms, deadline) do
    with {:ok, stale} <- Lease.read(ctx, cell),
         :ok <- Lease.release(ctx, stale) do
      take(config, cell, ctx, owner, poll_ms, deadline)
    else
      :none -> take(config, cell, ctx, owner, poll_ms, deadline)
      {:error, _} = error -> error
    end
  end

  # The owner releases its lease when it hibernates, and any call may wake the
  # cell again at once: take the lease right away, and wait only when the
  # owner did not answer.
  defp wait_for_owner(config, cell, ctx, owner, poll_ms, deadline) do
    if Router.call(config.name, cell, :hibernate, 2_000) != :ok, do: Process.sleep(poll_ms)
    take(config, cell, ctx, owner, poll_ms, deadline)
  end

  defp past?(deadline), do: System.monotonic_time(:millisecond) >= deadline
end
