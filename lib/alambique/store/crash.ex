defmodule Alambique.Store.Crash do
  @moduledoc false
  # What a crash of a store call says (design §3): whether it was the node's
  # own wait, and a short bounded summary for telemetry and the log.

  # Finch reraises the timeout of its pool's checkout with this message.
  @finch_checkout "Finch was unable to provide a connection"

  @doc """
  Whether a crash of `kind` with `reason` happened before the request left: the
  wait for a connection of a `NimblePool` pool gave up (as the exit of
  `NimblePool.checkout!/4` or as Finch reraises it), or the pool was not there.
  """
  @spec local?(atom, term) :: boolean
  def local?(:exit, {cause, {NimblePool, :checkout, _}}) when cause in [:timeout, :noproc],
    do: true

  def local?(:error, %RuntimeError{message: @finch_checkout <> _}), do: true

  def local?(_kind, _reason), do: false

  @doc """
  `reason` in at most `limit` characters: `:pool_checkout_timeout` or
  `:pool_checkout_noproc` for those two exits, else `inspect/2` of it cut to
  `limit`, a string. Finch's own error for the timeout is `:pool_checkout_timeout`.
  """
  @spec summary(atom, term, pos_integer) :: atom | String.t()
  def summary(:exit, {:timeout, {NimblePool, :checkout, _}}, _limit), do: :pool_checkout_timeout
  def summary(:exit, {:noproc, {NimblePool, :checkout, _}}, _limit), do: :pool_checkout_noproc

  def summary(:error, %RuntimeError{message: @finch_checkout <> _}, _limit),
    do: :pool_checkout_timeout

  def summary(_kind, reason, limit) do
    reason
    |> inspect(limit: 10, printable_limit: limit, pretty: false)
    |> String.slice(0, limit)
  end
end
