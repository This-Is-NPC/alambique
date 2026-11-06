defmodule Alambique.Clock do
  @moduledoc "Time source for leases; instances take the implementation from their config."

  @callback now_ms(config :: term) :: integer
end
