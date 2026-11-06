defmodule Alambique.Clock.System do
  @moduledoc "Wall clock."
  @behaviour Alambique.Clock

  @impl true
  def now_ms(_config), do: System.os_time(:millisecond)
end
