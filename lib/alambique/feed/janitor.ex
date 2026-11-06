defmodule Alambique.Feed.Janitor do
  @moduledoc false
  # Removes expired feed objects on a timer (design §13). Every node of an
  # instance runs one; deleting twice is harmless.

  use GenServer

  require Logger

  alias Alambique.Feed

  def start_link(config), do: GenServer.start_link(__MODULE__, config)

  @impl true
  def init(config) do
    schedule(config)
    {:ok, config}
  end

  @impl true
  def handle_info(:prune, config) do
    case Feed.prune(Feed.from_config(config), config.feed.retention_ms) do
      {:ok, _deleted} -> :ok
      {:error, reason} -> Logger.warning("Alambique could not prune the feed: #{inspect(reason)}")
    end

    schedule(config)
    {:noreply, config}
  end

  # The first run is spread over half an interval so nodes do not prune together.
  defp schedule(config) do
    interval = config.feed.prune_interval_ms
    Process.send_after(self(), :prune, div(interval, 2) + :rand.uniform(max(div(interval, 2), 1)))
  end
end
