defmodule Alambique.LeaseMemoryTest do
  use Alambique.LeaseContract

  def new_store do
    {:ok, pid} = start_supervised(AlambiqueTestkit.MemoryStore)
    {AlambiqueTestkit.MemoryStore, AlambiqueTestkit.MemoryStore.config(pid)}
  end
end
