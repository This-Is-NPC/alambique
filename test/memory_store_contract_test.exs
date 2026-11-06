defmodule AlambiqueTestkit.MemoryStoreContractTest do
  use Alambique.StoreContract, store: AlambiqueTestkit.MemoryStore

  def new_config do
    {:ok, pid} = start_supervised(AlambiqueTestkit.MemoryStore)
    AlambiqueTestkit.MemoryStore.config(pid)
  end
end
