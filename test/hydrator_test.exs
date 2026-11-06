defmodule Alambique.HydratorTest do
  @moduledoc "How the hydrator of a lazy restore ends: success, retries, failure (design §16)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [eventually: 1]
  import Alambique.CompactionHelpers

  alias Alambique.Replication
  alias Alambique.Replication.{Hydrator, Lazy, Native, Retry}
  alias AlambiqueTestkit.MemoryStore

  setup do
    env = setup_world()
    on_exit(fn -> File.rm_rf(env.dir) end)
    history(env, new_owner(env, "owner.db"), 4)
    %{env: env}
  end

  # A lazy restore of the cell; small reads make the file take many requests.
  defp lazy(env) do
    path = Path.join(env.dir, "lazy-#{System.unique_integer([:positive])}.db")
    lazy_opts = %{min_bytes: 0, wait_ms: 1_000}
    {:ok, result} = Replication.restore(env.ctx, cell(), path, :latest, lazy: lazy_opts)
    Lazy.new(result, env.ctx, miss_pages: 2, batch_bytes: 256)
  end

  defp start(lazy, fetch_ms \\ 1_000) do
    {:ok, hydrator} =
      Hydrator.start_link(lazy, concurrency: 2, fetch_ms: fetch_ms, poll_ms: 10)

    hydrator
  end

  test "reads that run out of time are tried again until the file is complete", %{env: env} do
    lazy = lazy(env)
    fault = MemoryStore.inject(env.cfg, kind: :error, ops: [:get_range])
    hydrator = start(lazy, 20)

    # Several reads have run out of their budget, one after the other.
    eventually(fn -> :sys.get_state(hydrator).failures >= 3 end)
    MemoryStore.remove(env.cfg, fault)

    assert_receive {:hydrated, ^hydrator}, 5_000
  end

  test "a message it does not know changes nothing", %{env: env} do
    hydrator = start(lazy(env))
    send(hydrator, :unexpected)

    assert_receive {:hydrated, ^hydrator}, 5_000
  end

  test "an object that is gone ends it with the key, without retrying", %{env: env} do
    lazy = lazy(env)
    key = elem(lazy.files, 0)
    :ok = env.mod.delete(env.cfg, key)
    hydrator = start(lazy)

    assert_receive {:hydration_failed, ^hydrator, {:not_found, ^key}}, 5_000
    refute_received {:hydrated, ^hydrator}
  end

  test "a checksum that does not match the restored one ends it", %{env: env} do
    lazy = lazy(env)
    hydrator = start(%{lazy | checksum: lazy.checksum + 1})

    assert_receive {:hydration_failed, ^hydrator, _reason}, 5_000
    refute_received {:hydrated, ^hydrator}
  end

  test "it ends when its owner ends normally, which does not take a linked process down", %{
    env: env
  } do
    lazy = lazy(env)
    MemoryStore.inject(env.cfg, kind: :latency, ms: 60_000, ops: [:get_range])
    test = self()

    owner =
      spawn(fn ->
        send(test, {:hydrator, start(lazy)})
        receive do: (:stop -> :ok)
      end)

    assert_receive {:hydrator, hydrator}
    ref = Process.monitor(hydrator)
    send(owner, :stop)

    assert_receive {:DOWN, ^ref, :process, ^hydrator, :normal}, 5_000
  end

  describe "Lazy reads" do
    defp deadline(ms), do: Retry.deadline_in(ms)

    test "fetching a page makes it local", %{env: env} do
      lazy = lazy(env)
      {pages, before} = Native.lazy_progress(lazy.handle)
      assert before == pages

      assert :ok = Lazy.fetch_pages(lazy, [1], deadline(1_000))
      assert {^pages, left} = Native.lazy_progress(lazy.handle)
      assert left < before
    end

    test "an object shorter than the run asked for is a short read", %{env: env} do
      lazy = lazy(env)
      key = elem(lazy.files, 0)
      {:ok, body, _etag, _meta} = env.mod.get(env.cfg, key)
      {:ok, _} = env.mod.put(env.cfg, key, binary_part(body, 0, 150), :none, %{})

      assert {:error, {:short_read, ^key}} = Lazy.fetch_page(lazy, 1, deadline(1_000))
    end

    test "a missing object is reported with its key", %{env: env} do
      lazy = lazy(env)
      key = elem(lazy.files, 0)
      :ok = env.mod.delete(env.cfg, key)

      assert {:error, {:not_found, ^key}} = Lazy.fetch_page(lazy, 1, deadline(1_000))
    end

    test "a store that raises or exits is a failed read, not a crash", %{env: env} do
      lazy = lazy(env)
      fault = MemoryStore.inject(env.cfg, kind: :raise, ops: [:get_range], reason: "bang")
      assert {:error, {:error, %RuntimeError{}}} = Lazy.fetch_page(lazy, 1, deadline(1_000))

      MemoryStore.remove(env.cfg, fault)
      MemoryStore.inject(env.cfg, kind: :exit, ops: [:get_range], reason: :gone)

      assert {:error, {:exit, {:memory_store_fault, :gone}}} =
               Lazy.fetch_page(lazy, 1, deadline(1_000))
    end

    test "fetching pages returns the first failure", %{env: env} do
      lazy = lazy(env)
      MemoryStore.inject(env.cfg, kind: :error, ops: [:get_range], reason: :down)

      assert {:error, _reason} = Lazy.fetch_pages(lazy, [1, 2, 3], deadline(100))
    end
  end
end
