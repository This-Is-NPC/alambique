defmodule Alambique.ReadCapsTest do
  @moduledoc """
  No read of the bucket is unbounded (design §3): an object larger than its
  cap is refused before it is held in memory, with `{:too_large, size, max}`.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.ReplicationHelpers

  alias Alambique.{CountingStore, Lease, Nodes, Replication, Store}
  alias Alambique.Replication.Objects
  alias Alambique.Store.S3
  alias AlambiqueTestkit.MemoryStore

  defp deadline, do: System.monotonic_time(:millisecond) + 2_000

  defp seed(sizes) do
    {_pid, {_, cfg} = store} = start_store()
    dir = tmp_dir()
    ctx = ctx(store, new_world(), timeout_ms: 2_000)
    {conn, state} = new_replicator(dir, "src.db")

    Enum.reduce(Enum.with_index(sizes, 1), state, fn {n, k}, state ->
      run_op(conn, {:put, k, n})
      {:ok, state} = Replication.Replicator.sync(state, ctx)
      state
    end)

    {conn, dir, ctx, cfg}
  end

  describe "Objects.fetch/4" do
    property "returns an object exactly when it is within the cap" do
      {:ok, pid} = MemoryStore.start_link()
      store = {MemoryStore, MemoryStore.config(pid)}
      ctx = ctx(store, new_world())

      # A size one under, at and one over the cap is as likely as any other.
      check all(
              cap <- integer(1..300),
              size <- one_of([integer(0..300), map(member_of([-1, 0, 1]), &(cap + &1))])
            ) do
        key = "p/#{size}"
        {m, c} = store
        {:ok, _} = m.put(c, key, :binary.copy("x", size), :none, %{})
        result = Objects.fetch(%{ctx | max_object_bytes: cap}, key, deadline())

        if size <= cap do
          assert {:ok, body, _} = result
          assert byte_size(body) == size
        else
          assert result == {:error, {:too_large, size, cap}}
        end
      end
    end

    test "a missing object is still missing, and the control cap is its own" do
      {_pid, store} = start_store()
      ctx = %{ctx(store, new_world()) | max_object_bytes: 10, max_control_bytes: 5}
      {m, c} = store
      {:ok, _} = m.put(c, "k", "123456789", :none, %{})

      assert Objects.fetch_optional(ctx, "none", deadline()) == :none
      assert {:ok, "123456789", _} = Objects.fetch_optional(ctx, "k", deadline())

      assert Objects.fetch_optional(ctx, "k", deadline(), :control) ==
               {:error, {:too_large, 9, 5}}
    end
  end

  describe "restore" do
    test "refuses a file larger than the cap without reading it" do
      {conn, dir, ctx, cfg} = seed([300_000])
      counting = CountingStore.wrap({MemoryStore, cfg})
      {_, counts} = counting
      ctx = Map.merge(ctx, %{store: counting, max_object_bytes: 100_000, edge_bytes: 4096})

      assert {:error, reason} = Replication.restore(ctx, "cell", Path.join(dir, "d.db"), :latest)
      assert inspect(reason) =~ "too_large"
      assert CountingStore.keys(counts, :get) == []
      Alambique.Sqlite.close(conn)
    end

    test "refuses a page index larger than the cap" do
      {conn, dir, ctx, _cfg} = seed([200_000])
      ctx = Map.merge(ctx, %{max_object_bytes: 300, edge_bytes: 256})

      assert {:error, reason} =
               Replication.restore(ctx, "cell", Path.join(dir, "d.db"), :latest,
                 lazy: %{min_bytes: 0, wait_ms: 1_000}
               )

      assert inspect(reason) =~ "too_large"
      Alambique.Sqlite.close(conn)
    end

    test "restores the same chain under a cap that fits it" do
      {conn, dir, ctx, _cfg} = seed(List.duplicate(20_000, 3))
      ctx = Map.merge(ctx, %{max_object_bytes: 1_000_000, edge_bytes: 4096})
      dest = Path.join(dir, "d.db")

      assert {:ok, %{txid: 3}} = Replication.restore(ctx, "cell", dest, :latest)
      assert dump_file(dest) == dump(conn)
    end
  end

  describe "writes" do
    test "a segment larger than the cap is not uploaded" do
      {_pid, {_, cfg} = store} = start_store()
      dir = tmp_dir()
      ctx = %{ctx(store, new_world(), timeout_ms: 2_000) | max_object_bytes: 1_000}
      {conn, state} = new_replicator(dir, "src.db")
      run_op(conn, {:put, 1, 20_000})

      assert {:error, reason} = Replication.Replicator.sync(state, ctx)
      assert inspect(reason) =~ "too_large"
      assert {:ok, [], nil} = MemoryStore.list(cfg, "repl-test/cells/", nil, 10)
    end
  end

  describe "control objects" do
    defp lease_ctx(store, cap) do
      {m, c} = store
      {:ok, _} = m.put(c, "p/cells/a/lease", String.duplicate("x", 500), :none, %{})

      Lease.ctx(
        store: store,
        clock: {Alambique.Clock.System, nil},
        prefix: "p",
        ttl_ms: 1_000,
        skew_margin_ms: 10,
        max_control_bytes: cap
      )
    end

    test "a lease larger than the cap is refused" do
      {_pid, store} = start_store()

      assert {:error, {:too_large, 500, 100}} = Lease.acquire(lease_ctx(store, 100), "a", "me")
    end

    test "a lease within the cap is read (and found corrupt, not too large)" do
      {_pid, store} = start_store()

      refute match?(
               {:error, {:too_large, _, _}},
               Lease.acquire(lease_ctx(store, 1_000), "a", "me")
             )
    end

    test "a node entry larger than the cap is refused" do
      {_pid, {m, c} = store} = start_store()
      {:ok, _} = m.put(c, "p/nodes/n1", String.duplicate("x", 500), :none, %{})

      ctx =
        Nodes.ctx(
          store: store,
          clock: {Alambique.Clock.System, nil},
          prefix: "p",
          ttl_ms: 1_000,
          skew_margin_ms: 10,
          max_control_bytes: 100
        )

      assert {:error, {:too_large, 500, 100}} = Nodes.register(ctx, "n1", "inc", "adv")
    end
  end

  describe "Store.get_capped/3" do
    test "emits a telemetry event when the object is too large" do
      {_pid, {m, c} = store} = start_store()
      {:ok, _} = m.put(c, "big", "123456", :none, %{})
      id = {__MODULE__, make_ref()}
      test = self()

      :telemetry.attach(
        id,
        [:alambique, :store, :too_large],
        fn _, measurements, meta, _ -> send(test, {:too_large, measurements, meta}) end,
        nil
      )

      assert Store.get_capped(store, "big", 5) == {:error, {:too_large, 6, 5}}
      assert_receive {:too_large, %{size: 6, max: 5}, %{key: "big"}}
      :telemetry.detach(id)
    end
  end

  describe "S3 store" do
    # Answers a GET with the headers of a huge object and then a trickle.
    defp huge_server(declared) do
      {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listener)
      test = self()

      pid =
        spawn_link(fn ->
          {:ok, socket} = :gen_tcp.accept(listener)
          {:ok, _request} = :gen_tcp.recv(socket, 0, 5_000)

          :ok =
            :gen_tcp.send(
              socket,
              "HTTP/1.1 200 OK\r\ncontent-length: #{declared}\r\netag: \"e\"\r\n\r\n"
            )

          :ok = :gen_tcp.send(socket, String.duplicate("x", 1_000))
          send(test, :sent)

          receive do
            :close -> :gen_tcp.close(socket)
          end
        end)

      {listener, pid, port}
    end

    test "get/3 stops at a declared size over the cap, before the body arrives" do
      {listener, pid, port} = huge_server(10_000_000_000)

      config = %{
        endpoint: "http://127.0.0.1:#{port}",
        bucket: "b",
        access_key_id: "id",
        secret_access_key: "secret",
        receive_timeout: 5_000
      }

      assert {:error, {:too_large, 10_000_000_000, 4096}} = S3.get(config, "k", 4096)
      assert_received :sent
      send(pid, :close)
      :gen_tcp.close(listener)
    end

    test "get/3 returns an object within the cap and refuses one past it" do
      options = Alambique.StubAdapter.install(fn _ -> {200, "12345"} end)

      config = %{
        endpoint: "http://s3.test",
        bucket: "b",
        access_key_id: "id",
        secret_access_key: "secret",
        req_options: options
      }

      assert {:ok, "12345", _, _} = S3.get(config, "k", 5)
      assert {:error, {:too_large, 5, 4}} = S3.get(config, "k", 4)
    end
  end
end
