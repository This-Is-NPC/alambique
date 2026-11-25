defmodule Alambique.LazyRestoreUntrustedReadsTest do
  @moduledoc """
  What a lazy restore refuses from its ranged reads (design §16). The frames
  carry no checksum of their own, and the database checksum is a XOR of
  linear CRCs, so errors that cancel in it pass it. A read is refused on
  arrival when the object is not the one that was staged (ETag, size, epoch)
  or the frame is malformed; page content is compared with the whole objects
  at the end of the hydration.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias Alambique.Tx
  alias AlambiqueTestkit.{CrashPoints, HostileStore, MemoryStore}

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v BLOB)"}]
  # One page per read, and no file is read whole: every page stays unchecked
  # until the end.
  @lazy [min_bytes: 0, concurrency: 2, batch_bytes: 1_024, miss_pages: 1, verify_bytes: 0]
  @all "SELECT n, v FROM t ORDER BY n"
  @row 150
  @one "SELECT n, v FROM t WHERE n = #{@row}"
  @append "UPDATE t SET v = CAST(v || 'x' AS BLOB) WHERE n = #{@row}"

  defmodule PlainStore do
    @moduledoc false
    # A memory store without `get_range/5`: the library can only compare the
    # ETag in the metadata of the read.
    @behaviour Alambique.Store

    alias AlambiqueTestkit.MemoryStore

    @impl true
    def location(inner), do: inner

    @impl true
    def put(inner, key, body, condition, meta),
      do: MemoryStore.put(inner, key, body, condition, meta)

    @impl true
    def get(inner, key), do: MemoryStore.get(inner, key)

    @impl true
    def head(inner, key), do: MemoryStore.head(inner, key)

    @impl true
    def get_range(inner, key, offset, length),
      do: MemoryStore.get_range(inner, key, offset, length)

    @impl true
    def list(inner, prefix, start_after, limit),
      do: MemoryStore.list(inner, prefix, start_after, limit)

    @impl true
    def delete(inner, key), do: MemoryStore.delete(inner, key)
  end

  setup do
    capture([[:alambique, :replication, :hydrate]])
    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  # 300 rows of 1 KB values (random, or all zeros: the pages of a table of
  # zeros compress to one length), hibernated, so that the state is in the
  # store only. Returns the cell and its rows.
  defp seed(ctx, value \\ "randomblob(1024)") do
    seeder = start_instance(ctx, migrations: @schema, lazy_restore: false)
    cell = unique_cell()

    {:ok, :ok} =
      Alambique.transaction(seeder, cell, fn tx ->
        for n <- 1..300 do
          {:ok, _} = Tx.execute(tx, "INSERT INTO t VALUES (?, #{value})", [n])
        end

        :ok
      end)

    # The feed keeps the rows until they are published and the next write
    # drops them: a cell that wakes with them would read all of them.
    eventually(fn ->
      {:ok, _} = Alambique.execute(seeder, cell, "DELETE FROM t WHERE n = 0")

      match?(
        {:ok, %{rows: [[0]]}},
        Alambique.query(seeder, cell, "SELECT count(*) FROM _alambique_feed WHERE op <> 'floor'")
      )
    end)

    {:ok, %{rows: rows}} = Alambique.query(seeder, cell, @all)
    :ok = Alambique.hibernate(seeder, cell)
    {cell, rows}
  end

  # A lazy instance over `store`, with the background reads held. The cell is
  # awake when this returns.
  defp wake(ctx, cell, store, lazy \\ @lazy) do
    release = hold_hydration(cell)
    inst = start_instance(%{ctx | store: store}, migrations: @schema, lazy_restore: lazy)
    assert {:ok, _node} = Alambique.touch(inst, cell)
    {inst, release}
  end

  defp hostile_wake(ctx, cell, lazy \\ @lazy) do
    hostile = HostileStore.config(ctx.store_config)
    {inst, release} = wake(ctx, cell, {HostileStore, hostile}, lazy)
    {inst, hostile, release}
  end

  # What a new owner reads from the store: no lazy restore, so nothing of the
  # first owner's memory.
  defp reread(ctx, cell) do
    fresh = start_instance(ctx, migrations: @schema, lazy_restore: false)

    eventually(fn ->
      with {:ok, %{rows: rows}} <- Alambique.query(fresh, cell, @all), do: rows
    end)
  end

  # A write waits for the end of the hydration (the write gate); the damaged
  # pages must not let it through.
  defp refused_write(inst, cell, release) do
    config = Alambique.Config.fetch!(inst)
    write = Task.async(fn -> Alambique.execute(inst, cell, @append) end)
    pid = Alambique.Cell.whereis(config, cell)
    eventually(fn -> length(:sys.get_state(pid).parked) == 1 end)

    release.()
    {_, meta} = await_event([:alambique, :replication, :hydrate], cell, 30_000)

    assert meta.result == :error
    assert {:error, {:hydration_failed, _}} = Task.await(write, 30_000)
    meta
  end

  # Statements that miss pages until `count` reads were damaged.
  defp read_until_damaged(inst, cell, hostile, count) do
    eventually(fn ->
      _unchecked = Alambique.query(inst, cell, @all)
      HostileStore.damaged(hostile) >= count
    end)
  end

  describe "errors that cancel in the database checksum" do
    test "errors A, B and A xor B on three pages fail the hydration", ctx do
      {cell, rows} = seed(ctx)
      {inst, hostile, release} = hostile_wake(ctx, cell)

      # The last byte of the first three reads that statements miss: their
      # CRCs differ from the store's by amounts that add up to nothing.
      HostileStore.damage(hostile, {:flip, 99, [0x01, 0x02, 0x03]}, 3)
      read_until_damaged(inst, cell, hostile, 3)

      refused_write(inst, cell, release)
      assert reread(ctx, cell) == rows
    end

    test "an error that is a multiple of the CRC polynomial fails the hydration", ctx do
      {cell, rows} = seed(ctx)
      {inst, hostile, release} = hostile_wake(ctx, cell)

      # The root and the first leaf are local now: the next read is a leaf
      # whose last bytes are the random value of a row.
      {:ok, _} = Alambique.query(inst, cell, "SELECT n FROM t WHERE n = 1")
      HostileStore.damage(hostile, :codeword, 1)
      _unchecked = Alambique.query(inst, cell, @one)
      assert HostileStore.damaged(hostile) == 1

      refused_write(inst, cell, release)
      assert reread(ctx, cell) == rows
    end

    test "two pages that swap contents fail the hydration", ctx do
      {cell, rows} = seed(ctx, "zeroblob(1024)")
      {inst, hostile, release} = hostile_wake(ctx, cell, Keyword.put(@lazy, :miss_pages, 8))

      # Two adjacent frames of a read that have the same length (pages of rows
      # of the same size do).
      HostileStore.damage(hostile, :swap, 1_000)
      read_until_damaged(inst, cell, hostile, 1)
      HostileStore.heal(hostile)

      refused_write(inst, cell, release)
      assert reread(ctx, cell) == rows
    end
  end

  describe "a read of an object that is not the one that was staged" do
    # The same bytes under the same key: another ETag.
    defp overwrite(ctx) do
      {:ok, keys, nil} = MemoryStore.list(ctx.store_config, "", nil, 10_000)

      for key <- keys, String.ends_with?(key, ".ltx") do
        {:ok, body, _etag, meta} = MemoryStore.get(ctx.store_config, key)
        {:ok, _} = MemoryStore.put(ctx.store_config, key, body, :none, meta)
      end
    end

    defp refused_by_the_pin(ctx, store) do
      {cell, _rows} = seed(ctx)
      {inst, release} = wake(ctx, cell, store)
      overwrite(ctx)

      assert {:error, _} = Alambique.query(inst, cell, @one)

      release.()
      {_, meta} = await_event([:alambique, :replication, :hydrate], cell, 30_000)
      assert meta.result == :error
      assert {:changed, key} = meta.reason
      assert String.ends_with?(key, ".ltx")
    end

    test "an object overwritten under the same key is refused (If-Match)", ctx do
      refused_by_the_pin(ctx, ctx.store)
    end

    test "a store without If-Match is pinned by the ETag of the read", ctx do
      refused_by_the_pin(ctx, {PlainStore, ctx.store_config})
    end

    for kind <- [:epoch, :total, :flags] do
      test "a read that answers another #{kind} fails the statement, not the hydration", ctx do
        {cell, rows} = seed(ctx)
        {inst, hostile, release} = hostile_wake(ctx, cell)

        HostileStore.damage(hostile, unquote(kind), 1)
        assert {:error, _} = Alambique.query(inst, cell, @one)
        assert HostileStore.damaged(hostile) == 1

        # Nothing of it was installed: the page is read again.
        release.()
        assert {_, %{result: :ok}} = await_event([:alambique, :replication, :hydrate], cell)
        assert {:ok, %{rows: ^rows}} = Alambique.query(inst, cell, @all)
      end
    end
  end

  test "the end of the hydration reports what the comparison with the objects read", ctx do
    {cell, _rows} = seed(ctx)
    {inst, release} = wake(ctx, cell, ctx.store)
    assert {:ok, _} = Alambique.query(inst, cell, @one)

    release.()
    {measurements, meta} = await_event([:alambique, :replication, :hydrate], cell, 30_000)

    assert meta.result == :ok
    assert measurements.check_objects >= 1
    assert measurements.check_bytes > 0
    assert measurements.check_duration > 0
    assert {:ok, _} = Alambique.query(inst, cell, @one)
  end
end
