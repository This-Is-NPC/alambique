defmodule Alambique.PageSidecarRestoreTest do
  @moduledoc """
  A lazy restore trusts the pages of a file whose page-hash sidecar it read,
  and reads the other files whole (design §16). A bad sidecar is never an
  outage: the file is then checked as if it had none.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers, only: [capture: 1, await_event: 3]
  import Alambique.CompactionHelpers

  alias Alambique.CountingStore
  alias Alambique.Replication
  alias Alambique.Replication.{Hydrator, Keys, Lazy, Native}
  alias AlambiqueTestkit.HostileStore

  @files 4

  setup do
    capture([[:alambique, :replication, :sidecar], [:alambique, :replication, :hydrate]])
    env = setup_world()
    on_exit(fn -> File.rm_rf(env.dir) end)

    # Four segments, each with a sidecar.
    writer = %{env | ctx: Map.put(env.ctx, :page_sidecar, 0)}
    history(writer, new_owner(writer, "owner.db"), @files)

    hostile = HostileStore.config(env.cfg)
    counted = CountingStore.wrap({HostileStore, hostile})
    {_, counter} = counted
    %{env: %{env | ctx: %{env.ctx | store: counted}}, hostile: hostile, counter: counter}
  end

  defp key(env, txid), do: Keys.segment(env.ctx.prefix, cell(), 0, txid, txid)

  # A lazy restore of the cell, with small reads.
  defp restore(env, opts \\ []) do
    path = Path.join(env.dir, "lazy-#{System.unique_integer([:positive])}.db")
    ctx = Keyword.get(opts, :ctx, env.ctx)
    lazy_opts = %{min_bytes: 0, wait_ms: 1_000, verify_bytes: 0}
    {:ok, result} = Replication.restore(ctx, cell(), path, :latest, lazy: lazy_opts)
    lazy = Lazy.new(result, ctx, miss_pages: 2, batch_bytes: 256, verify_bytes: 0)
    {result, lazy}
  end

  defp hydrate(lazy) do
    {:ok, hydrator} = Hydrator.start_link(lazy, concurrency: 2, fetch_ms: 1_000, poll_ms: 10)

    receive do
      {:hydrated, ^hydrator} -> :ok
      {:hydration_failed, ^hydrator, reason} -> {:error, reason}
    after
      10_000 -> flunk("the hydration did not end")
    end
  end

  defp whole_reads(counter),
    do: counter |> CountingStore.keys(:get) |> Enum.filter(&String.ends_with?(&1, ".ltx"))

  defp sidecar_events do
    for {:telemetry, [:alambique, :replication, :sidecar], _, meta} <- drain_all(), do: meta
  end

  defp drain_all do
    receive do
      {:telemetry, [:alambique, :replication, :sidecar], _, _} = event -> [event | drain_all()]
    after
      0 -> []
    end
  end

  defp put_sidecar(env, txid, body) do
    {:ok, _} = env.mod.put(env.cfg, Keys.sidecar(key(env, txid)), body, :none, %{})
  end

  defp sidecar(env, txid) do
    {:ok, body, _etag, _meta} = env.mod.get(env.cfg, Keys.sidecar(key(env, txid)))
    body
  end

  test "files with a sidecar are not read whole, and the hydration still ends", %{env: env} = t do
    {_result, lazy} = restore(env)
    assert Native.lazy_covered(lazy.handle) == Enum.to_list(0..(@files - 1))

    assert :ok = hydrate(lazy)
    assert whole_reads(t.counter) == []

    assert {%{check_covered: @files, check_objects: 0}, %{result: :ok}} =
             await_event([:alambique, :replication, :hydrate], cell(), 5_000)
  end

  test "a file without a sidecar is read whole and the others are not", %{env: env} = t do
    :ok = env.mod.delete(env.cfg, Keys.sidecar(key(env, 2)))
    {_result, lazy} = restore(env)
    assert Native.lazy_covered(lazy.handle) == [0, 2, 3]

    assert :ok = hydrate(lazy)
    assert whole_reads(t.counter) == [key(env, 2)]
  end

  test "an object that never had a sidecar is checked whole", %{env: env} = t do
    for txid <- 1..@files, do: :ok = env.mod.delete(env.cfg, Keys.sidecar(key(env, txid)))
    {_result, lazy} = restore(env)
    assert Native.lazy_covered(lazy.handle) == []

    assert :ok = hydrate(lazy)
    assert Enum.sort(whole_reads(t.counter)) == Enum.map(1..@files, &key(env, &1))
  end

  describe "a bad sidecar falls back to the whole object" do
    for {name, damage} <- [
          {"with a flipped byte", {:flip, 70}},
          {"cut short", :cut},
          {"with a page missing", :drop_entry},
          {"with an extra byte", :extra}
        ] do
      test name, %{env: env} = t do
        side = sidecar(env, 1)
        put_sidecar(env, 1, damaged(side, unquote(Macro.escape(damage))))
        {_result, lazy} = restore(env)

        assert Native.lazy_covered(lazy.handle) == [1, 2, 3]
        assert [%{result: :rejected, key: key}] = sidecar_events()
        assert key == key(env, 1)

        assert :ok = hydrate(lazy)
        assert whole_reads(t.counter) == [key(env, 1)]
      end
    end

    test "that describes another file", %{env: env} = t do
      put_sidecar(env, 1, sidecar(env, 2))
      {_result, lazy} = restore(env)

      assert Native.lazy_covered(lazy.handle) == [1, 2, 3]
      assert [%{result: :rejected}] = sidecar_events()
      assert :ok = hydrate(lazy)
      assert whole_reads(t.counter) == [key(env, 1)]
    end

    defp damaged(side, {:flip, at}) do
      <<head::binary-size(^at), byte, rest::binary>> = side
      <<head::binary, Bitwise.bxor(byte, 1), rest::binary>>
    end

    defp damaged(side, :cut), do: binary_part(side, 0, byte_size(side) - 1)
    defp damaged(side, :extra), do: side <> <<0>>

    defp damaged(side, :drop_entry) do
      size = byte_size(side)
      binary_part(side, 0, size - 36) <> binary_part(side, size - 16, 16)
    end
  end

  test "a sidecar that is intact and wrong is dropped and the cell hydrates", %{env: env} = t do
    put_sidecar(env, 1, Native.forge_sidecar(sidecar(env, 1)))
    {_result, lazy} = restore(env)
    assert Native.lazy_covered(lazy.handle) == [0, 1, 2, 3]

    assert :ok = hydrate(lazy)
    assert [%{result: :mismatch, key: key}] = sidecar_events()
    assert key == key(env, 1)
    assert Native.lazy_covered(lazy.handle) == [1, 2, 3]
    # The object that proved it was read whole, and so was its check.
    assert whole_reads(t.counter) == [key(env, 1), key(env, 1)]
  end

  test "hashes that do not fit the budget are not used", %{env: env} do
    ctx = %{env.ctx | max_inflight_bytes: 400}
    {_result, lazy} = restore(env, ctx: ctx)

    assert Native.lazy_covered(lazy.handle) == []
    assert Enum.all?(sidecar_events(), &(&1.result == :budget))
    assert lazy.hash_bytes == 0
  end

  test "the hashes of a restore are counted against the budget of its hydration", %{env: env} do
    {result, lazy} = restore(env)
    sizes = for txid <- 1..@files, do: byte_size(sidecar(env, txid))

    assert result.hash_bytes == Enum.sum(sizes)
    assert lazy.hash_bytes == result.hash_bytes
    assert :atomics.get(lazy.budget.held, 1) == result.hash_bytes
  end

  describe "a damaged ranged read" do
    for kind <- [:codeword, {:flip, 30}, {:flip, 60}, :shifted] do
      test "#{inspect(kind)} is refused before any page of the run is installed", %{
        env: env,
        hostile: hostile
      } do
        {_result, lazy} = restore(env)
        {_total, unknown} = Native.lazy_progress(lazy.handle)
        run = Native.lazy_next_batch(lazy.handle, 1_000_000)
        assert {_file, _offset, _len, [_, _ | _]} = run

        HostileStore.damage(hostile, unquote(Macro.escape(kind)), 1)
        deadline = System.monotonic_time(:millisecond) + 2_000

        assert {:error, {reason, _}} = Lazy.fetch_run(lazy, run, deadline)
        assert reason in [:bad_range, :invalid, :changed, :short_read]
        assert HostileStore.damaged(hostile) == 1

        # Nothing of the run was installed.
        assert {_, ^unknown} = Native.lazy_progress(lazy.handle)
      end
    end
  end

  test "a damaged read whose object fails its checksum fails as without a sidecar", %{env: env} do
    key = key(env, 1)
    {:ok, object, _etag, meta} = env.mod.get(env.cfg, key)
    # The object itself is damaged, in a page: the sidecar is the good one.
    damaged = Alambique.LtxTamper.alter_frames(object, :all)
    {:ok, _} = env.mod.put(env.cfg, key, damaged, :none, meta)

    {_result, lazy} = restore(env)
    assert {:error, {:invalid, _}} = hydrate(lazy)
  end
end
