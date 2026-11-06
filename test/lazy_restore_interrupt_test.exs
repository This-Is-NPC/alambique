defmodule Alambique.LazyRestoreInterruptTest do
  @moduledoc """
  A VM killed while a lazy restore is hydrating must leave a local file the
  next wake discards and restores again, also with a single dirty I/O
  scheduler.
  """
  use ExUnit.Case, async: false

  alias Alambique.{Cell, ClusterFuns, Config}
  alias AlambiqueTestkit.{Cluster, CrashPoints}

  @moduletag :cluster
  @moduletag timeout: 180_000
  @schema [
    {1, "CREATE TABLE log (id INTEGER PRIMARY KEY AUTOINCREMENT, n INTEGER NOT NULL, v BLOB)"}
  ]

  test "a kill on a page miss is followed by a wake with one dirty I/O scheduler" do
    {:ok, c} =
      Cluster.start_link(
        names: [:n1],
        ttl_ms: 2_000,
        skew_margin_ms: 400,
        migrations: @schema,
        peer_args: ["+SDio", "1"],
        instance_opts: [
          op_timeout_ms: 1_000,
          lazy_restore: [concurrency: 2, batch_bytes: 1_024, miss_pages: 4]
        ]
      )

    try do
      assert {:ok, 1} =
               Cluster.call(c, :n1, :erlang, :system_info, [:dirty_io_schedulers])

      inst = Cluster.instance(c)
      cell = "lazy-miss-#{System.unique_integer([:positive])}"

      assert {:ok, :ok} =
               Cluster.call(c, :n1, ClusterFuns, :lazy_updates, [inst, cell, 20, 300], 120_000)

      :ok = Cluster.call(c, :n1, Alambique, :hibernate, [inst, cell], 60_000) |> elem(1)
      {:ok, config} = Cluster.call(c, :n1, Config, :fetch!, [inst])
      {:ok, _} = Cluster.call(c, :n1, File, :rm_rf, [Cell.Local.dir(config, cell)])

      test = self()
      name = :"lazy_miss_#{System.unique_integer([:positive])}"
      :global.register_name(name, test)
      {:ok, :yes} = Cluster.call(c, :n1, :global, :register_name, [name, test])

      {:ok, :ok} =
        Cluster.call(c, :n1, CrashPoints, :arm, [
          :hydrate_before_miss,
          ClusterFuns.hold(name),
          [times: 1]
        ])

      task =
        Task.async(fn ->
          Cluster.call(c, :n1, Alambique, :query, [inst, cell, "SELECT sum(n) FROM log"], 60_000)
        end)

      assert_receive {:hydrating, _hydrator}, 30_000
      :ok = Cluster.kill(c, :n1)
      _ = Task.yield(task, 5_000)
      :ok = Cluster.restart(c, :n1)

      assert {:ok, {:ok, %{rows: [[210]]}}} =
               Cluster.call(
                 c,
                 :n1,
                 Alambique,
                 :query,
                 [inst, cell, "SELECT sum(n) FROM log"],
                 60_000
               )
    after
      Process.unlink(c)
      Cluster.stop(c)
      CrashPoints.reset()
    end
  end

  test "a kill during hydration is followed by a wake on the same and another node" do
    {:ok, c} =
      Cluster.start_link(
        names: [:n1, :n2],
        ttl_ms: 2_000,
        skew_margin_ms: 400,
        migrations: @schema,
        peer_args: ["+SDio", "1"],
        instance_opts: [
          op_timeout_ms: 1_000,
          lazy_restore: [concurrency: 2, batch_bytes: 1_024, miss_pages: 4]
        ]
      )

    try do
      assert {:ok, 1} =
               Cluster.call(c, :n1, :erlang, :system_info, [:dirty_io_schedulers])

      inst = Cluster.instance(c)
      cell = "lazy-interrupt-#{System.unique_integer([:positive])}"

      assert {:ok, :ok} =
               Cluster.call(c, :n1, ClusterFuns, :lazy_updates, [inst, cell, 20, 300], 120_000)

      :ok = Cluster.call(c, :n1, Alambique, :hibernate, [inst, cell], 60_000) |> elem(1)
      {:ok, config} = Cluster.call(c, :n1, Config, :fetch!, [inst])
      dir = Cell.Local.dir(config, cell)
      {:ok, _} = Cluster.call(c, :n1, File, :rm_rf, [dir])

      test = self()
      name = :"lazy_interrupt_#{System.unique_integer([:positive])}"
      :global.register_name(name, test)
      {:ok, :yes} = Cluster.call(c, :n1, :global, :register_name, [name, test])

      {:ok, :ok} =
        Cluster.call(c, :n1, CrashPoints, :arm, [
          :hydrate_before_fetch,
          ClusterFuns.hold(name),
          [times: 1]
        ])

      task =
        Task.async(fn ->
          Cluster.call(c, :n1, Alambique, :take_over, [inst, cell, :lazy_test], 60_000)
        end)

      assert {:ok, {:ok, _node}} = Task.await(task, 60_000)
      assert_receive {:hydrating, _hydrator}, 30_000

      # The hydrator is held during a background read: kill the VM with the
      # file half filled, no marker.
      :ok = Cluster.kill(c, :n1)
      :ok = Cluster.restart(c, :n1)

      assert {:ok, {:ok, %{rows: [[20]]}}} =
               Cluster.call(
                 c,
                 :n1,
                 Alambique,
                 :query,
                 [inst, cell, "SELECT count(*) FROM log"],
                 60_000
               )

      # Another node has no local file at all: it must return the same data.
      :ok = Cluster.kill(c, :n1)

      assert {:ok, {:ok, _node}} =
               Cluster.call(c, :n2, Alambique, :take_over, [inst, cell, :lazy_test], 60_000)

      assert {:ok, {:ok, %{rows: [[20]]}}} =
               Cluster.call(
                 c,
                 :n2,
                 Alambique,
                 :query,
                 [inst, cell, "SELECT count(*) FROM log"],
                 60_000
               )
    after
      Process.unlink(c)
      Cluster.stop(c)
      CrashPoints.reset()
    end
  end
end
