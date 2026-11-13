defmodule Alambique.OpsTest do
  @moduledoc "The mix tasks and the options they read (design §10)."
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers, only: [start_instance: 2, setup_store: 0, eventually: 1]
  import Alambique.CompactionHelpers

  alias Alambique.Compaction
  alias Alambique.Ops.Options
  alias AlambiqueTestkit.MemoryStore

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY, v TEXT)"}]

  setup do
    env = setup_world(prefix: "ops-test")
    shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    old = Application.get_env(:alambique, :ops)
    Application.put_env(:alambique, :ops, store: env.store, prefix: "ops-test")

    on_exit(fn ->
      Mix.shell(shell)

      if old,
        do: Application.put_env(:alambique, :ops, old),
        else: Application.delete_env(:alambique, :ops)

      File.rm_rf(env.dir)
    end)

    env
  end

  defp run(task, args) do
    Mix.Task.rerun(task, args)
    drain([])
  end

  defp drain(acc) do
    receive do
      {:mix_shell, :info, [line]} -> drain([line | acc])
    after
      0 -> acc |> Enum.reverse() |> Enum.join("\n")
    end
  end

  describe "alambique.inspect" do
    test "describes a cell with segments, a snapshot and sizes", env do
      history(env, new_owner(env, "a.db"), 10)

      {:ok, _} =
        Compaction.run(env.ctx, cell(), fanout: 2, max_level: 1, snapshot_interval_txids: 10)

      out = run("alambique.inspect", [cell()])
      assert out =~ "Cell:       cell"
      assert out =~ "Prefix:     ops-test"
      assert out =~ "Frozen:     no"
      assert out =~ "Lease:      none"
      assert out =~ "Head:       TXID 10, epoch 1, page size 4096"
      assert out =~ ~r/level 0: 10 files, TXID 1-10, [\d.]+ (KiB|B)/
      assert out =~ ~r/level 1: 5 files, TXID 1-10, /
      assert out =~ ~r/TXID 10, [\d.]+ KiB, epoch 1, state of 2023-/
    end

    test "--json prints a decodable object and --no-sizes skips the sizes", env do
      history(env, new_owner(env, "a.db"), 3)

      assert %{"cell" => "cell", "frozen" => false, "head" => %{"txid" => 3}, "levels" => [level]} =
               "alambique.inspect" |> run([cell(), "--json", "--no-sizes"]) |> Jason.decode!()

      assert level == %{"level" => 0, "files" => 3, "min" => 1, "max" => 3, "bytes" => false}
    end

    test "an unknown cell is empty", env do
      out = run("alambique.inspect", ["nobody"])
      assert out =~ "Head:       empty"
      assert out =~ "Segments:\n  none"
      assert out =~ "Snapshots:\n  none"
      _ = env
    end

    test "shows the lease of a cell that was used and its freeze marker" do
      ctx = setup_store()
      inst = start_instance(ctx, migrations: @schema, prefix: "ops-test")
      Application.put_env(:alambique, :ops, store: ctx.store, prefix: "ops-test")
      {:ok, _} = Alambique.execute(inst, "c1", "INSERT INTO t (n, v) VALUES (1, 'x')")

      assert run("alambique.inspect", ["c1"]) =~
               ~r/Lease:      epoch 1, owner instance_\d+, held, expires in/

      :ok = Alambique.hibernate(inst, "c1")
      assert run("alambique.inspect", ["c1"]) =~ ~r/epoch 1, owner instance_\d+, released/

      run("alambique.freeze", ["c1"])
      assert run("alambique.inspect", ["c1"]) =~ "Frozen:     yes"
    end

    test "usage errors" do
      assert_raise Mix.Error, ~r/usage: mix alambique.inspect/, fn ->
        run("alambique.inspect", [])
      end
    end
  end

  describe "store failures" do
    setup env do
      Application.put_env(:alambique, :ops, store: env.store, prefix: "ops-test", timeout_ms: 200)
      :ok
    end

    test "freeze, unfreeze and inspect report the store error and raise", env do
      MemoryStore.inject(env.cfg, kind: :error, times: 1_000)

      for {task, message} <- [
            {"freeze", ~r/could not freeze c1: /},
            {"unfreeze", ~r/could not unfreeze c1: /},
            {"inspect", ~r/could not read c1: /}
          ] do
        assert_raise Mix.Error, message, fn -> run("alambique." <> task, ["c1"]) end
      end
    end

    test "a failed freeze leaves the cell unfrozen", env do
      MemoryStore.inject(env.cfg, kind: :error, times: 1_000, ops: [:put])
      assert_raise Mix.Error, fn -> run("alambique.freeze", ["c1"]) end
      MemoryStore.clear(env.cfg)
      assert {:ok, false} = Alambique.Freeze.frozen?(env.ctx, "c1")
    end
  end

  describe "alambique.inspect output" do
    test "counts the crashes recorded for the cell", env do
      settings = %{threshold: 5, window_ms: 60_000}
      {:ok, _} = Alambique.Quarantine.record(env.ctx, "c1", settings, ["cell"])
      {:ok, _} = Alambique.Quarantine.record(env.ctx, "c1", settings, ["cell"])
      assert run("alambique.inspect", ["c1"]) =~ "Crashes:    2 recorded, 2 in the window"
      assert run("alambique.inspect", ["nobody"]) =~ "Crashes:    none"
    end

    test "sizes use the unit that fits", env do
      owner = new_owner(env, "a.db")
      write!(env, owner, [{:put, 1, 2_500_000}])
      assert run("alambique.inspect", [cell()]) =~ ~r/level 0: 1 file, TXID 1-1, 2\.4 MiB/
    end

    test "a count of one is singular and more are plural", env do
      owner = new_owner(env, "a.db")
      owner = write!(env, owner, [{:put, 1, 100}])
      assert run("alambique.inspect", [cell()]) =~ ~r/level 0: 1 file, TXID 1-1/
      write!(env, owner, [{:put, 2, 100}])
      assert run("alambique.inspect", [cell()]) =~ ~r/level 0: 2 files, TXID 1-2/
    end
  end

  describe "Ops.describe/3 with objects it cannot read" do
    alias Alambique.Ops
    alias Alambique.Replication.Keys

    setup env do
      history(env, new_owner(env, "a.db"), 4)

      {:ok, _} =
        Compaction.run(env.ctx, cell(), fanout: 2, max_level: 1, snapshot_interval_txids: 4)

      :ok
    end

    defp fail_gets(env, key),
      do: MemoryStore.inject(env.cfg, kind: :error, ops: [:get], key: exactly(key), times: 1_000)

    defp exactly(key), do: Regex.compile!("\\A" <> Regex.escape(key) <> "\\z")

    defp level_bytes(info, level), do: Enum.find(info.levels, &(&1.level == level)).bytes

    test "a segment that cannot be fetched is left out of the sizes, not an error", env do
      {:ok, whole} = Ops.describe(env.ctx, cell())
      {:ok, body, _, _} = env.mod.get(env.cfg, Keys.segment("ops-test", cell(), 0, 3, 3))

      fail_gets(env, Keys.segment("ops-test", cell(), 0, 3, 3))
      {:ok, partial} = Ops.describe(env.ctx, cell(), timeout_ms: 400)

      assert level_bytes(partial, 0) == level_bytes(whole, 0) - byte_size(body)
      assert level_bytes(partial, 1) == level_bytes(whole, 1)
      assert partial.levels |> Enum.map(& &1.files) == whole.levels |> Enum.map(& &1.files)
    end

    test "a snapshot that cannot be fetched keeps what its metadata says", env do
      {:ok, %{snapshots: [whole]}} = Ops.describe(env.ctx, cell())
      assert whole.bytes > 0

      fail_gets(env, Keys.snapshot("ops-test", cell(), whole.txid))
      {:ok, %{snapshots: [partial]}} = Ops.describe(env.ctx, cell(), timeout_ms: 400)

      assert partial == %{whole | bytes: nil}
    end

    test "a snapshot that cannot even be read by its metadata is still listed", env do
      {:ok, %{snapshots: [whole]}} = Ops.describe(env.ctx, cell(), sizes: false)
      key = Keys.snapshot("ops-test", cell(), whole.txid)
      MemoryStore.inject(env.cfg, kind: :error, ops: [:head], key: exactly(key), times: 1_000)

      assert {:ok, %{snapshots: [%{txid: txid, epoch: nil, ts: nil, bytes: nil}]}} =
               Ops.describe(env.ctx, cell(), sizes: false, timeout_ms: 400)

      assert txid == whole.txid
    end

    test "the lease that cannot be read is the error of the inspection", env do
      MemoryStore.inject(env.cfg, kind: :error, ops: [:get], key: ~r{/lease\z}, times: 1_000)
      assert {:error, _} = Ops.describe(env.ctx, cell(), timeout_ms: 400)
    end
  end

  describe "alambique.restore" do
    setup env do
      owner = history(env, new_owner(env, "a.db"), 6)

      {:ok, _} =
        Compaction.run(env.ctx, cell(), fanout: 2, max_level: 2, snapshot_interval_txids: 4)

      Map.put(env, :owner, owner)
    end

    defp restored(env, args) do
      path = Path.join(env.dir, "out-#{System.unique_integer([:positive])}.db")
      out = run("alambique.restore", [cell(), path] ++ args)
      {out, Alambique.ReplicationHelpers.dump_file(path)}
    end

    test "restores the latest state by default", env do
      {out, dump} = restored(env, [])
      assert out =~ ~r/Restored cell to .*out-\d+\.db: TXID 6, epoch 1, \d+ files applied/
      assert dump == env.owner.dumps[6]
    end

    test "restores a TXID", env do
      {out, dump} = restored(env, ["--at", "txid:3"])
      assert out =~ "TXID 3"
      assert dump == env.owner.dumps[3]
    end

    test "restores a point in time given as ISO 8601 or milliseconds", env do
      {:ok, _, _, meta} =
        env.mod.get(env.cfg, Alambique.Replication.Keys.segment("ops-test", cell(), 0, 4, 4))

      ms = String.to_integer(meta["ts"])
      iso = ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()

      for at <- [iso, "ms:#{ms}"] do
        {out, dump} = restored(env, ["--at", at])
        assert out =~ "TXID 4"
        assert dump == env.owner.dumps[4]
      end
    end

    test "bad targets and usage errors", env do
      path = Path.join(env.dir, "x.db")

      for at <- ["yesterday", "txid:x", "ms:"] do
        assert_raise Mix.Error, ~r/bad --at/, fn ->
          run("alambique.restore", [cell(), path, "--at", at])
        end
      end

      assert_raise Mix.Error, ~r/usage: mix alambique.restore/, fn ->
        run("alambique.restore", [cell()])
      end

      assert_raise Mix.Error, ~r/could not restore cell/, fn ->
        run("alambique.restore", [cell(), path, "--at", "txid:99"])
      end
    end
  end

  describe "alambique.freeze and unfreeze" do
    test "write and remove the marker, and a frozen cell refuses to wake" do
      ctx = setup_store()
      inst = start_instance(ctx, migrations: @schema, prefix: "ops-test")
      Application.put_env(:alambique, :ops, store: ctx.store, prefix: "ops-test")
      {:ok, _} = Alambique.execute(inst, "c1", "INSERT INTO t (n, v) VALUES (1, 'x')")
      :ok = Alambique.hibernate(inst, "c1")

      assert run("alambique.freeze", ["c1"]) =~ "Froze c1"
      assert {:error, :frozen} = Alambique.query(inst, "c1", "SELECT n FROM t")
      assert {:ok, true} = Alambique.frozen?(inst, "c1")

      assert run("alambique.unfreeze", ["c1"]) =~ "Unfroze c1"
      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, "c1", "SELECT n FROM t")
    end

    test "an awake cell hibernates at its next renewal" do
      ctx = setup_store()

      inst =
        start_instance(ctx,
          migrations: @schema,
          prefix: "ops-test",
          lease_ttl_ms: 600,
          skew_margin_ms: 100
        )

      Application.put_env(:alambique, :ops, store: ctx.store, prefix: "ops-test")
      {:ok, _} = Alambique.execute(inst, "c1", "INSERT INTO t (n, v) VALUES (1, 'x')")
      assert {:ok, node()} == Alambique.owner(inst, "c1")

      assert run("alambique.freeze", ["c1"]) =~ "hibernates at its next lease renewal"
      eventually(fn -> Alambique.owner(inst, "c1") == :none end)
      assert {:error, :frozen} = Alambique.query(inst, "c1", "SELECT n FROM t")
    end

    test "usage errors" do
      assert_raise Mix.Error, ~r/usage: mix alambique.freeze/, fn ->
        run("alambique.freeze", [])
      end

      assert_raise Mix.Error, ~r/usage: mix alambique.unfreeze/, fn ->
        run("alambique.unfreeze", [])
      end
    end

    test "--connect without --instance is refused before anything is written", env do
      assert_raise Mix.Error, ~r/--instance NAME is required/, fn ->
        run("alambique.freeze", ["c1", "--connect", "nobody@127.0.0.1"])
      end

      assert {:ok, false} = Alambique.Freeze.frozen?(env.ctx, "c1")
    end
  end

  describe "alambique.move" do
    test "needs an instance and a reachable node" do
      assert_raise Mix.Error, ~r/usage: mix alambique.move/, fn ->
        run("alambique.move", ["c1"])
      end

      assert_raise Mix.Error, ~r/--instance NAME is required/, fn ->
        run("alambique.move", ["c1", "nobody@127.0.0.1"])
      end

      unless Node.alive?() do
        assert_raise Mix.Error, ~r/could not (start distribution|connect)/, fn ->
          run("alambique.move", ["c1", "nobody@127.0.0.1", "--instance", "cells"])
        end

        Node.stop()
      end
    end
  end

  describe "store options" do
    setup do
      names = for {name, _} <- System.get_env(), String.starts_with?(name, "ALAMBIQUE_"), do: name
      saved = Map.new(names, &{&1, System.get_env(&1)})
      Enum.each(names, &System.delete_env/1)

      on_exit(fn ->
        for {name, _} <- System.get_env(),
            String.starts_with?(name, "ALAMBIQUE_"),
            do: System.delete_env(name)

        Enum.each(saved, fn {k, v} -> System.put_env(k, v) end)
      end)

      Application.delete_env(:alambique, :ops)
    end

    @flags ~w(--endpoint http://s3.test:9000 --bucket b --access-key-id k --secret-access-key s --prefix p1)

    test "flags build an S3 store" do
      {parsed, _} =
        OptionParser.parse!(@flags ++ ["--region", "auto", "--no-path-style"],
          strict: Options.switches()
        )

      ctx = Options.ctx(parsed)

      assert ctx.store ==
               {Alambique.Store.S3,
                %{
                  endpoint: "http://s3.test:9000",
                  bucket: "b",
                  access_key_id: "k",
                  secret_access_key: "s",
                  region: "auto",
                  path_style: false
                }}

      assert ctx.prefix == "p1"
    end

    test "environment variables fill in and flags win" do
      System.put_env("ALAMBIQUE_S3_ENDPOINT", "http://env.test")
      System.put_env("ALAMBIQUE_S3_BUCKET", "env-bucket")
      System.put_env("ALAMBIQUE_S3_ACCESS_KEY_ID", "ek")
      System.put_env("ALAMBIQUE_S3_SECRET_ACCESS_KEY", "es")
      System.put_env("ALAMBIQUE_S3_PATH_STYLE", "false")
      System.put_env("ALAMBIQUE_PREFIX", "env-prefix")

      ctx = Options.ctx([])

      assert {Alambique.Store.S3,
              %{endpoint: "http://env.test", bucket: "env-bucket", path_style: false}} = ctx.store

      assert ctx.prefix == "env-prefix"

      ctx = Options.ctx(bucket: "flag-bucket", prefix: "flag-prefix")
      assert {_, %{bucket: "flag-bucket", endpoint: "http://env.test"}} = ctx.store
      assert ctx.prefix == "flag-prefix"
    end

    test "the app config names any store module" do
      Application.put_env(:alambique, :ops, store: {MemoryStore, %{server: :x}}, prefix: "cfg")
      ctx = Options.ctx([])
      assert ctx.store == {MemoryStore, %{server: :x}}
      assert ctx.prefix == "cfg"

      Application.put_env(:alambique, :ops, store: {MemoryStore, %{server: :x}})
      assert Options.ctx([]).prefix == "alambique"
    end

    test "the endpoint is optional and the session token is a flag or a variable" do
      flags = ~w(--bucket b --region eu-west-1 --access-key-id k --secret-access-key s)

      {parsed, _} =
        OptionParser.parse!(flags ++ ~w(--session-token t), strict: Options.switches())

      assert {Alambique.Store.S3, %{bucket: "b", region: "eu-west-1", session_token: "t"} = c} =
               Options.ctx(parsed).store

      refute Map.has_key?(c, :endpoint)

      System.put_env("ALAMBIQUE_S3_SESSION_TOKEN", "env-token")

      assert {_, %{session_token: "env-token"}} =
               Options.ctx(bucket: "b", access_key_id: "k", secret_access_key: "s").store
    end

    test "a missing store or a missing S3 setting is explained" do
      assert_raise Mix.Error, ~r/no store configured/, fn -> Options.ctx([]) end

      assert_raise Mix.Error,
                   ~r/missing store settings: --access-key-id, --secret-access-key/,
                   fn ->
                     Options.ctx(bucket: "b")
                   end
    end
  end
end
