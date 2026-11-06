defmodule Alambique.CompactionHelpers do
  @moduledoc "Builds cell histories in a memory store and inspects them, for compaction tests."

  import Alambique.ReplicationHelpers

  alias Alambique.{Compaction, Replication}
  alias Alambique.Replication.{Keys, Replicator}
  alias AlambiqueTestkit.{ManualClock, MemoryStore}

  @cell "cell"

  def cell, do: @cell

  @doc "A store, a manual clock, a temporary directory and a context over them."
  def setup_world(opts \\ []) do
    {pid, {mod, cfg} = store} = start_store()
    world = new_world()
    dir = tmp_dir()
    ctx = ctx(store, world, Keyword.merge([timeout_ms: 2_000], opts))
    %{pid: pid, mod: mod, cfg: cfg, store: store, world: world, dir: dir, ctx: ctx}
  end

  @doc """
  An owner of the cell: a database file, its connection and a replicator.
  `dumps` maps each acknowledged TXID to the database content at that TXID.
  """
  def new_owner(env, name, epoch \\ 1) do
    {conn, state} = new_replicator(env.dir, name, epoch)
    %{conn: conn, state: state, dumps: %{}}
  end

  @doc "Takes the cell over as a new owner: restore, then fence."
  def take_over(env, name, epoch, dumps, opts \\ []) do
    path = Path.join(env.dir, name)
    {:ok, result} = Replication.restore(env.ctx, @cell, path, :latest)
    state = Replicator.from_restore(result, epoch)
    {:ok, state} = Replicator.fence(state, env.ctx, opts)
    # The fence holds the restored file; opening may add the helper's table.
    at_fence = dump_file(path)
    conn = open_db(path)
    %{conn: conn, state: state, dumps: Map.put(dumps, state.txid, at_fence)}
  end

  @doc """
  Runs `ops`, replicates them and records the dump; advances the clock by
  `step_ms` first. `opts` go to `Replicator.sync/3`.
  """
  def write(env, owner, ops, step_ms \\ 1, opts \\ []) do
    ManualClock.advance(env.world, step_ms)
    Enum.each(ops, &safe_op(owner.conn, &1))

    case Replicator.sync(owner.state, env.ctx, opts) do
      {:ok, state} ->
        dumps =
          if state.txid != owner.state.txid,
            do: Map.put(owner.dumps, state.txid, dump(owner.conn)),
            else: owner.dumps

        {:ok, %{owner | state: state, dumps: dumps}}

      {:error, _} = error ->
        error
    end
  end

  def write!(env, owner, ops, step_ms \\ 1) do
    {:ok, owner} = write(env, owner, ops, step_ms)
    owner
  end

  @doc "`n` small writes."
  def history(env, owner, n, step_ms \\ 1) do
    Enum.reduce(1..n//1, owner, fn i, owner ->
      write!(env, owner, [{:put, rem(i, 7) + 1, 300 + i * 40}], step_ms)
    end)
  end

  @doc """
  One compaction pass with a small fanout. It keeps the merged files and the snapshots, so the
  retention and trash tests see only the manifests they make themselves.
  """
  def compact_pass(env, opts) do
    defaults = [fanout: 2, max_level: 2, delete_superseded: false, snapshot_keep_ms: 0]
    Compaction.run(env.ctx, @cell, Keyword.merge(defaults, opts))
  end

  @day 86_400_000

  @doc "Fifteen TXIDs a day apart, a snapshot after each five; leaves the clock three days on."
  def daily_history(env) do
    Enum.reduce(1..3, new_owner(env, "a.db"), fn day, owner ->
      owner = history(env, owner, 5, 1)
      {:ok, %{snapshot: snap}} = compact_pass(env, snapshot_interval_txids: 5)
      ^snap = day * 5
      ManualClock.advance(env.world, @day)
      owner
    end)
  end

  @doc """
  A stale owner (epoch 1) writes TXID 5 after the new owner (epoch 2) fenced
  TXID 4. Returns both owners.
  """
  def orphaned(env) do
    a = history(env, new_owner(env, "a.db"), 3)
    b = take_over(env, "b.db", 2, a.dumps)
    safe_op(a.conn, {:put, 9, 100})
    {:ok, _} = Replicator.sync(%{a.state | txid: b.state.txid}, env.ctx)
    {a, b}
  end

  @doc "Restores `target` into a fresh file and returns `{:ok, result, dump}` or the error."
  def restore(env, target) do
    path = Path.join(env.dir, "restore-#{System.unique_integer([:positive])}.db")

    case Replication.restore(env.ctx, @cell, path, target) do
      {:ok, result} -> {:ok, result, dump_file(path)}
      {:error, _} = error -> error
    end
  end

  @doc "All keys of the cell, relative to the cell prefix."
  def keys(env) do
    prefix = Keys.cell_prefix(env.ctx.prefix, @cell) <> "/"
    {:ok, keys, nil} = env.mod.list(env.cfg, prefix, nil, 10_000)
    Enum.map(keys, &String.replace_prefix(&1, prefix, ""))
  end

  @doc "`{level, min, max}` of every segment and `{:snapshot, txid}` of every snapshot."
  def files(env) do
    Enum.map(keys(env), fn key ->
      case String.split(key, "/") do
        [level, <<min::binary-size(16), "-", max::binary-size(16), ".ltx">>] ->
          case String.to_integer(level, 16) do
            9 -> {:snapshot, String.to_integer(max, 16)}
            level -> {level, String.to_integer(min, 16), String.to_integer(max, 16)}
          end

        _ ->
          key
      end
    end)
  end

  def segments(env, level), do: for({^level, min, max} <- files(env), do: {min, max})
  def snapshots(env), do: for({:snapshot, txid} <- files(env), do: txid)

  def clear_faults(env), do: MemoryStore.clear(env.cfg)
end
