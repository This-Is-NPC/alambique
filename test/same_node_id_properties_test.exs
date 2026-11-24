defmodule Alambique.SameNodeIdPropertiesTest do
  @moduledoc """
  Three instances with one `node_id` over one store, without a cluster
  (design §5). Random scenarios mix writes through any instance, hibernations,
  killed cells and pauses. The invariants:

    * a cell acknowledges a write from an instance only when no other
      instance holds it: the holder is awake, and a hibernated or killed cell
      gives the cell up (a killed cell's lease is left to expire);
    * every acknowledged write is in the durable state of its cell exactly
      once, and nothing that nobody attempted is;
    * no `(cell, txid)` is acknowledged by two cell processes, so there is one
      writer per TXID;
    * a write is acknowledged only once its segment is in the store, also when
      the later of two batches in flight lands first;
    * a write to a cell nobody else holds, that was never killed, is
      acknowledged; any other instance is refused while the holder lives.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{AckGuard, Cell, Config, HeldWrites}
  alias AlambiqueTestkit.{GatedStore, MemoryStore}

  @moduletag timeout: 600_000
  @moduletag capture_log: true

  @cells ["c1", "c2"]
  @who [:x, :y, :z]
  @id "same-id"
  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]

  setup do
    setup_store()
  end

  defp op do
    who = member_of(@who)
    cell = member_of(@cells)

    frequency([
      {10, tuple({constant(:write), who, cell})},
      {3, tuple({constant(:held_pair), who, cell})},
      {3, tuple({constant(:hibernate), who, cell})},
      {2, tuple({constant(:kill), who, cell})},
      {3, tuple({constant(:pass), member_of([50, 200, 1_100])})}
    ])
  end

  property "one holder per cell and every acknowledged write exactly once", ctx do
    check all(
            ops <- resize(list_of(op(), min_length: 10, max_length: 30), 30),
            max_runs: 12,
            max_shrinking_steps: 0
          ) do
      run_scenario(ctx, ops)
    end
  end

  defp run_scenario(ctx, ops) do
    tag = System.unique_integer([:positive])
    prefix = "sn#{tag}"
    log = :ets.new(:log, [:public, :bag])
    handler = {__MODULE__, tag}
    :telemetry.attach(handler, [:alambique, :cell, :commit], &__MODULE__.record/4, log)

    {:ok, gate} = GatedStore.start_link()
    guard = AckGuard.attach(ctx.store, prefix, &(&1 in @cells))
    gated = %{ctx | store: GatedStore.wrap(gate, ctx.store)}

    insts =
      for who <- @who, into: %{} do
        name = :"#{who}#{tag}"

        start_instance(gated,
          name: name,
          prefix: prefix,
          node_id: @id,
          migrations: @schema,
          lease_ttl_ms: 1_000,
          skew_margin_ms: 200,
          op_timeout_ms: 1_000,
          compaction: false
        )

        {who, name}
      end

    run = %{
      insts: insts,
      seq: 0,
      holder: %{},
      acked: %{},
      attempted: %{},
      killed: MapSet.new(),
      gate: gate,
      guard: guard,
      store: ctx.store_config,
      raw: ctx.store,
      prefix: prefix,
      epochs: %{}
    }

    try do
      run = Enum.reduce(ops, run, &observed_step/2)
      verify(ctx, run, prefix, log)
    after
      AckGuard.detach(guard)
      GenServer.stop(gate)
      :telemetry.detach(handler)
      MemoryStore.clear(ctx.store_config)
      for {_, name} <- insts, do: stop_supervised(name)
      :ets.delete(log)
    end
  end

  @doc false
  def record(_event, _m, %{result: :ok} = meta, log),
    do: :ets.insert(log, {{meta.cell, meta.txid}, self()})

  def record(_event, _m, _meta, _log), do: :ok

  defp observed_step(op, run), do: op |> step(run) |> observe()

  # The lease object of each cell as the store holds it: a renewal keeps its
  # epoch and every new acquisition, of any instance, has a higher one.
  defp observe(run) do
    {store, config} = run.raw

    Enum.reduce(@cells, run, fn cell, run ->
      case store.get(config, "#{run.prefix}/cells/#{cell}/lease") do
        {:ok, body, _etag, _meta} ->
          %{"epoch" => epoch, "token" => token} = Jason.decode!(body)
          seen = {token, epoch}
          assert_advanced(run.epochs[cell], seen, cell)
          %{run | epochs: Map.put(run.epochs, cell, seen)}

        {:error, :not_found} ->
          run
      end
    end)
  end

  defp assert_advanced(nil, _seen, _cell), do: :ok

  defp assert_advanced({token, before}, {token, now}, cell),
    do: assert(now == before, "#{cell}: epoch #{before} became #{now} within one acquisition")

  defp assert_advanced({_, before}, {_, now}, cell),
    do: assert(now > before, "#{cell}: a new acquisition kept epoch #{before}")

  # The model keeps the holder of each cell: `{who, :live | :killed}`. A write
  # from another instance may be acknowledged only after the holder gave the
  # cell up, or when its cell died and its lease may have expired.
  defp step({:write, who, cell}, run) do
    n = run.seq + 1
    run = %{run | seq: n}
    answer = Alambique.execute(run.insts[who], cell, "INSERT INTO t VALUES (?)", [n])
    account(run, who, cell, n, answer)
  end

  # Two writes of the holder whose puts are held, the later one landing first.
  defp step({:held_pair, who, cell}, run) do
    run = step({:write, who, cell}, run)
    pid = Cell.whereis(Config.fetch!(run.insts[who]), cell)

    if pid && run.holder[cell] == {who, :live} do
      ns = [run.seq + 1, run.seq + 2]
      run = %{run | seq: run.seq + 2}

      answers =
        HeldWrites.run(run.gate, cell, pid, 8, ns, fn n ->
          Alambique.execute(run.insts[who], cell, "INSERT INTO t VALUES (?)", [n])
        end)

      Enum.reduce(answers, run, fn {n, answer}, run -> account(run, who, cell, n, answer) end)
    else
      run
    end
  end

  defp step({:hibernate, who, cell}, run) do
    case Alambique.hibernate(run.insts[who], cell) do
      :ok -> release(run, who, cell)
      {:error, _} -> run
    end
  end

  defp step({:kill, who, cell}, run) do
    case Cell.whereis(Config.fetch!(run.insts[who]), cell) do
      pid when is_pid(pid) ->
        Process.exit(pid, :kill)

        %{
          run
          | holder: Map.replace(run.holder, cell, {who, :killed}),
            killed: MapSet.put(run.killed, cell)
        }

      nil ->
        run
    end
  end

  defp step({:pass, ms}, run), do: tap(run, fn _ -> Process.sleep(ms) end)

  # What the model says of the answer to write `n`: an instance that holds the
  # cell, or finds it free and was never killed, is acknowledged.
  defp account(run, who, cell, n, answer) do
    run = %{run | attempted: Map.update(run.attempted, cell, [n], &[n | &1])}
    free? = run.holder[cell] in [nil, {who, :live}] and cell not in run.killed

    case answer do
      {:ok, _} ->
        case run.holder[cell] do
          {other, :live} when other != who ->
            flunk("#{who} acknowledged a write of #{cell} while #{other} holds it")

          _ ->
            %{
              run
              | holder: Map.put(run.holder, cell, {who, :live}),
                acked: Map.update(run.acked, cell, [n], &[n | &1])
            }
        end

      {:error, _} = error ->
        refute free?, "#{who} was refused a free cell #{cell}: #{inspect(error)}"
        run
    end
  end

  defp release(run, who, cell) do
    case run.holder[cell] do
      {^who, _} -> %{run | holder: Map.delete(run.holder, cell)}
      _ -> run
    end
  end

  defp verify(ctx, run, prefix, log) do
    assert AckGuard.violations(run.guard) == [],
           "acknowledged before the put landed: #{inspect(AckGuard.violations(run.guard))}"

    for cell <- @cells, acked = Map.get(run.acked, cell, []), acked != [] do
      hibernate_everywhere(run, cell)
      rows = durable_rows(ctx, cell, "SELECT n FROM t ORDER BY n", prefix) |> List.flatten()

      assert Enum.sort(acked) -- rows == [], "lost acknowledged writes of #{cell}"
      assert rows == Enum.uniq(rows)
      assert rows -- Map.fetch!(run.attempted, cell) == [], "unattempted rows in #{cell}"
    end

    writers = :ets.tab2list(log) |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))

    for {key, pids} <- writers do
      assert pids == Enum.uniq(pids), "#{inspect(key)} acknowledged by two cell processes"
    end
  end

  # Ends the holder of the cell, so that its writes are all in the store.
  defp hibernate_everywhere(run, cell),
    do: for({_, name} <- run.insts, do: Alambique.hibernate(name, cell))
end
