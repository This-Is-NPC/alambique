defmodule Alambique.TakeoverPropertiesTest do
  @moduledoc """
  Early takeover under faults (design §5, §6), in one VM.

  Three instances with their own node ids and data directories share one
  memory store and prefix, so they share leases and segments but not the
  membership: a takeover is always a wrong suspicion, because the old owner
  stays alive and keeps serving. Random scenarios mix writes, plain and
  checked reads, takeovers, hibernations, killed cells, drains, store faults
  and crash points. After the faults are removed:

    * every acknowledged write is in the durable state of its cell exactly
      once, in acknowledgement order, and nothing that nobody attempted is;
    * epochs of acknowledged batches never go back along the TXIDs, and no
      epoch is acknowledged from two cell processes;
    * a takeover replaces the epoch it found with the next one;
    * a batch of an older epoch acknowledged after a takeover returned is
      below the head the new owner woke with, so the fence did not let it in;
    * a checked read never misses a write acknowledged before it was issued;
    * a plain read misses an acknowledged write only on a cell whose epoch
      was already replaced, and only within the documented window;
    * a write is acknowledged only once its segment is in the store, also when
      the later of two batches in flight lands first, and an owner that was
      taken over does not answer a checked read once its batch landed;
    * until a fault, a kill or a drain, the instance that owns a cell
      acknowledges every write to it.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{AckGuard, Cell, Config, HeldWrites, Membership}
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.{CrashPoints, GatedStore, MemoryStore}

  @moduletag timeout: 600_000
  @moduletag capture_log: true

  @cells ["c1", "c2"]
  @who [:x, :y, :z]
  @ttl_ms 1_500
  @skew_ms 300
  @cooldown_ms 500
  @call_timeout_ms 2_000
  # Slack for scheduling and for the requests of a renewal.
  @slack_ms 500
  @schema [
    {1, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, n INTEGER NOT NULL)"}
  ]
  @crash_points [
    :lease_before_put,
    :lease_after_put,
    :repl_before_capture,
    :repl_after_encode,
    :repl_after_put,
    :cell_after_ack
  ]
  @events [
    [:alambique, :lease, :acquire],
    [:alambique, :lease, :take_over],
    [:alambique, :cell, :commit],
    [:alambique, :cell, :wake]
  ]

  setup do
    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  # Most traffic goes through :x, so takeovers by :y and :z find a live owner.
  # Without faults, the waits straddle the renewal bound of the old owner.
  defp op(faults?) do
    user = frequency([{6, constant(:x)}, {2, constant(:y)}, {2, constant(:z)}])
    taker = frequency([{1, constant(:x)}, {3, constant(:y)}, {3, constant(:z)}])
    cell = member_of(@cells)
    waits = if faults?, do: [0, 0, 200, 700, 1_900], else: [0, 0, 200, 900, 1_000, 1_100]

    common = [
      {10, tuple({constant(:write), user, cell})},
      {4, tuple({constant(:read), user, cell})},
      {5, tuple({constant(:checked_read), user, cell})},
      {4, tuple({constant(:take_over), taker, cell})},
      # The wrong suspicion: a write, a takeover, a write by the new owner, and
      # reads on the old one.
      {6, tuple({constant(:handover), user, taker, cell, member_of(waits)})},
      {3, tuple({constant(:held_pair), user, cell})},
      {3, tuple({constant(:stale_checked), user, taker, cell})},
      {2, tuple({constant(:hibernate), user, cell})},
      {2, tuple({constant(:kill), user, cell})},
      {1, tuple({constant(:drain), user})},
      {4, tuple({constant(:pass), integer(50..300)})}
    ]

    faults = [
      {3,
       tuple(
         {constant(:fault), member_of([:error, :timeout, :lost_ack, :latency, :raise, :exit]),
          member_of([[:put], [:get], [:put, :get]]), integer(1..4), integer(50..300)}
       )},
      {2, tuple({constant(:crash), member_of(@crash_points), member_of([:raise, :kill])})}
    ]

    frequency(if faults?, do: common ++ faults, else: common)
  end

  property "acknowledged writes survive takeovers, and stale reads stay inside the window", ctx do
    check all(
            ops <- resize(list_of(op(true), min_length: 15, max_length: 40), 40),
            max_runs: 15,
            max_shrinking_steps: 0
          ) do
      run_scenario(ctx, ops)
    end
  end

  property "without faults the old owner stops answering at its next renewal", ctx do
    check all(
            ops <- resize(list_of(op(false), min_length: 15, max_length: 30), 30),
            max_runs: 10,
            max_shrinking_steps: 0
          ) do
      run_scenario(ctx, ops)
    end
  end

  # Scenario

  defp run_scenario(ctx, ops) do
    tag = System.unique_integer([:positive])
    prefix = "tk#{tag}"
    log = :ets.new(:log, [:public, :ordered_set])
    handler = {__MODULE__, tag}
    :telemetry.attach_many(handler, @events, &__MODULE__.record/4, log)

    {:ok, gate} = GatedStore.start_link()
    guard = AckGuard.attach(ctx.store, prefix, &(&1 in @cells))
    gated = %{ctx | store: GatedStore.wrap(gate, ctx.store)}

    insts =
      for who <- @who, into: %{} do
        name = :"#{who}#{tag}"

        start_instance(gated,
          name: name,
          prefix: prefix,
          migrations: @schema,
          lease_ttl_ms: @ttl_ms,
          skew_margin_ms: @skew_ms,
          op_timeout_ms: 1_000,
          call_timeout_ms: @call_timeout_ms,
          breaker: [cooldown_ms: @cooldown_ms],
          compaction: false
        )

        {who, name}
      end

    run = %{
      store: ctx.store_config,
      insts: insts,
      log: log,
      seq: 0,
      acked: %{},
      attempted: %{},
      faulty?: false,
      calm?: true,
      disturbed: MapSet.new(),
      owner: %{},
      prefix: prefix,
      gate: gate,
      guard: guard,
      takeovers: []
    }

    try do
      run = Enum.reduce(ops, run, &step/2)
      settle(ctx, run, prefix)
      verify_log(run)
    after
      AckGuard.detach(guard)
      GenServer.stop(gate)
      :telemetry.detach(handler)
      CrashPoints.reset()
      MemoryStore.clear(ctx.store_config)
      for {_, name} <- insts, do: stop_supervised(name)
      :ets.delete(log)
    end
  end

  @doc false
  def record([:alambique, :lease, :take_over], _m, %{result: :ok} = meta, log) do
    insert(log, {:lease, :take_over, meta.cell, meta.epoch})

    if Map.has_key?(meta, :previous_epoch),
      do: insert(log, {:replaced, meta.cell, meta.previous_epoch, meta.epoch})
  end

  def record([:alambique, :lease, kind], _m, %{result: :ok} = meta, log) do
    insert(log, {:lease, kind, meta.cell, meta.epoch})
  end

  def record([:alambique, :lease, _], _m, _meta, _log), do: :ok

  def record([:alambique, :cell, :commit], _m, %{result: :ok} = meta, log),
    do: insert(log, {:commit, meta.cell, meta.txid, meta.epoch, self()})

  def record([:alambique, :cell, :wake], _m, %{result: :ok} = meta, log),
    do: insert(log, {:wake, meta.cell, meta.epoch, meta.txid, self()})

  def record(_event, _m, _meta, _log), do: :ok

  defp insert(log, entry),
    do: :ets.insert(log, {System.unique_integer([:monotonic]), now(), entry})

  defp now, do: System.monotonic_time(:millisecond)

  # Entries recorded so far: `{key, at_ms, entry}`.
  defp entries(run), do: :ets.tab2list(run.log)

  defp inst(run, who), do: run.insts[who]

  defp call_opts, do: [timeout: @call_timeout_ms]

  # Operations

  defp step({:handover, from, to, cell, wait_ms}, run) when from != to do
    Enum.reduce(
      [
        {:take_over, from, cell},
        {:write, from, cell},
        {:take_over, to, cell},
        {:write, to, cell},
        {:read, from, cell},
        {:pass, wait_ms},
        {:read, from, cell},
        {:checked_read, from, cell}
      ],
      run,
      &step/2
    )
  end

  defp step({:handover, _from, _to, _cell, _wait}, run), do: run

  defp step({:write, who, cell}, run) do
    n = run.seq + 1
    run = %{run | seq: n, attempted: Map.update(run.attempted, cell, [n], &[n | &1])}

    answer =
      Alambique.execute(inst(run, who), cell, "INSERT INTO t (n) VALUES (?)", [n], call_opts())

    account(run, who, cell, n, answer)
  end

  # Two writes of one owner whose puts are held, the later one landing first.
  defp step({:held_pair, who, cell}, run) do
    run = step({:write, who, cell}, run)
    pid = Cell.whereis(Config.fetch!(inst(run, who)), cell)

    if pid && run.owner[cell] == who do
      ns = [run.seq + 1, run.seq + 2]

      run = %{
        run
        | seq: run.seq + 2,
          attempted: Map.update(run.attempted, cell, Enum.reverse(ns), &(Enum.reverse(ns) ++ &1))
      }

      answers =
        HeldWrites.run(run.gate, cell, pid, 8, ns, fn n ->
          Alambique.execute(
            inst(run, who),
            cell,
            "INSERT INTO t (n) VALUES (?)",
            [n],
            call_opts()
          )
        end)

      Enum.reduce(answers, run, fn {n, answer}, run -> account(run, who, cell, n, answer) end)
    else
      run
    end
  end

  # The owner has a write in flight, with a checked read waiting behind it,
  # whose put landed but whose answer was lost. It is taken over while the cell
  # puts the batch again, and the new owner writes. When the put is found, the
  # write is acknowledged (it is below the head the new owner woke with), but
  # the old cell does not answer the checked read: the lease it is checked
  # against is lost. The caller gets no answer or, from the new owner, a state
  # with the write of the new owner.
  defp step({:stale_checked, who, taker, cell}, run) when who != taker do
    run = step({:write, who, cell}, run)
    pid = Cell.whereis(Config.fetch!(inst(run, who)), cell)

    # A refused write leaves its pid registered while the cell stops (`:fenced`),
    # and a plain read of a stale owner also marks it the owner: the step then
    # has no live cell to hold, and is skipped.
    head = run.calm? and pid != nil and run.owner[cell] == who and head_of(pid)

    if head do
      n = run.seq + 1
      run = %{run | seq: n, attempted: Map.update(run.attempted, cell, [n], &[n | &1])}
      key = Keys.segment(run.prefix, cell, 0, head + 1, head + 1)
      GatedStore.hold(run.gate, Regex.compile!("^" <> Regex.escape(key) <> "$"))
      write = Task.async(fn -> execute(run, who, cell, n) end)
      eventually(fn -> key in GatedStore.held(run.gate) end)
      read = Task.async(fn -> checked_query(run, who, cell) end)
      eventually(fn -> checked_waits?(pid) end)

      # The put lands, the cell is told it failed and puts it again.
      GatedStore.lose_ack(run.gate, key)
      eventually(fn -> key in GatedStore.held(run.gate) end)
      run = %{run | disturbed: MapSet.put(run.disturbed, cell)}
      taken = Alambique.take_over(inst(run, taker), cell, {:suspect, who}, call_opts())
      newest = n + 1

      run = %{
        run
        | seq: newest,
          attempted: Map.update(run.attempted, cell, [newest], &[newest | &1])
      }

      newest_answer = execute(run, taker, cell, newest)
      GatedStore.release(run.gate, key)
      GatedStore.open(run.gate)

      answer = Task.await(write, 15_000)
      checked = Task.await(read, 15_000)

      with {:ok, _} <- taken,
           {:ok, %{rows: rows}} <- checked,
           {:ok, _} <- newest_answer do
        assert newest in List.flatten(rows),
               "#{cell}: the old owner answered a checked read without the write #{newest} " <>
                 "of the owner that took it over: #{inspect(rows)}"
      end

      # In the order of the chain: the write of the old owner is below the fence.
      run
      |> account(who, cell, n, answer)
      |> account(taker, cell, newest, newest_answer)
    else
      run
    end
  end

  defp step({:stale_checked, _who, _taker, _cell}, run), do: run

  defp step({:read, who, cell}, run) do
    name = inst(run, who)
    pid = Cell.whereis(Config.fetch!(name), cell)
    issued = now()

    case Alambique.query(name, cell, "SELECT n FROM t", [], call_opts()) do
      {:ok, %{rows: rows}} ->
        check_plain_read(run, cell, pid, issued, List.flatten(rows))
        %{run | owner: Map.put(run.owner, cell, who)}

      {:error, _} ->
        run
    end
  end

  defp step({:checked_read, who, cell}, run) do
    issued = now()
    opts = [verify_lease: true] ++ call_opts()

    case Alambique.query(inst(run, who), cell, "SELECT n FROM t", [], opts) do
      {:ok, %{rows: rows}} ->
        missed = missing(run, cell, issued, List.flatten(rows))

        assert missed == [],
               "a checked read of #{cell} missed acknowledged writes #{inspect(missed)}"

        %{run | owner: Map.put(run.owner, cell, who)}

      {:error, _} ->
        run
    end
  end

  defp step({:take_over, who, cell}, run) do
    started = System.unique_integer([:monotonic])
    run = %{run | disturbed: MapSet.put(run.disturbed, cell)}

    case Alambique.take_over(inst(run, who), cell, {:suspect, who}, call_opts()) do
      {:ok, _} -> record_takeover(%{run | owner: Map.put(run.owner, cell, who)}, cell, started)
      {:error, _} -> run
    end
  end

  defp step({:hibernate, who, cell}, run) do
    Alambique.hibernate(inst(run, who), cell, call_opts())
    run
  end

  defp step({:kill, who, cell}, run) do
    with pid when is_pid(pid) <- Cell.whereis(Config.fetch!(inst(run, who)), cell),
         do: Process.exit(pid, :kill)

    %{run | calm?: false}
  end

  defp step({:drain, who}, run) do
    config = Config.fetch!(inst(run, who))
    Alambique.drain(inst(run, who), timeout: @call_timeout_ms)
    Membership.join(config)
    %{run | calm?: false}
  end

  defp step({:fault, kind, ops, times, ms}, run) do
    MemoryStore.inject(run.store, kind: kind, ops: ops, times: times, ms: ms)
    %{run | faulty?: true, calm?: false}
  end

  defp step({:crash, point, action}, run) do
    CrashPoints.arm(point, action, times: 1)
    %{run | faulty?: true, calm?: false}
  end

  defp step({:pass, ms}, run), do: tap(run, fn _ -> Process.sleep(ms) end)

  # A takeover that returned: its epoch is the newest takeover event of the
  # cell since the call started. A cell that was already here has none.
  defp record_takeover(run, cell, started) do
    returned = now()

    epochs =
      for {key, _at, {:lease, :take_over, ^cell, epoch}} <- entries(run), key > started, do: epoch

    case epochs do
      [] -> run
      _ -> %{run | takeovers: [{cell, Enum.max(epochs), returned} | run.takeovers]}
    end
  end

  # Helpers of the steps

  defp execute(run, who, cell, n),
    do: Alambique.execute(inst(run, who), cell, "INSERT INTO t (n) VALUES (?)", [n], call_opts())

  defp checked_query(run, who, cell) do
    Alambique.query(
      inst(run, who),
      cell,
      "SELECT n FROM t",
      [],
      [verify_lease: true] ++ call_opts()
    )
  catch
    :exit, reason -> {:exit, reason}
  end

  # The TXID the cell has reached, or nil when it already stopped.
  defp head_of(pid) do
    :sys.get_state(pid).repl.txid
  catch
    :exit, _ -> nil
  end

  # A checked read waits in the newest batch of the cell.
  defp checked_waits?(pid) do
    pid
    |> :sys.get_state()
    |> Map.fetch!(:pipe)
    |> Map.fetch!(:queue)
    |> Enum.any?(fn batch -> Enum.any?(batch.items, &match?({:checked_read, _, _}, &1)) end)
  catch
    :exit, _ -> false
  end

  # What the model says of the answer to write `n`: the owner of a cell that
  # nothing has disturbed acknowledges it.
  defp account(run, who, cell, n, answer) do
    case answer do
      {:ok, _} ->
        ack = {n, now()}

        %{
          run
          | acked: Map.update(run.acked, cell, [ack], &[ack | &1]),
            owner: Map.put(run.owner, cell, who)
        }

      {:error, _} ->
        refute run.calm? and cell not in run.disturbed and run.owner[cell] in [nil, who],
               "#{who} was refused its write to the calm cell #{cell}: #{inspect(answer)}"

        run
    end
  end

  # Reads

  # Acknowledged writes (acknowledged before `issued`) that `rows` lacks.
  defp missing(run, cell, issued, rows) do
    for {n, at} <- Map.get(run.acked, cell, []), at < issued, n not in rows, do: n
  end

  defp check_plain_read(run, cell, pid, issued, rows) do
    case missing(run, cell, issued, rows) do
      [] ->
        :ok

      missed ->
        epoch = pid && epoch_of(run, cell, pid)

        assert epoch,
               "#{cell}: a read of a fresh wake missed acknowledged writes #{inspect(missed)}"

        bump =
          for {_key, at, {:lease, _, ^cell, e}} <- entries(run), e > epoch, do: at

        assert bump != [],
               "#{cell}: epoch #{epoch} missed #{inspect(missed)} without a newer epoch"

        late = issued - Enum.min(bump)

        assert late <= window(run),
               "#{cell}: a read answered #{late} ms after epoch #{epoch} was replaced " <>
                 "(window #{window(run)} ms) and missed #{inspect(missed)}"
    end
  end

  defp epoch_of(run, cell, pid) do
    Enum.find_value(entries(run), fn
      {_key, _at, {:wake, ^cell, epoch, _txid, ^pid}} -> epoch
      _ -> nil
    end)
  end

  # The documented bound (design §5): a renewal every ttl/3 finds the new epoch;
  # when renewals fail, the lease ends at ttl - skew after the last one.
  defp window(%{faulty?: false}), do: div(@ttl_ms, 3) + @slack_ms
  defp window(%{faulty?: true}), do: @ttl_ms - @skew_ms + @slack_ms

  # After the faults

  # A store the breaker still counts as down refuses the heal at once
  # (`:store_unavailable`): that is the breaker's contract. The first heal
  # after its cool-down is the probe, so the heal is repeated until the
  # breaker lets it through, bounded.
  defp heal(name, cell) do
    eventually(
      fn ->
        case Alambique.take_over(name, cell, :heal, call_opts()) do
          {:ok, _} -> true
          {:error, :store_unavailable} -> false
          other -> flunk("heal of #{cell}: #{inspect(other)}")
        end
      end,
      @cooldown_ms * 10
    )
  end

  defp settle(ctx, run, prefix) do
    CrashPoints.reset()
    MemoryStore.clear(ctx.store_config)

    # Every instance gives its cells back, so the store holds what survives.
    for {_who, name} <- run.insts do
      assert {:ok, _} = Alambique.drain(name, timeout: 5_000)
      Membership.join(Config.fetch!(name))
    end

    for cell <- @cells do
      stored = durable(ctx, cell, prefix)

      verify_cell(run, cell, stored)

      # The cell is served again, from any instance, and agrees with the store.
      name = inst(run, :x)
      heal(name, cell)
      opts = [verify_lease: true] ++ call_opts()

      assert {:ok, %{rows: rows}} =
               Alambique.query(name, cell, "SELECT n FROM t ORDER BY id", [], opts)

      assert List.flatten(rows) == stored
    end
  end

  # A cell that never stored anything has no table.
  defp durable(ctx, cell, prefix) do
    ctx |> durable_rows(cell, "SELECT n FROM t ORDER BY id", prefix) |> List.flatten()
  rescue
    error in MatchError ->
      if match?({:error, "no such table" <> _}, error.term),
        do: [],
        else: reraise(error, __STACKTRACE__)
  end

  defp verify_cell(run, cell, stored) do
    acked = run.acked |> Map.get(cell, []) |> Enum.reverse() |> Enum.map(&elem(&1, 0))
    attempted = run.attempted |> Map.get(cell, []) |> Enum.reverse()

    assert stored == Enum.uniq(stored), "cell #{cell} has duplicate writes: #{inspect(stored)}"
    assert stored -- attempted == [], "cell #{cell} has writes nobody issued"

    assert acked -- stored == [],
           "cell #{cell} lost acknowledged writes #{inspect(acked -- stored)}"

    assert Enum.filter(stored, &(&1 in acked)) == acked,
           "cell #{cell} reordered acknowledged writes"
  end

  defp verify_log(run) do
    entries = entries(run)

    for {_, _, {:replaced, cell, previous, epoch}} <- entries do
      assert epoch == previous + 1,
             "cell #{cell}: a takeover replaced epoch #{previous} with epoch #{epoch}"
    end

    assert AckGuard.violations(run.guard) == [],
           "acknowledged before the put landed: #{inspect(AckGuard.violations(run.guard))}"

    commits =
      for {_, at, {:commit, cell, txid, epoch, pid}} <- entries, do: {cell, txid, epoch, pid, at}

    for {cell, batches} <- Enum.group_by(commits, &elem(&1, 0)) do
      txids = Enum.map(batches, &elem(&1, 1))
      assert txids == Enum.uniq(txids), "cell #{cell}: one TXID acknowledged twice"

      by_txid = Enum.sort_by(batches, &elem(&1, 1))
      epochs = Enum.map(by_txid, &elem(&1, 2))

      assert epochs == Enum.sort(epochs),
             "cell #{cell}: epochs go back along TXIDs: #{inspect(by_txid)}"

      for {epoch, group} <- Enum.group_by(batches, &elem(&1, 2)) do
        pids = group |> Enum.map(&elem(&1, 3)) |> Enum.uniq()
        assert length(pids) == 1, "cell #{cell}: epoch #{epoch} acknowledged by #{inspect(pids)}"
      end
    end

    # After a takeover returned, an older epoch may only acknowledge a batch
    # that landed before the fence, which is below the new owner's head.
    for {cell, new_epoch, returned} <- run.takeovers,
        {cell2, txid, epoch, _pid, at} <- commits,
        cell2 == cell and epoch < new_epoch and at > returned do
      heads = for {_, _, {:wake, ^cell, ^new_epoch, head, _}} <- entries, do: head

      assert Enum.all?(heads, &(txid < &1)),
             "cell #{cell}: epoch #{epoch} acknowledged TXID #{txid} after the takeover to " <>
               "epoch #{new_epoch} returned (new owner's head #{inspect(heads)})"
    end
  end
end
