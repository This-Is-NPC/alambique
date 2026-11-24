defmodule Alambique.PipelinePropertiesTest do
  @moduledoc """
  Pipelined commits (design §6) under any landing order.

  One owner `x` uploads through a gated store, so the test decides when each
  put lands, fails or loses its acknowledgement. A second instance `y` takes
  the cell over. Random commands mix writes, landings, crashes at the fault
  points of the cell and the upload process, killed cells and takeovers. After
  every command:

    * P1: every acknowledged write is in the state a restore gives, so no
      caller was answered before its batch and every batch before it landed;
      nothing that nobody attempted is in it;
    * P2: what a restore gives only grows (a later state contains the earlier);
    * P3: nothing was deleted from the store, whatever a fenced owner left;
    * P4: a write answered with a definite refusal (`:fenced`, `:lease_lost`,
      `:lease_expired` or any error but `:outcome_unknown`) is never in it, so
      every durable write was acknowledged or answered `:outcome_unknown`.
      Puts that landed and were not acknowledged, puts that never landed, and
      stops by takeover, fence and lease expiry exercise it.

  Beyond the end state, every acknowledgement is checked when it is given: the
  segment of its TXID is already in the store, so a caller is never answered
  while its put is held, failed or behind a held put. A write that nothing
  holds back is acknowledged: the waits are bounded, so a cell that never
  answers fails the property at once. A put that fails once the lease ended
  stops the cell and is not retried.

  Each property runs at depth 1 as the control and at depth 4.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  alias Alambique.{AckGuard, Cell, Config}
  alias Alambique.Replication.Keys
  alias AlambiqueTestkit.{CrashPoints, GatedStore, ManualClock, MemoryStore}

  @moduletag timeout: 600_000
  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @points [
    :cell_after_land,
    :cell_before_ack,
    :cell_after_ack,
    :repl_after_encode,
    :repl_after_put
  ]

  setup do
    on_exit(&CrashPoints.reset/0)
    setup_store()
  end

  defp command do
    frequency([
      {8, map(integer(1..3), &{:write, &1})},
      {6, map(integer(0..7), &{:land, :release, &1})},
      {2, map(integer(0..7), &{:land, :fail, &1})},
      {2, map(integer(0..7), &{:land, :lose_ack, &1})},
      {2, map(member_of(@points), &{:crash, &1, :cell})},
      {1, constant({:crash, :repl_after_put, :upload})},
      {1, constant(:kill)},
      {2, constant(:take_over)},
      {1, constant(:expire)},
      {1, constant(:fail_expired)},
      {2, constant(:recover)}
    ])
  end

  for depth <- [1, 4] do
    property "acknowledged writes are durable and the store only grows, depth #{depth}", ctx do
      check all(
              commands <- resize(list_of(command(), min_length: 10, max_length: 30), 30),
              max_runs: 15,
              max_shrinking_steps: 0
            ) do
        run(ctx, unquote(depth), commands)
      end
    end
  end

  # Scenario

  defp run(ctx, depth, commands) do
    tag = System.unique_integer([:positive])
    cell = "pipe#{tag}"
    prefix = "pp#{tag}"
    audit = Alambique.AuditStore.wrap({MemoryStore, ctx.store_config}, fn -> 0 end)
    {:ok, gate} = GatedStore.start_link()
    gated = GatedStore.wrap(gate, audit)
    clock = ManualClock.new(1_800_000_000_000)

    opts = [
      prefix: prefix,
      migrations: @schema,
      pipeline_depth: depth,
      lease_ttl_ms: 20_000,
      skew_margin_ms: 500,
      op_timeout_ms: 2_000,
      # The gate fails puts on purpose; an open breaker would hold later uploads
      # back for its cool-down, past the wait of `GatedStore.await/3`.
      breaker: [failures: 1_000_000],
      # A failed put reaches the cell, which retries it after a short backoff.
      retry: [max_attempts: 1],
      upload: [retry_backoff_ms: 5, retry_backoff_max_ms: 20],
      compaction: false,
      clock: {ManualClock, ManualClock.config(clock, 0)}
    ]

    x = start_instance(%{ctx | store: gated}, [name: :"x#{tag}"] ++ opts)
    guard = AckGuard.attach({MemoryStore, ctx.store_config}, prefix, &(&1 == cell))
    capture([[:alambique, :cell, :upload_retry]])

    y =
      start_instance(
        %{ctx | store: {Alambique.AuditStore, elem(audit, 1)}},
        [name: :"y#{tag}"] ++ opts
      )

    world = %{
      ctx: ctx,
      cell: cell,
      prefix: prefix,
      gate: gate,
      clock: clock,
      x: x,
      y: y,
      audit: elem(audit, 1),
      seq: 0,
      reqs: [],
      acked: MapSet.new(),
      refused: MapSet.new(),
      attempted: MapSet.new([0]),
      rows: MapSet.new(),
      guard: guard,
      expired?: false
    }

    try do
      world = wake(world)
      world = Enum.reduce(commands, world, &verify(step(&1, &2)))
      world = finish(world)
      verify(world)
    after
      AckGuard.detach(guard)
      CrashPoints.reset()
      GenServer.stop(gate)
      for name <- [x, y], do: stop_supervised(name)
      MemoryStore.clear(ctx.store_config)
    end
  end

  # Gives `x` an awake cell (its first batches land at once), then holds its puts.
  defp wake(w) do
    GatedStore.open(w.gate)
    w = sync_write(w, w.x, 0, :ok)
    GatedStore.hold(w.gate, ~r{/0000/})
    w
  end

  # With `expect` `:ok` nothing stands in the way of the write, so its answer
  # must be `{:ok, _}`; `:expired` also allows `:lease_expired`, the answer of
  # an awake cell whose lease ended.
  defp sync_write(w, inst, n, expect) do
    w = %{w | attempted: MapSet.put(w.attempted, n)}
    answer = Alambique.execute(inst, w.cell, "INSERT INTO t (n) VALUES (?)", [n], timeout: 5_000)

    ok? =
      match?({:ok, _}, answer) or (expect == :expired and answer == {:error, :lease_expired})

    assert expect == :any or ok?, "write #{n} was answered #{inspect(answer)}"

    case answer do
      {:ok, _} -> answered(w, n, answer)
      {:error, reason} when reason in [:fenced, :lease_lost, :lease_expired] -> refuse(w, n)
      {:error, _} -> w
    end
  end

  defp refuse(w, n), do: %{w | refused: MapSet.put(w.refused, n)}

  # What the caller of write `n` learned from the cell. An exit says nothing.
  defp answered(w, n, {:ok, _}), do: %{w | acked: MapSet.put(w.acked, n)}

  defp answered(w, _n, {:error, :outcome_unknown}), do: w
  defp answered(w, n, {:error, _reason}), do: refuse(w, n)
  defp answered(w, _n, _answer), do: w

  defp owner(w), do: Cell.whereis(Config.fetch!(w.x), w.cell)

  defp pipe(pid) do
    :sys.get_state(pid).pipe
  catch
    :exit, _ -> nil
  end

  defp key(w, txid), do: Keys.segment(w.prefix, w.cell, 0, txid, txid)

  # Commands

  defp step({:write, count}, w) do
    case owner(w) do
      nil ->
        step({:write, count}, recover(w))

      pid ->
        before = length((pipe(pid) || %{queue: []}).queue)
        first = w.seq + 1

        w =
          Enum.reduce(1..count, w, fn _, w ->
            n = w.seq + 1
            sql = {:execute, "INSERT INTO t (n) VALUES (?)", [n], nil}
            req = :gen_server.send_request(pid, sql)
            %{w | seq: n, attempted: MapSet.put(w.attempted, n), reqs: [{n, req} | w.reqs]}
          end)

        # The first call queues the flush behind the others; the second
        # barrier returns after the flush ran.
        pipe(pid)
        after_flush = pipe(pid)

        with %{queue: queue} when length(queue) > before <- after_flush do
          GatedStore.await(w.gate, key(w, List.last(queue).txid))
        end

        # Every put of the owner is held: nobody may have been answered.
        w = collect(w)
        early = MapSet.intersection(w.acked, MapSet.new(first..w.seq))
        assert MapSet.size(early) == 0, "writes #{inspect(early)} were answered with puts held"
        w
    end
  end

  defp step({:land, how, index}, w) do
    with pid when is_pid(pid) <- owner(w),
         [_ | _] = held <- GatedStore.held(w.gate),
         %{queue: queue} <- pipe(pid) do
      key = Enum.at(held, rem(index, length(held)))
      batch = Enum.find(queue, &(key(w, &1.txid) == key))
      apply(GatedStore, how, [w.gate, key])

      # The upload ends, or retries and waits at the gate again.
      if batch,
        do: eventually(fn -> not Process.alive?(batch.pid) or key in GatedStore.held(w.gate) end)

      pipe(pid)
    end

    collect(w)
  end

  defp step({:crash, point, :cell}, w) do
    pid = owner(w)

    CrashPoints.arm(point, fn _, ctx ->
      target = if is_map(ctx) and Map.has_key?(ctx, :owner_pid), do: ctx.owner_pid, else: self()
      if target == pid, do: Process.exit(target, :kill)
    end)

    w
  end

  defp step({:crash, point, :upload}, w) do
    pid = owner(w)

    CrashPoints.arm(point, fn _, ctx ->
      if ctx.owner_pid == pid, do: Process.exit(self(), :kill)
    end)

    w
  end

  defp step(:kill, w) do
    with pid when is_pid(pid) <- owner(w), do: Process.exit(pid, :kill)
    collect(w)
  end

  defp step(:take_over, w) do
    case Alambique.take_over(w.y, w.cell, :test, timeout: 5_000) do
      {:ok, _} ->
        n = w.seq + 1
        expect = if w.expired?, do: :expired, else: :ok
        sync_write(%{w | seq: n, expired?: false}, w.y, n, expect)

      {:error, _} ->
        w
    end
  end

  # The lease ends while a put is held, then the put fails: the cell stops,
  # answers its callers and does not put again.
  defp step(:fail_expired, w) do
    with pid when is_pid(pid) <- owner(w),
         %{queue: queue} when queue != [] <- pipe(pid),
         keys = for(batch <- queue, do: key(w, batch.txid)),
         [key | _] <- Enum.filter(GatedStore.held(w.gate), &(&1 in keys)) do
      drain([:alambique, :cell, :upload_retry], w.cell)
      mon = Process.monitor(pid)
      ManualClock.advance(w.clock, 25_000)
      GatedStore.fail(w.gate, key)

      receive do
        {:DOWN, ^mon, _, _, _} -> :ok
      after
        5_000 -> flunk("the cell kept a failed put after its lease ended")
      end

      assert drain([:alambique, :cell, :upload_retry], w.cell) == [],
             "the cell retried a put after its lease ended"

      collect(%{w | expired?: true})
    else
      _ -> collect(w)
    end
  end

  # The lease of every owner ends: the next call of the old owner stops it.
  defp step(:expire, w) do
    ManualClock.advance(w.clock, 25_000)
    %{w | expired?: true}
  end

  defp step(:recover, w), do: recover(w)

  # Lets every held put land, ends the old owner and wakes `x` again.
  defp recover(w) do
    GatedStore.open(w.gate)
    w = settle(w)

    with pid when is_pid(pid) <- owner(w) do
      mon = Process.monitor(pid)
      Process.exit(pid, :kill)

      receive do
        {:DOWN, ^mon, _, _, _} -> :ok
      after
        5_000 -> raise "owner did not stop"
      end
    end

    CrashPoints.reset()
    Alambique.hibernate(w.y, w.cell, timeout: 5_000)
    n = w.seq + 1
    w = sync_write(%{w | seq: n}, w.x, n, :ok)
    GatedStore.hold(w.gate, ~r{/0000/})
    w
  end

  # Waits for the answers of every write sent, now that no put is held: a cell
  # that is alive answers each one.
  defp settle(w) do
    deadline = System.monotonic_time(:millisecond) + 10_000

    Enum.reduce(w.reqs, %{w | reqs: []}, fn {n, req}, w ->
      left = max(deadline - System.monotonic_time(:millisecond), 0)

      case :gen_server.wait_response(req, left) do
        {:reply, answer} -> answered(w, n, answer)
        {:error, _} -> w
        :timeout -> flunk("write #{n} was never answered although every put could land")
      end
    end)
  end

  defp finish(w) do
    GatedStore.open(w.gate)
    settle(w)
  end

  # Answers that arrived.
  defp collect(w) do
    Enum.reduce(w.reqs, %{w | reqs: []}, fn {n, req}, w ->
      case :gen_server.wait_response(req, 0) do
        {:reply, answer} -> answered(w, n, answer)
        {:error, _} -> w
        :timeout -> %{w | reqs: [{n, req} | w.reqs]}
      end
    end)
  end

  # Properties

  defp verify(w) do
    w = collect(w)

    rows =
      w.ctx
      |> Map.put(:store, {Alambique.AuditStore, w.audit})
      |> durable_rows(w.cell, "SELECT n FROM t", w.prefix)
      |> List.flatten()
      |> MapSet.new()

    assert AckGuard.violations(w.guard) == [],
           "acknowledged before the put landed: #{inspect(AckGuard.violations(w.guard))}"

    missing = MapSet.difference(w.acked, rows)
    assert MapSet.size(missing) == 0, "acknowledged writes #{inspect(missing)} are not durable"

    invented = MapSet.difference(rows, w.attempted)
    assert MapSet.size(invented) == 0, "rows #{inspect(invented)} were never written"

    refused = MapSet.intersection(w.refused, rows)

    assert MapSet.size(refused) == 0,
           "writes #{inspect(refused)} were refused definitively and are durable"

    lost = MapSet.difference(w.rows, rows)
    assert MapSet.size(lost) == 0, "the store lost rows #{inspect(lost)}"

    assert Alambique.AuditStore.deletes(w.audit) == []
    %{w | rows: rows}
  end
end
