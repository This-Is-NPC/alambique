defmodule Alambique.AdmissionTest do
  @moduledoc """
  A node refuses work past its limits instead of taking it (design §7):
  `awake.max_cells` (after hibernating idle cells), `awake.max_wakes`,
  `awake.max_pending` and the memory budget of the awake cells,
  `awake.max_bytes` with `awake.cache_kib`.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  alias AlambiqueTestkit.GatedStore

  @moduletag capture_log: true

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]
  @insert "INSERT INTO t (n) VALUES (?)"

  setup do
    setup_store()
  end

  defp awake(inst), do: Registry.count(Alambique.Config.fetch!(inst).registry)

  test "awake.max_cells hibernates the least recently used cell to make room", env do
    inst = start_instance(env, migrations: @schema, awake: [max_cells: 3])
    config = Alambique.Config.fetch!(inst)
    [a, b, c, d] = cells = for _ <- 1..4, do: unique_cell()

    for cell <- [a, b, c] do
      assert {:ok, _} = Alambique.execute(inst, cell, @insert, [1])
      Process.sleep(5)
    end

    # `b` is now the least recently used.
    assert {:ok, _} = Alambique.touch(inst, a)
    Process.sleep(5)
    assert {:ok, _} = Alambique.touch(inst, d)

    eventually(fn ->
      Enum.sort(Enum.filter(cells, &(Alambique.load(inst, &1) != :none))) == Enum.sort([a, c, d])
    end)

    assert awake(inst) == 3
    assert Alambique.Cell.whereis(config, b) == nil

    # The cell that hibernated wakes again, with its data, and takes another place.
    assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, b, "SELECT count(*) FROM t")
    assert awake(inst) == 3
  end

  test "awake.max_cells refuses a wake when every awake cell has callers waiting", env do
    {:ok, gate} = GatedStore.start_link()
    on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
    store = GatedStore.wrap(gate, env.store)
    inst = start_instance(%{env | store: store}, migrations: @schema, awake: [max_cells: 1])
    [busy, other] = [unique_cell(), unique_cell()]
    {:ok, _} = Alambique.execute(inst, busy, @insert, [0])

    GatedStore.hold(gate, ~r{/0000/})
    writer = Task.async(fn -> Alambique.execute(inst, busy, @insert, [1]) end)
    eventually(fn -> Alambique.load(inst, busy) == {:ok, %{pending: 1}} end)

    assert {:error, :overloaded} = Alambique.touch(inst, other)
    assert awake(inst) == 1

    GatedStore.open(gate)
    assert {:ok, _} = Task.await(writer, 10_000)

    # Idle again, the cell gives its place.
    eventually(fn -> match?({:ok, _}, Alambique.touch(inst, other)) end)
    assert awake(inst) == 1
  end

  test "a wake above awake.max_wakes waits for a slot instead of being refused", env do
    {:ok, gate} = GatedStore.start_link()
    on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
    store = GatedStore.wrap(gate, env.store)
    GatedStore.hold(gate, ~r{/0000/})

    inst = start_instance(%{env | store: store}, migrations: @schema, awake: [max_wakes: 2])

    cells = for _ <- 1..6, do: unique_cell()
    tasks = for cell <- cells, do: Task.async(fn -> Alambique.touch(inst, cell) end)

    # Two wakes wait for the store and the other four wait for a slot.
    eventually(fn -> length(GatedStore.held(gate)) == 2 end)
    eventually(fn -> Enum.count(cells, &(Alambique.load(inst, &1) != :none)) == 2 end)

    GatedStore.open(gate)
    assert Enum.all?(tasks, &match?({:ok, _}, Task.await(&1, 10_000)))
  end

  test "a wake that does not get a slot in awake.wake_queue_ms is refused", env do
    {:ok, gate} = GatedStore.start_link()
    on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
    store = GatedStore.wrap(gate, env.store)
    GatedStore.hold(gate, ~r{/0000/})

    inst =
      start_instance(%{env | store: store},
        migrations: @schema,
        awake: [max_wakes: 1, wake_queue_ms: 100, max_wake_queue: 1]
      )

    waking = Task.async(fn -> Alambique.touch(inst, unique_cell()) end)
    eventually(fn -> length(GatedStore.held(gate)) == 1 end)

    handler = "wake-queue-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler,
      [:alambique, :admission, :wake_queue],
      &__MODULE__.forward_queue/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    # The first waits for the slot and expires; the second finds the queue full.
    queued = Task.async(fn -> timed(fn -> Alambique.touch(inst, unique_cell()) end) end)
    config = Alambique.Config.fetch!(inst)
    eventually(fn -> map_size(:sys.get_state(config.wakes).waiting) == 1 end)
    assert {:error, :overloaded} = Alambique.touch(inst, unique_cell())
    assert_receive {:wake_queue, :full}

    assert {{:error, :overloaded}, ms} = Task.await(queued, 5_000)
    assert ms >= 100 and ms < 1_000
    assert_receive {:wake_queue, :expired}

    GatedStore.open(gate)
    assert {:ok, _} = Task.await(waking, 10_000)
  end

  defp timed(fun) do
    started = System.monotonic_time(:millisecond)
    {fun.(), System.monotonic_time(:millisecond) - started}
  end

  @doc false
  def forward_queue(_event, _measurements, meta, test), do: send(test, {:wake_queue, meta.result})

  test "the wake slot of a cell killed while waking comes back", env do
    {:ok, gate} = GatedStore.start_link()
    on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
    store = GatedStore.wrap(gate, env.store)
    GatedStore.hold(gate, ~r{/0000/})
    inst = start_instance(%{env | store: store}, migrations: @schema, awake: [max_wakes: 1])
    config = Alambique.Config.fetch!(inst)

    stuck = unique_cell()
    task = Task.async(fn -> Alambique.touch(inst, stuck) end)
    eventually(fn -> length(GatedStore.held(gate)) == 1 end)
    assert {:error, :overloaded} = Alambique.touch(inst, unique_cell())

    pid = Alambique.Cell.whereis(config, stuck)
    Process.exit(pid, :kill)
    Task.shutdown(task, :brutal_kill)
    GatedStore.open(gate)

    eventually(fn -> match?({:ok, _}, Alambique.touch(inst, unique_cell())) end)
  end

  test "the wake slot of a caller killed before its cell took it over comes back", env do
    inst = start_instance(env, migrations: @schema, awake: [max_wakes: 1])
    config = Alambique.Config.fetch!(inst)
    stuck = unique_cell()
    test = self()

    # The caller holds the slot and waits here, before it starts the cell.
    Alambique.Fault.install(fn
      :wake_slot_taken, %{cell: ^stuck} ->
        send(test, {:in_window, self()})
        Process.sleep(:infinity)

      _, _ ->
        :ok
    end)

    on_exit(&Alambique.Fault.uninstall/0)

    task = Task.async(fn -> Alambique.touch(inst, stuck) end)
    assert_receive {:in_window, caller}
    assert map_size(:sys.get_state(config.wakes).holders) == 1

    Process.exit(caller, :kill)
    Task.shutdown(task, :brutal_kill)

    eventually(fn -> :sys.get_state(config.wakes).holders == %{} end)
    assert {:ok, _} = Alambique.touch(inst, unique_cell())
  end

  test "awake.max_pending is never passed, however many callers check at once", env do
    {:ok, gate} = GatedStore.start_link()
    on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
    store = GatedStore.wrap(gate, env.store)
    inst = start_instance(%{env | store: store}, migrations: @schema, awake: [max_pending: 3])

    # Reads wait for the held upload and count until it ends, even once their
    # callers gave up: the writer and 2 of them fill the cell. Each round is a
    # new chance for the callers to check at the same moment.
    for _ <- 1..10 do
      cell = unique_cell()
      {:ok, _} = Alambique.execute(inst, cell, @insert, [0])

      GatedStore.hold(gate, ~r{/0000/})
      writer = Task.async(fn -> Alambique.execute(inst, cell, @insert, [1]) end)
      eventually(fn -> Alambique.load(inst, cell) == {:ok, %{pending: 1}} end)

      replies = burst_queries(inst, cell, 100, 50)

      assert Enum.count(replies, &(&1 == {:error, :timeout})) == 2
      assert Enum.count(replies, &(&1 == {:error, :busy})) == 98
      assert Alambique.load(inst, cell) == {:ok, %{pending: 3}}

      GatedStore.open(gate)
      assert {:ok, _} = Task.await(writer, 10_000)
    end
  end

  test "a call whose caller gave up before the cell reached it does not run", env do
    inst = start_instance(env, migrations: @schema)
    cell = unique_cell()
    {:ok, _} = Alambique.execute(inst, cell, @insert, [0])

    # The cell is busy with a long read; the write behind it times out first.
    slow =
      "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 5000000) SELECT count(*) FROM c"

    reader = Task.async(fn -> Alambique.query(inst, cell, slow, [], timeout: 10_000) end)
    eventually(fn -> Alambique.load(inst, cell) == {:ok, %{pending: 1}} end)

    assert {:error, :timeout} = Alambique.execute(inst, cell, @insert, [1], timeout: 20)
    assert {:ok, _} = Task.await(reader, 10_000)

    assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
    eventually(fn -> Alambique.load(inst, cell) == {:ok, %{pending: 0}} end)
  end

  @doc false
  def forward_evict(_event, measurements, meta, test),
    do: send(test, {:evicted, meta.cell, measurements})

  describe "the memory of the awake cells" do
    defp cache_size(inst, cell),
      do: Alambique.query(inst, cell, "PRAGMA cache_size")

    # A write and a read: the cell reports its memory after each call.
    defp fill(inst, cell) do
      {:ok, _} = Alambique.execute(inst, cell, @insert, [1])
      {:ok, _} = Alambique.query(inst, cell, "SELECT count(*) FROM t")
    end

    defp awake_cells(inst, cells),
      do: Enum.filter(cells, &(Alambique.load(inst, &1) != :none))

    # The bytes a cell like those of the tests holds once filled.
    defp bytes_of_a_filled_cell(env) do
      inst = start_instance(env, migrations: @schema, awake: [max_bytes: 1_000_000_000])
      fill(inst, unique_cell())
      bytes = inst |> Alambique.Config.fetch!() |> Alambique.Admission.bytes()
      stop_supervised!(inst)
      bytes
    end

    test "awake.cache_kib sets the page cache of every awake cell", env do
      default = start_instance(env, migrations: @schema)
      assert {:ok, %{rows: [[-512]]}} = cache_size(default, unique_cell())

      small = start_instance(env, migrations: @schema, awake: [cache_kib: 64])
      assert {:ok, %{rows: [[-64]]}} = cache_size(small, unique_cell())
    end

    test "nothing is measured without awake.max_bytes", env do
      inst = start_instance(env, migrations: @schema)
      fill(inst, unique_cell())
      assert Alambique.Admission.bytes(Alambique.Config.fetch!(inst)) == 0
    end

    test "the least recently used cells hibernate before another wakes", env do
      bytes = bytes_of_a_filled_cell(env)
      assert bytes > 0

      # Room for two cells and a half: the fourth wake needs one gone.
      inst = start_instance(env, migrations: @schema, awake: [max_bytes: div(bytes * 5, 2)])
      config = Alambique.Config.fetch!(inst)
      [a, b, c, d] = cells = for _ <- 1..4, do: unique_cell()

      handler = "evict-#{System.unique_integer()}"

      :telemetry.attach(
        handler,
        [:alambique, :admission, :evict],
        &__MODULE__.forward_evict/4,
        self()
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      for cell <- [a, b, c] do
        fill(inst, cell)
        Process.sleep(5)
      end

      assert awake_cells(inst, cells) == [a, b, c]

      # `b` is now the least recently used.
      {:ok, _} = Alambique.query(inst, a, "SELECT count(*) FROM t")
      Process.sleep(5)
      fill(inst, d)

      assert awake_cells(inst, cells) == [a, c, d]
      assert_received {:evicted, ^b, %{bytes: _}}
      assert Alambique.Admission.bytes(config) < div(bytes * 5, 2) + bytes

      # The evicted cell wakes again, with its data.
      assert {:ok, %{rows: [[1]]}} = Alambique.query(inst, b, "SELECT count(*) FROM t")
      assert length(awake_cells(inst, cells)) == 3
    end

    test "a wake is refused when every awake cell has callers waiting", env do
      {:ok, gate} = GatedStore.start_link()
      on_exit(fn -> if Process.alive?(gate), do: GenServer.stop(gate) end)
      store = GatedStore.wrap(gate, env.store)
      inst = start_instance(%{env | store: store}, migrations: @schema, awake: [max_bytes: 1])
      [busy, other] = [unique_cell(), unique_cell()]
      fill(inst, busy)

      GatedStore.hold(gate, ~r{/0000/})
      writer = Task.async(fn -> Alambique.execute(inst, busy, @insert, [2]) end)
      eventually(fn -> Alambique.load(inst, busy) == {:ok, %{pending: 1}} end)

      assert {:error, :overloaded} = Alambique.touch(inst, other)
      assert Alambique.load(inst, busy) == {:ok, %{pending: 1}}

      GatedStore.open(gate)
      assert {:ok, _} = Task.await(writer, 10_000)

      # Idle again, the cell gives its place.
      eventually(fn -> match?({:ok, _}, Alambique.touch(inst, other)) end)
      assert Alambique.load(inst, busy) == :none
    end

    test "the record of a killed cell is dropped", env do
      inst = start_instance(env, migrations: @schema, awake: [max_bytes: 1_000_000_000])
      config = Alambique.Config.fetch!(inst)
      cell = unique_cell()
      fill(inst, cell)
      assert Alambique.Admission.bytes(config) > 0

      Process.exit(Alambique.Cell.whereis(config, cell), :kill)
      eventually(fn -> Alambique.Admission.bytes(config) == 0 end)
    end
  end

  test "the limits are validated" do
    bad = [[max_cells: 0], [max_wakes: -1], [max_pending: :many], [other: 1]]
    bad = bad ++ [[cache_kib: 0], [max_bytes: 0], [max_bytes: :lots]]

    for opts <- bad do
      assert_raise ArgumentError, ~r/awake/, fn ->
        Alambique.Config.build(
          store: {AlambiqueTestkit.MemoryStore, nil},
          data_dir: "/tmp/x",
          awake: opts
        )
      end
    end
  end
end
