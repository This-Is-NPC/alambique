defmodule Alambique.Replica do
  @moduledoc false
  # A read replica of one cell on a node that does not own it (design §21).
  #
  # The process keeps its own database file and one read-only connection. It
  # builds the file with a full restore, then polls the store: a task sends a
  # LIST of the level-0 files after the head, a LIST of the snapshots and a GET
  # of the lease at time `s`, and fetches the new segments. The process
  # applies them in place, reopens the connection and sets `confirmed_at = s`.
  # `s` is taken before anything is sent, which is what makes the bound of a
  # read hold: a write acknowledged before `s` is in the listing.
  #
  # A read starting at `q` with bound `B` is served from the file when
  # `q - confirmed_at <= B - margin`. Otherwise it waits for a refresh whose
  # `s` is recent enough (`on_stale: :refresh`), or goes to the owner. Nothing
  # here writes to the store or to anything but the replica's own file.

  use GenServer, restart: :temporary

  alias Alambique.{Cell, Config, Lease, Placement, Telemetry}
  alias Alambique.Cell.Db
  alias Alambique.Replica.{Manager, Marker}
  alias Alambique.Replication
  alias Alambique.Replication.{Chain, Keys, Native, Objects, Retry}

  # Segments fetched at once by a refresh, as in a restore.
  @fetch_concurrency 16
  # Bytes of segments one refresh keeps in memory before it applies them.
  @round_bytes 33_554_432

  @type reply :: {:ok, map} | {:error, term}

  def start_link({config, cell, location}) do
    GenServer.start_link(__MODULE__, {config, cell, location},
      name: {:via, Registry, {config.replica_registry, {location.id, cell}}}
    )
  end

  @doc "Directory that holds the files of the replicas of a node."
  @spec dir(map) :: Path.t()
  def dir(config), do: Path.join(config.data_dir, "read-replicas")

  @doc """
  Reads from the replica a read that started at `q` (monotonic microseconds)
  with a bound of `bound` milliseconds. `{:replica, reply, meta}` with `meta`
  holding `txid` and `lag_ms`, or `{:owner, reason}` when the owner must
  answer.
  """
  @spec read(pid, String.t(), list, integer, pos_integer, pos_integer) ::
          {:replica, reply, map} | {:owner, atom}
  def read(pid, sql, params, q, bound, timeout) do
    GenServer.call(pid, {:read, sql, params, q, bound, timeout}, timeout)
  catch
    :exit, _ -> {:owner, :stopped}
  end

  @doc "Removes the replica and its file."
  @spec stop(pid, atom) :: :ok
  def stop(pid, reason) do
    GenServer.call(pid, {:stop, reason}, 5_000)
  catch
    :exit, _ -> :ok
  end

  @impl true
  def init({config, cell, location}) do
    Process.flag(:trap_exit, true)
    settings = config.replicas
    suffix = location.id |> :erlang.phash2() |> Integer.to_string(16)
    dir = Path.join(dir(config), "#{cell}@#{suffix}")
    File.rm_rf!(dir)
    File.mkdir_p!(dir)
    ctx = Config.repl_ctx(config, location)

    state = %{
      config: config,
      settings: settings,
      cell: cell,
      location: location,
      key: {location.id, cell},
      ctx: ctx,
      follow_ctx: Map.put(ctx, :missing_final, true),
      lease_ctx: Config.lease_ctx(config, location),
      dir: dir,
      path: Path.join(dir, "replica.db"),
      conn: nil,
      txid: 0,
      epoch: 0,
      checksum: nil,
      page_size: nil,
      confirmed_at: nil,
      last_read: now(),
      marker_at: now(),
      waiters: [],
      task: nil,
      timer: nil,
      reason: :shutdown
    }

    {:ok, state, {:continue, :build}}
  end

  @impl true
  def handle_continue(:build, state), do: {:noreply, start_build(state)}

  @impl true
  def handle_call({:stop, reason}, _from, state) do
    state = finish(%{state | reason: reason})
    {:stop, :shutdown, :ok, state}
  end

  def handle_call({:read, sql, params, q, bound, timeout}, from, state) do
    state = touch(state)
    read = %{from: from, sql: sql, params: params, q: q, bound: bound, timeout: timeout}

    cond do
      state.confirmed_at == nil ->
        {:reply, {:owner, :building}, state}

      fresh?(state, q, bound) ->
        {:reply, serve(state, read), state}

      state.settings.on_stale == :owner ->
        {:reply, {:owner, :stale}, state}

      true ->
        ref = make_ref()
        timer = Process.send_after(self(), {:give_up, ref}, state.settings.refresh_timeout_ms)
        state = %{state | waiters: state.waiters ++ [Map.merge(read, %{ref: ref, timer: timer})]}
        {:noreply, if(state.task, do: state, else: start_refresh(state))}
    end
  end

  @impl true
  def handle_info(:poll, %{task: task} = state) when task != nil, do: {:noreply, state}

  def handle_info(:poll, state) do
    cond do
      now() - state.last_read >= state.settings.idle_ms -> halt(state, :idle)
      Cell.whereis(state.config, state.cell) != nil -> halt(state, :owner_here)
      true -> {:noreply, start_refresh(state)}
    end
  end

  def handle_info({:give_up, ref}, state) do
    {gone, waiters} = Enum.split_with(state.waiters, &(&1.ref == ref))
    for w <- gone, do: GenServer.reply(w.from, {:owner, :timeout})
    {:noreply, %{state | waiters: waiters}}
  end

  def handle_info({ref, result}, %{task: %{ref: ref} = task} = state) do
    Process.demonitor(ref, [:flush])
    complete(%{state | task: nil}, task, result)
  end

  def handle_info({:DOWN, ref, :process, _, reason}, %{task: %{ref: ref} = task} = state),
    do: complete(%{state | task: nil}, task, {:error, {:task_down, reason}})

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    state = finish(state)
    for w <- state.waiters, do: GenServer.reply(w.from, {:owner, :stopped})
    Telemetry.emit([:replica, :stop], %{}, %{cell: state.cell, reason: state.reason})
  end

  # Build

  defp start_build(state) do
    data = Map.take(state, [:config, :ctx, :cell, :key, :path])
    task = Task.Supervisor.async(state.config.query_supervisor, fn -> build(data) end)

    %{
      state
      | task: %{handle: task, ref: task.ref, kind: :build, started: System.monotonic_time()}
    }
  end

  # A full restore. The bytes are reserved first from the size the head
  # announces, so the files of the node never pass `max_bytes`.
  defp build(%{config: config, ctx: ctx, cell: cell, key: key, path: path}) do
    with {:ok, head} when head != nil <- Replication.head(ctx, cell) |> empty(),
         :ok <- reserve(config, key, head.commit * head.page_size),
         {:ok, result} <- Replication.restore(ctx, cell, path, :latest),
         {:ok, %{size: size}} <- File.stat(path),
         :ok <- reserve(config, key, size) do
      {:ok, Map.take(result, [:txid, :epoch, :checksum, :page_size])}
    end
  end

  defp empty({:ok, nil}), do: {:error, :empty}
  defp empty(other), do: other

  defp reserve(config, key, bytes) do
    case Manager.reserve(config, key, bytes) do
      :ok -> :ok
      :refused -> {:error, :max_bytes}
    end
  end

  # Refresh

  defp start_refresh(state) do
    check_marker? = now() - state.marker_at >= state.settings.marker_ttl_ms

    data =
      state
      |> Map.take([:config, :follow_ctx, :lease_ctx, :cell, :location, :key, :txid])
      |> Map.put(:check_marker?, check_marker?)

    task = Task.Supervisor.async(state.config.query_supervisor, fn -> fetch(data) end)

    %{
      state
      | task: %{handle: task, ref: task.ref, kind: :refresh, started: System.monotonic_time()}
    }
  end

  # What a refresh learns from the store. `s` is taken before the first
  # request is sent.
  defp fetch(data) do
    s = us()
    %{config: config, follow_ctx: ctx, cell: cell, txid: txid} = data
    deadline = Retry.deadline_in(config.op_timeout_ms)
    after_key = Keys.segment(ctx.prefix, cell, 0, txid, txid)

    listing =
      Task.async(fn ->
        Objects.list(ctx, Keys.level_prefix(ctx.prefix, cell, 0), deadline, after_key)
      end)

    snapshots =
      Task.async(fn ->
        Objects.list(ctx, Keys.level_prefix(ctx.prefix, cell, Keys.snapshot_level()), deadline)
      end)

    lease = Task.async(fn -> Lease.read(data.lease_ctx, cell) end)

    marker =
      if data.check_marker?,
        do: Task.async(fn -> Marker.enabled?(ctx, cell, deadline) end)

    with {:ok, lease} <- Task.await(lease, :infinity) |> lease_ok(),
         :ok <- placed?(data, lease),
         {:ok, keys} <- Task.await(listing, :infinity),
         {:ok, snapshot_keys} <- Task.await(snapshots, :infinity),
         {:ok, run} <- plan(ctx, cell, txid, keys, snapshot_keys),
         {:ok, run, more?} <- bodies(ctx, run, config.op_timeout_ms),
         :ok <- grow(data, run) do
      {:ok, %{s: s, run: run, more: more?, marker: marker && Task.await(marker, :infinity)}}
    end
  end

  defp lease_ok({:error, reason}), do: {:error, {:lease, reason}}
  defp lease_ok(other), do: {:ok, other}

  # A moved cell keeps its old chain for a while, which would look fresh.
  defp placed?(data, lease) do
    moved? = match?({:ok, %{moved_to: to}} when to != nil, lease)

    case Placement.locate(data.config, data.cell) do
      {:ok, %{id: id}} when id == elem(data.key, 0) and not moved? -> :ok
      _ -> {:drop, :moved}
    end
  end

  defp plan(ctx, cell, txid, keys, snapshot_keys) do
    {segments, snapshots} = Keys.parse(ctx.prefix, cell, snapshot_keys ++ keys)
    newest = snapshots |> Enum.map(& &1.txid) |> Enum.max(fn -> 0 end)

    case Chain.run(Enum.sort_by(segments, & &1.min), txid, :infinity) do
      _ when newest > txid -> {:rebuild, :snapshot}
      {_run, true} -> {:rebuild, :gap}
      {run, false} -> {:ok, run}
    end
  end

  # The bodies of the first segments of the run, fetched `@fetch_concurrency`
  # at a time, each with a deadline of its own. A refresh keeps what it
  # fetched: it stops at the first segment it cannot read, or after
  # `@round_bytes`, and `more?` says that the run goes on. A replica far behind
  # so catches up in rounds instead of starting again at every poll, and a
  # round holds a bounded number of bodies.
  defp bodies(ctx, run, timeout_ms), do: bodies(ctx, run, timeout_ms, [], 0)

  defp bodies(_ctx, [], _timeout_ms, acc, _bytes), do: {:ok, Enum.reverse(acc), false}

  defp bodies(_ctx, _run, _timeout_ms, acc, bytes) when bytes >= @round_bytes,
    do: {:ok, Enum.reverse(acc), true}

  defp bodies(ctx, run, timeout_ms, acc, bytes) do
    {chunk, rest} = Enum.split(run, @fetch_concurrency)

    fetched =
      chunk
      |> Task.async_stream(&body(ctx, &1, Retry.deadline_in(timeout_ms)),
        max_concurrency: @fetch_concurrency,
        timeout: :infinity,
        ordered: true
      )
      |> Enum.map(fn {:ok, result} -> result end)

    {ok, failed} = Enum.split_while(fetched, &match?({:ok, _}, &1))
    items = Enum.map(ok, fn {:ok, item} -> item end)
    acc = Enum.reverse(items, acc)

    case failed do
      [] ->
        bodies(ctx, rest, timeout_ms, acc, bytes + Enum.sum_by(items, &byte_size(&1.body)))

      [error | _] when acc == [] ->
        error

      _ ->
        {:ok, Enum.reverse(acc), true}
    end
  end

  defp body(ctx, seg, deadline) do
    case Objects.fetch(ctx, seg.key, deadline) do
      {:ok, body, meta} ->
        case Native.ltx_info(body, false) do
          {:ok, {min, max, _ts, commit, _pre, page_size}, _post}
          when min == seg.min and max == seg.max ->
            {:ok,
             %{seg: seg, body: body, epoch: Objects.epoch_of(meta), size: commit * page_size}}

          _ ->
            {:rebuild, :segment}
        end

      {:error, :not_found} ->
        {:rebuild, :vanished}

      {:error, reason} ->
        {:error, {:fetch, reason}}
    end
  end

  defp grow(_data, []), do: :ok

  defp grow(%{config: config, key: key}, run) do
    case Manager.reserve(config, key, List.last(run).size) do
      :ok -> :ok
      :refused -> {:drop, :max_bytes}
    end
  end

  # Results

  defp complete(state, %{kind: :build} = task, result) do
    meta =
      case result do
        {:ok, built} -> %{txid: built.txid, result: :ok}
        {:error, reason} -> %{result: :error, reason: reason}
      end

    Telemetry.span_stop([:replica, :build], task.started, Map.put(meta, :cell, state.cell))

    with {:ok, built} <- result,
         state = Map.merge(state, built),
         {:ok, conn} <- open(state) do
      {:noreply, start_refresh(%{state | conn: conn})}
    else
      {:error, reason} -> halt(state, build_failure(reason))
    end
  end

  defp complete(state, %{kind: :refresh} = task, {:ok, %{s: s, run: run, marker: marker} = got}) do
    case apply_run(state, run) do
      {:ok, state} ->
        refreshed_event(state, task, run, %{result: :ok})
        # Not at the end of the chain yet: the replica is not confirmed.
        if got.more, do: {:noreply, start_refresh(state)}, else: refreshed(state, s, marker)

      {:error, reason, state} ->
        refreshed_event(state, task, [], %{result: :rebuild, reason: reason})
        rebuild(state)
    end
  end

  defp complete(state, %{kind: :refresh} = task, {:rebuild, reason}) do
    refreshed_event(state, task, [], %{result: :rebuild, reason: reason})
    rebuild(state)
  end

  defp complete(state, %{kind: :refresh} = task, {:drop, reason}) do
    refreshed_event(state, task, [], %{result: :drop, reason: reason})
    halt(state, reason)
  end

  defp complete(state, %{kind: :refresh} = task, {:error, reason}) do
    refreshed_event(state, task, [], %{result: :error, reason: reason})
    {:noreply, schedule(state)}
  end

  defp refreshed_event(state, task, run, meta) do
    Telemetry.span_stop(
      [:replica, :refresh],
      task.started,
      %{segments: length(run)},
      Map.put(meta, :cell, state.cell)
    )
  end

  defp build_failure({:open, _}), do: :open_failed
  defp build_failure(:empty), do: :empty
  defp build_failure(:max_bytes), do: :max_bytes
  defp build_failure(_), do: :build_failed

  defp refreshed(state, s, marker) do
    state = %{state | confirmed_at: s}

    case marker do
      {:ok, false} ->
        Marker.remember(state.config, state.location, state.cell, false)
        halt(state, :marker_off)

      {:ok, true} ->
        Marker.remember(state.config, state.location, state.cell, true)
        proceed(%{state | marker_at: now()})

      _ ->
        proceed(state)
    end
  end

  # Waiters a refresh did not satisfy start another one at once.
  defp proceed(state) do
    state = serve_waiters(state)
    {:noreply, if(state.waiters == [], do: schedule(state), else: start_refresh(state))}
  end

  # The file cannot follow the chain: start again from a restore. Reads go to
  # the owner until a refresh confirms the new file.
  defp rebuild(state) do
    state = close(state)
    File.rm(state.path)
    state = %{state | confirmed_at: nil, txid: 0, epoch: 0, checksum: nil}
    {:noreply, start_build(state)}
  end

  defp schedule(state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    %{state | timer: Process.send_after(self(), :poll, state.settings.poll_ms)}
  end

  # Applies the segments between reads: the connection is closed, the file
  # changes in place, and a new connection (no cached page) is opened.
  defp apply_run(state, []), do: {:ok, state}

  defp apply_run(state, run) do
    state = close(state)

    with {:ok, state} <- apply_all(state, run),
         {:ok, conn} <- open(state) do
      {:ok, %{state | conn: conn}}
    else
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp apply_all(state, []), do: {:ok, state}

  defp apply_all(state, [item | rest] = run) do
    # A lower epoch than the chain so far ends it: a tail, or history lost.
    if item.epoch < state.epoch do
      if Chain.tail?(Enum.map(run, & &1.seg), [], state.txid),
        do: {:ok, state},
        else: {:error, :epoch}
    else
      case Native.apply_ltx(state.path, item.body, state.checksum, state.page_size) do
        {:ok, _header, post} ->
          apply_all(%{state | checksum: post, epoch: item.epoch, txid: item.seg.max}, rest)

        {:error, reason} ->
          {:error, {:apply, reason}}
      end
    end
  end

  defp open(state) do
    kib = state.settings.cache_kib

    with {:ok, conn} <-
           Db.open_read_only(state.path, timeout_ms: Db.script_timeout_ms(state.config)),
         :ok <- Alambique.Sqlite.execute(conn, "PRAGMA cache_size = -#{kib}"),
         :ok <- Alambique.Sqlite.execute(conn, "PRAGMA mmap_size = 0") do
      {:ok, conn}
    else
      {:error, reason} -> {:error, {:open, reason}}
    end
  end

  defp close(%{conn: nil} = state), do: state

  defp close(state) do
    Db.close(state.conn)
    %{state | conn: nil}
  end

  # Reads

  # Times are monotonic microseconds, the bound is in milliseconds. The margin
  # covers the drift between two monotonic clocks, `drift_ppm` of the bound
  # (rounded up), and the microsecond the clocks are read in.
  defp fresh?(state, q, bound) do
    limit = bound * 1_000
    margin = div(limit * state.settings.drift_ppm + 999_999, 1_000_000) + 1
    q - state.confirmed_at <= limit - margin
  end

  defp serve(state, read) do
    reply =
      Db.read_within(state.conn, read.sql, read.params, read.timeout, Db.limits(state.config))

    {:replica, reply, %{txid: state.txid, lag_ms: div(max(us() - state.confirmed_at, 0), 1_000)}}
  end

  defp serve_waiters(state) do
    {ready, waiting} = Enum.split_with(state.waiters, &fresh?(state, &1.q, &1.bound))

    for w <- ready do
      Process.cancel_timer(w.timer)
      GenServer.reply(w.from, serve(state, w))
    end

    %{state | waiters: waiting}
  end

  defp touch(state) do
    now = now()
    Manager.touch(state.config, state.key, now)
    %{state | last_read: now}
  end

  # Stop

  defp halt(state, reason) do
    if reason in [:empty, :max_bytes, :build_failed, :open_failed, :moved] do
      Manager.memo_put(state.config, {:refused, state.key}, true, state.settings.marker_ttl_ms)
    end

    {:stop, :shutdown, %{state | reason: reason}}
  end

  # The task stops first, so that nothing writes in the directory after it is
  # removed.
  defp finish(state) do
    if state.task, do: Task.shutdown(state.task.handle, :brutal_kill)
    state = close(%{state | task: nil})
    File.rm_rf(state.dir)
    state
  end

  defp now, do: System.monotonic_time(:millisecond)
  defp us, do: System.monotonic_time(:microsecond)
end
