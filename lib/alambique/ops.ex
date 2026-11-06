defmodule Alambique.Ops do
  @moduledoc """
  What the `mix alambique.*` tasks do, as functions (design §10).

  They work on the store alone, with an `Alambique.Replication.ctx/1`, so they
  need no running instance and can be used from a release console too. Moving
  a cell, or between locations, is the exception: it needs the nodes that run
  the instance, see `move/5` and `relocate/5`.
  """

  alias Alambique.{Freeze, Lease, Quarantine, Replication}
  alias Alambique.Replication.{Objects, Retry}

  @type inspection :: %{
          cell: String.t(),
          prefix: String.t(),
          frozen: boolean,
          crashes: Quarantine.counter() | nil,
          lease: map | nil,
          head: map | nil,
          levels: [map],
          snapshots: [map]
        }

  @doc """
  Reads everything the store knows about `cell`: freeze marker, crashes, lease, head,
  and the segments by level and the snapshots with their sizes.

  Options: `:sizes` (default `true`) fetches every object to measure it, which
  is slow for cells with many files; with `false` sizes are `nil`.
  `:timeout_ms` bounds the whole inspection.
  """
  @spec describe(Replication.ctx(), String.t(), keyword) :: {:ok, inspection} | {:error, term}
  def describe(ctx, cell, opts \\ []) do
    deadline = Retry.deadline_in(Keyword.get(opts, :timeout_ms, 5 * ctx.timeout_ms))
    sizes? = Keyword.get(opts, :sizes, true)

    with {:ok, frozen} <- Freeze.frozen?(ctx, cell, deadline: deadline),
         {:ok, crashes} <- Quarantine.read(ctx, cell, deadline: deadline),
         {:ok, lease} <- lease(ctx, cell),
         {:ok, head} <- Replication.head(ctx, cell, timeout_ms: remaining(deadline)),
         {:ok, {segments, snapshots}} <- Objects.list_cell(ctx, cell, deadline) do
      {:ok,
       %{
         cell: cell,
         prefix: ctx.prefix,
         frozen: frozen,
         crashes: crashes,
         lease: lease,
         head: head,
         levels: levels(ctx, segments, sizes?, deadline),
         snapshots: Enum.map(snapshots, &snapshot(ctx, &1, sizes?, deadline))
       }}
    end
  end

  defp lease(ctx, cell) do
    lease_ctx =
      Lease.ctx(
        store: ctx.store,
        clock: ctx.clock,
        prefix: ctx.prefix,
        ttl_ms: 30_000,
        skew_margin_ms: 2_000
      )

    case Lease.read(lease_ctx, cell) do
      :none ->
        {:ok, nil}

      {:ok, l} ->
        {clock, config} = ctx.clock
        now = clock.now_ms(config)

        state =
          cond do
            l.released -> :released
            l.expires_at_ms < now -> :expired
            true -> :held
          end

        {:ok,
         %{
           epoch: l.epoch,
           owner: l.owner,
           state: state,
           moved_to: l.moved_to,
           expires_at_ms: l.expires_at_ms,
           expires_in_ms: l.expires_at_ms - now
         }}

      {:error, _} = error ->
        error
    end
  end

  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 1)

  defp levels(ctx, segments, sizes?, deadline) do
    segments
    |> Enum.group_by(& &1.level)
    |> Enum.sort()
    |> Enum.map(fn {level, files} ->
      %{
        level: level,
        files: length(files),
        min: files |> Enum.map(& &1.min) |> Enum.min(),
        max: files |> Enum.map(& &1.max) |> Enum.max(),
        bytes: sizes? && sum_sizes(ctx, files, deadline)
      }
    end)
  end

  defp sum_sizes(ctx, files, deadline) do
    files
    |> Task.async_stream(&size(ctx, &1.key, deadline), max_concurrency: 8, timeout: :infinity)
    |> Enum.reduce(0, fn
      {:ok, {:ok, {bytes, _meta}}}, acc -> acc + bytes
      _, acc -> acc
    end)
  end

  defp snapshot(ctx, snap, sizes?, deadline) do
    base = %{txid: snap.txid, bytes: nil, epoch: nil, ts: nil, created: nil}

    case sizes? and size(ctx, snap.key, deadline) do
      {:ok, {bytes, meta}} -> info(%{base | bytes: bytes}, meta)
      _ -> with_meta(ctx, snap, base, deadline)
    end
  end

  defp with_meta(ctx, snap, base, deadline) do
    case Objects.head(ctx, snap.key, deadline) do
      {:ok, meta} -> info(base, meta)
      _ -> base
    end
  end

  defp info(base, meta) do
    %{
      base
      | epoch: Objects.epoch_of(meta),
        ts: Objects.int(meta, "ts"),
        created: Objects.int(meta, "created")
    }
  end

  defp size(ctx, key, deadline) do
    case Objects.fetch(ctx, key, deadline) do
      {:ok, body, meta} -> {:ok, {byte_size(body), meta}}
      {:error, _} = error -> error
    end
  end

  @doc """
  Restores `cell` into the file `path` and returns what `Replication.restore/5`
  returns.
  """
  @spec restore(Replication.ctx(), String.t(), Path.t(), Replication.target(), keyword) ::
          {:ok, Replication.Restore.result()} | {:error, term}
  def restore(ctx, cell, path, target, opts \\ []) do
    Replication.restore(ctx, cell, path, target, opts)
  end

  @doc """
  Parses a restore target: `"latest"`, `"txid:42"`, or a time as an ISO 8601
  UTC date-time (`2026-10-26T12:00:00Z`) or Unix milliseconds (`ms:1790000000000`).
  """
  @spec parse_target(String.t()) :: {:ok, Replication.target()} | :error
  def parse_target("latest"), do: {:ok, :latest}

  def parse_target("txid:" <> n) do
    case Integer.parse(n) do
      {txid, ""} when txid >= 0 -> {:ok, {:txid, txid}}
      _ -> :error
    end
  end

  def parse_target("ms:" <> ms) do
    case Integer.parse(ms) do
      {value, ""} -> {:ok, {:timestamp, value}}
      _ -> :error
    end
  end

  def parse_target(text) do
    case DateTime.from_iso8601(text) do
      {:ok, datetime, _offset} -> {:ok, {:timestamp, DateTime.to_unix(datetime, :millisecond)}}
      _ -> :error
    end
  end

  @doc """
  Moves `cell` to `target` through a node that runs the instance: that node
  hibernates the cell at its owner and `target` wakes it.
  """
  @spec move(node, atom, String.t(), node, timeout) :: {:ok, node} | {:error, term}
  def move(via, instance, cell, target, timeout \\ 15_000) do
    call(via, Alambique, :move, [instance, cell, target, [timeout: timeout]], timeout + 2_000)
  end

  @doc """
  Moves `cell` to the location `to` (an id of the placement) through a node
  that runs the instance, which does the whole move (`Alambique.relocate/4`).
  """
  @spec relocate(node, atom, String.t(), String.t(), timeout) ::
          {:ok, Alambique.Location.t()} | {:error, term}
  def relocate(via, instance, cell, to, timeout \\ 60_000) do
    call(via, Alambique, :relocate, [instance, cell, to, [timeout: timeout]], timeout + 5_000)
  end

  @doc "The id of the location the placement gives `cell`, asked through a node of the instance."
  @spec locate(node, atom, String.t(), timeout) :: {:ok, String.t()} | {:error, term}
  def locate(via, instance, cell, timeout \\ 15_000) do
    call(via, __MODULE__, :where, [instance, cell], timeout)
  end

  @doc false
  def where(instance, cell) do
    with {:ok, location} <-
           Alambique.Placement.locate(Alambique.Config.fetch!(instance), cell, fresh: true),
         do: {:ok, location.id}
  end

  @doc """
  Deletes what `cell` left at the location `from` through a node that runs the
  instance (`Alambique.cleanup_moved/4`).
  """
  @spec cleanup_moved(node, atom, String.t(), String.t(), keyword, timeout) ::
          {:ok, map} | {:error, term}
  def cleanup_moved(via, instance, cell, from, opts \\ [], timeout \\ 60_000) do
    call(via, Alambique, :cleanup_moved, [instance, cell, from, opts], timeout)
  end

  @doc false
  # `:erpc.call/5` that answers a failure as `{:error, _}`: the call itself
  # (`{:erpc, reason}`) or an exception raised on the node (`{:remote, text}`).
  def call(via, module, function, args, timeout) do
    :erpc.call(via, module, function, args, timeout)
  catch
    :error, {:erpc, reason} ->
      {:error, {:erpc, reason}}

    :error, {:exception, reason, stack} ->
      {:error, {:remote, Exception.format_banner(:error, reason, stack)}}
  end
end
