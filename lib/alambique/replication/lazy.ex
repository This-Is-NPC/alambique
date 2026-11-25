defmodule Alambique.Replication.Lazy do
  @moduledoc false
  # A lazily restored database (design §16): the page map built by
  # `Restore` (a NIF resource the VFS reads) and where its frames live in the
  # store. `fetch_run/3` is the one way a page gets from the store into the file;
  # `check_objects/3` compares the pages it installed with the whole objects.

  alias Alambique.Replication.{Budget, Native, Objects, Retry}

  @enforce_keys [:handle, :files, :pins, :ctx, :budget]
  defstruct [
    :handle,
    :files,
    :pins,
    :ctx,
    :budget,
    :checksum,
    :cell,
    miss_pages: 16,
    batch_bytes: 4_194_304,
    verify_bytes: 0
  ]

  @type t :: %__MODULE__{
          handle: reference,
          files: tuple,
          pins: tuple,
          ctx: map,
          budget: Budget.t(),
          checksum: integer | nil,
          cell: String.t() | nil,
          miss_pages: pos_integer,
          batch_bytes: pos_integer,
          verify_bytes: non_neg_integer
        }

  @doc "The lazy part of a `Restore` result."
  @spec new(map, map, keyword) :: t
  def new(%{lazy: handle, files: files, pins: pins} = result, ctx, opts \\ []) do
    lazy =
      struct!(
        __MODULE__,
        [
          handle: handle,
          files: List.to_tuple(files),
          pins: List.to_tuple(pins),
          ctx: ctx,
          budget: Budget.new(ctx.max_inflight_bytes),
          checksum: result.checksum,
          cell: result.cell
        ] ++ opts
      )

    :ok = Native.lazy_set_whole(handle, lazy.verify_bytes)
    lazy
  end

  @doc """
  Reads one run of frames with a single ranged read and installs its pages.
  Returns `{:ok, done}`, where `done` says that this was the last missing page.
  The run waits for room in the budget of the cell, so the runs under way hold
  at most `ctx.max_inflight_bytes` (a longer run is read alone).

  The read is pinned to the object as the restore staged it: its ETag, its
  size and its epoch. A different one is `{:error, {:changed, key}}`.
  """
  @spec fetch_run(t, {non_neg_integer, non_neg_integer, pos_integer, list}, integer) ::
          {:ok, boolean} | {:error, term}
  def fetch_run(%__MODULE__{} = lazy, {file, offset, len, pages}, deadline) do
    key = elem(lazy.files, file)
    {etag, epoch, size} = elem(lazy.pins, file)
    weight = Budget.weigh(lazy.budget, len)

    with :ok <- Budget.acquire(lazy.budget, weight, deadline) do
      try do
        case fetch_range(lazy.ctx, key, offset, len, etag, deadline) do
          {:ok, body, total, meta} ->
            cond do
              total != size or Objects.epoch_of(meta) != epoch -> {:error, {:changed, key}}
              byte_size(body) != len -> {:error, {:short_read, key}}
              true -> Native.lazy_deliver(lazy.handle, file, body, pages)
            end

          {:error, reason} ->
            {:error, if(reason == :not_found, do: {:not_found, key}, else: reason)}
        end
      after
        Budget.release(lazy.budget, weight)
      end
    end
  end

  # A store that raises or exits is a failed read, not a crash of the caller.
  defp fetch_range(ctx, key, offset, len, etag, deadline) do
    Objects.fetch_range(ctx, key, offset, len, deadline, etag)
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  @doc """
  Reads every object of the chain whole, once every page is local, and
  compares each with the pages the ranged reads installed: the object must
  pass its file checksum, and each page it provides to the file must be the
  one that was read. This is what the database checksum cannot say, because
  errors can cancel in it. Up to `concurrency` objects are read at once, each
  within `fetch_ms`. Returns the objects and bytes read.
  """
  @spec check_objects(t, pos_integer, pos_integer) ::
          {:ok, %{objects: non_neg_integer, bytes: non_neg_integer}} | {:error, term}
  def check_objects(%__MODULE__{} = lazy, concurrency, fetch_ms) do
    0..(tuple_size(lazy.files) - 1)//1
    |> Task.async_stream(&check_object(lazy, &1, fetch_ms),
      max_concurrency: concurrency,
      timeout: :infinity
    )
    |> Enum.reduce_while({:ok, %{objects: 0, bytes: 0}}, fn
      {:ok, {:ok, bytes}}, {:ok, sum} ->
        {:cont, {:ok, %{objects: sum.objects + 1, bytes: sum.bytes + bytes}}}

      {:ok, {:error, _} = error}, _ ->
        {:halt, error}

      {:exit, reason}, _ ->
        {:halt, {:error, {:exit, reason}}}
    end)
  end

  defp check_object(lazy, file, fetch_ms) do
    key = elem(lazy.files, file)
    {_etag, _epoch, size} = elem(lazy.pins, file)
    deadline = Retry.deadline_in(fetch_ms)
    weight = Budget.weigh(lazy.budget, size)

    with :ok <- Budget.acquire(lazy.budget, weight, deadline) do
      try do
        with {:ok, body} <- fetch_whole(lazy.ctx, key, deadline),
             :ok <- if(byte_size(body) == size, do: :ok, else: {:error, {:changed, key}}),
             {:ok, _pages} <- Native.lazy_check_object(lazy.handle, file, body) do
          {:ok, byte_size(body)}
        end
      after
        Budget.release(lazy.budget, weight)
      end
    end
  end

  defp fetch_whole(ctx, key, deadline) do
    case Objects.fetch_optional(ctx, key, deadline) do
      {:ok, body, _meta} -> {:ok, body}
      :none -> {:error, {:not_found, key}}
      {:error, _} = error -> error
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  @doc """
  Makes `pages` local and records their checksums, in parallel, for the
  replicator, which needs the old checksum of every page it replaces.
  """
  @spec fetch_pages(t, [pos_integer], integer) :: :ok | {:error, term}
  def fetch_pages(%__MODULE__{} = lazy, pages, deadline) do
    pages
    |> Task.async_stream(&fetch_page(lazy, &1, deadline),
      max_concurrency: 8,
      timeout: :infinity
    )
    |> Enum.find_value(:ok, fn
      {:ok, :ok} -> nil
      {:ok, {:error, _} = error} -> error
      {:exit, reason} -> {:error, {:exit, reason}}
    end)
  end

  @doc "Makes one missed page local, with the pages after it in the same file."
  @spec fetch_page(t, pos_integer, integer) :: :ok | {:error, term}
  def fetch_page(%__MODULE__{} = lazy, pgno, deadline) do
    case Native.lazy_miss_run(lazy.handle, pgno, lazy.miss_pages) do
      :none ->
        :ok

      run ->
        with {:ok, _done} <- fetch_run(lazy, run, deadline), do: :ok
    end
  end
end
