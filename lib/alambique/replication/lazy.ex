defmodule Alambique.Replication.Lazy do
  @moduledoc false
  # A lazily restored database (design §16): the page map built by
  # `Restore` (a NIF resource the VFS reads) and where its frames live in the
  # store. `fetch_run/3` is the one way a page gets from the store into the file.

  alias Alambique.Replication.{Native, Objects}

  @enforce_keys [:handle, :files, :ctx]
  defstruct [:handle, :files, :ctx, :checksum, :cell, miss_pages: 16, batch_bytes: 4_194_304]

  @type t :: %__MODULE__{
          handle: reference,
          files: tuple,
          ctx: map,
          checksum: integer | nil,
          cell: String.t() | nil,
          miss_pages: pos_integer,
          batch_bytes: pos_integer
        }

  @doc "The lazy part of a `Restore` result."
  @spec new(map, map, keyword) :: t
  def new(%{lazy: handle, files: files, checksum: checksum, cell: cell}, ctx, opts \\ []) do
    struct!(
      __MODULE__,
      [handle: handle, files: List.to_tuple(files), ctx: ctx, checksum: checksum, cell: cell] ++
        opts
    )
  end

  @doc """
  Reads one run of frames with a single ranged read and installs its pages.
  Returns `{:ok, done}`, where `done` says that this was the last missing page.
  """
  @spec fetch_run(t, {non_neg_integer, non_neg_integer, pos_integer, list}, integer) ::
          {:ok, boolean} | {:error, term}
  def fetch_run(%__MODULE__{} = lazy, {file, offset, len, pages}, deadline) do
    key = elem(lazy.files, file)

    case fetch_range(lazy.ctx, key, offset, len, deadline) do
      {:ok, body, _total, _meta} when byte_size(body) == len ->
        Native.lazy_deliver(lazy.handle, body, pages)

      {:ok, _short, _total, _meta} ->
        {:error, {:short_read, key}}

      {:error, reason} ->
        {:error, if(reason == :not_found, do: {:not_found, key}, else: reason)}
    end
  end

  # A store that raises or exits is a failed read, not a crash of the caller.
  defp fetch_range(ctx, key, offset, len, deadline) do
    Objects.fetch_range(ctx, key, offset, len, deadline)
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
