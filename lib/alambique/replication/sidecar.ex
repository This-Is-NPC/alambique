defmodule Alambique.Replication.Sidecar do
  @moduledoc false
  # Page-hash sidecars of segments and snapshots (design §16): the object
  # `<key>.pages` that holds one hash per page and one of every other byte of
  # the object `<key>`. Every writer of an object calls `write/7` right after
  # the object landed, so the order is always object, then sidecar. A crash in
  # between leaves an object without a sidecar, which a restore reads whole.

  alias Alambique.Fault
  alias Alambique.Replication.{Keys, Native, Objects}
  alias Alambique.Telemetry

  # The file checksum is the last 8 bytes of an LTX file.
  @checksum 8

  @doc """
  Writes the sidecar of `object` (the bytes of the LTX file at `key`, of
  `level`) when the context asks for sidecars and the object is larger than
  `ctx.page_sidecar` (smaller files are read whole and checked before they
  are served anyway). `:ours` says the stored object is the one just written;
  `:verify` says another writer may have written it, and the sidecar is
  written only when the stored file checksum is the one of `object`.

  A sidecar that already exists and differs is replaced (If-Match): the
  object is ours, so it is a stale one. A failure never fails the caller: it
  is retried once within the deadline and then reported.
  """
  @spec write(map, String.t(), 0..9, binary, map, integer, :ours | :verify) ::
          :ok | :skipped | {:error, term}
  def write(ctx, key, level, object, info, deadline, owner) do
    if enabled?(ctx, byte_size(object)) do
      ctx = %{ctx | max_attempts: min(ctx.max_attempts, 2)}

      case owner == :ours or same_object?(ctx, key, object, deadline) do
        true -> report(key, put(ctx, key, level, object, info, deadline))
        false -> :skipped
        {:error, _} = error -> report(key, error)
      end
    else
      :skipped
    end
  end

  defp enabled?(%{page_sidecar: min}, size) when is_integer(min), do: size > min
  defp enabled?(_ctx, _size), do: false

  defp put(ctx, key, level, object, info, deadline) do
    Fault.point(:repl_before_sidecar, %{key: key})
    meta = Objects.meta(ctx, info)

    with {:ok, side} <- Native.page_sidecar(level, object),
         do: store(ctx, Keys.sidecar(key), side, meta, deadline)
  end

  defp store(ctx, skey, side, meta, deadline) do
    case Objects.put(ctx, skey, side, :if_none_match, meta, deadline) do
      :exists -> replace(ctx, skey, side, meta, deadline)
      other -> other
    end
  end

  defp replace(ctx, skey, side, meta, deadline) do
    case Objects.fetch_optional(ctx, skey, deadline) do
      {:ok, ^side, _} ->
        :ok

      {:ok, _stale, %{"etag" => etag}} ->
        with :exists <- Objects.put(ctx, skey, side, {:if_match, etag}, meta, deadline), do: :ok

      :none ->
        with :exists <- Objects.put(ctx, skey, side, :if_none_match, meta, deadline), do: :ok

      other ->
        other
    end
  end

  # The key exists: it is ours only if it is the same file.
  defp same_object?(ctx, key, object, deadline) do
    size = byte_size(object)
    mine = binary_part(object, size - @checksum, @checksum)

    case Objects.fetch_range(ctx, key, size - @checksum, @checksum, deadline) do
      {:ok, ^mine, ^size, _meta} -> true
      {:ok, _other, _total, _meta} -> false
      {:error, _} = error -> error
    end
  end

  defp report(_key, :ok), do: :ok

  defp report(key, {:error, reason} = error) do
    Telemetry.emit([:replication, :sidecar], %{key: key, result: :write_failed, reason: reason})
    error
  end
end
