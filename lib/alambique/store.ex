defmodule Alambique.Store do
  @moduledoc """
  The only interface to object storage (design §3).

  `get_range/4` returns up to `length` bytes starting at `offset`, the size
  of the whole object and its metadata. The body is shorter at the end of the object and empty
  when `offset` is at or past it. A store puts the ETag of the object that
  served the read in the metadata, under the key `"etag"`: a lazy restore
  pins the ranged reads of an object to the ETag it staged (design §16).
  A store without it gives no such pin.

  A store can implement `c:get_range/5`, a `get_range` that is applied only
  when the ETag of the object is the one given, as `If-Match` does for a
  `put`; any other ETag is `{:error, :precondition_failed}`. Without it the
  library reads with `c:get_range/4` and compares the ETag in the metadata.

  A store can implement `c:get/3`, a `get` that gives up on an object larger
  than `max_bytes` with `{:error, {:too_large, size, max_bytes}}` before it
  holds the body in memory (`Alambique.Store.S3` stops at the `Content-Length`).
  The library reads every object of the bucket with a limit (design §3);
  without `c:get/3` the whole object is read and its size checked after.

  A callback that raises, throws or exits is treated as a failed request: the
  library wraps the store and turns it into `{:error, reason}`.

  A failed `put` may or may not have been applied: callers must treat any
  `{:error, reason}` other than `:precondition_failed` as unknown.

  A store whose config holds credentials can implement `c:conceal/1`: it
  returns the config with the credentials in closures, which print as
  `#Function<...>`. The library applies it to the `:store` option, so crash
  reports, process dumps and errors never show a credential (design §3).

  A store can also implement `c:location/1`: a term naming where it keeps its
  data, equal on every node that shares it and free of credentials. Nodes use
  it to tell stores apart (design §8). Without it the whole config is used,
  credentials included, so nodes of one deployment whose configs differ would
  count as different stores. A store that wraps another delegates to
  `location/1` of the inner store.
  """

  @type config :: term
  @type key :: String.t()
  @type etag :: String.t()
  @type condition :: :none | :if_none_match | {:if_match, etag}
  @type meta :: %{optional(String.t()) => String.t()}

  @callback put(config, key, body :: iodata(), condition, meta) ::
              {:ok, etag} | {:error, :precondition_failed} | {:error, term}
  @callback get(config, key) ::
              {:ok, body :: binary, etag, meta} | {:error, :not_found} | {:error, term}
  @callback head(config, key) ::
              {:ok, etag, meta} | {:error, :not_found} | {:error, term}
  @callback get_range(config, key, offset :: non_neg_integer, length :: pos_integer) ::
              {:ok, body :: binary, total_size :: non_neg_integer, meta}
              | {:error, :not_found}
              | {:error, term}
  @callback get_range(config, key, offset :: non_neg_integer, length :: pos_integer, etag) ::
              {:ok, body :: binary, total_size :: non_neg_integer, meta}
              | {:error, :not_found}
              | {:error, :precondition_failed}
              | {:error, term}
  @callback get(config, key, max_bytes :: pos_integer) ::
              {:ok, body :: binary, etag, meta}
              | {:error, :not_found}
              | {:error, {:too_large, size :: non_neg_integer, max_bytes :: pos_integer}}
              | {:error, term}
  @callback list(config, prefix :: key, start_after :: key | nil, limit :: pos_integer) ::
              {:ok, [key], next_start_after :: key | nil} | {:error, term}
  @callback delete(config, key) :: :ok | {:error, term}
  @callback location(config) :: term
  @callback conceal(config) :: config

  @optional_callbacks get: 3, get_range: 5, location: 1, conceal: 1

  @doc "Where `store` keeps its data: its `c:location/1`, or its config without one."
  @spec location({module, config}) :: term
  def location({module, config}) do
    if Code.ensure_loaded?(module) and function_exported?(module, :location, 1),
      do: module.location(config),
      else: config
  end

  @doc "`store` with the credentials of its config hidden: its `c:conceal/1`, or itself."
  @spec conceal({module, config}) :: {module, config}
  def conceal({module, config} = store) do
    if Code.ensure_loaded?(module) and function_exported?(module, :conceal, 1),
      do: {module, module.conceal(config)},
      else: store
  end

  def conceal(other), do: other

  @doc """
  `get` of an object of at most `max_bytes`; a larger one is
  `{:error, {:too_large, size, max_bytes}}` (and a `[:alambique, :store,
  :too_large]` event). Final: retrying cannot make the object smaller.
  """
  @spec get_capped({module, config}, key, pos_integer) ::
          {:ok, binary, etag, meta} | {:error, term}
  def get_capped(store, key, max_bytes) do
    case call_get(store, key, max_bytes) do
      {:error, {:too_large, size, _max}} = error ->
        too_large(key, size, max_bytes)
        error

      other ->
        other
    end
  end

  @doc "Like `get_capped/3`, without the event: for a store that wraps another."
  @spec call_get({module, config}, key, pos_integer) ::
          {:ok, binary, etag, meta} | {:error, term}
  def call_get({module, config}, key, max_bytes) do
    if Code.ensure_loaded?(module) and function_exported?(module, :get, 3) do
      module.get(config, key, max_bytes)
    else
      case module.get(config, key) do
        {:ok, body, _etag, _meta} when byte_size(body) > max_bytes ->
          {:error, {:too_large, byte_size(body), max_bytes}}

        other ->
          other
      end
    end
  end

  @doc """
  `get_range` of the object with ETag `etag` (any object when `nil`): the
  store's `c:get_range/5`, or its `c:get_range/4` for a store without it, whose
  answer the caller checks against the ETag in its metadata.
  """
  @spec call_get_range({module, config}, key, non_neg_integer, pos_integer, etag | nil) ::
          {:ok, binary, non_neg_integer, meta} | {:error, term}
  def call_get_range({module, config}, key, offset, length, etag) do
    if etag != nil and Code.ensure_loaded?(module) and
         function_exported?(module, :get_range, 5) do
      module.get_range(config, key, offset, length, etag)
    else
      module.get_range(config, key, offset, length)
    end
  end

  @doc "Reports an object of `size` bytes, over its cap of `max_bytes`, that was not read."
  @spec too_large(key, non_neg_integer, pos_integer) :: :ok
  def too_large(key, size, max_bytes),
    do: Alambique.Telemetry.emit([:store, :too_large], %{size: size, max: max_bytes}, %{key: key})

  @doc "Default cap of the objects the library reads whole: segments, snapshots and feed objects."
  @spec max_object_bytes :: pos_integer
  def max_object_bytes, do: 1_073_741_824

  @doc "Default cap of the small objects: leases, node entries, markers, manifests, crash counters."
  @spec max_control_bytes :: pos_integer
  def max_control_bytes, do: 16_777_216
end
