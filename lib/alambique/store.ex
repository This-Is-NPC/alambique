defmodule Alambique.Store do
  @moduledoc """
  The only interface to object storage (design §3).

  `get_range/4` returns up to `length` bytes starting at `offset`, the size
  of the whole object and its metadata. The body is shorter at the end of the object and empty
  when `offset` is at or past it.

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
  @callback list(config, prefix :: key, start_after :: key | nil, limit :: pos_integer) ::
              {:ok, [key], next_start_after :: key | nil} | {:error, term}
  @callback delete(config, key) :: :ok | {:error, term}
  @callback location(config) :: term
  @callback conceal(config) :: config

  @optional_callbacks location: 1, conceal: 1

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
end
