defmodule Alambique.Nodes do
  @moduledoc """
  Registry of the nodes of a deployment in the store (design §22), so that
  nodes find each other through the bucket they already share. Discovery only:
  who owns a cell is decided by the lease and its fencing (design §5), never
  by this registry.

  Pure functions over the store: there is no process (`Alambique.NodeRegistry`
  is the one that renews). Every function takes a `t:ctx/0`, built with
  `ctx/1`.

  Each node is one JSON object under `<prefix>/nodes/<node_id>`, outside
  `cells/`, so no listing of the library reaches it. The object carries
  `node_id`, `incarnation`, `advertise`, `meta`, `started_at_ms`,
  `expires_at_ms`, `seq`, `token` and `released`. Each write bumps `seq`, so
  two writes never have the same bytes (S3 etags are content hashes), and
  `token` identifies one registration. Every rewrite is conditional on the
  etag the writer holds, so a node never overwrites a key somebody else took.

  An entry is live until `expires_at_ms` plus the skew margin of the reader
  and until it is released. Whoever can write under `<prefix>/nodes/` decides
  which addresses the nodes of the deployment connect to.

  Telemetry: see `docs/telemetry.md`.
  """

  alias Alambique.{Conditional, Telemetry}
  alias Alambique.Replication.Retry

  @enforce_keys [:node_id, :incarnation, :advertise, :seq, :token, :expires_at_ms]
  defstruct [
    :node_id,
    :incarnation,
    :advertise,
    :started_at_ms,
    :expires_at_ms,
    :seq,
    :token,
    :etag,
    meta: %{},
    released: false
  ]

  @type t :: %__MODULE__{
          node_id: String.t(),
          incarnation: String.t(),
          advertise: String.t(),
          meta: map,
          started_at_ms: integer | nil,
          expires_at_ms: integer,
          seq: pos_integer,
          token: String.t(),
          etag: Alambique.Store.etag() | nil,
          released: boolean
        }

  @typedoc """
    * `:store` - `{module, config}` implementing `Alambique.Store`
    * `:clock` - `{module, config}` implementing `Alambique.Clock`
    * `:prefix` - key prefix of the instance; entries live under `<prefix>/nodes/`
    * `:ttl_ms` - how long a registration or a renewal keeps the entry live
    * `:skew_margin_ms` - tolerated clock skew between nodes; `:ttl_ms` must
      be greater than twice this
    * `:page_size` - keys per store listing request (default 100)
  """
  @type ctx :: %{
          store: {module, term},
          clock: {module, term},
          prefix: String.t(),
          ttl_ms: pos_integer,
          skew_margin_ms: non_neg_integer,
          page_size: pos_integer
        }

  @type error :: {:error, term}

  @read_concurrency 8

  @doc "Builds and validates a context from the keys of `t:ctx/0`; raises `ArgumentError`."
  @spec ctx(keyword | map) :: ctx
  def ctx(opts) do
    ctx =
      opts
      |> Map.new()
      |> Map.take([:store, :clock, :prefix, :ttl_ms, :skew_margin_ms, :page_size])
      |> Map.put_new(:page_size, 100)

    valid? =
      match?(
        %{store: {_, _}, clock: {_, _}, prefix: p, page_size: n}
        when is_binary(p) and is_integer(n) and n > 0,
        ctx
      ) and
        Conditional.timing?(ctx[:ttl_ms], ctx[:skew_margin_ms])

    unless valid?, do: raise(ArgumentError, "invalid nodes context: #{inspect(ctx)}")
    ctx
  end

  @doc """
  Registers `node_id`, an instance (`incarnation`) that is reachable at
  `advertise`, an opaque string that the library never interprets.

  The key is created with `If-None-Match`. An entry that exists is rewritten
  with `If-Match` on its etag when it is released, expired (by this node's
  clock plus the skew margin) or written by the same `incarnation`. Another
  incarnation of the same node id that is still live is taken over too: that is
  a restart, faster than the TTL of the old entry; the old instance finds
  `{:error, :lost}` at its next `renew/2`. Options:

    * `:meta` - a JSON-encodable map for the application (default `%{}`)
    * `:takeover` - `:restart` (default) as above, or `:free`, which takes only a
      released or expired entry and answers `{:error, {:held, incarnation,
      expires_at_ms}}` for a live one of another incarnation. An instance that
      lost its entry uses `:free`, so two live instances with one node id do
      not take it from each other forever.

  A key written meanwhile gives `{:error, :contended}`; the caller may retry.
  """
  @spec register(ctx, String.t(), String.t(), String.t(), keyword) :: {:ok, t} | error
  def register(ctx, node_id, incarnation, advertise, opts \\ [])
      when is_binary(node_id) and is_binary(incarnation) and is_binary(advertise) do
    meta = Keyword.get(opts, :meta, %{})
    takeover = Keyword.get(opts, :takeover, :restart)

    unless is_map(meta) and takeover in [:restart, :free],
      do: raise(ArgumentError, "invalid register options: #{inspect(opts)}")

    mine = %__MODULE__{
      node_id: node_id,
      incarnation: incarnation,
      advertise: advertise,
      meta: meta,
      seq: 1,
      token: Conditional.token(),
      started_at_ms: now(ctx),
      expires_at_ms: now(ctx) + ctx.ttl_ms
    }

    case fetch(ctx, node_id) do
      {:error, :not_found} ->
        write(ctx, mine, :if_none_match, :new)

      {:ok, current} ->
        case takeover_kind(ctx, current, mine, takeover) do
          {:ok, kind} -> take(ctx, current, mine, kind)
          {:error, _} = held -> held
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp takeover_kind(ctx, current, mine, takeover) do
    cond do
      current.released -> {:ok, :released}
      dead?(ctx, current) -> {:ok, :expired}
      current.incarnation == mine.incarnation -> {:ok, :restart}
      takeover == :restart -> {:ok, :restart}
      true -> {:error, {:held, current.incarnation, current.expires_at_ms}}
    end
  end

  defp take(ctx, current, mine, kind) do
    if kind == :expired, do: expired(current, :register)
    write(ctx, %{mine | seq: current.seq + 1}, {:if_match, current.etag}, kind)
  end

  defp write(ctx, mine, condition, kind) do
    case put(ctx, mine, condition) do
      {:ok, etag} ->
        Telemetry.emit([:nodes, :registered], %{
          node_id: mine.node_id,
          incarnation: mine.incarnation,
          advertise: mine.advertise,
          takeover: kind
        })

        {:ok, %{mine | etag: etag}}

      {:error, :precondition_failed} ->
        {:error, :contended}

      {:error, reason} ->
        case fetch(ctx, mine.node_id) do
          {:ok, %{token: token} = stored} when token == mine.token -> {:ok, stored}
          _ -> {:error, reason}
        end
    end
  end

  @doc """
  Extends the entry: same registration, new expiry, `seq` + 1, written with
  `If-Match` on the etag `entry` holds. `{:error, :lost}` means somebody else
  wrote the key (another incarnation took it, or it was swept): the node is
  no longer registered. Any other error leaves `entry` usable for a retry, and
  the retry adopts a renewal that landed after all.
  """
  @spec renew(ctx, t) :: {:ok, t} | error
  def renew(ctx, %__MODULE__{} = entry) do
    next = %{entry | seq: entry.seq + 1, expires_at_ms: now(ctx) + ctx.ttl_ms}

    case put(ctx, next, {:if_match, entry.etag}) do
      {:ok, etag} ->
        Telemetry.emit([:nodes, :renewed], %{
          node_id: next.node_id,
          incarnation: next.incarnation,
          seq: next.seq
        })

        {:ok, %{next | etag: etag}}

      {:error, reason} ->
        reconcile_renew(ctx, entry, reason)
    end
  end

  defp reconcile_renew(ctx, entry, reason) do
    case fetch(ctx, entry.node_id) do
      {:ok, stored} ->
        cond do
          stored.token != entry.token or stored.released -> lost(entry, :renew)
          stored.seq > entry.seq -> {:ok, stored}
          true -> {:error, reason}
        end

      {:error, :not_found} ->
        lost(entry, :renew)

      {:error, read_reason} ->
        {:error, read_reason}
    end
  end

  @doc """
  Marks the entry released, so it leaves `list/1` at once and the next node of
  that id registers without waiting for the TTL. It is a conditional write on
  the entry's etag, not a delete: the store cannot delete conditionally, and an
  unconditional delete could remove the entry of a node that took the id
  meanwhile. `sweep/2` removes the released entries later. Releasing an entry
  that is already released is `:ok`; `{:error, :lost}` means somebody else wrote
  the key.
  """
  @spec deregister(ctx, t) :: :ok | error
  def deregister(ctx, %__MODULE__{} = entry), do: release(ctx, entry, true)

  defp release(ctx, entry, retry?) do
    case put(ctx, %{entry | seq: entry.seq + 1, released: true}, {:if_match, entry.etag}) do
      {:ok, _etag} ->
        :ok

      {:error, reason} ->
        case fetch(ctx, entry.node_id) do
          {:ok, %{token: token}} when token != entry.token -> lost(entry, :deregister)
          {:ok, %{released: true}} -> :ok
          {:ok, stored} when stored.seq > entry.seq and retry? -> release(ctx, stored, false)
          {:ok, _} -> {:error, reason}
          {:error, :not_found} -> lost(entry, :deregister)
          {:error, read_reason} -> {:error, read_reason}
        end
    end
  end

  @doc """
  The live nodes, sorted by node id: every entry under `<prefix>/nodes/` that is
  not released and has not expired by this node's clock plus the skew margin.
  The store listing is paginated and the entries are read a few at a time. An
  object that is not a node entry is skipped.
  """
  @spec list(ctx) :: {:ok, [t]} | error
  def list(ctx) do
    with {:ok, entries} <- entries(ctx),
         do: {:ok, entries |> Enum.reject(&dead?(ctx, &1)) |> Enum.sort_by(& &1.node_id)}
  end

  @doc """
  Deletes the entries that are released or expired, never a live one, and
  returns how many. Each entry is read again just before it is deleted. Option
  `:limit` caps the deletions of one call (default: no cap).

  The store has no conditional delete, so a node that registers in the instant
  between the read and the delete loses its entry; its next `renew/2` answers
  `{:error, :lost}` and the node registers again.
  """
  @spec sweep(ctx, keyword) :: {:ok, %{removed: non_neg_integer}} | error
  def sweep(ctx, opts \\ []) do
    limit = Keyword.get(opts, :limit, :infinity)

    with {:ok, entries} <- entries(ctx) do
      entries
      |> Enum.filter(&dead?(ctx, &1))
      |> take(limit)
      |> Enum.reduce_while({:ok, 0}, fn entry, {:ok, removed} ->
        case remove(ctx, entry) do
          {:ok, n} -> {:cont, {:ok, removed + n}}
          {:error, _} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, removed} -> {:ok, %{removed: removed}}
        error -> error
      end
    end
  end

  defp take(entries, :infinity), do: entries
  defp take(entries, limit), do: Enum.take(entries, limit)

  defp remove(ctx, entry) do
    case fetch(ctx, entry.node_id) do
      {:ok, current} ->
        if dead?(ctx, current) do
          with :ok <- Conditional.delete(ctx, key(ctx, entry.node_id)) do
            unless current.released, do: expired(current, :sweep)
            {:ok, 1}
          end
        else
          {:ok, 0}
        end

      {:error, :not_found} ->
        {:ok, 0}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Whether nobody can be using the entry: released, or expired for this node's
  # clock with the skew margin. A node whose clock runs ahead sees it dead early
  # and may only take it over early; `ttl > 2 * skew` keeps a renewal in time.
  defp dead?(ctx, entry),
    do: entry.released or now(ctx) > entry.expires_at_ms + ctx.skew_margin_ms

  defp expired(entry, by) do
    Telemetry.emit([:nodes, :expired], %{
      node_id: entry.node_id,
      incarnation: entry.incarnation,
      expires_at_ms: entry.expires_at_ms,
      by: by
    })
  end

  defp lost(entry, op) do
    Telemetry.emit([:nodes, :lost], %{
      node_id: entry.node_id,
      incarnation: entry.incarnation,
      op: op
    })

    {:error, :lost}
  end

  # Store and encoding

  defp key(ctx, node_id), do: "#{prefix(ctx)}#{node_id}"
  defp prefix(ctx), do: "#{ctx.prefix}/nodes/"

  defp put(ctx, entry, condition),
    do: Conditional.put(ctx, key(ctx, entry.node_id), encode(entry), condition)

  defp fetch(ctx, node_id), do: read(ctx, node_id, key(ctx, node_id))

  # An object that does not decode is read as a released entry with `seq` 0, so
  # a node can take its key and `sweep/2` removes it.
  defp read(ctx, node_id, key) do
    with {:ok, body, etag} <- Conditional.get(ctx, key) do
      {:ok, decode(body, etag) || junk(node_id, etag)}
    end
  end

  defp junk(node_id, etag) do
    %__MODULE__{
      node_id: node_id,
      incarnation: "",
      advertise: "",
      seq: 0,
      token: "",
      expires_at_ms: 0,
      etag: etag,
      released: true
    }
  end

  defp entries(ctx) do
    with {:ok, keys} <- keys(ctx, nil, []) do
      keys
      |> Task.async_stream(&read(ctx, String.replace_prefix(&1, prefix(ctx), ""), &1),
        max_concurrency: @read_concurrency,
        timeout: :infinity
      )
      |> Enum.reduce_while({:ok, []}, fn
        {:ok, {:ok, entry}}, {:ok, acc} -> {:cont, {:ok, [entry | acc]}}
        {:ok, {:error, :not_found}}, acc -> {:cont, acc}
        {:ok, {:error, reason}}, _acc -> {:halt, {:error, reason}}
      end)
    end
  end

  defp keys(%{store: {store, config}} = ctx, start_after, acc) do
    page = fn -> store.list(config, prefix(ctx), start_after, ctx.page_size) end

    case Retry.once(ctx.store, ctx.ttl_ms - ctx.skew_margin_ms, page) do
      {:ok, keys, nil} -> {:ok, Enum.concat(Enum.reverse([keys | acc]))}
      {:ok, keys, next} -> keys(ctx, next, [keys | acc])
      {:error, reason} -> {:error, reason}
    end
  end

  defp now(ctx), do: Conditional.now(ctx)

  defp encode(entry) do
    Jason.encode!(%{
      node_id: entry.node_id,
      incarnation: entry.incarnation,
      advertise: entry.advertise,
      meta: entry.meta,
      started_at_ms: entry.started_at_ms,
      expires_at_ms: entry.expires_at_ms,
      seq: entry.seq,
      token: entry.token,
      released: entry.released
    })
  end

  # Unknown fields are ignored; `nil` is an object that is not a node entry.
  defp decode(body, etag) do
    with {:ok, %{} = map} <- Jason.decode(body),
         %{
           "node_id" => node_id,
           "incarnation" => incarnation,
           "advertise" => advertise,
           "expires_at_ms" => expires,
           "seq" => seq,
           "token" => token,
           "released" => released
         }
         when is_binary(node_id) and is_binary(incarnation) and is_binary(advertise) and
                is_integer(expires) and is_integer(seq) and is_binary(token) and
                is_boolean(released) <- map do
      %__MODULE__{
        node_id: node_id,
        incarnation: incarnation,
        advertise: advertise,
        meta: if(is_map(map["meta"]), do: map["meta"], else: %{}),
        started_at_ms: map["started_at_ms"],
        expires_at_ms: expires,
        seq: seq,
        token: token,
        etag: etag,
        released: released
      }
    else
      _ -> nil
    end
  end
end
