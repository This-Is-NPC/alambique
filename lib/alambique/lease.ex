defmodule Alambique.Lease do
  @moduledoc """
  Lease with epochs and fencing (design §5).

  Pure functions over the store: there is no process. The cell process calls
  them and schedules renewals. Every function takes a `t:ctx/0`, built with
  `ctx/1`.

  The lease object is JSON under `<prefix>/cells/<cell>/lease`. Each write
  bumps `seq`, so two writes never have the same bytes (S3 etags are content
  hashes). `token` identifies one acquisition; `{token, seq}` identifies one
  exact write.

  `owner` is the node id and `incarnation` is random per start of an
  instance (`t:ctx/0`). A holder is one `{owner, incarnation}`, never the node
  id alone: two instances with one node id are two holders (design §5).

  A `put` that fails with anything but `:precondition_failed` may have been
  applied. Every function re-reads the object in that case and reports
  success only when the stored object is the caller's own write.

  Telemetry: see `docs/telemetry.md`.
  """

  alias Alambique.{Fault, Telemetry}

  @enforce_keys [:cell, :epoch, :owner, :token, :seq, :expires_at_ms]
  defstruct [
    :cell,
    :epoch,
    :owner,
    :incarnation,
    :token,
    :seq,
    :expires_at_ms,
    :etag,
    :moved_to,
    :moved_from,
    :moved_at_ms,
    :cleaned_at_ms,
    released: false
  ]

  @type t :: %__MODULE__{
          cell: String.t(),
          epoch: pos_integer,
          owner: String.t(),
          incarnation: String.t() | nil,
          token: String.t(),
          seq: pos_integer,
          expires_at_ms: integer,
          etag: Alambique.Store.etag() | nil,
          moved_to: String.t() | nil,
          moved_from: String.t() | nil,
          moved_at_ms: integer | nil,
          cleaned_at_ms: integer | nil,
          released: boolean
        }

  @typedoc """
    * `:store` - `{module, config}` implementing `Alambique.Store`
    * `:clock` - `{module, config}` implementing `Alambique.Clock`
    * `:prefix` - key prefix of the instance
    * `:ttl_ms` - lease duration granted by each acquire and renew
    * `:skew_margin_ms` - tolerated clock skew between nodes
    * `:incarnation` - random id of the instance that writes (default `nil`,
      for a reader or a tool that holds no cell). It is stored with every
      lease the context writes.
  """
  @type ctx :: %{
          store: {module, term},
          clock: {module, term},
          prefix: String.t(),
          ttl_ms: pos_integer,
          skew_margin_ms: non_neg_integer,
          incarnation: String.t() | nil
        }

  @type error :: {:error, term}

  @doc "Builds and validates a context from the keys of `t:ctx/0`."
  @spec ctx(keyword | map) :: ctx
  def ctx(opts) do
    ctx =
      opts
      |> Map.new()
      |> Map.take([:store, :clock, :prefix, :ttl_ms, :skew_margin_ms, :incarnation])
      |> Map.put_new(:incarnation, nil)

    valid? =
      match?(
        %{store: {_, _}, clock: {_, _}, prefix: p, ttl_ms: t, skew_margin_ms: s}
        when is_binary(p) and is_integer(t) and is_integer(s) and s >= 0 and t > 2 * s,
        ctx
      ) and (is_nil(ctx.incarnation) or is_binary(ctx.incarnation))

    unless valid?, do: raise(ArgumentError, "invalid lease context: #{inspect(ctx)}")
    ctx
  end

  @doc """
  Takes the lease of `cell` for `owner`.

  A lease that is released, or expired by this node's clock plus the skew
  margin, is taken with epoch + 1. Any other lease gives
  `{:error, {:owned, owner, expires_at_ms}}`.

  Option `:seen` is the answer of an earlier `read/2`, `{:ok, lease}` or
  `:none`, which saves reading the object again. The write is conditional on
  what was read, so a stale answer cannot take a lease that changed since:
  any error with it is decided again from a fresh read.
  """
  @spec acquire(ctx, String.t(), String.t(), keyword) :: {:ok, t} | error
  def acquire(ctx, cell, owner, opts \\ []) do
    traced(:acquire, %{cell: cell, owner: owner}, fn ->
      do_acquire(ctx, cell, owner, opts[:seen])
    end)
  end

  @doc """
  Takes the lease of `cell` for `owner` at once, whoever holds it and whatever
  its expiry (early takeover, design §5): epoch + 1 with `{:if_match, etag}`,
  so the old holder's next renewal fails and its uploads collide with the
  fence of the new epoch (design §6). Returns `{:ok, lease, previous}`, where
  `previous` is `%{owner: owner, incarnation: incarnation, epoch: epoch}` of the lease that was
  replaced, or `nil` when there was none.

  A lease that changed between the read and the write gives
  `{:error, {:contended, current_owner, epoch}}`; the caller may retry.
  Options:

    * `:epoch` - the epoch the caller saw. If the stored one differs,
      `{:error, {:epoch_changed, stored_epoch}}` and nothing is written: the
      cell has a new holder since the suspicion was formed.
    * `:cause` - any term for the telemetry metadata. It is never stored.

  A cell that moved gives `{:error, {:moved, to}}`.
  """
  @spec take_over(ctx, String.t(), String.t(), keyword) ::
          {:ok, t, %{owner: String.t(), incarnation: String.t() | nil, epoch: pos_integer} | nil}
          | error
  def take_over(ctx, cell, owner, opts \\ []) do
    started = System.monotonic_time()
    {result, previous} = do_take_over(ctx, cell, owner, opts[:epoch])

    meta =
      %{cell: cell, owner: owner, cause: opts[:cause]}
      |> Map.merge(previous_meta(previous))
      |> Map.merge(outcome(result))

    Telemetry.span_stop([:lease, :take_over], started, meta)
    result
  end

  @doc """
  Extends the lease: same epoch, new expiry. `{:error, :lost}` means another
  node holds the cell now. Any other error leaves the old lease usable for a
  retry, and the retry adopts a renewal that landed after all.
  """
  @spec renew(ctx, t) :: {:ok, t} | error
  def renew(ctx, %__MODULE__{} = lease) do
    traced(:renew, lease, fn ->
      next = %{lease | seq: lease.seq + 1, expires_at_ms: now(ctx) + ctx.ttl_ms}
      Fault.point(:lease_before_put, point_context(:renew, next))

      case put(ctx, next, {:if_match, lease.etag}) do
        {:ok, etag} ->
          Fault.point(:lease_after_put, point_context(:renew, next))
          {:ok, %{next | etag: etag}}

        {:error, reason} ->
          reconcile_renew(ctx, lease, reason)
      end
    end)
  end

  @doc """
  Marks the lease released, keeping the epoch, so the next node can acquire
  at once. Releasing an already released lease of the same acquisition is `:ok`.
  """
  @spec release(ctx, t) :: :ok | error
  def release(ctx, %__MODULE__{} = lease) do
    traced(:release, lease, fn -> do_release(ctx, lease, true) end)
  end

  @doc """
  Marks the lease of a cell that moved to the location `to` (design §18): the
  lease is released and `moved_to` is set, with `If-Match` on the holder's
  lease, so it fails with `{:error, :lost}` if anyone took the cell meanwhile.
  From then on `acquire/3` answers `{:error, {:moved, to}}` for good.
  """
  @spec mark_moved(ctx, t, String.t()) :: :ok | error
  def mark_moved(ctx, %__MODULE__{} = lease, to) do
    next = %{lease | seq: lease.seq + 1, released: true, moved_to: to, moved_at_ms: now(ctx)}
    Fault.point(:lease_before_put, point_context(:mark_moved, next))

    case put(ctx, next, {:if_match, lease.etag}) do
      {:ok, _etag} ->
        Fault.point(:lease_after_put, point_context(:mark_moved, next))
        :ok

      {:error, reason} ->
        case fetch(ctx, lease.cell) do
          {:ok, %{moved_to: ^to} = stored} ->
            if ours?(stored, lease), do: :ok, else: lost(:mark, lease)

          {:ok, _} ->
            lost(:mark, lease)

          {:error, :not_found} ->
            lost(:mark, lease)

          {:error, _} ->
            {:error, reason}
        end
    end
  end

  @doc """
  Creates the lease of a cell that moved here from the location `from`: free
  (released) and at `epoch`, so the first node to wake the cell takes
  `epoch + 1`. Installing it again after an interrupted move succeeds and
  raises the epoch if needed; a lease that belongs to anything else gives
  `{:error, :destination_not_empty}`.
  """
  @spec install(ctx, String.t(), pos_integer, String.t(), String.t()) :: :ok | error
  def install(ctx, cell, epoch, from, owner) do
    mine = %__MODULE__{
      cell: cell,
      epoch: epoch,
      owner: owner,
      incarnation: ctx.incarnation,
      token: Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false),
      seq: 1,
      expires_at_ms: 0,
      released: true,
      moved_from: from
    }

    case put(ctx, mine, :if_none_match) do
      {:ok, _etag} ->
        :ok

      {:error, :precondition_failed} ->
        reinstall(ctx, mine)

      {:error, reason} ->
        case fetch(ctx, cell) do
          {:ok, %{token: token}} when token == mine.token -> :ok
          _ -> {:error, reason}
        end
    end
  end

  defp reinstall(ctx, mine) do
    case fetch(ctx, mine.cell) do
      {:ok, %{moved_from: from, moved_to: nil, released: true} = stored}
      when from == mine.moved_from ->
        if stored.epoch >= mine.epoch do
          :ok
        else
          with {:ok, _} <-
                 put(ctx, %{mine | seq: stored.seq + 1}, {:if_match, stored.etag}),
               do: :ok
        end

      {:ok, %{moved_to: to, cleaned_at_ms: cleaned} = stored} when to != nil and cleaned != nil ->
        epoch = max(mine.epoch, stored.epoch + 1)

        with {:ok, _} <-
               put(ctx, %{mine | epoch: epoch, seq: stored.seq + 1}, {:if_match, stored.etag}),
             do: :ok

      {:ok, _} ->
        {:error, :destination_not_empty}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Starts the cleanup of the objects a moved cell left at this location (design
  §18) and cancels any `cleaned_at_ms` stamp. The write is conditional, so two
  cleanups cannot both hold the lease object: the later one makes the earlier
  one's `mark_cleaned/2` fail with `{:error, :contended}`.
  """
  @spec claim_cleanup(ctx, t) :: {:ok, t} | error
  def claim_cleanup(ctx, %__MODULE__{moved_to: to} = lease) when to != nil do
    next = %{lease | seq: lease.seq + 1, cleaned_at_ms: nil}

    case put(ctx, next, {:if_match, lease.etag}) do
      {:ok, etag} -> {:ok, %{next | etag: etag}}
      {:error, :precondition_failed} -> {:error, :contended}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Stamps the end of the cleanup claimed with `claim_cleanup/2`. A cell can come
  back to this location only after its objects are gone, which this stamp
  records.
  """
  @spec mark_cleaned(ctx, t) :: :ok | error
  def mark_cleaned(ctx, %__MODULE__{moved_to: to} = lease) when to != nil do
    next = %{lease | seq: lease.seq + 1, cleaned_at_ms: now(ctx)}

    case put(ctx, next, {:if_match, lease.etag}) do
      {:ok, _etag} -> :ok
      {:error, :precondition_failed} -> {:error, :contended}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Whether the stored lease is still the acquisition of `lease`: `:ok`, or
  `{:error, :lost}` when another node took the cell or it was released. One
  read; it renews nothing.
  """
  @spec verify(ctx, t) :: :ok | error
  def verify(ctx, %__MODULE__{} = lease) do
    case fetch(ctx, lease.cell) do
      {:ok, stored} ->
        if ours?(stored, lease) and not stored.released, do: :ok, else: {:error, :lost}

      {:error, :not_found} ->
        {:error, :lost}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Whether `lease` was written by the instance `{node_id, incarnation}`. A lease
  without an incarnation is never held by anyone's instance: fail safe.
  """
  @spec held_by?(t, String.t(), String.t()) :: boolean
  def held_by?(%__MODULE__{} = lease, node_id, incarnation),
    do: lease.incarnation != nil and lease.owner == node_id and lease.incarnation == incarnation

  @doc "Owner-side validity: the lease may be used until `expires_at_ms - skew_margin_ms`."
  @spec valid?(ctx, t) :: boolean
  def valid?(ctx, %__MODULE__{} = lease),
    do: not lease.released and now(ctx) < lease.expires_at_ms - ctx.skew_margin_ms

  @doc "Reads the current lease object, for routing. It may be released or expired."
  @spec read(ctx, String.t()) :: {:ok, t} | :none | error
  def read(ctx, cell) do
    case fetch(ctx, cell) do
      {:error, :not_found} -> :none
      other -> other
    end
  end

  # Acquire

  defp do_acquire(ctx, cell, owner, nil), do: acquire_from(ctx, cell, owner, fetch(ctx, cell))

  defp do_acquire(ctx, cell, owner, seen) do
    case acquire_from(ctx, cell, owner, as_fetched(seen)) do
      {:ok, _} = ok -> ok
      {:error, _} -> do_acquire(ctx, cell, owner, nil)
    end
  end

  defp as_fetched(:none), do: {:error, :not_found}
  defp as_fetched({:ok, _} = seen), do: seen

  defp acquire_from(ctx, cell, owner, read) do
    case read do
      {:error, :not_found} ->
        attempt(ctx, :acquire, cell, owner, 1, 1, :if_none_match)

      {:ok, %{moved_to: to}} when to != nil ->
        {:error, {:moved, to}}

      {:ok, current} ->
        if current.released or now(ctx) > current.expires_at_ms + ctx.skew_margin_ms do
          successor(ctx, :acquire, current, owner)
        else
          {:error, {:owned, current.owner, current.expires_at_ms}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_take_over(ctx, cell, owner, expected) do
    case fetch(ctx, cell) do
      {:error, :not_found} ->
        {check_epoch(0, expected, fn ->
           ctx |> attempt(:take_over, cell, owner, 1, 1, :if_none_match) |> with_previous(nil)
         end), nil}

      {:ok, %{moved_to: to}} when to != nil ->
        {{:error, {:moved, to}}, nil}

      {:ok, current} ->
        previous = %{owner: current.owner, incarnation: current.incarnation, epoch: current.epoch}

        {check_epoch(current.epoch, expected, fn ->
           ctx |> successor(:take_over, current, owner) |> with_previous(previous)
         end), previous}

      {:error, reason} ->
        {{:error, reason}, nil}
    end
  end

  defp check_epoch(stored, expected, take) when expected in [nil, stored], do: take.()
  defp check_epoch(stored, _expected, _take), do: {:error, {:epoch_changed, stored}}

  defp with_previous({:ok, lease}, previous), do: {:ok, lease, previous}
  defp with_previous(error, _previous), do: error

  defp successor(ctx, op, current, owner) do
    attempt(
      ctx,
      op,
      current.cell,
      owner,
      current.epoch + 1,
      current.seq + 1,
      {:if_match, current.etag}
    )
  end

  defp attempt(ctx, op, cell, owner, epoch, seq, condition) do
    mine = %__MODULE__{
      cell: cell,
      epoch: epoch,
      owner: owner,
      incarnation: ctx.incarnation,
      token: Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false),
      seq: seq,
      expires_at_ms: now(ctx) + ctx.ttl_ms
    }

    Fault.point(:lease_before_put, point_context(op, mine))

    case put(ctx, mine, condition) do
      {:ok, etag} ->
        Fault.point(:lease_after_put, point_context(op, mine))
        {:ok, %{mine | etag: etag}}

      {:error, :precondition_failed} ->
        case fetch(ctx, cell) do
          {:ok, current} -> {:error, rival(op, current)}
          {:error, :not_found} -> {:error, :precondition_failed}
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        case fetch(ctx, cell) do
          {:ok, %{token: token} = stored} when token == mine.token -> {:ok, stored}
          _ -> {:error, reason}
        end
    end
  end

  defp rival(:acquire, current), do: {:owned, current.owner, current.expires_at_ms}
  defp rival(:take_over, current), do: {:contended, current.owner, current.epoch}

  # Renew and release

  defp reconcile_renew(ctx, lease, reason) do
    case fetch(ctx, lease.cell) do
      {:ok, stored} ->
        cond do
          not ours?(stored, lease) or stored.released -> lost(:renew, lease)
          stored.seq > lease.seq -> {:ok, stored}
          true -> {:error, reason}
        end

      {:error, :not_found} ->
        lost(:renew, lease)

      {:error, read_reason} ->
        {:error, read_reason}
    end
  end

  defp do_release(ctx, lease, retry?) do
    next = %{lease | seq: lease.seq + 1, released: true}
    Fault.point(:lease_before_put, point_context(:release, next))

    case put(ctx, next, {:if_match, lease.etag}) do
      {:ok, _etag} ->
        Fault.point(:lease_after_put, point_context(:release, next))
        :ok

      {:error, reason} ->
        case fetch(ctx, lease.cell) do
          {:ok, stored} -> reconcile_release(ctx, lease, stored, reason, retry?)
          {:error, :not_found} -> lost(:release, lease)
          {:error, read_reason} -> {:error, read_reason}
        end
    end
  end

  defp reconcile_release(ctx, lease, stored, reason, retry?) do
    cond do
      not ours?(stored, lease) -> lost(:release, lease)
      stored.released -> :ok
      # An earlier renewal landed without being acknowledged: release on top of it.
      stored.seq > lease.seq and retry? -> do_release(ctx, stored, false)
      true -> {:error, reason}
    end
  end

  defp ours?(stored, lease), do: stored.token == lease.token and stored.epoch == lease.epoch

  defp lost(op, lease) do
    Telemetry.emit([:lease, :lost], %{
      op: op,
      cell: lease.cell,
      owner: lease.owner,
      epoch: lease.epoch
    })

    {:error, :lost}
  end

  # Store and encoding

  defp key(ctx, cell), do: "#{ctx.prefix}/cells/#{cell}/lease"

  defp put(%{store: {store, config}} = ctx, lease, condition),
    do: store.put(config, key(ctx, lease.cell), encode(lease), condition, %{})

  defp fetch(%{store: {store, config}} = ctx, cell) do
    case store.get(config, key(ctx, cell)) do
      {:ok, body, etag, _meta} -> decode(body, etag)
      {:error, reason} -> {:error, reason}
    end
  end

  defp now(%{clock: {clock, config}}), do: clock.now_ms(config)

  defp encode(lease) do
    %{
      cell: lease.cell,
      epoch: lease.epoch,
      owner: lease.owner,
      token: lease.token,
      seq: lease.seq,
      expires_at_ms: lease.expires_at_ms,
      released: lease.released
    }
    |> put_unless_nil(:incarnation, lease.incarnation)
    |> put_unless_nil(:moved_to, lease.moved_to)
    |> put_unless_nil(:moved_from, lease.moved_from)
    |> put_unless_nil(:moved_at_ms, lease.moved_at_ms)
    |> put_unless_nil(:cleaned_at_ms, lease.cleaned_at_ms)
    |> Jason.encode!()
  end

  defp put_unless_nil(map, _key, nil), do: map
  defp put_unless_nil(map, key, value), do: Map.put(map, key, value)
  # Unknown fields are ignored. A lease without an incarnation has none: it is
  # never ours.
  defp decode(body, etag) do
    with {:ok, %{} = map} <- Jason.decode(body),
         %{
           "cell" => cell,
           "epoch" => epoch,
           "owner" => owner,
           "token" => token,
           "seq" => seq,
           "expires_at_ms" => expires,
           "released" => released
         }
         when is_binary(cell) and is_integer(epoch) and is_binary(owner) and is_binary(token) and
                is_integer(seq) and is_integer(expires) and is_boolean(released) <- map do
      {:ok,
       %__MODULE__{
         cell: cell,
         epoch: epoch,
         owner: owner,
         incarnation: incarnation(map["incarnation"]),
         token: token,
         seq: seq,
         expires_at_ms: expires,
         released: released,
         moved_to: map["moved_to"],
         moved_from: map["moved_from"],
         moved_at_ms: map["moved_at_ms"],
         cleaned_at_ms: map["cleaned_at_ms"],
         etag: etag
       }}
    else
      _ -> {:error, :corrupt_lease}
    end
  end

  defp incarnation(value) when is_binary(value), do: value
  defp incarnation(_value), do: nil

  # Telemetry and fault points

  defp point_context(op, lease), do: %{op: op, cell: lease.cell, epoch: lease.epoch}

  defp traced(op, %{cell: cell} = subject, fun) do
    started = System.monotonic_time()
    result = fun.()

    Telemetry.span_stop(
      [:lease, op],
      started,
      Map.merge(%{cell: cell, owner: subject.owner}, outcome(result))
    )

    result
  end

  defp outcome({:ok, lease}), do: %{result: :ok, epoch: lease.epoch}
  defp outcome(:ok), do: %{result: :ok}

  defp outcome({:error, {:owned, owner, expires}}),
    do: %{result: :owned, current_owner: owner, expires_at_ms: expires}

  defp outcome({:ok, lease, _previous}), do: %{result: :ok, epoch: lease.epoch}

  defp outcome({:error, {:contended, owner, epoch}}),
    do: %{result: :contended, current_owner: owner, current_epoch: epoch}

  defp outcome({:error, {:epoch_changed, epoch}}),
    do: %{result: :epoch_changed, current_epoch: epoch}

  defp outcome({:error, :lost}), do: %{result: :lost}
  defp outcome({:error, {:moved, to}}), do: %{result: :moved, moved_to: to}
  defp outcome({:error, reason}), do: %{result: :error, reason: reason}

  defp previous_meta(nil), do: %{}

  defp previous_meta(previous),
    do: %{previous_owner: previous.owner, previous_epoch: previous.epoch}
end
