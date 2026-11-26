defmodule Alambique.Replication do
  @moduledoc """
  Replication of a cell's SQLite file to the store (design §6).

    * `Alambique.Replication.Replicator` captures new committed WAL frames,
      uploads them as LTX segments, fences, snapshots and checkpoints.
    * `restore/4` rebuilds a database file from the store, up to the latest
      state or a point in time.
    * `head/2` reads where the store's chain ends, so a cell can tell whether a
      local file is still current.

  The segment format is described in `docs/segment-format.md`. Telemetry is
  described in `docs/telemetry.md`.
  """

  alias Alambique.Replication.{Head, Restore}

  @typedoc """
    * `:store` - `{module, config}` implementing `Alambique.Store`
    * `:prefix` - key prefix of the instance
    * `:node` - name recorded in the `node` metadata of every upload
    * `:clock` - `{module, config}` implementing `Alambique.Clock`; stamps
      segments (default `{Alambique.Clock.System, nil}`)
    * `:backoff_ms` / `:max_backoff_ms` - first and largest retry delay
      (default 50 and 1000)
    * `:max_attempts` - attempts of one retried request, whatever time is
      left (default 10)
    * `:check_timeout_ms` - time one attempt of a check that is not retried
      may take before it is killed: the freeze marker at each renewal, the
      replica marker (default 1000)
    * `:timeout_ms` - default time budget of one call (default 10000)
    * `:edge_bytes` - bytes read from each end of an LTX file to find its
      header, trailer and page index (default 65 536)
    * `:fetch_concurrency` - requests a restore makes at once to read ahead
      (default 16)
    * `:max_inflight_bytes` - most bytes of the store a restore, or the
      hydration of a lazily restored cell, holds in memory at once (default
      67 108 864); a single file larger than that is read alone
    * `:max_object_bytes` - largest segment or snapshot read
      whole (default 1 073 741 824); a larger one is refused with
      `{:too_large, size, max}` before it is read
    * `:max_control_bytes` - the same for the small objects: leases, node
      entries, markers, manifests and crash counters (default 16 777 216)
    * `:page_sidecar` - `nil` (default) writes no page-hash sidecar; a size
      writes one beside every segment and snapshot larger than that many bytes
      (design §16)
  """
  @type ctx :: %{
          optional(:budget_cut) => boolean,
          store: {module, term},
          prefix: String.t(),
          node: String.t(),
          clock: {module, term},
          backoff_ms: pos_integer,
          max_backoff_ms: pos_integer,
          max_attempts: pos_integer,
          check_timeout_ms: pos_integer,
          timeout_ms: pos_integer,
          edge_bytes: pos_integer,
          fetch_concurrency: pos_integer,
          max_inflight_bytes: pos_integer,
          max_object_bytes: pos_integer,
          max_control_bytes: pos_integer,
          page_sidecar: non_neg_integer | nil
        }

  @type target :: :latest | {:txid, non_neg_integer} | {:timestamp, integer}

  @doc "Builds and validates a context from the keys of `t:ctx/0`."
  @spec ctx(keyword | map) :: ctx
  def ctx(opts) do
    ctx =
      %{
        clock: {Alambique.Clock.System, nil},
        backoff_ms: 50,
        max_backoff_ms: 1000,
        max_attempts: 10,
        check_timeout_ms: 1_000,
        timeout_ms: 10_000,
        edge_bytes: 65_536,
        fetch_concurrency: 16,
        max_inflight_bytes: 67_108_864,
        max_object_bytes: Alambique.Store.max_object_bytes(),
        max_control_bytes: Alambique.Store.max_control_bytes(),
        page_sidecar: nil
      }
      |> Map.merge(Map.new(opts))
      |> Map.take([
        :store,
        :prefix,
        :node,
        :clock,
        :backoff_ms,
        :max_backoff_ms,
        :max_attempts,
        :check_timeout_ms,
        :timeout_ms,
        :edge_bytes,
        :fetch_concurrency,
        :max_inflight_bytes,
        :max_object_bytes,
        :max_control_bytes,
        :page_sidecar
      ])

    valid? =
      match?(
        %{
          store: {_, _},
          clock: {_, _},
          prefix: p,
          node: n,
          backoff_ms: b,
          max_backoff_ms: m,
          max_attempts: a,
          check_timeout_ms: k,
          timeout_ms: t,
          edge_bytes: e,
          fetch_concurrency: c,
          max_inflight_bytes: pb,
          max_object_bytes: ob,
          max_control_bytes: cb,
          page_sidecar: ps
        }
        when is_binary(p) and is_binary(n) and is_integer(b) and b > 0 and is_integer(m) and
               m >= b and is_integer(a) and a > 0 and is_integer(k) and k > 0 and is_integer(t) and
               t > 0 and is_integer(e) and
               e > 0 and is_integer(c) and
               c > 0 and
               is_integer(pb) and pb > 0 and is_integer(ob) and ob > 0 and is_integer(cb) and
               cb > 0 and (is_nil(ps) or (is_integer(ps) and ps >= 0)),
        ctx
      )

    unless valid?, do: raise(ArgumentError, "invalid replication context: #{inspect(ctx)}")
    ctx
  end

  @doc """
  Restores `cell` into `dest_path` and returns where it stopped.

  `target` is `:latest`, `{:txid, n}` or `{:timestamp, ms}`. The file is built
  beside `dest_path` and renamed into place, so `dest_path` is either the old
  file or the complete restored one. Any `-wal` or `-shm` file of the old
  database is removed first.

  Options: `:timeout_ms`, overriding the context's.

  The result feeds `Alambique.Replication.Replicator.from_restore/3`.
  """
  @spec restore(ctx, String.t(), Path.t(), target, keyword) ::
          {:ok, Restore.result()} | {:error, term}
  defdelegate restore(ctx, cell, dest_path, target, opts \\ []), to: Restore, as: :run

  @doc """
  Where the chain of `cell` ends in the store: `{:ok, nil}` when it holds
  nothing, else the TXID, database checksum, epoch and page size of the
  last file of the chain. A tail past a gap, and lower-epoch files after
  the chain, are not the head (design §6, R3).

  It lists the cell, then in parallel reads the epochs of its last 16 files
  and the ends of the last one: two sequential requests.
  Options: `:timeout_ms`, overriding the context's.
  """
  @spec head(ctx, String.t(), keyword) :: {:ok, Head.t() | nil} | {:error, term}
  defdelegate head(ctx, cell, opts \\ []), to: Head, as: :run
end
