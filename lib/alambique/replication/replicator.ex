defmodule Alambique.Replication.Replicator do
  @moduledoc """
  Replicates one cell's database to the store as LTX segments (design §6).

  The state is a plain struct; every function returns the next state and
  nothing runs in the background. The cell process owns the SQLite connection
  and calls these functions after commits:

      {:ok, state} = Replicator.sync(state, ctx)

  Rules the caller must follow:

    * Only one process writes the database, and it does not write between
      `sync/3` and `checkpoint/2`.
    * Open the database with `Alambique.Sqlite` in WAL mode. Its connections
      never checkpoint on their own, when they close (also when garbage
      collected) or after a commit, so frames reach the database file only
      through `checkpoint/2`. Any other `Alambique.Sqlite.checkpoint/1` moves
      frames that were never captured into the file, and a snapshot then
      refuses with `:snapshot_checksum_mismatch`.

  `sync/3` is `encode/4` (capture and state advance, in the owner) followed by
  `upload/3` (the put, in any process).

  After an error the owner stops serving; do not reuse the state. Without a
  lazy restore, `sync/3` can be called again with the same state: if the
  failed attempt landed in the store without being acknowledged, the next
  attempt notices the identical object and continues. With a lazy restore it
  cannot, because `encode/4` already marked the batch's page checksums as
  replaced in the native handle.
  """

  alias Alambique.Fault
  alias Alambique.Replication.{Keys, Lazy, Native, Objects, Retry}
  alias Alambique.Telemetry

  @flag Bitwise.bsl(1, 63)
  @unpositioned {0, 0, 0, 0, 0}

  @enforce_keys [:cell, :db_path, :page_size, :epoch]
  defstruct [
    :cell,
    :db_path,
    :page_size,
    :epoch,
    txid: 0,
    last_epoch: 0,
    data: 0,
    wal_pos: @unpositioned,
    sums: nil,
    checksum: @flag,
    commit: 0,
    lazy: nil
  ]

  @type t :: %__MODULE__{
          cell: String.t(),
          db_path: Path.t(),
          page_size: pos_integer,
          epoch: pos_integer,
          txid: non_neg_integer,
          last_epoch: non_neg_integer,
          data: non_neg_integer,
          wal_pos: Native.wal_pos(),
          sums: Native.sums() | nil,
          checksum: integer,
          commit: non_neg_integer,
          lazy: Lazy.t() | nil
        }

  @doc """
  State for a cell with nothing in the store. The first `sync/3` or `fence/3`
  uploads the whole database as TXID 1.
  """
  @spec new(String.t(), Path.t(), pos_integer, pos_integer) :: t
  def new(cell, db_path, page_size, epoch) when epoch >= 1 do
    %__MODULE__{cell: cell, db_path: db_path, page_size: page_size, epoch: epoch}
  end

  @doc """
  State that continues from a `Alambique.Replication.restore/5` result.

  Options: `:page_size`, used when the store held nothing (default 4096), and
  `:lazy`, the `Alambique.Replication.Lazy` of a lazy restore result. The file
  of such a result is not complete: the state then knows only the page
  checksums it has seen (`sync/3` fetches those it needs) until `hydrated/1`.

  Raises if `epoch` is lower than the epoch of the restored chain.
  """
  @spec from_restore(Alambique.Replication.Restore.result(), pos_integer, keyword) :: t
  def from_restore(result, epoch, opts \\ []) do
    if epoch < result.epoch do
      raise ArgumentError, "epoch #{epoch} is older than restored epoch #{result.epoch}"
    end

    page_size = result.page_size || Keyword.get(opts, :page_size, 4096)
    state = new(result.cell, result.path, page_size, epoch)

    cond do
      result.txid == 0 ->
        state

      lazy = Keyword.get(opts, :lazy) ->
        %{
          state
          | txid: result.txid,
            last_epoch: result.epoch,
            data: result.data,
            sums: Native.sums_zeroed(result.commit),
            checksum: result.checksum,
            commit: result.commit,
            lazy: lazy
        }

      true ->
        {:ok, sums, checksum, commit} = Native.file_state(result.path, page_size)
        {:ok, sums} = Native.sums_new(sums)

        if checksum != result.checksum do
          raise ArgumentError, "restored file does not match checksum #{result.checksum}"
        end

        %{
          state
          | txid: result.txid,
            last_epoch: result.epoch,
            data: result.data,
            sums: sums,
            checksum: checksum,
            commit: commit
        }
    end
  end

  @doc "TXID the next segment will take."
  @spec next_txid(t) :: pos_integer
  def next_txid(%__MODULE__{txid: txid}), do: txid + 1

  @typedoc """
  What `encode/4` hands to `upload/3`: one segment and everything the put
  needs. It does not depend on the replicator state.
  """
  @type upload :: %{
          cell: String.t(),
          txid: pos_integer,
          key: String.t(),
          segment: binary,
          info: %{epoch: pos_integer, data: non_neg_integer, ts: integer},
          floor_epoch: non_neg_integer,
          kind: :batch | :fence,
          started: integer,
          owner_pid: pid
        }

  @doc """
  Replicates the transactions committed since the last call as one segment:
  `encode/4` followed by `upload/3`.

  Returns `{:ok, state}`; the state is unchanged when there was nothing new.
  `{:error, :fenced}` means another owner holds the TXID: stop serving the
  cell. Any other `{:error, reason}` means the upload did not succeed before
  the deadline; the segment may or may not have landed.

  Options: `:deadline`, an absolute `System.monotonic_time(:millisecond)`
  (default: now plus the context's `:timeout_ms`).
  """
  @spec sync(t, Alambique.Replication.ctx(), keyword) :: {:ok, t} | {:error, term}
  def sync(%__MODULE__{} = s, ctx, opts \\ []) do
    case encode(s, ctx, :batch, opts) do
      :none ->
        {:ok, s}

      {:ok, next, up} ->
        case upload(up, ctx, opts) do
          :ok -> {:ok, next}
          :taken -> {:error, :fenced}
          {:error, _} = error -> error
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Captures the transactions committed since the last call as the next
  segment and advances the state as if it had landed. Nothing is sent: pass
  the upload to `upload/3`, in any process.

  Use the returned state only when the upload succeeds: after a failure the
  segment may or may not have landed, so the owner stops. (`sync/3` returns
  no state on error, and the old one is reusable only without a lazy
  restore.) `:none` means there was nothing
  new (`:batch` only). `kind` `:fence` claims the TXID with an empty segment.

  Options: `:deadline`, for the page fetches of a lazy restore.
  """
  @spec encode(t, Alambique.Replication.ctx(), :batch | :fence, keyword) ::
          {:ok, t, upload} | :none | {:error, term}
  def encode(%__MODULE__{} = s, ctx, kind, opts) do
    started = System.monotonic_time()
    txid = s.txid + 1
    if kind == :batch, do: Fault.point(:repl_before_capture, %{cell: s.cell, txid: txid})
    ts = now(ctx)

    with {:ok, s, touched} <- prepare_sums(s, ctx, kind, opts) do
      case capture(s, kind, ts) do
        :none ->
          :none

        {:error, reason} ->
          {:error, {:capture, reason}}

        {:ok, segment, pos, sums, checksum, commit} ->
          # A fence changes no data: it keeps the data TXID (design §6).
          data = if kind == :fence, do: s.data, else: txid
          Fault.point(:repl_after_encode, %{cell: s.cell, txid: txid})
          if s.lazy, do: Native.lazy_mark_sums(s.lazy.handle, touched)

          up = %{
            cell: s.cell,
            txid: txid,
            key: Keys.segment(ctx.prefix, s.cell, 0, txid, txid),
            segment: segment,
            info: %{epoch: s.epoch, data: data, ts: ts},
            floor_epoch: s.last_epoch,
            kind: kind,
            started: started,
            owner_pid: self()
          }

          {:ok,
           %{
             s
             | txid: txid,
               last_epoch: s.epoch,
               data: data,
               wal_pos: pos,
               sums: sums,
               checksum: checksum,
               commit: commit
           }, up}
      end
    end
  end

  @doc """
  Puts the segment of `encode/4` in the store, retrying until the deadline.
  The `:repl_after_put` fault point runs in the calling process; its context
  carries the `owner_pid` that encoded the segment.

  `:ok` means it landed. `:taken` means another owner holds the TXID (the
  put rule of design §6). Any `{:error, reason}` leaves the outcome unknown.
  It reads no replicator state, so it can run in any process.

  Options: `:deadline` (default: now plus the context's `:timeout_ms`);
  `:landed`, a function called with the upload once it landed.
  """
  @spec upload(upload, Alambique.Replication.ctx(), keyword) :: :ok | :taken | {:error, term}
  def upload(up, ctx, opts) do
    {store, config} = ctx.store
    deadline = Keyword.get_lazy(opts, :deadline, fn -> Retry.deadline_in(ctx.timeout_ms) end)

    result =
      put(ctx, store, config, up.key, up.segment, up.info, deadline, {:floor, up.floor_epoch})

    meta = %{cell: up.cell, txid: up.txid, epoch: up.info.epoch, kind: up.kind}
    bytes = %{bytes: byte_size(up.segment)}

    case result do
      :ok ->
        Fault.point(:repl_after_put, %{cell: up.cell, txid: up.txid, owner_pid: up.owner_pid})
        if landed = opts[:landed], do: landed.(up)
        Telemetry.span_stop([:replication, :sync], up.started, bytes, Map.put(meta, :result, :ok))
        :ok

      :taken ->
        Telemetry.span_stop(
          [:replication, :sync],
          up.started,
          bytes,
          Map.put(meta, :result, :fenced)
        )

        Telemetry.emit([:replication, :fenced], meta)
        :taken

      {:error, reason} ->
        Telemetry.span_stop(
          [:replication, :sync],
          up.started,
          bytes,
          Map.merge(meta, %{result: :error, reason: reason})
        )

        {:error, reason}
    end
  end

  # While a lazy restore is hydrating, the CRC of a page that a batch replaces
  # (or drops, when the file shrinks) may be unknown: fetch those pages.
  defp prepare_sums(%__MODULE__{lazy: nil} = s, _ctx, _kind, _opts), do: {:ok, s, []}
  defp prepare_sums(s, _ctx, :fence, _opts), do: {:ok, s, []}

  defp prepare_sums(%__MODULE__{lazy: lazy} = s, ctx, :batch, opts) do
    deadline = Keyword.get_lazy(opts, :deadline, fn -> Retry.deadline_in(ctx.timeout_ms) end)

    case Native.wal_changed(wal_path(s), s.page_size, s.wal_pos) do
      {:ok, nil, _pages} ->
        {:ok, s, []}

      {:ok, commit, pages} ->
        dropped = if commit < s.commit, do: Enum.to_list((commit + 1)..s.commit//1), else: []
        touched = pages ++ dropped
        with {:ok, s} <- known_sums(s, lazy, touched, deadline), do: {:ok, s, touched}

      {:error, reason} ->
        {:error, {:capture, {:wal, reason}}}
    end
  end

  defp known_sums(s, lazy, pages, deadline) do
    case Native.lazy_prepare_sums(lazy.handle, s.sums, pages) do
      {:ok, sums} ->
        {:ok, %{s | sums: sums}}

      {:missing, missing, sums} ->
        case Lazy.fetch_pages(lazy, missing, deadline) do
          :ok -> known_sums(%{s | sums: sums}, lazy, missing, deadline)
          {:error, reason} -> {:error, {:hydrate, reason}}
        end

      {:error, reason} ->
        {:error, {:capture, reason}}
    end
  end

  @doc """
  Ends the lazy phase once every page is local and the checksum of the pages
  as restored was verified: fills the page checksums that were never needed
  and checks that they add up to the state's checksum.
  """
  @spec hydrated(t) :: {:ok, t} | {:error, :checksum_mismatch}
  def hydrated(%__MODULE__{lazy: nil} = s), do: {:ok, s}

  def hydrated(%__MODULE__{lazy: lazy} = s) do
    {:ok, sums, checksum} = Native.lazy_finish_sums(lazy.handle, s.sums)
    :ok = Native.lazy_release(lazy.handle)

    if checksum == s.checksum,
      do: {:ok, %{s | sums: sums, lazy: nil}},
      else: {:error, :checksum_mismatch}
  end

  @doc """
  Claims the next TXID with an empty segment (design §6, "Fence on wake").

  Call it after restoring, before serving. `{:error, :txid_taken}` means a
  stale owner wrote that TXID first: restore again and retry.
  """
  @spec fence(t, Alambique.Replication.ctx(), keyword) :: {:ok, t} | {:error, term}
  def fence(%__MODULE__{} = s, ctx, opts \\ []) do
    with {:ok, next, up} <- encode(s, ctx, :fence, opts) do
      case upload(up, ctx, opts) do
        :ok -> {:ok, next}
        :taken -> {:error, :txid_taken}
        {:error, _} = error -> error
      end
    end
  end

  # TXID 1 is always the whole database, because LTX files that start at
  # TXID 1 are snapshots.
  defp capture(%{txid: 0} = s, _kind, ts),
    do: Native.capture_full(s.db_path, wal_path(s), s.page_size, 1, ts, nil)

  defp capture(s, :batch, ts) do
    Native.capture_incremental(
      s.db_path,
      wal_path(s),
      s.page_size,
      s.wal_pos,
      s.sums,
      s.checksum,
      s.txid + 1,
      ts
    )
  end

  defp capture(s, :fence, ts) do
    Native.capture_empty(
      s.db_path,
      wal_path(s),
      s.page_size,
      s.wal_pos,
      s.sums,
      s.checksum,
      s.txid + 1,
      ts
    )
  end

  @doc """
  Uploads the whole database as a level 9 file (`0009/0000000000000001-<txid>.ltx`) for the current TXID.

  The snapshot is built from the database file plus the WAL frames already
  replicated, and its checksum must equal the state's. It needs the whole file:
  while a lazy restore is hydrating the result is `{:error, :hydrating}`. Compaction policy
  decides when to call this (design §9). An existing snapshot for the TXID is
  kept.
  """
  @spec snapshot(t, Alambique.Replication.ctx(), keyword) :: {:ok, String.t()} | {:error, term}
  def snapshot(s, ctx, opts \\ [])
  def snapshot(%__MODULE__{txid: 0}, _ctx, _opts), do: {:error, :nothing_replicated}
  def snapshot(%__MODULE__{lazy: lazy}, _ctx, _opts) when lazy != nil, do: {:error, :hydrating}

  def snapshot(%__MODULE__{} = s, ctx, opts) do
    {store, config} = ctx.store
    key = Keys.snapshot(ctx.prefix, s.cell, s.txid)
    deadline = Keyword.get_lazy(opts, :deadline, fn -> Retry.deadline_in(ctx.timeout_ms) end)

    ts = now(ctx)

    case Native.capture_full(s.db_path, wal_path(s), s.page_size, s.txid, ts, s.wal_pos) do
      {:ok, segment, _pos, _sums, checksum, _commit} when checksum == s.checksum ->
        info = %{epoch: s.last_epoch, data: s.data, ts: ts, created: ts}

        case put(ctx, store, config, key, segment, info, deadline, :exists_ok) do
          {:error, _} = error -> error
          _ok_or_exists -> {:ok, key}
        end

      {:ok, _, _, _, _, _} ->
        {:error, :snapshot_checksum_mismatch}

      {:error, reason} ->
        {:error, {:capture, reason}}
    end
  end

  @doc """
  Replaces the state of the chain with the database file at `db_path`
  (design §20): uploads one level-0 segment, TXID `head.txid + 1`, that lists
  every page and starts from the checksum of `head`, with `epoch`.

  The put is a compare-and-swap on the chain: it succeeds only if nobody
  wrote `head.txid + 1`, and a restore applies the segment only to the state
  whose checksum is `head.checksum`. `{:error, :txid_taken}` means the chain
  moved; any other `{:error, reason}` leaves the outcome unknown. The file may
  have committed frames in its `-wal`; they are part of the new state.
  Options: `:pages` (required), the page count the file must have: a file
  that is empty, truncated or not the reconciled one is refused with
  `{:error, {:capture, {:page_count, found, expected}}}` before anything is
  sent. `:deadline`.
  """
  @spec replace(
          Alambique.Replication.ctx(),
          String.t(),
          Path.t(),
          pos_integer,
          map,
          pos_integer,
          keyword
        ) ::
          {:ok, pos_integer} | {:error, term}
  def replace(ctx, cell, db_path, page_size, head, epoch, opts) do
    started = System.monotonic_time()
    txid = head.txid + 1
    ts = now(ctx)
    deadline = Keyword.get_lazy(opts, :deadline, fn -> Retry.deadline_in(ctx.timeout_ms) end)
    pages = Keyword.fetch!(opts, :pages)

    case Native.capture_replace(db_path, db_path <> "-wal", page_size, head.checksum, txid, ts) do
      {:ok, _segment, _pos, _sums, _checksum, commit} when commit != pages or pages < 1 ->
        {:error, {:capture, {:page_count, commit, pages}}}

      {:ok, segment, _pos, _sums, _checksum, _commit} ->
        {store, config} = ctx.store
        key = Keys.segment(ctx.prefix, cell, 0, txid, txid)

        info = %{epoch: epoch, data: txid, ts: ts}

        result =
          case put(ctx, store, config, key, segment, info, deadline, {:floor, head.epoch}) do
            :ok -> {:ok, txid}
            :taken -> {:error, :txid_taken}
            {:error, _} = error -> error
          end

        meta = %{cell: cell, txid: txid, epoch: epoch, kind: :replace}

        meta =
          case result do
            {:ok, _} -> Map.put(meta, :result, :ok)
            {:error, :txid_taken} -> Map.put(meta, :result, :fenced)
            {:error, reason} -> Map.merge(meta, %{result: :error, reason: reason})
          end

        Telemetry.span_stop([:replication, :sync], started, %{bytes: byte_size(segment)}, meta)

        result

      {:error, reason} ->
        {:error, {:capture, reason}}
    end
  end

  @doc """
  Checkpoints the database with `Alambique.Sqlite.checkpoint/1` through the
  open connection `conn`, then restarts WAL tracking.

  Refuses with `{:error, :unreplicated_frames}` when committed frames have
  not been replicated yet, and with `{:error, :checkpoint_busy}` when SQLite
  could not finish (the state stays valid in both cases).
  """
  @spec checkpoint(t, Alambique.Sqlite.conn()) :: {:ok, t} | {:error, term}
  def checkpoint(%__MODULE__{} = s, conn) do
    Fault.point(:repl_before_checkpoint, %{cell: s.cell, txid: s.txid})

    with {:ok, false} <- pending(s),
         :ok <- truncate(conn) do
      {:ok, %{s | wal_pos: @unpositioned}}
    else
      {:ok, true} -> {:error, :unreplicated_frames}
      {:error, _} = error -> error
    end
  end

  defp pending(s) do
    case Native.wal_pending(wal_path(s), s.page_size, s.wal_pos) do
      {:ok, bool} -> {:ok, bool}
      {:error, reason} -> {:error, {:wal, reason}}
    end
  end

  defp truncate(conn) do
    case Alambique.Sqlite.checkpoint(conn) do
      :ok -> :ok
      {:error, :busy} -> {:error, :checkpoint_busy}
      {:error, reason} -> {:error, {:checkpoint, reason}}
    end
  end

  # An upload that is rejected because the key exists (design §6, R3):
  #
  #   * the object is ours (same bytes and epoch, an earlier attempt landed):
  #     done;
  #   * `{:floor, epoch}` and the object's epoch is lower, so it is a zombie
  #     that a fenced-out owner left past the chain: overwrite it, only if it
  #     is still the object just read;
  #   * anything else belongs to a newer or equal epoch: `:taken`.
  #
  # `:exists_ok` (snapshots) takes any existing object as done.
  defp put(ctx, store, config, key, body, info, deadline, mode) do
    meta = Objects.meta(ctx, info)

    Retry.run(ctx, deadline, fn ->
      case store.put(config, key, body, :if_none_match, meta) do
        {:ok, _etag} ->
          :ok

        {:error, :precondition_failed} when mode == :exists_ok ->
          :taken

        {:error, :precondition_failed} ->
          collide(store, config, key, body, meta, meta["epoch"], mode)

        {:error, reason} ->
          {:retry, reason}
      end
    end)
  end

  defp collide(store, config, key, body, meta, epoch, {:floor, floor}) do
    case store.get(config, key) do
      {:ok, ^body, _etag, %{"epoch" => ^epoch}} ->
        :ok

      {:ok, _body, etag, found} ->
        if Objects.epoch_of(found) < floor,
          do: overwrite(store, config, key, body, meta, etag),
          else: :taken

      {:error, reason} ->
        {:retry, reason}
    end
  end

  defp overwrite(store, config, key, body, meta, etag) do
    case store.put(config, key, body, {:if_match, etag}, meta) do
      {:ok, _etag} -> :ok
      {:error, reason} -> {:retry, reason}
    end
  end

  defp now(%{clock: {mod, config}}), do: mod.now_ms(config)
  defp wal_path(%__MODULE__{db_path: path}), do: path <> "-wal"
end
