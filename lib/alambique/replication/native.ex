defmodule Alambique.Replication.Native do
  @moduledoc """
  Rust NIFs (design §2): byte-level replication work and the bundled SQLite.
  Internal: use `Alambique.Replication.Replicator`, `Alambique.Replication`
  and `Alambique.Sqlite`.

  Functions that touch files run on dirty I/O schedulers. Nothing here uses
  the network.

  A WAL position is `{offset, salt1, salt2, cksum1, cksum2}`; `{0, 0, 0, 0, 0}`
  means "not positioned": the next scan starts at the first frame. Page sums
  (one 64-bit CRC per database page) live in an immutable native resource, so
  a capture copies only the sums it changes.
  """

  # The test build adds NIFs that panic (cargo feature `test-panic`, design
  # §2.1); a release build of the library does not contain them.
  @test_build Mix.env() == :test

  use Rustler,
    otp_app: :alambique,
    crate: "alambique_nif",
    path: "native/alambique_nif",
    features: if(@test_build, do: ["test-panic"], else: [])

  # Every Mix environment loads the same artifact through the linked
  # `priv/native` directory, so a build of another environment replaces this
  # one's NIF. Track the artifact as an input: a switch then recompiles this
  # module and runs Rustler again with the right features.
  @external_resource "priv/native/alambique_nif.so"

  @type wal_pos ::
          {non_neg_integer, non_neg_integer, non_neg_integer, non_neg_integer, non_neg_integer}
  @type error ::
          {:error,
           {:invalid | :io, String.t()}
           | {:pre_apply_mismatch | :post_apply_mismatch, integer, integer}}
  @type captured ::
          {:ok, segment :: binary, wal_pos, sums :: sums, checksum :: integer,
           commit :: non_neg_integer}
  @typedoc "Page sums: a native resource that never changes."
  @type sums :: reference
  @type header ::
          {min_txid :: integer, max_txid :: integer, timestamp :: integer, commit :: integer,
           pre_apply :: integer, page_size :: integer}

  @doc "Segment for `txid` with the latest version of each page committed after `pos`, or `:none`."
  @spec capture_incremental(
          Path.t(),
          Path.t(),
          pos_integer,
          wal_pos,
          sums,
          integer,
          pos_integer,
          integer
        ) ::
          captured | :none | error
  def capture_incremental(_db, _wal, _page_size, _pos, _sums, _pre, _txid, _timestamp),
    do: nif_error()

  @doc "Segment for `txid` without pages."
  @spec capture_empty(
          Path.t(),
          Path.t(),
          pos_integer,
          wal_pos,
          sums,
          integer,
          pos_integer,
          integer
        ) :: captured | error
  def capture_empty(_db, _wal, _page_size, _pos, _sums, _pre, _txid, _timestamp), do: nif_error()

  @doc "Whole database as of `txid` (min TXID 1), leaving out WAL frames from `limit` on."
  @spec capture_full(Path.t(), Path.t(), pos_integer, pos_integer, integer, wal_pos | nil) ::
          captured | error
  def capture_full(_db, _wal, _page_size, _txid, _timestamp, _limit), do: nif_error()

  @doc """
  Whole database as the segment for `txid` that replaces the state with
  checksum `pre`.
  """
  @spec capture_replace(Path.t(), Path.t(), pos_integer, integer, pos_integer, integer) ::
          captured | error
  def capture_replace(_db, _wal, _page_size, _pre, _txid, _timestamp), do: nif_error()

  @doc "Whether committed frames exist after `pos`."
  @spec wal_pending(Path.t(), pos_integer, wal_pos) :: {:ok, boolean} | error
  def wal_pending(_wal, _page_size, _pos), do: nif_error()

  @doc """
  Page sums (a binary of little-endian 64-bit CRCs; `sums_new/1` makes the
  resource from it), database checksum and page count of a database file.
  """
  @spec file_state(Path.t(), pos_integer) ::
          {:ok, binary, integer, non_neg_integer} | error
  def file_state(_path, _page_size), do: nif_error()

  @doc "Page sums resource from the binary of `file_state/2`."
  @spec sums_new(binary) :: {:ok, sums} | error
  def sums_new(_binary), do: nif_error()

  @doc "Page sums resource of `pages` zeros."
  @spec sums_zeroed(non_neg_integer) :: sums
  def sums_zeroed(_pages), do: nif_error()

  @doc """
  Makes the existing empty file `dst` a copy-on-write clone of `src`.
  `{:ok, false}` when the filesystem cannot (the caller copies instead).
  """
  @spec reflink(Path.t(), Path.t()) :: {:ok, boolean} | error
  def reflink(_src, _dst), do: nif_error()

  @doc "Applies an LTX file to a database file; `page_size` 0 accepts any."
  @spec apply_ltx(Path.t(), binary, integer, non_neg_integer) ::
          {:ok, header, post_apply :: integer} | error
  def apply_ltx(_path, _ltx, _current, _page_size), do: nif_error()

  @doc "Header and post-apply checksum of an LTX file; `verify` checks the whole file."
  @spec ltx_info(binary, boolean) :: {:ok, header, post_apply :: integer} | error
  def ltx_info(_ltx, _verify), do: nif_error()

  @doc "Merges consecutive LTX files (ascending TXIDs) into one."
  @spec merge_ltx([binary]) :: {:ok, binary} | error
  def merge_ltx(_inputs), do: nif_error()

  # Lazy restore (see `Alambique.Replication.Lazy`)

  @doc false
  def wal_changed(_wal, _page_size, _pos), do: nif_error()
  @doc false
  def lazy_new(_path, _page_size, _wait_ms), do: nif_error()
  @doc false
  def lazy_stage(_lazy, _idx, _head, _tail, _total), do: nif_error()
  @doc false
  def lazy_apply(_lazy, _idx), do: nif_error()
  @doc false
  def lazy_ready(_lazy), do: nif_error()
  @doc false
  def lazy_attach(_lazy, _pid), do: nif_error()
  @doc false
  def lazy_deliver(_lazy, _data, _pages), do: nif_error()
  @doc false
  def lazy_miss_run(_lazy, _pgno, _max_pages), do: nif_error()
  @doc false
  def lazy_next_batch(_lazy, _max_bytes), do: nif_error()
  @doc false
  def lazy_requeue(_lazy, _pages), do: nif_error()
  @doc false
  def lazy_fail(_lazy, _pgno, _message), do: nif_error()
  @doc false
  def lazy_abort(_lazy, _message), do: nif_error()
  @doc false
  def lazy_release(_lazy), do: nif_error()
  @doc false
  def lazy_progress(_lazy), do: nif_error()
  @doc false
  def lazy_verify(_lazy, _expected), do: nif_error()
  @doc false
  def lazy_prepare_sums(_lazy, _sums, _pages), do: nif_error()
  @doc false
  def lazy_mark_sums(_lazy, _pages), do: nif_error()
  @doc false
  def lazy_finish_sums(_lazy, _sums), do: nif_error()

  # SQLite (see `Alambique.Sqlite`)

  @doc false
  def sqlite_open(_path, _readonly, _vfs, _attach, _timeout_ms), do: nif_error()
  @doc false
  def sqlite_close(_conn), do: nif_error()
  @doc false
  def sqlite_execute(_conn, _sql), do: nif_error()
  @doc false
  def sqlite_run(_conn, _sql, _params, _limits), do: nif_error()
  @doc false
  def sqlite_user_statements, do: nif_error()
  @doc false
  def sqlite_interrupt(_conn), do: nif_error()
  @doc false
  def sqlite_changes(_conn), do: nif_error()

  @doc false
  def sqlite_checkpoint(_conn), do: nif_error()
  @doc false
  def sqlite_transaction_status(_conn), do: nif_error()
  @doc false
  def sqlite_memory(_conn), do: nif_error()

  @doc false
  def stats, do: nif_error()

  @doc "Takes the exclusive lock on a data directory's lock file (design §1)."
  @spec dir_lock(Path.t()) :: {:ok, reference} | {:error, :in_use | {:io, String.t()}}
  def dir_lock(_path), do: nif_error()

  @doc "Releases a `dir_lock/1` lock."
  @spec dir_unlock(reference) :: :ok
  def dir_unlock(_lock), do: nif_error()

  if @test_build do
    @doc false
    def panic_normal, do: nif_error()
    @doc false
    def panic_dirty_cpu, do: nif_error()
    @doc false
    def panic_dirty_io, do: nif_error()
    @doc false
    def panic_resource, do: nif_error()
    @doc false
    def panic_drops, do: nif_error()
    @doc false
    def panic_in_callback(_site, _path), do: nif_error()
  end

  defp nif_error, do: :erlang.nif_error(:nif_not_loaded)
end
