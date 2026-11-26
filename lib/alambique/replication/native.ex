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

  # scripts/test-coverage run loads the instrumented NIF that its build step
  # made once, so Rustler must not compile another one.
  @prebuilt System.get_env("ALAMBIQUE_NIF_PREBUILT") == "1"

  use Rustler,
    otp_app: :alambique,
    crate: "alambique_nif",
    path: "native/alambique_nif",
    features: if(@test_build, do: ["test-panic"], else: []),
    load_data_fun: {Alambique.Replication.NativeOptions, :load_data},
    skip_compilation?: @prebuilt

  # Every Mix environment loads the same artifact through the linked
  # `priv/native` directory, so a build of another environment replaces this
  # one's NIF. Track the artifact as an input: a switch then recompiles this
  # module and runs Rustler again with the right features.
  @external_resource "priv/native/alambique_nif.so"

  @type wal_pos ::
          {non_neg_integer, non_neg_integer, non_neg_integer, non_neg_integer, non_neg_integer}
  @type error ::
          {:error,
           :disk_full
           | {:invalid | :io, String.t()}
           | {:pre_apply_mismatch | :post_apply_mismatch, integer, integer}}
  @type captured ::
          {:ok, segment :: binary, wal_pos, sums :: sums, checksum :: integer,
           commit :: non_neg_integer}
  @typedoc "Page sums: a native resource that never changes."
  @type sums :: reference
  @typedoc "A SQLite call failed: the reason is an atom or a tuple (`Alambique.Sqlite`)."
  @type sql_error :: {:error, term}
  @typedoc "A SQLite connection resource."
  @type conn :: reference
  @typedoc "A file kept held after a connection closed."
  @type kept :: reference
  @typedoc "A lazy restore plan resource."
  @type lazy :: reference
  @typedoc "`{file, offset, length, [{pgno, offset in the run, frame length}]}`."
  @type run ::
          {non_neg_integer, non_neg_integer, non_neg_integer,
           [{pos_integer, non_neg_integer, non_neg_integer}]}
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

  @doc """
  The page sidecar of the LTX file `segment` of `level` (`docs/segment-format.md`).
  The file is decoded and checked first.
  """
  @spec page_sidecar(0..9, binary) :: {:ok, binary} | error
  def page_sidecar(_level, _segment), do: nif_error()

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
  @spec wal_changed(Path.t(), pos_integer, wal_pos) ::
          {:ok, non_neg_integer | nil, [pos_integer]} | error
  def wal_changed(_wal, _page_size, _pos), do: nif_error()
  @doc false
  @spec lazy_new(Path.t(), pos_integer, non_neg_integer) :: {:ok, lazy} | error
  def lazy_new(_path, _page_size, _wait_ms), do: nif_error()
  @doc false
  @spec lazy_stage(lazy, non_neg_integer, binary, binary, non_neg_integer) ::
          {:ok, header, post_apply :: integer} | {:need_tail, pos_integer} | error
  def lazy_stage(_lazy, _idx, _head, _tail, _total), do: nif_error()
  @doc false
  @spec lazy_stage_hashes(lazy, non_neg_integer, binary, 0..9, non_neg_integer, non_neg_integer) ::
          :ok | error
  def lazy_stage_hashes(_lazy, _idx, _sidecar, _level, _min, _max), do: nif_error()
  @doc false
  @spec lazy_covered(lazy) :: [non_neg_integer]
  def lazy_covered(_lazy), do: nif_error()
  @doc false
  @spec lazy_settle_hashes(lazy, non_neg_integer, binary) :: :range | :dropped | error
  def lazy_settle_hashes(_lazy, _file, _data), do: nif_error()
  @doc false
  @spec lazy_apply(lazy, non_neg_integer) :: :ok | error
  def lazy_apply(_lazy, _idx), do: nif_error()
  @doc false
  @spec lazy_ready(lazy) :: :ok | error
  def lazy_ready(_lazy), do: nif_error()
  @doc false
  @spec lazy_attach(lazy, pid) :: :ok
  def lazy_attach(_lazy, _pid), do: nif_error()
  @doc false
  @spec lazy_set_whole(lazy, non_neg_integer) :: :ok
  def lazy_set_whole(_lazy, _bytes), do: nif_error()
  @doc false
  @spec lazy_deliver(lazy, non_neg_integer, binary, [
          {pos_integer, non_neg_integer, non_neg_integer}
        ]) ::
          {:ok, boolean} | {:error, {:page_hash, pos_integer}} | error
  def lazy_deliver(_lazy, _file, _data, _pages), do: nif_error()
  @doc false
  @spec lazy_miss_run(lazy, pos_integer, pos_integer) :: run | :none
  def lazy_miss_run(_lazy, _pgno, _max_pages), do: nif_error()
  @doc false
  @spec lazy_next_batch(lazy, non_neg_integer) :: run | :none
  def lazy_next_batch(_lazy, _max_bytes), do: nif_error()
  @doc false
  @spec lazy_requeue(lazy, [pos_integer]) :: :ok
  def lazy_requeue(_lazy, _pages), do: nif_error()
  @doc false
  @spec lazy_fail(lazy, pos_integer, String.t()) :: :ok
  def lazy_fail(_lazy, _pgno, _message), do: nif_error()
  @doc false
  @spec lazy_abort(lazy, String.t()) :: :ok
  def lazy_abort(_lazy, _message), do: nif_error()
  @doc false
  @spec lazy_release(lazy) :: :ok
  def lazy_release(_lazy), do: nif_error()
  @doc false
  @spec lazy_progress(lazy) :: {non_neg_integer, non_neg_integer}
  def lazy_progress(_lazy), do: nif_error()
  @doc false
  @spec lazy_verify(lazy, integer) :: :ok | :incomplete | {:error, {:checksum, integer, integer}}
  def lazy_verify(_lazy, _expected), do: nif_error()
  @doc false
  @spec lazy_check_object(lazy, non_neg_integer, binary) :: {:ok, non_neg_integer} | error
  def lazy_check_object(_lazy, _file, _data), do: nif_error()
  @doc false
  @spec lazy_prepare_sums(lazy, sums, [pos_integer]) ::
          {:ok, sums} | {:missing, [pos_integer], sums}
  def lazy_prepare_sums(_lazy, _sums, _pages), do: nif_error()
  @doc false
  @spec lazy_mark_sums(lazy, [pos_integer]) :: :ok
  def lazy_mark_sums(_lazy, _pages), do: nif_error()
  @doc false
  @spec lazy_finish_sums(lazy, sums) :: {:ok, sums, checksum :: integer}
  def lazy_finish_sums(_lazy, _sums), do: nif_error()

  # SQLite (see `Alambique.Sqlite`)

  @doc false
  @spec sqlite_open(
          Path.t(),
          boolean,
          String.t() | nil,
          boolean,
          non_neg_integer,
          boolean,
          boolean,
          {non_neg_integer, non_neg_integer} | nil
        ) :: {:ok, conn, secret :: non_neg_integer} | sql_error
  def sqlite_open(_path, _readonly, _vfs, _attach, _timeout_ms, _gated, _exclusive, _lookaside),
    do: nif_error()

  @doc false
  @spec sqlite_close(conn, non_neg_integer) :: :ok | sql_error
  def sqlite_close(_conn, _key), do: nif_error()
  @doc false
  @spec sqlite_close_keep(conn, non_neg_integer) :: {:ok, kept} | sql_error
  def sqlite_close_keep(_conn, _key), do: nif_error()
  @doc false
  @spec sqlite_kept_state(kept, Path.t()) :: :same | :changed | :missing
  def sqlite_kept_state(_kept, _path), do: nif_error()
  @doc false
  @spec sqlite_kept_release(kept) :: :ok
  def sqlite_kept_release(_kept), do: nif_error()
  @doc false
  @spec sqlite_assign(conn, non_neg_integer, pid) :: :ok | sql_error
  def sqlite_assign(_conn, _key, _pid), do: nif_error()
  @doc false
  @spec sqlite_execute(conn, binary, boolean, non_neg_integer) :: :ok | sql_error
  def sqlite_execute(_conn, _sql, _no_txn_control, _key), do: nif_error()
  @doc false
  @spec sqlite_run(conn, binary, list, tuple) ::
          {:ok, [String.t()], [[integer | float | binary | nil]]} | sql_error
  def sqlite_run(_conn, _sql, _params, _limits), do: nif_error()
  @doc false
  @spec sqlite_feed_rows(
          conn,
          binary,
          list,
          pos_integer | nil,
          non_neg_integer | nil,
          non_neg_integer | nil,
          pid,
          reference
        ) :: {:ok, [tuple], boolean, boolean} | sql_error
  def sqlite_feed_rows(_conn, _sql, _params, _bound, _resumed, _room, _pid, _token),
    do: nif_error()

  @doc false
  @spec sqlite_user_statements() :: non_neg_integer
  def sqlite_user_statements, do: nif_error()
  @doc false
  @spec sqlite_interrupt(conn) :: :ok | sql_error
  def sqlite_interrupt(_conn), do: nif_error()
  @doc false
  @spec sqlite_changes(conn) :: {:ok, non_neg_integer} | sql_error
  def sqlite_changes(_conn), do: nif_error()

  @doc false
  @spec sqlite_checkpoint(conn, non_neg_integer) :: :ok | sql_error
  def sqlite_checkpoint(_conn, _key), do: nif_error()
  @doc false
  @spec sqlite_transaction_status(conn) :: {:ok, :idle | :transaction} | sql_error
  def sqlite_transaction_status(_conn), do: nif_error()
  @doc false
  @spec sqlite_begin_transaction(conn, non_neg_integer) :: {:ok, non_neg_integer} | sql_error
  def sqlite_begin_transaction(_conn, _key), do: nif_error()
  @doc false
  @spec sqlite_finish_transaction(conn, non_neg_integer, boolean) :: :ok | sql_error
  def sqlite_finish_transaction(_conn, _token, _commit), do: nif_error()
  @doc false
  @spec sqlite_wal_advanced(conn) :: boolean | sql_error
  def sqlite_wal_advanced(_conn), do: nif_error()
  @doc false
  @spec sqlite_foreign_write(conn, non_neg_integer, 0..2) ::
          :none | :advanced | :restarted | :ok | sql_error
  def sqlite_foreign_write(_conn, _key, _mode), do: nif_error()
  @doc false
  @spec sqlite_capture_incremental(
          conn,
          non_neg_integer,
          Path.t(),
          Path.t(),
          pos_integer,
          wal_pos,
          sums,
          integer,
          pos_integer,
          integer
        ) :: captured | :none | error
  def sqlite_capture_incremental(
        _conn,
        _key,
        _db,
        _wal,
        _page_size,
        _pos,
        _sums,
        _pre,
        _txid,
        _timestamp
      ),
      do: nif_error()

  @doc false
  @spec sqlite_wal_pending(conn, non_neg_integer, Path.t(), pos_integer, wal_pos) ::
          {:ok, boolean} | error
  def sqlite_wal_pending(_conn, _key, _wal, _page_size, _pos), do: nif_error()
  @doc false
  @spec sqlite_capture_full(
          conn,
          non_neg_integer,
          Path.t(),
          pos_integer,
          pos_integer,
          integer,
          wal_pos | nil
        ) :: captured | error
  def sqlite_capture_full(_conn, _key, _wal, _page_size, _txid, _timestamp, _limit),
    do: nif_error()

  @doc false
  @spec sqlite_script(conn, binary) :: :ok | sql_error
  def sqlite_script(_conn, _sql), do: nif_error()
  @doc false
  @spec sqlite_starts_ddl(binary) :: boolean
  def sqlite_starts_ddl(_sql), do: nif_error()
  @doc false
  @spec sqlite_memory(conn) :: {:ok, %{atom => non_neg_integer}} | sql_error
  def sqlite_memory(_conn), do: nif_error()

  @doc false
  @spec stats() :: %{atom => integer}
  def stats, do: nif_error()

  @doc "Takes the exclusive lock on a data directory's lock file (design §1)."
  @spec dir_lock(Path.t()) :: {:ok, reference} | {:error, :in_use | {:io, String.t()}}
  def dir_lock(_path), do: nif_error()

  @doc "Releases a `dir_lock/1` lock."
  @spec dir_unlock(reference) :: :ok
  def dir_unlock(_lock), do: nif_error()

  if @test_build do
    @doc false
    @spec forge_sidecar(binary) :: binary
    def forge_sidecar(_sidecar), do: nif_error()
    @doc false
    @spec panic_normal() :: no_return
    def panic_normal, do: nif_error()
    @doc false
    @spec panic_dirty_cpu() :: no_return
    def panic_dirty_cpu, do: nif_error()
    @doc false
    @spec panic_dirty_io() :: no_return
    def panic_dirty_io, do: nif_error()
    @doc false
    @spec panic_resource() :: reference
    def panic_resource, do: nif_error()
    @doc false
    @spec panic_drops() :: non_neg_integer
    def panic_drops, do: nif_error()
    @doc false
    @spec panic_in_callback(String.t(), Path.t()) :: {integer, integer}
    def panic_in_callback(_site, _path), do: nif_error()
    @doc false
    @spec fail_next_query_only(conn) :: :ok
    def fail_next_query_only(_conn), do: nif_error()
    @doc false
    @spec commit_behind_back(conn) :: :ok | sql_error
    def commit_behind_back(_conn), do: nif_error()
    @doc false
    @spec foreign_exec(Path.t(), String.t()) :: integer
    def foreign_exec(_path, _sql), do: nif_error()
    @doc false
    @spec foreign_open(Path.t()) :: {:ok, reference, 0} | sql_error
    def foreign_open(_path), do: nif_error()
    @doc false
    @spec set_foreign_commit(conn, Path.t(), String.t()) :: :ok
    def set_foreign_commit(_conn, _path, _sql), do: nif_error()
    @doc false
    @spec set_header_unreadable(conn, boolean) :: :ok
    def set_header_unreadable(_conn, _on), do: nif_error()
    @doc false
    @spec vfs_fault_arm(Path.t(), atom, non_neg_integer, atom, non_neg_integer) :: :ok
    def vfs_fault_arm(_path, _kind, _skip, _rc, _times), do: nif_error()
    @doc false
    @spec vfs_fault_clear() :: :ok
    def vfs_fault_clear, do: nif_error()
    @doc false
    @spec vfs_fault_hits() :: non_neg_integer
    def vfs_fault_hits, do: nif_error()
  end

  defp nif_error, do: :erlang.nif_error(:nif_not_loaded)
end
