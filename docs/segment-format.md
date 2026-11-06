# Segment format

This page records which file format Alambique replicates in, how it maps to
transactions, and how replication and restore use it. Contributors read it
before touching `native/alambique_nif` or `Alambique.Replication`.

## Decision

Segments and snapshots are **LTX version 3**, the format of
[superfly/ltx](https://github.com/superfly/ltx), the one Litestream v0.5
writes. The files follow the specification of that repository, and the keys
follow the layout of Litestream's S3 replica client. Litestream itself has not
been run on our replicas: "How the format is checked" says what the tests
cover.

Followed exactly: upstream `main` at commit
`f299ac5b951cdb69ed7a99e957b4ed66cba32560` (2026-09-10), files `README.md`,
`ltx.go`, `encoder.go`, `decoder.go`, `checksum.go` and `compactor.go`. The
README of that commit says LTX files carry no version field and that versions
2 and 3 share the `LTX1` magic; we write and read version 3 only.

The encoder, decoder, checksums and merge are in Rust (`native/alambique_nif`,
modules `ltx`, `crc`, `dbsum`, `merge`). Compression is LZ4 block format
through the `lz4_flex` crate.

### What a file contains

| Part | Size | Notes |
| --- | --- | --- |
| Header | 100 bytes | Magic `LTX1`, flags, page size, `commit` (database size in pages), `min`/`max` TXID, timestamp (ms), pre-apply checksum, WAL offset, WAL size, WAL salts, node id, reserved bytes. |
| Page frames | variable | 6-byte page header (page number, flags with bit 0 set), 4-byte compressed size, LZ4 block of one page. Ascending page numbers. |
| End marker | 6 bytes | A zero page header. |
| Page index | variable | One varint triple per page (page number, frame offset, frame size), a zero varint, then the 8-byte byte size of the triples. |
| Trailer | 16 bytes | Post-apply database checksum, file checksum (CRC-64/ISO, top bit set). |

**Where the database checksums live.** The specification says that with the
`NoChecksum` header flag the pre-apply and post-apply checksums are zero, and
the sources of Litestream v0.5.17 merge the files of a chain with that flag
and reject inputs whose trailer holds a post-apply checksum. So every file we
write sets `NoChecksum`, with zero pre-apply checksum in the header and zero
post-apply checksum in the trailer, and keeps the two real checksums in the
reserved header bytes `80..88` (pre-apply) and `88..96` (post-apply), which
the specification writes as zero and gives no meaning. The file checksum
covers the header, so they are protected like everything else. The decoder
reads the same files, and also files without the flag (checksums in header
and trailer, as upstream writes them without `NoChecksum`), which the golden
tests use.

The current version compresses every page with LZ4 and has a page index, as
described above. Integers are big-endian.

**Database checksum.** `1 << 63 | xor(crc64_iso(be32(pgno) || page))` over
every page except the lock page (the page at byte `0x40000000`). Replacing a
page costs two page checksums, so the replicator tracks the checksum
incrementally from a per-page CRC list (`Replicator` field `sums`, 8 bytes per
page, kept in a native resource that a commit copies only where it changes
it; design §15). An empty database has the checksum `1 << 63`.

**File checksum.** CRC-64/ISO over the header, every page frame header and
size prefix as stored plus the *decompressed* page, the end marker, the page
index with its size field, and the post-apply checksum. Because it covers the
compressed *size* but not the compressed bytes, two compressors give
different file checksums for the same logical file. Every reader recomputes
the checksum from the sizes in the file, so files written by this crate verify
anywhere; they are not byte-identical to files from the Go encoder.

### Mapping to Alambique

| LTX field | Value |
| --- | --- |
| `min == max` TXID | The replicated batch: one level-0 file per batch, key `0000/<txid>-<txid>.ltx`, TXIDs as 16 lowercase hex digits. |
| Snapshot | `0009/0000000000000001-<max>.ltx`, a file of the snapshot level with `min == 1` and `max` = the TXID it represents. |
| TXID 1 | The whole database (an LTX file that starts at TXID 1 is a snapshot). A fence at TXID 1 is the same file. |
| `timestamp` | Milliseconds from the context's `Alambique.Clock` when the segment was encoded. Point-in-time restore compares against it. |
| WAL offset, size, salts | Filled for batch files: where the frames were in the WAL. Zero for snapshots, fences and merged files. |
| Pre-apply checksum | The database checksum after the previous TXID. Zero in snapshots. |
| `commit` | Database size in pages after the batch. A batch can shrink it (`VACUUM`); pages past `commit` are not part of the chain. |
| Fence | An empty file (no pages) at the next TXID, with equal pre- and post-apply checksums. |
| Node id | Zero. The node name is in the object metadata instead. |

**Epoch.** The epoch of the writer lives in the S3 object metadata
(`x-amz-meta-epoch`, read back as `epoch`), next to `node`, not in the LTX
file (design §6). A missing or unreadable `epoch` counts as 0. A merged file
has the highest epoch of its inputs and a snapshot the epoch of the last
segment it covers (compaction guarantees the first, see design §9;
`Replicator.snapshot/3` does the second). Restore reads the epoch from the
metadata of each object it fetches.

The other metadata keys are `node` (the writer), `ts` (the stamp of the newest
state in the file, as in the header) and, on snapshots, `created` (when the
object was written, used by retention's grace period). A reader that finds no
`ts` on a snapshot reads the header.

### Key layout

Followed from the sources of Litestream v0.5.17 (`s3/replica_client.go`,
`replica.go`, `compaction_level.go`), which were read, not run. The S3 client
reads and writes
`<path>/<level as %04x>/<min>-<max>.ltx` with 16 lowercase hex digits per
TXID. There is no `ltx/` directory in S3 (the `ltx/<level>/` shape belongs to
the file replica client). Level 0 holds the per-commit files, levels 1 to 8
merged files and `SnapshotLevel = 9` the snapshots. `CalcRestorePlan` picks
the newest snapshot at or below the target, then from levels 8 down to 0 the
file that reaches furthest from the next TXID. Objects whose last path
element is not an LTX filename are skipped, and only the level directories
are listed, so the `lease` and `frozen` objects of a cell never reach it.
For a timestamp restore it reads the stamp from the object metadata key
`litestream-timestamp` (RFC 3339), which we write next to `ts`, and falls
back to the last-modified time. Compaction `:max_level` is therefore
limited to 8.

### Deviations from upstream

- Only the format version 3 page frame (size prefix set) is read. Version 2
  frames (LZ4 frame format, no size prefix) are rejected.
- The pre- and post-apply checksums are in reserved header bytes, not in the
  places upstream defines (see above). Files that set `NoChecksum` without
  checksums in those bytes, such as the files Litestream writes, are not
  applied (apply needs the checksums).
- Page frames use `lz4_flex`'s block output, so file checksums differ from
  Go-written files with the same content (see above).
- Merge requires inputs that follow each other exactly (each starts at the
  next TXID and its pre-apply checksum equals the previous post-apply
  checksum), where upstream tolerates overlap. It verifies each input one page
  at a time and copies the compressed page blocks, so memory use is the
  inputs plus the output rather than the decompressed database (design §12).

### Verification against upstream

What was checked against the upstream sources:

- Both golden file checksums from upstream's `encoder_golden_test.go`
  (`0xe27f4c18f0da2c10` sparse non-snapshot, `0xd926967672f7ca53` snapshot)
  reproduce exactly when the frames use upstream's payload length (31 bytes
  for those pages). That pins header, page header, size prefix, page index,
  varints, trailer and CRC-64/ISO order byte for byte. The Rust tests do it
  with the Rust encoder, `test/ltx_format_test.exs` with the independent
  reader's code (below).
- The CRC-64/ISO check value for `123456789` equals Go's
  (`0xb909_56c7_75a4_1001`).
- The decoder enforces the same rules as `decoder.go`: header validation,
  page order, lock page, index entries equal to the decoded frames, size
  field, trailer length, file checksum. Flipping any single bit of a small
  test file makes decoding fail (Rust test).
- Beyond upstream, because a store object is untrusted input:
  - Page numbers strictly ascend in every file, so a page never repeats and
    an increment cannot leave a hole that no checksum covers.
  - A file carries every page the database grows by (each page past the
    previous size, except the lock page). Lazy restore checks this before it
    sizes the page map, so the map is never larger than the objects it was
    read from: a `commit` field that claims more pages than the file holds is
    an error, not an allocation.
  - Lazy restore reads the page index from the tail of the object. An index
    larger than 25 bytes per page of `commit` (the longest entry) is refused
    before the host reads it, so a forged size field cannot turn a ranged
    read of the edge into a download of the whole object.
- The merge follows `compactor.go`: header from the first and last input,
  latest page wins, pages past the final size dropped. Rust tests apply a
  merged file and the chain it replaces to the same database and compare the
  bytes.

### How the format is checked

Four things back the claim that our files are valid LTX version 3. None needs
Go, Litestream or an external service.

1. **The specification.** The `README.md` of superfly/ltx at commit
   `f299ac5b951cdb69ed7a99e957b4ed66cba32560` is the reference for header,
   page frames, page index, trailer and the file checksum. The database
   checksum comes from `checksum.go` of the same commit.
2. **Upstream examples.** That commit has no `.ltx` files. The only file
   checksums it pins are the two of `encoder_golden_test.go`. The test
   `Alambique.LtxExample` (`test/support/ltx_example.ex`) builds the two files
   from the inputs of that test: 512-byte pages with byte `i` equal to
   `seed + i mod 7`, the TXIDs, the page numbers, the timestamp 1000 and the
   checksums it sets. The file checksum covers the size of each compressed
   payload but not its bytes, and upstream's compressor writes 31 bytes for
   these pages, so the frames hold a valid 31-byte LZ4 block of the page.
   Upstream's license is Apache 2.0; only these inputs and the two constants
   are reused.
3. **An independent reader.** `Alambique.LtxReader` (`test/support/`) is
   written from the specification alone, in Elixir. It has its own LZ4 block
   decompressor and CRC-64/ISO and does not call the NIF. It checks the magic,
   flags, page size, TXID range, page order and the lock page, that each page
   decompresses to the page size, that the page index equals the frames and
   its size field, the trailer length and the file checksum. For our files it
   also reads the database checksums from the reserved header bytes and
   requires the specified fields to be zero. It applies a chain of files
   (snapshot, then files that follow each other) to an in-memory database,
   checking the pre- and post-apply checksums at each step, and produces the
   database image.
4. **Tests of the default suite** (`test/ltx_format_test.exs`):
   - the reader decodes both examples and their file checksums equal the
     constants upstream states; the CRC and the LZ4 decompressor have their own
     cases;
   - the Rust decoder verifies the sparse example, and applying the snapshot
     example (with the real database checksum of its pages in place of the
     arbitrary one upstream sets) writes the same pages the reader decodes;
   - a real workload (key-value writes, tables, drops, a `VACUUM` that shrinks
     the database, two compaction passes with merges to level 3 and two
     snapshots) replicates through `Alambique.Replication` into a memory
     store. The reader decodes every file. For the latest TXID and three
     earlier ones, applying the chain it selects (newest snapshot at or below
     the target, then the file reaching furthest) gives an image byte-identical
     to the database written by `Alambique.Replication.restore/5`;
   - flipping a bit in any byte of a small file, and in a spread of bytes of
     the largest one, makes the reader reject it. The exception is a flip
     inside an LZ4 payload that leaves the decompressed page unchanged, which
     the specification's checksum cannot see (it covers sizes, not payload
     bytes); the test requires then that the decoded file is identical.

Run it with `mise exec -- mix test test/ltx_format_test.exs`; it is part of
`mise run test`.

What this does not show: that Litestream (or any other implementation) reads
our files, that the Go encoder and ours write the same bytes (they do not, see
above), or the object metadata and key layout, which come from reading
Litestream's sources.

## Lazy restore

`Alambique.Replication.Restore` with the `:lazy` option (design §16) reads
only the first and last 64 KiB of each file of the chain, with ranged reads:
the header for the TXIDs, the page size, `commit` and the checksums; the
trailer for the post-apply checksum; and the page index at the end, a list of
`(page number, frame offset, frame size)` that points at each page's frame
(page header, size prefix, LZ4 block). The page index is what makes a page
readable on its own. A frame read this way is not covered by the file
checksum, which spans the whole file, so lazily read pages are verified by the
database checksum when the last page has arrived. Files that are not ours
(without the checksums in the reserved bytes) cannot be read lazily for the
same reason they cannot be applied.

## WAL capture

The cell opens the database in WAL mode; its connection never checkpoints on
its own (design §6).
`Replicator.sync/3` reads the WAL from the last position (`wal_pos`: offset,
the two salts and the running frame checksum), and builds a segment with the
latest committed version of each changed page.

- A frame counts only if its salts equal the WAL header's, its checksum
  continues the chain, and a commit frame closes its transaction. Frames after
  the last commit frame (uncommitted or torn) are ignored and read again next
  time.
- After `Replicator.checkpoint/2` (`Alambique.Sqlite.checkpoint/1`) the
  position is cleared. The next write starts a new WAL with new salts, and the
  scan starts at its first frame. A WAL whose salts changed any other way is
  treated the same.
- `checkpoint/2` refuses to run while committed frames are not replicated,
  because a checkpoint discards them from the WAL. Closing a connection does
  not checkpoint, so frames that were not replicated stay in the WAL of a
  cell that crashed; its next wake restores and removes them.
- The database checksum before a batch comes from the per-page CRC list, not
  from the database file, so unchanged pages are never read.
- Snapshots are built from the database file plus the WAL frames before the
  replicated position, and must produce the state's checksum.

## Restore

`Alambique.Replication.restore/5` lists the cell prefix, then tries
starting points from the newest snapshot at or below the target down to
"from TXID 1":

1. Apply the snapshot into a temporary file next to the destination.
2. Repeatedly take the file whose `min` is the next TXID, preferring the
   largest `max` that does not pass the target, and apply it. The pre-apply
   checksum must equal the current database checksum; a mismatch tries the
   next candidate, and the post-apply checksum must equal the trailer's. When
   nothing starts at the next TXID, the chain ends there if the files left are
   a tail: level-0 files of one TXID, none more than 16 past the chain, and no
   snapshot (design §6, R3). Otherwise, when later files exist inside the
   target, restore fails with `{:gap, txid}`; a start from scratch that finds files but
   none at TXID 1 fails with `:history_truncated`; a timestamp target stopped
   only by merged files fails with `{:history_coarse, txid}` (design §6).
3. Epochs must not decrease (R2): a candidate with a lower epoch than the
   chain so far is skipped, and if only such candidates are left the chain
   ends there when the files left are a tail (step 2). Otherwise restore
   fails with `{:error, {:epoch_regression, key, epoch, chain_epoch}}`, and it
   never falls back to another starting point after that error.
4. Stop at the target. `{:txid, n}` must be reached exactly or restore fails
   with `{:target_not_reached, txid}`. `{:timestamp, ms}` stops before the
   first file stamped later than `ms`; stamps from clocks with skew can make
   this order differ slightly from TXID order. A store with nothing, or a
   timestamp before the first file, gives an empty database at TXID 0.
5. Sync the temporary file, remove the old `-wal` and `-shm`, rename.

Litestream is stricter about a tail. Its `CalcRestorePlan` (v0.5.17, read in
the sources, not run) fails a restore of the latest state with
"non-contiguous ltx files" when a file starts after the end of the plan. A
restore of a TXID up to the head is not affected. Nothing writes a tail yet;
when pipelined commits do, a cell that crashed with several uploads in flight
cannot be restored to its latest state by Litestream until a later owner
writes over the gap.

A crash leaves `.restore-*` temporary files behind at worst; the destination
is either untouched or complete. If a crash lands between removing the old
`-wal` and the rename, the old file loses its WAL: treat a local file as a
cache that restore may replace.

## Crash points

Named for `AlambiqueTestkit.CrashPoints`:

| Point | Where |
| --- | --- |
| `:repl_before_capture` | `sync` start: the transaction is committed locally and nothing is captured. |
| `:repl_after_encode` | The segment is encoded and not uploaded. |
| `:repl_after_put` | The segment landed and the caller was not told. Runs in the upload process; the context has `owner_pid`, the cell that encoded the segment. |
| `:cell_after_land` | An upload process reported a landed put and the cell has not acknowledged anything yet. |
| `:cell_before_ack` | A batch is at the head of the queue and landed; its callers are not answered yet. |
| `:cell_after_ack` | The callers of a batch were answered and nothing else ran yet. |
| `:repl_before_checkpoint` | The batch is acknowledged and the checkpoint has not run. |
| `:cell_after_checkpoint` | An awake cell checkpointed its WAL (`wal.checkpoint_bytes`, design §6) and nothing else ran yet. |
| `:repl_mid_restore` | Between two applied files of a restore. |
| `:local_before_mark` | The file is checkpointed and closed; its complete-file marker is not written yet. |
| `:local_after_mark` | The complete-file marker was renamed into place. |
| `:hydrate_before_fetch` | In a background read of a lazy restore, before the ranged read. Pages that statements miss do not pass it. |
| `:hydrate_before_miss` | In the hydrator, after a statement missed a page and before its fetch. |
| `:wake_lazy_fallback` | A lazy page-1 fetch failed and the wake discards the plan to restore the chain in full. |
| `:compact_before_put` | A merged file is built and not uploaded. |
| `:compact_after_put` | A merged file landed and the pass has not noted it. |
| `:compact_before_snapshot` | A snapshot is built and not uploaded. |
| `:compact_after_snapshot` | A snapshot landed and the pass has not noted it. |
| `:trash_before_mark` | A deletion manifest is built and not uploaded (design §9.1). |
| `:trash_after_mark` | A deletion manifest landed and retention has not noted it. |
| `:retain_before_marker` | The manifests landed; the `trashed` marker is not written. |
| `:trash_before_delete` | In a deletion task, before the `DELETE` of an object. |
| `:trash_after_delete` | In a deletion task, after the `DELETE` of an object. |
| `:trash_before_entry_delete` | The reaper deleted the objects of a manifest and not the manifest. |

## Telemetry

See [telemetry.md](telemetry.md), section "Replication events".
