# What does not work

Known defects that are still open. Limits that follow from the design are in
[design.md](design.md) under the section of the feature they belong to. Each
entry says what happens,
what to do about it, and where the evidence is.

## A query replica refresh costs the size of the database

**What happens.** `Alambique.Query` refreshes a cached replica by copying the
file, reading it once to compute its checksum, applying the new segments and
flushing the copy. The cost is proportional to the database, not to the
change: a 1 GB replica refreshed by one 4 KiB segment copies, reads and flushes
1 GB. The copy is needed because other queries may have the old file open.
Recording the checksum with the cache entry and using reflinks where the
filesystem has them would remove two of the passes, but change the cache
naming and need a platform check.

**Workaround.** Use `max_age_ms:` so that a run reuses a replica instead of
refreshing it, and keep the cache on a filesystem with fast copies.

**Evidence.** `Alambique.Query.Replica` (`apply_chain`); design §14.

## Data directories are not checked

**What happens.** Two requirements of the instance are not enforced:

- `:data_dir` must belong to one instance. A second instance (or VM) on the
  same directory shares the files of cells with equal ids, and its start removes
  the first one's `read-replicas` and `work` directories.
- `:data_dir` must be on a case-sensitive file system. On a case-insensitive
  one, ids that differ only in case (`Org`, `org`) have separate leases but one
  local file.

A duplicate `node_id` is not a defect: leases name the instance by `node_id`
and a random incarnation, so two instances with one `node_id` cannot take each
other's cells (design §5). The cost is that a crashed node waits for its leases
to expire after a restart.

**Workaround.** Give every instance its own `data_dir`, and use a
case-sensitive file system (Linux ext4, xfs; not a default macOS or Windows
volume or a bind mount of one).

**Evidence.** None needed: the checks do not exist. They need a lock file or a
case-insensitive mount to test.

## Litestream has not been run on our replicas

**What happens.** The key layout, the level-9 snapshots and the
`litestream-timestamp` metadata follow Litestream's S3 replica client, and the
files are LTX version 3, but no Litestream binary has read a replica of
Alambique. Nothing here shows that `litestream restore` works on one.
Alambique also cannot apply files written by Litestream itself, because they
carry no database checksums (the NIF and the lazy restore need them).

Since the tail rule (design §6), a restore of ours ends at its head when
level-0 files of one TXID are left past a gap. Litestream's restore of the
latest state fails on such a tail ("non-contiguous ltx files"). This was read
in Litestream's sources and not run.

**Workaround.** Restore with `mix alambique.restore`, which is tested.

**Evidence.** [segment-format.md](segment-format.md), "How the format is
checked", lists what the tests cover.

## Cleanups after a move are not rediscovered after a restart

**What happens.** The deletion of a moved cell's old objects waits for a
grace period in the compactor's memory. If that node restarts first, the
old objects stay until someone runs the cleanup, and moving the cell back
there answers `{:error, :destination_not_cleaned}` until it runs.

**Workaround.** `mix alambique.cleanup CELL LOCATION_ID --instance NAME --connect NODE`
or `Alambique.cleanup_moved/4`.

**Evidence.** [design.md §18](design.md#18-placement).
