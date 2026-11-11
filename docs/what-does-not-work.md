# What does not work

Known defects that are still open. Limits that follow from the design are in
[design.md](design.md) under the section of the feature they belong to. Each
entry says what happens,
what to do about it, and where the evidence is.

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
