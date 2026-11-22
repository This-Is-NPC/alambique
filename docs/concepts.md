# Concepts

This page defines the terms the other pages use. Each entry describes the
current behavior. [design.md](design.md) explains how each part works.

- **Cell.** One SQLite database with its own id, for example one organization
  or one mailbox. A cell is awake on one node while it is used. Asleep, it has
  no process and no open database file; its local files stay under `:data_dir`
  until a cache round removes them.
- **Instance.** The supervised tree that `{Alambique, opts}` starts on one node.
  Nodes that serve the same cells run instances with the same `:name` and
  placement.
- **Location.** A place where cell data lives: a store with its configuration,
  and a key prefix. The objects of a cell live under `<prefix>/cells/<cell>/`.
  A placement module maps each cell to one location. `Alambique.relocate/4`
  moves a cell to another location under a fence.
- **Segment (LTX).** One replicated batch of committed transactions, stored as
  one object `0000/<min>-<max>.ltx` under the cell. The file is LTX version 3,
  the format of Litestream 0.5. See [segment-format.md](segment-format.md).
- **Lease.** An object that names the owner of a cell and its epoch. A node
  takes and renews it with conditional writes, so only one node holds it. Each
  new owner gets the next epoch, and writes from an older epoch are refused.
  See [design.md](design.md), section 5.
- **Compaction.** Background work of an instance. It merges the segments of
  cells that were written into larger files and snapshots. Retention then
  deletes the files that fall outside the restore window.
- **Replica.** A read copy of a cell, kept by a node that does not own the
  cell. It answers reads with a bounded lag. Replicas exist only when the
  instance has the `:replicas` option, and the owner never keeps one. See
  [how-to-read-replicas.md](how-to-read-replicas.md).
- **Feed.** The change feed: the row changes of all cells, published as one
  object per group of batches, in one prefix per shard of each location, with
  keys ordered by time. A consumer reads it from a position it stores, and it never lists
  cells or wakes one. See [how-to-consume-the-change-feed.md](how-to-consume-the-change-feed.md).
- **Hibernation.** After `:idle_timeout_ms` without calls (default 300 000), a
  cell uploads what is pending, releases its lease and stops. A later call
  wakes it again.
