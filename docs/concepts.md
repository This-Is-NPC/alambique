# Concepts

This page names the parts of Alambique. It answers one question: what do the
words in the other pages mean? [design.md](design.md) describes how each part
works.

- **Cell.** One unit of data with its own SQLite file, for example one
  organization or one mailbox. A cell is alive only while it is used.
- **Instance.** The supervised tree started by `{Alambique, opts}` on one node.
  Nodes that serve the same cells run instances with the same name and
  placement.
- **Lease.** An object in the store that says which node owns a cell. It is
  taken and renewed with conditional writes, so only one node can win.
- **Epoch.** A counter on the lease that grows with every new owner. It is the
  fencing token: writes from an older epoch are rejected.
- **Owner.** The node whose cell process holds the lease. A call on any node is
  forwarded to the owner; a cell nobody owns wakes on the node that rendezvous
  hashing prefers.
- **Segment.** One replicated batch of committed changes, stored as an object.
  Segments are later compacted into snapshots.
- **Compaction and retention.** A background process per instance merges the
  small segments of cells that were written into bigger ones and into
  snapshots, and deletes what falls out of the restore window.
- **Hibernation.** After the idle timeout a cell uploads what is pending,
  releases its lease and stops. An asleep cell has no process, no open file and
  no timer.
- **Frozen cell.** A cell with a marker in the store: it refuses to wake until
  unfrozen.
- **Schema version.** A cell's `PRAGMA user_version`, raised by the instance's
  forward-only migrations when it wakes. Code serves cells one version ahead
  of it, so old and new releases can run together. See
  [how-to-roll-out-a-schema-change.md](how-to-roll-out-a-schema-change.md).
- **Change feed.** The stream of row changes of all cells, one object per
  batch, in time-ordered prefixes of the store. A consumer reads it from a
  stored position and never lists cells. See
  [how-to-consume-the-change-feed.md](how-to-consume-the-change-feed.md).
- **Placement.** A module you supply that says where a cell's data lives
  (bucket, endpoint, prefix) and what cell ids look like. A cell maps to one
  location at a time, and `Alambique.relocate/4` moves it under a fence. See
  [design.md](design.md), section 18.
