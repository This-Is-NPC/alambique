# Changelog

## [0.2.0](https://github.com/This-Is-NPC/alambique/compare/v0.1.0...v0.2.0) (2026-11-11)


### Features

* **drain:** add a deadline for the whole drain ([55dda59](https://github.com/This-Is-NPC/alambique/commit/55dda595f785df18d43257fdafc327d9da3224df))
* **drain:** add telemetry and drain_status/1 ([65335a2](https://github.com/This-Is-NPC/alambique/commit/65335a253aca2eb82a5480054a96b4a95de963fd))


### Bug Fixes

* **cell:** report a failed lease release ([9bbd40f](https://github.com/This-Is-NPC/alambique/commit/9bbd40fd7f9732b3c70de3751f0afbc712ded5d7))
* **lease:** fence same node_id with an incarnation ([9d4eae7](https://github.com/This-Is-NPC/alambique/commit/9d4eae7dd3bd5406cbf31081660cbc81e31dd175))

## 0.1.0 (2026-11-05)

First release.

### Storage and replication

* Cells are SQLite databases, one file each. A write is acknowledged only after its transaction is durable in the store.
* Committed changes replicate as LTX files, the format Litestream 0.5 writes, with pipelined commits and a checksum chain.
* A cell restores from the store, lazily page by page for large databases, so the first query does not wait for the whole download.
* Compaction merges small segments into larger ones and snapshots. Retention keeps a restorable window, seven days by default, and deletes what falls out of it after a grace period.
* SQLite is bundled in the native library, so a host application needs no separate SQLite package.
* Store credentials stay out of crash reports and errors. A panic in the native layer does not stop the VM.

### Routing and leases

* A cell is awake on one node at a time under a lease taken with conditional writes. The lease epoch fences writes from an older owner.
* A call on any node is forwarded to the owner. A cell nobody owns wakes on the node that rendezvous hashing prefers.
* `take_over/4` fences an owner at once when a failure detector suspects it. `move/3`, `hibernate/2`, `drain/1` and `join/1` support planned changes of owner.
* Placement modules decide where each cell lives and how cell ids look. `relocate/4` moves a cell between locations under a fence and deletes the old objects after a grace period.
* Writes whose outcome is unknown answer `{:error, :outcome_unknown}`. Idempotent calls can be retried safely.
* Admission limits (`awake: [max_cells:, max_wakes:, max_pending:]`, `max_bytes`) keep a node from running out of memory and answer `:overloaded` or `:busy`.
* Read replicas on other nodes answer reads with a bounded lag.

### Schema

* Forward-only migrations run at wake. Code serves cells one schema version ahead, so old and new releases run together. `mix alambique.check_deploy` checks a rollout against the store.
* `Alambique.Tx.script/2` runs several schema statements in one transaction.

### Change feed

* Every row change is published on a per-cell change feed that consumers read from a stored position, without listing cells. Delivery is at least once.
* Feed objects are JSON compressed with zlib and can hold several batches. Retention deletes them after seven days by default.
* `Alambique.Feed.Producer` reads the feed for Broadway, an optional dependency.

### Rewind and cross-cell queries

* `Alambique.position/3` and `Alambique.rewind/4` return a cell to an earlier state atomically, and the feed tells consumers about it.
* `Alambique.Query` runs read-only SQL over many cells without waking them.

### Operations

* `mix alambique.inspect`, `restore`, `freeze`, `unfreeze`, `move`, `relocate`, `cleanup` and `check_deploy` work on cells and the store.
* A cell whose crash stopped the VM is frozen by quarantine. An optional background scrub restores sampled cells to check their data.
* A local cache of restored files bounds the disk a node uses.

### Observability

* Telemetry events cover calls, commits, wakes, replication, compaction, the feed and the native layer. See `docs/telemetry.md`.

### Test kit

* `alambique_testkit` provides an in-memory store with fault injection, a manual clock, a clock with skew, a peer-node cluster helper and crash points.
* The repository checks the native code with unit tests, fuzz targets, Miri and sanitizers, and the lease and commit protocol with a TLA+ model.
