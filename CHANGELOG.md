# Changelog

## [0.3.1](https://github.com/This-Is-NPC/alambique/compare/v0.3.0...v0.3.1) (2026-11-12)


### Bug Fixes

* **cell:** check the freeze marker once per renewal ([c106d92](https://github.com/This-Is-NPC/alambique/commit/c106d92f259f854a74d57cdd5a29ff64552deff8))
* **load:** fetch and compile the driver first ([df7432f](https://github.com/This-Is-NPC/alambique/commit/df7432fc3cd00e5f9f0fc611b84c87e18446d0f3))


### Performance Improvements

* **query:** cap a query at a share of the slots ([106bfa4](https://github.com/This-Is-NPC/alambique/commit/106bfa473c56d394940959e27dc88ac460be617b))
* **query:** list both levels of a cell together ([5211fb8](https://github.com/This-Is-NPC/alambique/commit/5211fb8e003743438d7c765d8cbb107ade8ab50b))
* **query:** make the default concurrency 16 ([25adae2](https://github.com/This-Is-NPC/alambique/commit/25adae20745ff4417ce21c5eb298aeea6162d7d5))

## [0.3.0](https://github.com/This-Is-NPC/alambique/compare/v0.2.0...v0.3.0) (2026-11-11)


### Features

* **sql:** let a call lower its statement deadline ([90ea5f7](https://github.com/This-Is-NPC/alambique/commit/90ea5f7304237b3e3628d8b3b6b4c534beb86517))


### Bug Fixes

* **admission:** hibernate the lru cell at max_cells ([af2dc19](https://github.com/This-Is-NPC/alambique/commit/af2dc19486bea2c031d5f0575695f8c83f2b8881))
* **admission:** take max_pending places atomically ([336968c](https://github.com/This-Is-NPC/alambique/commit/336968cd9d635b55d054449fb991e1fff24f2ad6))
* **cell:** drop calls whose caller gave up ([033ec41](https://github.com/This-Is-NPC/alambique/commit/033ec41918f3c7209c5d2edef1493ac9a2984127))
* **feed:** keep object keys in commit order ([69f30d2](https://github.com/This-Is-NPC/alambique/commit/69f30d20f37a86599d653c20177cc6de2364280c))
* **instance:** refuse a case-insensitive data_dir ([7d71c16](https://github.com/This-Is-NPC/alambique/commit/7d71c16f0d0603c0969092beeabf37b43fe8163f))
* **instance:** refuse a data_dir held by another ([a37f81f](https://github.com/This-Is-NPC/alambique/commit/a37f81fa896fdc4c67995e1bd9bb9768798166ce))
* **query:** refresh a replica without reading it ([045fb0e](https://github.com/This-Is-NPC/alambique/commit/045fb0e901d7131a6f50eaee06e4de2139d02c6b))
* **query:** remove cache files of the old name ([314cda1](https://github.com/This-Is-NPC/alambique/commit/314cda193cf2bef2f738583cd7d2a80a9b67b49d))
* **relocate:** drop stray cleanup markers ([b3e66d4](https://github.com/This-Is-NPC/alambique/commit/b3e66d4bc6889979bd35fe68d4c7118350e357a1))
* **relocate:** rediscover cleanups after a restart ([f987077](https://github.com/This-Is-NPC/alambique/commit/f9870774c79c418ecb48e0255370a84215a1dcf5))
* **replication:** kill store calls at the deadline ([126d0dd](https://github.com/This-Is-NPC/alambique/commit/126d0dd096a41eb9a928f01f84e20e138c333b33))
* **sql:** bound migrations, scripts and own statements ([491c015](https://github.com/This-Is-NPC/alambique/commit/491c015f43062a766ac9f18353160f8e123eee1e))
* **sql:** limit concurrent user statements per node ([c75b4c0](https://github.com/This-Is-NPC/alambique/commit/c75b4c052f058452f9808d61b5437f8288844a8f))
* **store:** bound the lease and feed requests ([ce7affb](https://github.com/This-Is-NPC/alambique/commit/ce7affb5aa5abd550a3fcd6d08a8412b67314ec6))


### Performance Improvements

* **replica:** poll idle replicas less often ([67178d2](https://github.com/This-Is-NPC/alambique/commit/67178d2bd0cb07b40f52ef19d77e76b84420bdec))

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
