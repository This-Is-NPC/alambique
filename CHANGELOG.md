# Changelog

## [0.9.0](https://github.com/This-Is-NPC/alambique/compare/v0.8.0...v0.9.0) (2026-11-14)


### ⚠ BREAKING CHANGES

* **compaction:** compaction: [batch_factor:] is no longer an option. [:alambique, :compactor, :tick] is emitted when the passes of a busy period end, with the passes started in it as cells. Para o Atanor: it does not set batch_factor, nothing to change there.

### Features

* **bench:** saturate the writer and audit the GETs ([55616c3](https://github.com/This-Is-NPC/alambique/commit/55616c30d0fbe82b9494c36e7f11cd3254c4baef))
* **compaction:** compact the tail on graceful release ([db90efd](https://github.com/This-Is-NPC/alambique/commit/db90efd21b18492d0cd1584817fea3baa16f0688))
* **telemetry:** add store time and reductions to wake ([b5f61c9](https://github.com/This-Is-NPC/alambique/commit/b5f61c9b3ab380d895ea1b1f8152988ec0016f27))


### Bug Fixes

* **compaction:** start passes as slots free ([8a1d514](https://github.com/This-Is-NPC/alambique/commit/8a1d514c9e352844e8f3867f4f942943c11e554e))
* **drain:** bound release compaction by the call ([016e5b8](https://github.com/This-Is-NPC/alambique/commit/016e5b836cf6d62d70a2b15be822d7a0c6d1822c))

## [0.8.0](https://github.com/This-Is-NPC/alambique/compare/v0.7.1...v0.8.0) (2026-11-14)


### Features

* **api:** tell whether a cell is waking in load ([aa87772](https://github.com/This-Is-NPC/alambique/commit/aa87772773f7f904131f94bf32120b6a39ae1885))


### Bug Fixes

* **compaction:** skip blocks a higher level holds ([e598ed9](https://github.com/This-Is-NPC/alambique/commit/e598ed994f4f89cb055cd1cd80db4e41ffd06939))

## [0.7.1](https://github.com/This-Is-NPC/alambique/compare/v0.7.0...v0.7.1) (2026-11-14)


### Bug Fixes

* **compaction:** wake the compactor at the chain cap ([8512107](https://github.com/This-Is-NPC/alambique/commit/85121077f54b6f275bd0e6587d80611da5ef4aa2))
* **restore:** reuse the heads of a lazy attempt ([92f1d92](https://github.com/This-Is-NPC/alambique/commit/92f1d92da671704cfdab033f20669e3419aa102c))


### Performance Improvements

* **bench:** add a node swap under load ([a897b77](https://github.com/This-Is-NPC/alambique/commit/a897b77220b3c69eee53dd358d1ddf56dc729e20))
* **bench:** restore many cells at once ([99090c6](https://github.com/This-Is-NPC/alambique/commit/99090c6a2e7b614d1900e8535ec88dd1ce346b00))
* **bench:** restore through SeaweedFS and a delay proxy ([138c67a](https://github.com/This-Is-NPC/alambique/commit/138c67a72471417d35201a5ba751c35b19f1e1f5))
* **restore:** index the chain by start TXID ([bcc701c](https://github.com/This-Is-NPC/alambique/commit/bcc701cd54a81558891831ac1ee75d0249645d7f))
* **restore:** keep the listing out of read-ahead tasks ([d75d687](https://github.com/This-Is-NPC/alambique/commit/d75d6876a60603e8ee74d95c582d2994f513b73d))
* **s3:** add pool_size, default 256 ([33ada8f](https://github.com/This-Is-NPC/alambique/commit/33ada8f7bc59b359a23bb970942a15fd88751738))

## [0.7.0](https://github.com/This-Is-NPC/alambique/compare/v0.6.1...v0.7.0) (2026-11-14)


### Features

* **compaction:** force a pass above a chain cap ([a31ea30](https://github.com/This-Is-NPC/alambique/commit/a31ea30325566bc6e7da56d4b08dbb5168f7dd1d))


### Bug Fixes

* **compaction:** pick the longest-waiting cells first ([1ec185f](https://github.com/This-Is-NPC/alambique/commit/1ec185fb52816c930b1245eaed85db46c99bd49d))


### Performance Improvements

* **bench:** measure restore by fetch concurrency ([addfbff](https://github.com/This-Is-NPC/alambique/commit/addfbffc4dec55673f6c8bce6976677bce46ccde))
* **restore:** refill the read-ahead as budget frees ([e529b9e](https://github.com/This-Is-NPC/alambique/commit/e529b9eb8f1fc35af7a775113def863a62c0e0de))

## [0.6.1](https://github.com/This-Is-NPC/alambique/compare/v0.6.0...v0.6.1) (2026-11-13)


### Bug Fixes

* **native:** use try_update, declare the Rust version ([bd7909b](https://github.com/This-Is-NPC/alambique/commit/bd7909b19d990a3a3fc87e1e9b23db9c462b63bb))

## [0.6.0](https://github.com/This-Is-NPC/alambique/compare/v0.5.0...v0.6.0) (2026-11-13)


### Features

* **breaker:** report the breaker status ([9396ae6](https://github.com/This-Is-NPC/alambique/commit/9396ae6a8cd2da6ed197e02acda86c22be393f74))
* **nodes:** add the node registry in the store ([abe77c9](https://github.com/This-Is-NPC/alambique/commit/abe77c9094abb5c9d8cb3e20c2169802234a6334))
* **ops:** accept AWS settings in the mix tasks ([449bbf1](https://github.com/This-Is-NPC/alambique/commit/449bbf1958ac42f8b87ca27eafb86924729e4d70))
* **registry:** keep the node in the registry ([7a45bf2](https://github.com/This-Is-NPC/alambique/commit/7a45bf23b3625b66defaa518a2dca37aebd2e705))
* **s3:** accept AWS endpoint and renewable credentials ([d1c8032](https://github.com/This-Is-NPC/alambique/commit/d1c80320653d414a0f08b999eede10e1f6d0c6c7))

## [0.5.0](https://github.com/This-Is-NPC/alambique/compare/v0.4.0...v0.5.0) (2026-11-13)


### ⚠ BREAKING CHANGES

* **restore:** the restore option prefetch_bytes is removed; use max_inflight_bytes.

### Features

* **cell:** retry a failed upload while leased ([72bb0f1](https://github.com/This-Is-NPC/alambique/commit/72bb0f13d4fc9bd315e75698456165fa18c720c6))
* **restore:** cap the bytes a restore holds ([9be3b13](https://github.com/This-Is-NPC/alambique/commit/9be3b13d23e5bff65d5807416a1804cf2587f017))
* **router:** add owner lookup from peer registries ([ba9baee](https://github.com/This-Is-NPC/alambique/commit/ba9baeeba1bd60dbc054bf0a81847c687387b0ca))
* **router:** back off the lease read of a failing store ([801ec32](https://github.com/This-Is-NPC/alambique/commit/801ec323c682beedfb1f877fad393e767fbb71f0))
* **store:** add a circuit breaker and max_attempts ([c27f96e](https://github.com/This-Is-NPC/alambique/commit/c27f96e9870d29e0d932f956b9e36428674b2490))
* **wake:** back off wakes that fail on the store ([4cc09a9](https://github.com/This-Is-NPC/alambique/commit/4cc09a9b1108a51bbe5b036c569994b655783c76))


### Bug Fixes

* **lease:** renew through a breaker that is open ([233a5f0](https://github.com/This-Is-NPC/alambique/commit/233a5f0d3ef5581ec7b840c65bdaee7ea5c87497))
* **router:** let take_over end the wake backoff ([7d9cd3f](https://github.com/This-Is-NPC/alambique/commit/7d9cd3f9c5919df5031d6d1d1c075c3d7c985fa2))
* **wake:** read the store only after the lease ([dfc98fb](https://github.com/This-Is-NPC/alambique/commit/dfc98fb0d343167a88b19a54bae0d68548f7bd4a))

## [0.4.0](https://github.com/This-Is-NPC/alambique/compare/v0.3.1...v0.4.0) (2026-11-12)


### Features

* **admission:** queue wakes above max_wakes ([0c26fc3](https://github.com/This-Is-NPC/alambique/commit/0c26fc34b32fd46a2d99e4df20e576e2de9f1957))
* **cell:** add Alambique.checkpoint/3 for awake cells ([18b2be1](https://github.com/This-Is-NPC/alambique/commit/18b2be156cac001e03fd3981979af31093fd006f))
* **cells:** list last use and wal size ([9e7cbf3](https://github.com/This-Is-NPC/alambique/commit/9e7cbf30db24dfc9dd8e6195a94c421b1b05ee76))
* **native:** report a full disk by its code ([c160ab9](https://github.com/This-Is-NPC/alambique/commit/c160ab98b54439136a5d4f01ff10a9b074f645f2))
* return {:error, :disk_full} for a full disk ([af6a308](https://github.com/This-Is-NPC/alambique/commit/af6a308afd732a4e0dcaf782ae3521bee545648a))
* **wal:** add a node budget for the WAL files ([499abe5](https://github.com/This-Is-NPC/alambique/commit/499abe50cd50eb3b4e47fcd37c1973b004260f06))


### Bug Fixes

* **admission:** free a wake slot when its caller dies ([7d14ff7](https://github.com/This-Is-NPC/alambique/commit/7d14ff73f09ac68f81365a1393cb19343e81cf7a))
* **admission:** never wake above max_wakes ([0540d22](https://github.com/This-Is-NPC/alambique/commit/0540d22919cf42b934716505f1d7e037337fa184))
* **cell:** report a full disk when the wake makes its dir ([08e1a81](https://github.com/This-Is-NPC/alambique/commit/08e1a81ae30388f48dc6c04a8bbc6c5724da255b))
* **load:** list the variant once in the command ([deb31c5](https://github.com/This-Is-NPC/alambique/commit/deb31c58c0d5155895071e9a21ceb94fd3f0b45f))
* **load:** treat store 5xx near the limit as full ([419ded3](https://github.com/This-Is-NPC/alambique/commit/419ded3ff9e499e253ae23fd8e21d11d98cfd742))
* **query:** report a full disk on the replica ([5de3ef9](https://github.com/This-Is-NPC/alambique/commit/5de3ef9f9759538489fcd9c7d3d66449a13c14c9))


### Performance Improvements

* **feed:** save the feed time in the feed table ([7be9cdb](https://github.com/This-Is-NPC/alambique/commit/7be9cdbc047286460254e498669e00d8b4074cc0))

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
