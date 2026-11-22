# Storage compatibility

Alambique needs atomic conditional writes on the store: `If-None-Match: *`
and `If-Match`, with HTTP 412 on failure. The shared contract suite
(`Alambique.StoreContract`) checks this against the in-memory store and,
under the `:seaweedfs` tag, against a real server.

At every start the instance checks this on a scratch object under each
store's prefix (`<prefix>/store-check/<random>`, four puts and a delete, then
removed): a create repeated, and a replace with a wrong etag, must both give
412, and a replace with the right etag must succeed. A store that fails refuses
the start with `{:error, {:store_not_conformant, location_id, failed}}`, where
`failed` lists `:if_none_match` and/or `:if_match`. If the store cannot be
reached for the check, the start logs a warning and goes on. Once a store
version is known to pass, `check_store: false` skips it (design section 3).
For a store this page does not list, keep the check on until it passes.

## SeaweedFS 4.48

Status: **passes**. Image `chrislusf/seaweedfs:4.48`.

The contract suite includes two races, each repeated over 5 keys:

- 50 concurrent `If-None-Match: *` puts to one key: exactly one `200`, 49 `412`.
- 50 concurrent `If-Match` puts with the same etag: exactly one winner.

The suite passes on this version. How to run it is in
[contributing.md](contributing.md).

Limitation: the server serializes conditional writes to one key. A writer that
races others can wait longer than an uncontended put. Set a receive timeout
above that wait when many nodes can race for one lease.

Other behavior of this store that the S3 client accounts for:

- ETags are content hashes, so rewriting identical bytes keeps the etag. The
  in-memory store changes the etag on every put instead; lease objects differ
  on every write because they carry an expiry.
- Metadata header names come back lowercased.

## Store settings Alambique relies on

Alambique needs nothing beyond the conditional writes above. The settings
below are optional protection against loss that Alambique's own rules cannot
see. The versioning and object lock results below are checked on SeaweedFS 4.48.
SeaweedFS support for the other settings is **not verified**; check it on your
version before you count on it.

**Delayed deletions (an Alambique feature, no store setting).** Retention and
the cleanup of a moved cell only mark objects; the reaper deletes them after
`deletion_grace_ms` (default one day, design section 9.1). This covers a
wrong window or a buggy run, not a store-wide mistake.

**Versioning (checked on SeaweedFS 4.48).** A bucket accepts
`PUT ?versioning` with `Status=Enabled`. A delete through `Alambique.Store.S3`
leaves a delete marker as the latest version, and the previous version stays
in `list-object-versions`. Alambique's SeaweedFS tests cover the delete markers,
listings, `scrub` and a cell that moves back to a location while its marked
objects wait for the reaper.

Object lock: `put-object-lock-configuration` with `ObjectLockEnabled=Enabled`
and `DefaultRetention={Mode=GOVERNANCE,Days=1}` was accepted and read back by
SeaweedFS 4.48. A version listed after the delete showed no retention date,
so enforcement of the lock was not observed and must not be relied on.

**Bucket versioning.** With versioning on, a `DELETE` or an overwrite keeps the
old version. It protects against what happens to the bucket outside Alambique:
a script that empties a prefix, an operator mistake, a bug in the reaper that
passes the grace. Alambique does not read versions: recovery means restoring
the versions with the store's tools, then restoring the cell. Cost: deleted
objects still occupy space until a lifecycle rule removes the noncurrent
versions. Set that rule to expire noncurrent versions after at least
`deletion_grace_ms` plus the time you want to keep as a safety net; a shorter
rule makes the store forget what the grace period keeps. Lease objects are
overwritten at every renewal (roughly every `lease_ttl_ms / 3` per awake cell),
so each awake cell adds many noncurrent versions of its lease: keep their
expiry short, or exclude `cells/*/lease` with a lifecycle filter if the store
allows it.

**Object lock on snapshots.** An object lock makes a version immutable until a
date, even for credentials that could delete it. It protects against
ransomware and stolen credentials, which versioning alone does not stop (the
attacker can delete versions too). Object lock needs versioning. `Alambique.Store.S3`
sends no lock headers, so only a bucket *default retention* applies, and it
applies to every object: when Alambique deletes an object the store adds a
delete marker, when it overwrites one (leases, the `trashed` marker) it adds a
version, and the locked versions stay until the retention ends. Set the default
retention to at least `pitr_window_ms + deletion_grace_ms` if you want every
restore point of the window, and the snapshots they start from, to survive such
an attack; the price is that no storage is freed before the retention ends and
that leases add many locked versions. A lock limited to snapshot objects needs
per-object locks (the writer sets a header on `cells/*/0009/*`) or a default
retention per prefix; Alambique does not do the first, and whether SeaweedFS
offers the second is not verified. Snapshots are the only objects a restore
cannot do without; the segments after the newest snapshot are not protected
more than the other objects are.

**Lifecycle rules.** Do not use rules that expire current objects of
`cells/` or `trash/` by age: they delete files that restores or the reaper
still need and make retention's rules meaningless. Rules for noncurrent
versions and for incomplete multipart uploads are safe. Never expire objects
under `trash/` earlier than `deletion_grace_ms`: the manifests would
disappear before their objects, and the objects would stay.

**SeaweedFS.** It is the only store Alambique is tested on. Versioning and
object lock are features of its S3 gateway; which of them 4.48 supports, and
how it lists versions and honors filters, is not verified here.

## Other stores

SeaweedFS is the only store Alambique is tested and supported on. AWS S3 and
Cloudflare R2 have not been run, so they are not supported.
