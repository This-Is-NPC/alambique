# Storage compatibility

Alambique needs atomic conditional writes on the store: `If-None-Match: *`
and `If-Match`, with HTTP 412 on failure. The shared contract suite
(`Alambique.StoreContract`) checks this against the in-memory store and,
under the `:seaweedfs` tag, against a real server.

## SeaweedFS 4.48

Status: **passes**. Observed on 2026-10-26 with image
`chrislusf/seaweedfs:4.48`, started by `mise run seaweedfs:up` as the
disposable container `alambique-seaweedfs`.

The suite includes two races, each repeated over 5 keys:

- 50 concurrent `If-None-Match: *` puts to one key: exactly one `200`, 49 `412`.
- 50 concurrent `If-Match` puts with the same etag: exactly one winner.

Output of `mise run test:seaweedfs`:

```text
Finished in 56.2 seconds (0.07s async, 56.1s sync)
Result: 8 passed, 22 excluded
```

A manual run with 50 parallel `curl` clients (SigV4, `If-None-Match: *`) agrees:
one `200` and 49 `412`.

Observed limitation: contended conditional writes to one key are serialized
by the server. With 50 concurrent writers the last answer arrives after about
3 seconds with Req, and about 8 seconds with 50 `curl` processes, in steps of
one second. A single uncontended put takes about 1 ms. Callers should set a
receive timeout above this when many nodes can race for one lease.

Other behavior of this store that the S3 client accounts for:

- ETags are content hashes, so rewriting identical bytes keeps the etag. The
  in-memory store changes the etag on every put instead; lease objects differ
  on every write because they carry an expiry.
- Metadata header names come back lowercased.

## Store settings Alambique relies on

Alambique needs nothing beyond the conditional writes above. The settings
below are optional protection against loss that Alambique's own rules cannot
see. Nothing here was run: SeaweedFS support for each setting is **not
verified** and has to be checked on your version before you count on it.

**Delayed deletions (an Alambique feature, no store setting).** Retention and
the cleanup of a moved cell only mark objects; the reaper deletes them after
`deletion_grace_ms` (default one day, design section 9.1). This covers a
wrong window or a buggy run, not a store-wide mistake.

**Observed on SeaweedFS 4.48 (2026-10-27).** A dedicated bucket accepted
`PUT ?versioning` with `Status=Enabled`. Deleting an object through
`Alambique.Store.S3` returned a `DeleteMarker` as the latest version while
`list-object-versions` kept the previous `Version` (a `probe.txt` had one
`DeleteMarker` and one version with its 5-byte body). Listings through the
store did not return the deleted key, `Retention`/`Trash` removed the data as
usual, a restore of the latest snapshot still passed its checksums, and
`Alambique.scrub/2` reported the cell sound. A cell moved back to a cleaned
location while its `deletion_grace_ms` manifests were pending and kept its
rows after the reaper ran. The tagged test is
`test/versioning_seaweedfs_test.exs`; it uses a fresh bucket per run.

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
Cloudflare R2 have not been run and are out of scope for now.
