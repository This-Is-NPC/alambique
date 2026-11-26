# How to check the pipeline model

**The question:** how do I run the TLA+ model of pipelined commits, the lease and
the fence, and what do its checks mean for the code?

This guide stands alone. The protocol the model describes is in
[design.md](design.md), section 6. The commands are for contributors who change
the commit path, the restore rule or the lease.

The model is `tla/Pipeline.tla`; `tla/Pipeline.cfg` holds the constants. It is a
model of the protocol, not of the code: it shows that the rules are safe
together, and it does not run the Elixir modules. The table in "Where each action
lives" links each action to the code that implements it.

The per-cell options (`cell_options`) and the timeouts of the callbacks that
choose them or the migrations change no variable or action of the model; they
are outside it.

## Prerequisites

- `mise` installed. `mise install` provides Java (`openjdk-17.0.2`).
- Network access once. `scripts/tla-tools` downloads `tla2tools.jar` into
  `~/.cache/alambique/tla/` (`$XDG_CACHE_HOME` is honoured). The file is
  TLC 1.8.0 (github.com/tlaplus/tlaplus), served from the
  `tools-tla2tools-1.8.0` release of this repository. It checks the SHA-256
  against `ALAMBIQUE_TLA_SHA256` in `mise.toml`.
- Enough free memory for TLC. If the run runs out of memory, lower `MaxTx` or
  `Depth` (step 2).

## 1. Check the model

```sh
mise run check:tla
```

Expected output, last lines (the state counts come from the checked-in
constants):

```
Model checking completed. No error has been found.
10756947 states generated, 2657393 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 34.
```

The task exits non-zero when TLC finds a violation. It then prints the sequence
of steps that leads to the violation.

## 2. Check a smaller or a larger model

Edit `tla/Pipeline.cfg` and run step 1 again. `MaxTx = 3` gives a much smaller
state space. `Depth = 3` grows far faster than the default and is not a routine
check. Put the checked-in values back afterwards: `mise run check:tla` runs the
default configuration.

## 3. Check that the model can fail

```sh
mise run check:tla:mutations
```

The task runs the model once for each deliberate bug (the `Bug` constant) and
requires a violation for each one. The `ChainNoPre` bug must not be caught. The
output has one line per bug:

```
ok   AckOutOfOrder: Error: Invariant NoAckedCommitLost is violated.
ok   OverwriteAny: Error: Action property ChainPrefixStable is violated.
ok   NoFence: Error: Invariant FenceEffective is violated.
ok   ChainNoEpoch: Error: Invariant ChainEpochsMonotone is violated.
ok   ChainNoEpochNoPre: Error: Invariant ChainEpochsMonotone is violated.
ok   RefuseSent: Error: Invariant NoRefusedDurable is violated.
ok   ChainNoPre is covered by the epoch rule (no violation)
```

| `Bug` | What it changes | Why TLC finds it |
| --- | --- | --- |
| `AckOutOfOrder` | a landed batch is answered before an older one | a later batch is acknowledged, the older one fails, and the chain stops below the acknowledged TXID. With only `InOrderAck` checked, TLC reports `InOrderAck` too |
| `OverwriteAny` | a 412 object is replaced whatever its epoch | the new owner replaces a real segment of the old owner |
| `NoFence` | the new owner serves without writing the fence | an old owner acknowledges a TXID above the point where the new owner starts |
| `ChainNoEpoch` | the chain does not require non-decreasing epochs | a stale owner's batch that kept the checksum (a neutral batch) carries the checksum the new owner's chain ends with, so it joins the chain after a higher epoch: `ChainEpochsMonotone` |
| `ChainNoEpochNoPre` | neither check | same violation |
| `RefuseSent` | a stop refuses the writers of every batch it sent | a sent batch lands after the refusal: `NoRefusedDurable` |
| `ChainNoPre` | the chain does not compare checksums | **not caught**, on purpose: given the epoch rule and the fence, a segment whose epoch does not decrease always has the right checksum, because a later epoch always writes on top of the chain it restored. The checksum check guards the data (a damaged or missing page change), which the model does not represent. This line must stay silent. If a change makes the checksum matter, update the model and this table |

To run one bug by hand, use `scripts/tla-check --mutation OverwriteAny`, through
`mise exec --` so that the pinned versions are used.

## What the model contains

- **Nodes** acquire the lease with increasing epochs. The old holder is not
  told and keeps running.
- **Restore** computes the chain: the longest contiguous run from the start
  whose checksums chain and whose epochs do not decrease. Everything after it is
  a tail and is ignored. A fence changes no page, so it leaves the checksum it
  found.
- **Fence** at head + 1 with `if-none-match`, after a lease check.
- **Encode** of up to `Depth` batches beyond the acknowledged prefix, each chained
  to the previous one. Nothing is encoded twice. A batch either leaves a fresh
  checksum or keeps the one it found (it rewrites pages with the same bytes, or
  changes and reverts them), chosen at random.
- **Puts** land in any order and at any time, also after the sender crashed or
  after a new owner's fence. Writers only create; a replace needs `if-match` on
  the object just read.
- **412 handling:** the same bytes and epoch mean the put landed. An object with
  an epoch below the epoch of the predecessor is a zombie and is replaced.
  Anything else means taken, so the cell stops (for the fence: restore again).
- **Acknowledgement** from a FIFO: only the oldest batch, once it landed.
- **Answers** as a history, not as callers: `refused` holds the batches whose
  writers got a definite refusal. A put that meets another writer's object (taken)
  refuses its own batch. A stop refuses nothing else, because the other sent
  batches may still land; their writers get `outcome_unknown`.
- **Crashes** at any point. A stop of the cell (a fence, a lost lease, or a failed
  upload that outlasts the lease) is modelled as a crash with the request still in
  flight. A failed upload that is retried while the lease holds is a put that has
  not landed yet, which the model already allows.

Not modelled: the 16-file window and the level-0 rule that keep real history loss
loud (the model treats every unreachable file after the chain as a tail), lease
deadlines (a stale owner keeps acknowledging until it is fenced, which is
stricter), compaction and retention, and a store whose LIST misses a completed
PUT. Restore, fence and lease check assume the store sees every completed PUT,
as the code does.

## What each check means

| Check | Meaning | If it fails |
| --- | --- | --- |
| `TypeOK` | variables stay in their types | the spec is wrong |
| `NoAckedCommitLost` | every acknowledged TXID is on the chain that any restore computes, with the bytes of the writer that acknowledged it (design R1) | a caller was told "durable" for a write a restore does not return |
| `InOrderAck` | no landed batch of an owner is unacknowledged below an acknowledged one, and nothing queued is older than what was acknowledged | an ack went out of TXID order |
| `OneWriterPerEpoch` | an epoch is granted once, only its holder writes under it, and it writes each TXID once | two owners share an epoch, or a batch was encoded twice |
| `ChainEpochsMonotone` | along the chain epochs never decrease (design R2) | an old owner's segment sits after a newer owner's |
| `FenceEffective` | once an owner serves, everything an older epoch acknowledged is below its fence | an old owner acknowledged a write the new owner's restore did not see |
| `NoRefusedDurable` | a batch refused definitively (`fenced`, repeatable by the caller) is never on the chain | a write was refused and is nevertheless durable, so a repeat stores it twice |
| `ChainPrefixStable` (action property) | the chain only grows: what a restore or head read returned stays a prefix of what the next one returns | a reader could see a state that later disappears |

## Where each action lives

Each action of the model has one place in the code. Private functions are marked
as such.

| Model action | Code |
| --- | --- |
| `Acquire` | `Alambique.Lease.acquire/4` (design section 5) |
| `Restore` | `Alambique.Replication.Restore` (its private `walk/2` and `tail?/2`). The lease check is the private `ensure_lease/1` of `Alambique.Cell.Wake` |
| fence put | `Alambique.Replication.Replicator.fence/3`, with the put rule below. "Restore again" is the private `restore_and_fence/2` of `Alambique.Cell.Wake` |
| `Encode` | `Replicator.encode/4`, called by `Alambique.Cell` (`send_batch/1`) once per batch, in the cell process |
| `Apply`, `Learn`, `Resend` | `Replicator.upload/3`, run by a linked upload process of the cell: this is the retry. The `if-none-match` put, the 412 `get` and the zombie replace with `{:if_match, etag}` are the private `Replicator.put/8` and `collide/8`, shared by `upload`, `snapshot` and `replace` |
| `Ack` | the private `pop/2` of `Alambique.Cell` over the FIFO of `Alambique.Cell.Pipe`: the only place that answers a write |
| `Crash` | a stopped cell: the private `Cell.stop_serving` on a failed or fenced upload (it answers the writers of every sent batch `outcome_unknown`, the others with the reason, and kills the upload processes). The private `refuse_taken/3` refuses the batch whose put was taken. Or a VM crash |
| `Chain`, tail rule | `Alambique.Replication.Chain` (`tail?/3`, `live/1`), used by `Restore`, `Alambique.Replication.Head` and `Alambique.Query.Replica` |

## Troubleshooting

- `does not match ALAMBIQUE_TLA_SHA256`: the download is not the pinned release.
  Do not change the hash to make the check pass. Check the version in `mise.toml`.
- `NullPointerException ... CgroupInfo` at start: this JDK cannot read some cgroup
  v2 setups. `scripts/tla-check` passes `-XX:-UseContainerSupport`; keep that flag
  if you run TLC by hand.
- The run is slow or runs out of memory: lower `MaxTx` or `Depth` in
  `tla/Pipeline.cfg`. State counts grow quickly (see step 2).

## Next steps

- [design.md](design.md), section 6: the rules the model checks.
- [segment-format.md](segment-format.md): the restore steps.
