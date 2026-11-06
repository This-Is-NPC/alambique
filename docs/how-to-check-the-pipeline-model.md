# How to check the pipeline model

**The question:** how do I check the TLA+ model of pipelined commits, the
lease and the fence, and what do its checks mean for the code?

This guide stands alone. The protocol it models is in
[design.md](design.md), section 6. The commands are for contributors who
change the commit path, the restore rule or the lease.

The model is `tla/Pipeline.tla`; `tla/Pipeline.cfg` holds the constants. It
is a model of the protocol, not of the code: it shows that the rules are
safe together, and it does not run the Elixir modules. The pipelined cell
itself is planned (see "Where each action lives" below).

## Before you start

- `mise` installed. `mise install` provides Java (`openjdk-17.0.2`).
- Network access once: `scripts/tla-tools` downloads `tla2tools.jar`
  (release v1.8.0 of github.com/tlaplus/tlaplus) into
  `~/.cache/alambique/tla/` (`$XDG_CACHE_HOME` is honoured) and checks its
  SHA-256 against `ALAMBIQUE_TLA_SHA256` in `mise.toml`.
- A few GB of free memory and about two minutes for the default run.

## 1. Check the model

```sh
mise run check:tla
```

Expected output, last lines (measured on a 12-thread laptop, TLC
`2026.10.30.154139`, constants `Nodes = {a, b}`, `MaxEpoch = 3`,
`MaxTx = 4`, `Depth = 2`):

```
Model checking completed. No error has been found.
10756947 states generated, 2657393 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 34.
Finished in 02min 09s
```

The task exits non-zero when TLC finds a violation and prints the
sequence of steps that leads to it.

## 2. Check a smaller or a larger model

Edit `tla/Pipeline.cfg` and run step 1 again. `MaxTx = 3` has 256,401
distinct states and takes 8 seconds. `Depth = 3` is far larger: it passed
10 million distinct states after 8 minutes and was stopped before it
finished, so it is not a routine check. Put the values back afterwards: the
checked-in configuration is the one the task is expected to finish in a few
minutes.

## 3. Check that the model can fail

```sh
mise run check:tla:mutations
```

It runs the model with a deliberate bug (the `Bug` constant) and requires a
violation. Expected output:

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
| `RefuseSent` | a stop refuses the writers of every batch it sent, as the cell did before it answered `outcome_unknown` | a sent batch lands after the refusal: `NoRefusedDurable` |
| `ChainNoPre` | the chain does not compare checksums | **not caught**, on purpose: given the epoch rule and the fence, a segment whose epoch does not decrease always has the right checksum, because a later epoch always writes on top of the chain it restored. The checksum check guards the data (a damaged or missing page change), which the model does not represent. The task requires this line to stay silent; if a change makes the checksum matter, it fails |

To run one by hand: `scripts/tla-check --mutation OverwriteAny` (through
`mise exec --`, which sets the pinned versions).

## What the model contains

- **Nodes** acquire the lease with increasing epochs. The old holder is not
  told and keeps running.
- **Restore** computes the chain: the longest contiguous run from the start
  whose checksums chain and whose epochs do not decrease. Everything after
  it is a tail and is ignored. A fence changes no page, so it leaves the
  checksum it found.
- **Fence** at head + 1 with `if-none-match`, after a lease check.
- **Encode** of up to `Depth` batches beyond the acknowledged prefix, each
  chained to the previous one. Nothing is encoded twice. A batch either
  leaves a fresh checksum or keeps the one it found (it rewrites pages with
  the same bytes, or changes and reverts them), chosen at random.
- **Puts** land in any order and at any time, also after the sender crashed
  or after a new owner's fence. Writers only create; a replace needs
  `if-match` on the object just read.
- **412 handling:** the same bytes and epoch mean it landed; an object with
  an epoch below the epoch of the predecessor is a zombie and is replaced;
  anything else means taken, so the cell stops (for the fence: restore
  again).
- **Acknowledgement** from a FIFO: only the oldest batch, once it landed.
- **Answers** as a history, not as callers: `refused` holds the batches whose
  writers got a definite refusal. A put that meets another writer's object
  (taken) refuses its own batch; a stop refuses nothing else, because the
  other sent batches may still land (their writers get `outcome_unknown`).
- **Crashes** at any point. An upload failure also stops the cell, so it is
  modelled as a crash with the request still in flight.

Not modelled: the 16-file window and the level-0 rule that keep real history
loss loud (the model treats every unreachable file after the chain as a
tail), lease deadlines (a stale owner keeps acknowledging until it is
fenced, which is stricter), compaction and retention, and a store whose LIST
misses a completed PUT. Restore, fence and lease check assume the store
sees every completed PUT, as the code does.

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

Each action of the model has one place in the code.

| Model action | Code |
| --- | --- |
| `Acquire` | `Alambique.Lease` (design section 5) |
| `Restore` | `Alambique.Replication.Restore` (its private `walk/2` and `tail?/2`); the lease check is `Alambique.Cell.Wake.ensure_lease/1` |
| fence put | `Alambique.Replication.Replicator.fence/3`, with the put rule below; "restore again" is `Alambique.Cell.Wake.restore_and_fence/2` |
| `Encode` | `Replicator.encode/4`, called by `Alambique.Cell` (`send_batch/1`) once per batch, in the cell process |
| `Apply`, `Learn`, `Resend` | `Replicator.upload/3`, run by a linked upload process of the cell: the retry. The `if-none-match` put, the 412 `get` and the zombie replace with `{:if_match, etag}` are the private `Replicator.put/8` and `collide/7`, shared by `upload`, `snapshot` and `replace` |
| `Ack` | the pop loop of `Alambique.Cell` (`pop/2`) over the FIFO of `Alambique.Cell.Pipe`: the only place that answers a write |
| `Crash` | a stopped cell: `Cell.stop_serving` on a failed or fenced upload (it answers the writers of every sent batch `outcome_unknown`, the others with the reason, and kills the upload processes; `refuse_taken/3` refuses the batch whose put was taken), or a VM crash |
| `Chain`, tail rule | `Alambique.Replication.Chain` (`tail?/3`, `live/1`), used by `Restore`, `Replication.Head` and `Query.Replica` |

## Troubleshooting

- `does not match ALAMBIQUE_TLA_SHA256`: the download is not the pinned
  release. Do not change the hash to make it pass; check the version in
  `mise.toml`.
- `NullPointerException ... CgroupInfo` at start: the old JDK 17.0.2 cannot
  read some cgroup v2 setups. `scripts/tla-check` already passes
  `-XX:-UseContainerSupport`; keep it if you run TLC by hand.
- The run is slow or out of memory: lower `MaxTx` or `Depth` in the
  configuration. State counts grow quickly (see step 2).

## Next steps

- [design.md](design.md), section 6, for the rules the model checks.
- [segment-format.md](segment-format.md) for the restore steps.
