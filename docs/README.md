# Documentation

Each page answers one question. The pages are in reading order within each
group. Users need only the first group; contributors read the second, which
may also use the first.

## For users

| Page | Question it answers |
| --- | --- |
| [concepts.md](concepts.md) | What do cell, lease, epoch and segment mean? |
| [how-to-run-a-first-cell.md](how-to-run-a-first-cell.md) | How do I run a first cell against a local store? |
| [how-to-call-cells.md](how-to-call-cells.md) | How do I start an instance and call cells from my application? |
| [storage-compatibility.md](storage-compatibility.md) | Which object stores pass the conditional-write tests? |
| [how-to-discover-nodes.md](how-to-discover-nodes.md) | How do nodes find each other through the store? |
| [how-to-swap-a-node.md](how-to-swap-a-node.md) | How do I replace a node with another one, and what do callers see meanwhile? |
| [cost-of-a-cell.md](cost-of-a-cell.md) | What does one cell cost, and how many fit on my node? |
| [how-to-speed-up-a-wake.md](how-to-speed-up-a-wake.md) | Why does the first call to a hibernated cell take long, and which options change it? |
| [how-to-operate-a-cell.md](how-to-operate-a-cell.md) | How do I inspect, restore, freeze or move a cell, and handle a full or stalled disk? |
| [how-to-monitor-a-node.md](how-to-monitor-a-node.md) | Which events, calls and host metrics show a node is healthy? |
| [how-to-manage-the-bucket.md](how-to-manage-the-bucket.md) | What does Alambique delete from the bucket, and how do I change how long old data stays? |
| [how-to-recover-from-disaster.md](how-to-recover-from-disaster.md) | How do I bring the cells back on a new node when only the bucket is left? |
| [how-to-configure-the-s3-store.md](how-to-configure-the-s3-store.md) | How do I point the S3 store at AWS or a compatible server, with static or renewing credentials? |
| [how-to-rewind-a-cell.md](how-to-rewind-a-cell.md) | How do I undo what a cell did after a point in time? |
| [how-to-query-across-cells.md](how-to-query-across-cells.md) | How do I run read-only SQL over many cells without waking them? |
| [how-to-read-replicas.md](how-to-read-replicas.md) | How do I serve the reads of a hot cell from other nodes with a bounded lag? |
| [how-to-consume-the-change-feed.md](how-to-consume-the-change-feed.md) | How do I read the row changes of all cells and resume where I stopped? |
| [how-to-roll-out-a-schema-change.md](how-to-roll-out-a-schema-change.md) | How do I change the schema while old and new code run together? |
| [segment-format.md](segment-format.md) | What bytes does a replicated segment contain, and how is the format checked? |
| [telemetry.md](telemetry.md) | Which telemetry events does Alambique emit, and with what data? |
| [known-limitations.md](known-limitations.md) | Which known limitations and open defects affect me, and what is the workaround? |

Recommended path for a first read: the project [README](../README.md), then
`concepts.md`, `how-to-run-a-first-cell.md`, `how-to-call-cells.md` and
`storage-compatibility.md`. Operators continue with `how-to-discover-nodes.md`,
`how-to-swap-a-node.md`, `cost-of-a-cell.md`, `how-to-monitor-a-node.md`,
`how-to-manage-the-bucket.md` and `how-to-recover-from-disaster.md`. Read
`known-limitations.md` before you run a release in production.

## For contributors

| Page | Question it answers |
| --- | --- |
| [contributing.md](contributing.md) | How do I build, test and measure the project? |
| [design.md](design.md) | How are the model, components, state and protocols built? |
| [how-to-check-the-pipeline-model.md](how-to-check-the-pipeline-model.md) | How do I model-check pipelined commits, the lease and the fence with TLA+? |
| [how-to-load-test.md](how-to-load-test.md) | How do I load-test one node in a limited container? |

Start with [contributing.md](contributing.md), which lists the commands and the
benchmarks, then read `design.md`. The TLA+ and load-test guides stand alone.
