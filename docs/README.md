# Documentation

| Page | Question it answers |
| --- | --- |
| [concepts.md](concepts.md) | What do cell, lease, epoch, segment and the other words mean? |
| [how-to-call-cells.md](how-to-call-cells.md) | How do I start an instance and call cells from my application? |
| [design.md](design.md) | How do cells, leases, replication, routing and the test kit work? |
| [how-to-operate-a-cell.md](how-to-operate-a-cell.md) | How do I inspect, restore, freeze or move a cell, what do I do with one the instance froze for crashing the VM, how do I restart a node without failed calls, and how do I check that the disk under the cells does not stall writes? |
| [how-to-swap-a-node.md](how-to-swap-a-node.md) | How do I replace one node by another that started beside it, with no cluster between them, and what do callers see meanwhile? |
| [how-to-size-a-node.md](how-to-size-a-node.md) | How much memory does a node need for N awake cells, and which `awake` options keep it from running out? |
| [how-to-speed-up-a-wake.md](how-to-speed-up-a-wake.md) | Why does the first call to a hibernated cell take long, how do I measure it for each user, and which options change it? |
| [how-to-load-test.md](how-to-load-test.md) | How many commits, awake cells, wakes and cross-cell queries does one node carry in a CPU- and memory-limited container, and what does each unit cost? |
| [how-to-size-storage.md](how-to-size-storage.md) | How many bytes does the store receive and keep for a cell that is written once in a while, what causes the difference from the data, and which options shrink it? |
| [how-to-rewind-a-cell.md](how-to-rewind-a-cell.md) | How do I undo what a cell did after a point, without readers seeing a mixed state, and what do the change feed's consumers do about it? |
| [how-to-query-across-cells.md](how-to-query-across-cells.md) | How do I run read-only SQL over many cells without waking them? |
| [how-to-read-replicas.md](how-to-read-replicas.md) | How do I serve the reads of a hot cell from other nodes with a bounded lag? |
| [how-to-check-the-pipeline-model.md](how-to-check-the-pipeline-model.md) | How do I model-check pipelined commits, the lease and the fence with TLA+, and what does each check mean? |
| [segment-format.md](segment-format.md) | What is in a replicated segment, how do replication and restore use it, and how is the format checked? |
| [how-to-consume-the-change-feed.md](how-to-consume-the-change-feed.md) | How do I read the row changes of all cells, and resume where I stopped? |
| [how-to-roll-out-a-schema-change.md](how-to-roll-out-a-schema-change.md) | How do I change the schema while old and new code run together? |
| [telemetry.md](telemetry.md) | Which telemetry events does Alambique emit, and with what data? |
| [storage-compatibility.md](storage-compatibility.md) | Which object stores pass the conditional-write tests? |
| [what-does-not-work.md](what-does-not-work.md) | Which known defects are still open? |
| [contributing.md](contributing.md) | How do I build, test and measure the project? |

Recommended path: read the project [README](../README.md), then
`concepts.md` and `how-to-call-cells.md`, then `design.md` sections 1 to 6 and
the storage compatibility page. Operators continue with
`how-to-operate-a-cell.md`, `how-to-swap-a-node.md` and `how-to-size-a-node.md`.
