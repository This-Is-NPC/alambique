# How to discover the other nodes

**The question:** how does a node find the other nodes of the same deployment,
using only the bucket they already share, so that my application can connect
them?

This guide needs the instance of [how-to-call-cells.md](how-to-call-cells.md)
and the store of [how-to-configure-the-s3-store.md](how-to-configure-the-s3-store.md).
Alambique only tells you which nodes are live. It never connects nodes, and it
never uses this list to choose the owner of a cell. The lease does that
([design.md](design.md), section 5). The registry is described in section 22.

## Prerequisites

- The same `:store` and `:prefix` (or `:placement`) on every node.
- A `:node_id` that is unique among the nodes. The default is
  `Atom.to_string(node())`.
- Credentials that can write under `<prefix>/nodes/` in the bucket. Whoever
  can write there decides which addresses your nodes find, so give that access
  only to the nodes.

## 1. Turn the registry on

Add `registry:` with the address other nodes should use for this node:

```elixir
{Alambique,
 name: MyApp.Cells,
 store: {Alambique.Store.S3, s3_config},
 prefix: "myapp",
 data_dir: "/var/lib/myapp/cells",
 node_id: "myapp@192.0.2.7",
 registry: [advertise: "192.0.2.7:4369", meta: %{"zone" => "a"}]}
```

| Option | Default | Meaning |
| --- | --- | --- |
| `:advertise` | none, required | Non-empty string. Alambique does not read it. |
| `:meta` | `%{}` | Map that encodes as JSON. For your application. |
| `:ttl_ms` | `:lease_ttl_ms` (30 000) | How long an entry stays live without a renewal. Must be more than twice `:skew_margin_ms`. |
| `:renew_ms` | a third of `:ttl_ms` | How often the node renews its entry. Must be less than `:ttl_ms`. |
| `:refresh_ms` | `:renew_ms` | How often `peers/1` reads the list again. |
| `:page_size` | `100` | Keys per listing request. |
| `:sweep_ms` | `3_600_000` | How often the expired and released entries of other nodes are deleted. |

Expected result: within `:renew_ms` the object `<prefix>/nodes/<node_id>`
exists in the bucket, with `"released": false`.

## 2. Read the peers

```elixir
Alambique.peers(MyApp.Cells)
#=> [%{node_id: "myapp@192.0.2.8", incarnation: "...", advertise: "192.0.2.8:4369",
#=>    meta: %{"zone" => "b"}}]
```

The list is the last listing, and it does not include this node. Reading it
never waits for the store. While the store fails, the list keeps its last
value. It is `[]` before the first listing, and when no other node is live.

## 3. React to changes

Attach to `[:alambique, :registry, :peers_changed]`. `joined` and `left` list
the peers that changed since the last event:

```elixir
:telemetry.attach(
  "connect-peers",
  [:alambique, :registry, :peers_changed],
  fn _event, _measurements, %{joined: joined, left: left}, _config ->
    for %{advertise: address} <- joined, do: MyApp.Mesh.connect(address)
    for %{advertise: address} <- left, do: MyApp.Mesh.forget(address)
  end,
  nil
)
```

`MyApp.Mesh.connect/1` is yours. It turns `advertise` into a `Node.connect/1`
or whatever your application uses. The handler runs in the registry process,
so keep it short.

Expected result: when a node starts or leaves, the handler runs once with the
peer in `joined` or `left`.

## What happens when a node leaves or dies

- `Alambique.drain/2` and a stop of the instance mark the entry released. The
  node leaves every peers list at the next refresh. After a drain, `Alambique.join/1`
  registers the node again within `:renew_ms`.
- A node that crashes or loses the network stops renewing. Its entry leaves the
  lists after `:ttl_ms` plus `:skew_margin_ms`: 32 s with the defaults. Lower
  `:ttl_ms` and `:renew_ms` to see it sooner. Each renewal is one store write.
- A restart with the same `:node_id` takes over the entry at once, without
  waiting for the TTL.

## Troubleshooting

- **`peers/1` is always `[]`.** Check that the nodes share `:store` and
  `:prefix`, that `registry:` is set on each of them, and that the bucket has
  objects under `<prefix>/nodes/`. Look for `[:alambique, :registry, :error]`
  events and for `node registry ... failed` warnings. The credentials may lack
  write access to `<prefix>/nodes/`.
- **A node keeps disappearing from the list.** Two live instances share a
  `:node_id`. The later start takes the entry. The earlier instance logs a
  warning that it lost the entry, and it stays out until the other instance
  leaves or its entry expires. Give each node its own `:node_id`. While
  [a swap of two nodes](how-to-swap-a-node.md) uses one id, the old node
  leaves the list. That is expected.
- **A stopped node is still listed.** It stays until its entry expires. A
  graceful stop or a drain removes it at once.

## Next steps

- [how-to-swap-a-node.md](how-to-swap-a-node.md) replaces a node.
- [telemetry.md](telemetry.md) lists the registry events.
- [design.md](design.md), section 22, gives the rules and the limits.
