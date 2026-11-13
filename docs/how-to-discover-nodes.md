# How to discover the other nodes through the store

**The question:** how does a node find the other nodes of the same deployment,
with nothing but the bucket they already share, so that my application can
connect them?

This guide stands alone. It needs the instance of
[how-to-call-cells.md](how-to-call-cells.md). Alambique only tells you who is
there; it never connects nodes and never uses this list to decide who owns a
cell (that is the lease, design section 5). The design is in
[design.md](design.md) section 22.

## Prerequisites

- The same `:store` and `:prefix` (or `:placement`) on every node.
- A `:node_id` that is unique among the nodes. The default is
  `Atom.to_string(node())`.
- Credentials that can write under `<prefix>/nodes/` in the bucket. Whoever can
  write there decides which addresses your nodes find, so give them only to
  the nodes.

## 1. Turn the registry on

Add `registry:` with the address other nodes should use for this one:

```elixir
{Alambique,
 name: MyApp.Cells,
 store: {Alambique.Store.S3, s3_config},
 prefix: "myapp",
 data_dir: "/var/lib/myapp/cells",
 node_id: "myapp@10.0.0.7",
 registry: [advertise: "10.0.0.7:4369", meta: %{"zone" => "a"}]}
```

`advertise` is any non-empty string; Alambique does not read it. `meta` is a
map that must encode as JSON. The defaults, derived from the lease, are:

| Option | Default | Meaning |
| --- | --- | --- |
| `ttl_ms` | `lease_ttl_ms` (30 000) | How long an entry is live without a renewal. Greater than twice `skew_margin_ms`. |
| `renew_ms` | a third of `ttl_ms` | How often the node renews. Less than `ttl_ms`. |
| `refresh_ms` | `renew_ms` | How often the list of peers is read again. |
| `page_size` | 100 | Keys per listing request. |
| `sweep_ms` | 3 600 000 | How often released and expired entries are deleted. |

Expected result: the instance starts, and within `renew_ms` the object
`<prefix>/nodes/<node_id>` exists in the bucket with `"released": false`.

## 2. Read the peers

```elixir
Alambique.peers(MyApp.Cells)
#=> [%{node_id: "myapp@10.0.0.8", incarnation: "...", advertise: "10.0.0.8:4369",
#=>    meta: %{"zone" => "b"}}]
```

The list is the last listing, without this node. It never waits for the store
and keeps its last value while the store fails. It is `[]` before the first
listing, and when no other node is live.

## 3. React to changes

Attach to `[:alambique, :registry, :peers_changed]`; `joined` and `left` list
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

`MyApp.Mesh.connect/1` is yours: it turns `advertise` into a `Node.connect/1`
or whatever your application uses. The handler runs in the registry process,
so keep it short.

## What happens when a node leaves or dies

- `Alambique.drain/2` and a stop of the instance mark the entry released: the
  node leaves every peers list at its next refresh. After a drain, `join/1`
  registers the node again within `renew_ms`.
- A node that crashes or loses the network stops renewing. Its entry leaves
  the lists after `ttl_ms` plus `skew_margin_ms` (32 s with the defaults for
  the lease). Lower `ttl_ms` and `renew_ms` to see it sooner; a renewal is one
  request, so lower values cost requests, not bytes.
- A restart with the same `node_id` takes the entry over at once, without
  waiting for the TTL.

## Troubleshooting

- **`peers/1` is always `[]`.** Check that the nodes share `:store` and
  `:prefix`, that the option `registry` is set on each, and that the bucket
  has `<prefix>/nodes/` objects. Look for `[:alambique, :registry, :error]`
  events or `node registry ... failed` log lines: the credentials may lack
  write access to `<prefix>/nodes/`.
- **A node keeps disappearing from the list.** Two live instances share a
  `node_id`. The later one takes the key; the earlier one logs that it lost
  the entry and stays out until the other leaves. Give each node its own
  `node_id`. During [a swap of two nodes](how-to-swap-a-node.md) that share
  an id this is expected: the old node is out of the list, which is what you
  want.
- **A node that stopped is still listed.** It is live until its entry
  expires; see above. A graceful stop or drain removes it at once.

## Next steps

- [how-to-swap-a-node.md](how-to-swap-a-node.md) to replace a node.
- [telemetry.md](telemetry.md) for the registry events.
- [design.md](design.md) section 22 for the rules and the limits.
