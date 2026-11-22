# How to run a first cell on a local SeaweedFS

**The question:** how do I see one Alambique cell write, sleep and wake again
against a disposable S3 store on my machine?

This guide stands alone. It runs one script on one node. The store is a
SeaweedFS container that you remove at the end.

## Prerequisites

- Docker, running, and permission to start containers.
- Erlang/OTP, Elixir 1.17 or later, a recent stable Rust (the native crate
  declares its minimum in `rust-version`) and a C compiler. Install them with
  your usual tools; `rustup` installs Rust.
- Port 18333 free on `127.0.0.1`.

## 1. Create a project with Alambique

```sh
mix new my_app
cd my_app
```

Add the dependency to `mix.exs`, as in [the README](../README.md#install), then:

```sh
mix deps.get
mix compile
```

Expected result: the first `mix compile` downloads the crates and builds the
native library. It ends without an error.

## 2. Start SeaweedFS

```sh
docker run -d --name alambique-first-try -p 127.0.0.1:18333:8333 \
  -e AWS_ACCESS_KEY_ID=alambique -e AWS_SECRET_ACCESS_KEY=alambique-secret \
  chrislusf/seaweedfs:4.48 \
  mini -dir=/data -ip=127.0.0.1 -ip.bind=0.0.0.0 -s3.port=8333 -admin.ui=false
```

Expected result: Docker prints the ID of the new container and returns to the
prompt. The data lives inside the container.

Check that the S3 gateway answers. Any HTTP status code is enough, even `403`:

```sh
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:18333/
```

If the command prints nothing or `000`, wait a few seconds and run it again.

Alambique does not create the bucket. SeaweedFS creates `demo-bucket` on the
first write. On AWS S3, create the bucket first.

## 3. Save the example

Save this as `example.exs` in the root of the project, next to `mix.exs`:

```elixir
s3_config = %{
  endpoint: "http://127.0.0.1:18333",
  bucket: "demo-bucket",
  access_key_id: "alambique",
  secret_access_key: "alambique-secret"
}

# One instance per node, with the same name, store and prefix on every node.
children = [
  {Alambique,
   name: MyApp.Cells,
   store: {Alambique.Store.S3, s3_config},
   prefix: "myapp",
   data_dir: Path.expand("data"),
   migrations: [{1, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"}]}
]

{:ok, _} = Supervisor.start_link(children, strategy: :one_for_one)

# Call any cell from any node.
IO.inspect(Alambique.execute(MyApp.Cells, "org-1", "INSERT INTO notes (body) VALUES (?)", ["hi"]))
IO.inspect(Alambique.query(MyApp.Cells, "org-1", "SELECT body FROM notes"))
IO.inspect(Alambique.drain(MyApp.Cells))
```

The script starts one instance, writes one row to the cell `org-1`, reads the
row back and drains the node.

## 4. Run the example

```sh
mix run example.exs
```

Expected output of the first run:

```
{:ok, %{columns: [], rows: []}}
{:ok, %{columns: ["body"], rows: [["hi"]]}}
{:ok, %{errors: [], hibernated: 1}}
```

`drain/2` writes what is pending and releases the leases before the script
ends. The `hibernated: 1` line means one awake cell went to sleep.

## 5. Run it again

```sh
mix run example.exs
```

The cell wakes with the data it wrote in the first run. The script inserts a
second row, so the query now returns two rows:

```
{:ok, %{columns: ["body"], rows: [["hi"], ["hi"]]}}
```

Each run adds one row, so a third run shows three.

## 6. Clean up

Remove the container. `-v` also removes the anonymous volume it created. The
bucket data goes with it.

```sh
docker rm -fv alambique-first-try
```

To remove the local copies of the cells, delete the `data` directory in the
project. Stop any running `mix run` first.

```sh
rm -rf data
```

## Troubleshooting

- **Docker cannot bind port 18333.** Another process or an earlier container
  holds it. Run `docker rm -fv alambique-first-try`, then step 2 again.
- **The curl check prints nothing.** SeaweedFS is still starting. Run
  `docker logs alambique-first-try` to see the server output.
- **`{:error, {:data_dir_in_use, path}}`.** Another live instance holds the
  `data` directory. Stop the other `mix run` or `iex` session.
- **`{:error, {:store_not_conformant, ...}}`.** The store does not honour
  conditional writes. The example uses SeaweedFS 4.48, the version the
  walkthrough is tested with; see [storage-compatibility.md](storage-compatibility.md)
  for other stores.

## Next steps

- [How to call cells from your application](how-to-call-cells.md) covers the
  options, placement and the other functions.
- [How to configure the S3 store](how-to-configure-the-s3-store.md) covers AWS
  and renewable credentials.
- [Concepts](concepts.md) defines cell, lease and segment.
