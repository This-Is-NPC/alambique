# How to configure the S3 store

**The question:** how do I point `Alambique.Store.S3` at AWS S3 or at an
S3-compatible server, with static keys or with temporary credentials that
renew?

This guide stands alone. The module documentation of `Alambique.Store.S3`
describes every option. The guarantees the store must give are in
[storage-compatibility.md](storage-compatibility.md).

The project tests the store against SeaweedFS. The AWS paths (derived
endpoint, session token, renewing credentials) are tested against a stub of
the HTTP client. AWS S3 itself is not part of the test runs.

## Prerequisites

- A bucket, and credentials that can read, write, list and delete its objects.
- For AWS, the region of the bucket. Without `:region` the store signs for
  `us-east-1`. A request for the wrong region can get a redirect, which the
  store reports as an error and does not follow.

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `:bucket` | none, required | Bucket name. |
| `:endpoint` | `https://s3.<region>.amazonaws.com` | Base URL. Set it for SeaweedFS, MinIO and R2. |
| `:region` | `us-east-1` | Region used for signing and for the derived AWS endpoint. |
| `:access_key_id`, `:secret_access_key` | none | Static keys. Required unless `:credentials` is set. |
| `:session_token` | none | Token of temporary credentials. It is sent and signed. |
| `:credentials` | none | Function that returns the keys and the token. See step 3. |
| `:path_style` | `true` | `true` puts the bucket in the path. `false` uses the virtual-hosted style. |
| `:receive_timeout` | `8000` | Milliseconds for one request. |
| `:pool_size` | `256` | Connections the store may hold open at once. |
| `:pool_timeout` | `2000` | Milliseconds a request waits for a free connection. |
| `:error_limit` | `512` | Bytes kept of the S3 error code and message. |

## 1. Static keys on AWS S3

```elixir
s3_config = %{
  bucket: "my-bucket",
  region: "eu-west-1",
  access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
  secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY")
}
```

Expected result: the store sends requests to
`https://s3.eu-west-1.amazonaws.com/my-bucket/<key>`.

The bucket stays in the path by default. AWS accepts that, and SeaweedFS needs
it. For the virtual-hosted style (`https://my-bucket.s3.eu-west-1.amazonaws.com/<key>`),
set `path_style: false`. The bucket name must then have no dots, because a
dotted name does not match the AWS certificate.

## 2. A compatible server

Set `:endpoint`. The region is only used for signing, so the default is fine.

```elixir
s3_config = %{
  endpoint: "http://127.0.0.1:18333",
  bucket: "my-bucket",
  access_key_id: "...",
  secret_access_key: "..."
}
```

Expected result: the store sends requests to `http://127.0.0.1:18333/my-bucket/<key>`.

## 3. Temporary credentials

Credentials from STS, an instance role, a container role or web identity expire.
A fixed token cannot be renewed by the store, so give a function instead:

```elixir
%{bucket: "my-bucket", region: "eu-west-1", credentials: &MyApp.AwsCredentials.fetch/0}
```

`MyApp.AwsCredentials.fetch/0` runs your credential chain (environment,
profile, ECS or EC2 metadata, web identity). It returns
`{access_key_id, secret_access_key, session_token}`, or the same tuple with a
`DateTime` of expiry as a fourth element. It may return `{:error, reason}`;
the request then answers `{:error, {:credentials, reason}}`.

The store caches the result. It calls the function again when:

- the expiry is less than `:credentials_refresh_margin_ms` away (default `300_000`);
- the function gave no expiry and `:credentials_ttl_ms` (default `300_000`) have passed;
- a request is answered `ExpiredToken` with status 400 or 403. The request is
  sent once more with the new credentials. A second refusal is the result.

Pass a stable function, such as `&MyApp.AwsCredentials.fetch/0`. The cache is
keyed by the function, so a new closure for each config adds a cache entry
each time. Requests that run at the same time can call the function more than
once, so keep it cheap or let it cache by itself.

## 4. From an `s3://` URL

```elixir
{:ok, s3_config, prefix} =
  Alambique.Store.S3.from_url("s3://my-bucket/myapp", region: "eu-west-1", credentials: fun)

{Alambique, name: MyApp.Cells, store: {Alambique.Store.S3, s3_config}, prefix: prefix || "alambique", ...}
```

`from_url/2` takes the bucket and the prefix from the URL. The other keys come
from the second argument. The prefix is `nil` when the URL has no path, so
`prefix || "alambique"` gives the default. Any URL other than `s3://bucket` or
`s3://bucket/path` answers `{:error, {:invalid_s3_url, url}}`.

Expected result: `{:ok, s3_config, "myapp"}`.

## Settings for the `mix alambique.*` tasks

The tasks read the same settings from flags or environment variables:
`--endpoint` / `ALAMBIQUE_S3_ENDPOINT`, `--bucket` / `ALAMBIQUE_S3_BUCKET`,
`--region` / `ALAMBIQUE_S3_REGION`, `--access-key-id` /
`ALAMBIQUE_S3_ACCESS_KEY_ID`, `--secret-access-key` /
`ALAMBIQUE_S3_SECRET_ACCESS_KEY`, `--session-token` /
`ALAMBIQUE_S3_SESSION_TOKEN`, `--path-style` or `--no-path-style` /
`ALAMBIQUE_S3_PATH_STYLE`, and `--prefix` / `ALAMBIQUE_PREFIX`. They do not
take a `:credentials` function. See
[how-to-operate-a-cell.md](how-to-operate-a-cell.md).

## Troubleshooting

- `{:error, {:http, 301, summary}}`: the bucket is in another region. Set
  `:region`, or `:endpoint`, to the region of the bucket.
- `{:error, {:http, 403, "SignatureDoesNotMatch: ..."}}` or `"InvalidToken: ..."`:
  `summary` is the S3 error code and message, never the response body, which
  can echo the token. Either the token does not belong to the keys, or the
  region is wrong.
- `{:error, {:http, 403, _}}` on a `HEAD` request with expired credentials: a
  `HEAD` answer has no body, so the store cannot see `ExpiredToken` and does
  not renew. The next request that gets a body renews them.
- `{:error, {:credentials, reason}}`: the `:credentials` function returned an
  error or an invalid value.
- `{:error, {:store_not_conformant, location_id, failed}}` on start: the store
  accepted a second create with `If-None-Match: *` (`:if_none_match`), or a
  write with a wrong `If-Match` etag (`:if_match`). Alambique cannot fence
  cells on that store. Use a store that honours conditional writes
  ([storage-compatibility.md](storage-compatibility.md)). At each start the
  check writes four objects and deletes one under `<prefix>/store-check/`.
  `check_store: false` in the instance options skips it. Use that only for a
  store known to pass.
- `{:error, {:request_crashed, kind, reason}}`: the HTTP client did not return
  a connection in time. The store holds at most `:pool_size` connections, and a
  request waits at most `:pool_timeout` (2000 ms, below `:receive_timeout`).
  The wait then fails at once with this error, instead of a read timeout after
  `:receive_timeout`. Raise `:pool_size` if many cells wake or compact at the
  same time. Raise `:pool_timeout` too if bursts queue longer. A `:finch`
  option in `:req_options` overrides both. The wait never left the node, so
  `[:alambique, :store, :failure]` reports it with `counted: false`, and the
  library retries the operation until its deadline.
- The secrets never appear in errors or crash reports: the store keeps the
  keys in functions. Keep them out of the code you commit. Read them from the
  environment or from your secret store.

## Next steps

- [how-to-call-cells.md](how-to-call-cells.md) starts an instance with this store.
- [how-to-operate-a-cell.md](how-to-operate-a-cell.md) runs the `mix alambique.*` tasks.
- [storage-compatibility.md](storage-compatibility.md) lists the guarantees the store needs.
