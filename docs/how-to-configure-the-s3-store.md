# How to configure the S3 store

**The question:** how do I point `Alambique.Store.S3` at AWS S3, or at a
compatible server, with static keys or with temporary credentials that renew?

This guide stands alone. The full list of options is in the documentation of
`Alambique.Store.S3`; what the store must guarantee is in
[storage-compatibility.md](storage-compatibility.md).

AWS S3 has not been run against by the project's tests: they use SeaweedFS,
and the AWS paths (derived endpoint, session token, renewing credentials) are
tested against a stub of the HTTP client and an independent SigV4 signer.

## Prerequisites

- A bucket and credentials that can read, write, list and delete its objects.
- The region of the bucket. A request to another region answers a redirect,
  which the store reports as an error and does not follow.

## 1. AWS S3 with static keys

Give the bucket, the region and the keys. Without `:endpoint` the store uses
`https://s3.<region>.amazonaws.com`.

```elixir
s3_config = %{
  bucket: "my-bucket",
  region: "eu-west-1",
  access_key_id: System.fetch_env!("AWS_ACCESS_KEY_ID"),
  secret_access_key: System.fetch_env!("AWS_SECRET_ACCESS_KEY")
}
```

The bucket goes in the path (`https://s3.eu-west-1.amazonaws.com/my-bucket/key`).
That is the default of the store, AWS accepts it, and SeaweedFS needs it. For
the virtual-hosted style (`https://my-bucket.s3.eu-west-1.amazonaws.com/key`)
add `path_style: false`; the bucket name must then have no dots.

## 2. A compatible server

Give `:endpoint`. The region only signs the request.

```elixir
s3_config = %{
  endpoint: "http://127.0.0.1:18333",
  bucket: "my-bucket",
  access_key_id: "...",
  secret_access_key: "..."
}
```

## 3. Temporary credentials

For STS, an instance role, a container role or web identity, give the token
with the keys:

```elixir
%{bucket: "my-bucket", region: "eu-west-1",
  access_key_id: key, secret_access_key: secret, session_token: token}
```

These credentials expire, and the store cannot renew a fixed token. Give a
function instead. It returns `{access_key_id, secret_access_key, token}`, or
the same with the `DateTime` of expiry as a fourth element:

```elixir
# MyApp.AwsCredentials.fetch/0 runs your chain: environment, profile, ECS or
# EC2 metadata, web identity. It returns {key, secret, token, expires_at}.
%{bucket: "my-bucket", region: "eu-west-1", credentials: &MyApp.AwsCredentials.fetch/0}
```

It may also return `{:error, reason}`; the request then answers
`{:error, {:credentials, reason}}`.

The store calls the function once and caches the result. It calls it again:

- when the expiry is less than `:credentials_refresh_margin_ms` away
  (default `300_000`);
- when the function gave no expiry and `:credentials_ttl_ms` have passed
  (default `300_000`);
- when a request is answered with `ExpiredToken` (status 400 or 403). The
  request is sent once more with the new credentials. A second refusal is the
  result.

Pass a stable function, such as `&MyApp.AwsCredentials.fetch/0`: the cache is
keyed by the function, so a new closure for every config leaves an extra cache
entry. The instance does not restart. Requests that run at the same time can call the
function more than once, so it should be cheap or cache by itself.

## 4. From an `s3://` URL

```elixir
{:ok, s3_config, prefix} =
  Alambique.Store.S3.from_url("s3://my-bucket/myapp", region: "eu-west-1", credentials: fun)

{Alambique, name: MyApp.Cells, store: {Alambique.Store.S3, s3_config}, prefix: prefix || "alambique", ...}
```

`from_url/2` takes the bucket and the prefix from the URL and the other keys
from the second argument. It answers `{:error, {:invalid_s3_url, url}}` for
anything but `s3://bucket` or `s3://bucket/path`.

## Troubleshooting

- `{:error, {:http, 301, _}}`: the bucket is in another region. Set `:region`
  (or `:endpoint`) to its region.
- `{:error, {:http, 403, body}}` with `SignatureDoesNotMatch` or
  `InvalidToken`: the token does not belong to the keys, or the region is wrong.
- `{:error, {:credentials, reason}}`: the `:credentials` function failed.
- The secrets never appear in an error or a crash report: the store keeps them
  in closures (design section 3). Keep them out of the code you commit; read
  them from the environment or from your secret store.

The `mix alambique.*` tasks take the same settings as flags or environment
variables, including `ALAMBIQUE_S3_SESSION_TOKEN` and an optional endpoint,
but not a `:credentials` function
([how-to-operate-a-cell.md](how-to-operate-a-cell.md)).

## Next steps

- [how-to-call-cells.md](how-to-call-cells.md) starts an instance with this store.
- [storage-compatibility.md](storage-compatibility.md) lists the guarantees the
  store needs.
