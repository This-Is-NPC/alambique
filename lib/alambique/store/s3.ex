defmodule Alambique.Store.S3 do
  @moduledoc """
  `Alambique.Store` over the S3 API (AWS S3, Cloudflare R2, SeaweedFS).

  Config is a map:

    * `:bucket` - required
    * `:endpoint` - base URL, for example `"http://127.0.0.1:18333"`. Without
      it the store uses AWS: `"https://s3.<region>.amazonaws.com"`, from
      `:region`. Give it for SeaweedFS, MinIO and R2.
    * `:access_key_id`, `:secret_access_key` - the credentials, binaries or
      functions of no arguments that return one. Required without
      `:credentials`.
    * `:session_token` - the temporary-credentials token (STS, IMDS, web
      identity), a binary, a function of no arguments or `nil`. It is sent as
      `x-amz-security-token` and signed.
    * `:credentials` - a function of no arguments that returns
      `{access_key_id, secret_access_key, token | nil}` or the same with a
      `DateTime` of expiry as a fourth element, or `{:error, reason}` (the
      request then answers `{:error, {:credentials, reason}}`). It replaces
      the three options above. The result is cached: the function is called
      again when the expiry is less than `:credentials_refresh_margin_ms`
      away (default `300_000`), when it gave no expiry and
      `:credentials_ttl_ms` (default `300_000`) have passed, or when the store
      answers `ExpiredToken` with status 400 or 403 (the request is then sent
      once more with the new credentials). Concurrent requests may call it
      more than once. The cache lives in `:persistent_term`, keyed by the
      function: pass a stable one such as `&MyMod.creds/0`, because a new
      closure per config leaves an extra entry.
    * The library keeps the credentials in functions (see `conceal/1`), which
      print as `#Function<...>`, so a crash report, a process dump or an error
      never shows them. A placement that builds its own store configs should
      do the same.
    * `:region` - signing region, default `"us-east-1"` (R2 uses `"auto"`)
    * `:path_style` - `true` (default) puts the bucket in the path, as
      SeaweedFS needs and AWS accepts; `false` uses `bucket.host`, the
      virtual-hosted style (`bucket.s3.<region>.amazonaws.com` on AWS; a
      bucket name with dots does not match the AWS certificate)
    * `:receive_timeout` - milliseconds, default `8_000`, below the default `:op_timeout_ms` (10 000) so that the client gives up before the library does; raise it only together with `:op_timeout_ms`
    * `:req_options` - extra options merged into every `Req` request, for
      example `finch: [pool_timeout: 5_000]` for the wait for a pooled connection

  `from_url/2` builds the config from `"s3://bucket/prefix"`.

  A response with a redirect status is an error, `{:error, {:http, status,
  body}}`: following it would send the body and a fresh signature to another
  host. Point `:endpoint` at the region of the bucket.

  A request that fails answers `{:error, reason}`, also when the HTTP client
  raises or exits (`{:request_crashed, kind, reason}`), for example when its
  pool of connections is exhausted.

  Returned etags have their quotes stripped. Metadata keys come back
  lowercased, because HTTP header names are case-insensitive.
  """
  @behaviour Alambique.Store

  @default_timeout 8_000
  @credentials [:access_key_id, :secret_access_key, :session_token]
  @credentials_margin_ms 300_000
  @credentials_ttl_ms 300_000

  @impl true
  def conceal(config) when is_map(config) do
    Enum.reduce(@credentials, config, fn key, acc ->
      case acc do
        %{^key => value} when is_binary(value) -> %{acc | key => fn -> value end}
        _ -> acc
      end
    end)
  end

  def conceal(config), do: config

  @doc """
  Builds a config and a prefix from `"s3://bucket/prefix"`.

  `opts` (a keyword list or a map) are the other config keys. The bucket of the
  URL wins over a `:bucket` in `opts`. The prefix is the path without its
  slashes, or `nil` when there is none, so `prefix || "alambique"` gives the
  default.

      {:ok, config, prefix} = Alambique.Store.S3.from_url("s3://my-bucket/app", region: "eu-west-1")
  """
  @spec from_url(String.t(), keyword | map) ::
          {:ok, map, String.t() | nil} | {:error, {:invalid_s3_url, String.t()}}
  def from_url(url, opts \\ []) when is_binary(url) do
    case URI.parse(url) do
      %URI{scheme: "s3", host: host, query: nil, fragment: nil, path: path}
      when is_binary(host) and host != "" ->
        prefix = path |> to_string() |> URI.decode() |> String.trim("/")
        {:ok, Map.put(Map.new(opts), :bucket, host), if(prefix == "", do: nil, else: prefix)}

      _ ->
        {:error, {:invalid_s3_url, url}}
    end
  end

  @impl true
  def put(config, key, body, condition, meta) do
    headers = condition_headers(condition) ++ meta_headers(meta)

    case request(config, :put, key, headers: headers, body: IO.iodata_to_binary(body)) do
      {:ok, %{status: status} = resp} when status in 200..299 -> {:ok, etag(resp)}
      other -> error(other)
    end
  end

  @impl true
  def get(config, key) do
    case request(config, :get, key) do
      {:ok, %{status: 200, body: body} = resp} -> {:ok, body, etag(resp), meta(resp)}
      other -> error(other)
    end
  end

  @impl true
  def get_range(config, key, offset, length) do
    range = "bytes=#{offset}-#{offset + length - 1}"

    case request(config, :get, key, headers: [{"range", range}]) do
      {:ok, %{status: 206, body: body} = resp} ->
        {:ok, body, total_size(resp), meta(resp)}

      # The whole object was returned: the server ignored the range.
      {:ok, %{status: 200, body: body} = resp} ->
        {:ok, binary_part_at(body, offset, length), byte_size(body), meta(resp)}

      # The offset is at or past the end.
      {:ok, %{status: 416} = resp} ->
        {:ok, "", total_size(resp), meta(resp)}

      other ->
        error(other)
    end
  end

  @impl true
  def head(config, key) do
    case request(config, :head, key) do
      {:ok, %{status: 200} = resp} -> {:ok, etag(resp), meta(resp)}
      other -> error(other)
    end
  end

  @impl true
  def list(config, prefix, start_after, limit) do
    params =
      [{"list-type", "2"}, {"prefix", prefix}, {"max-keys", Integer.to_string(limit)}] ++
        if(start_after, do: [{"start-after", start_after}], else: [])

    case request(config, :get, nil, params: params) do
      {:ok, %{status: 200, body: xml}} ->
        keys = ~r{<Key>(.*?)</Key>}s |> Regex.scan(xml) |> Enum.map(fn [_, k] -> unescape(k) end)
        truncated? = xml =~ "<IsTruncated>true</IsTruncated>"
        {:ok, keys, if(truncated?, do: List.last(keys))}

      other ->
        error(other)
    end
  end

  @impl true
  def delete(config, key) do
    case request(config, :delete, key) do
      {:ok, %{status: status}} when status in [200, 204, 404] -> :ok
      other -> error(other)
    end
  end

  defp request(config, method, key, opts \\ []) do
    with {:ok, creds} <- credentials(config) do
      case signed_request(config, creds, method, key, opts) do
        {:ok, %{status: status, body: body}} = resp when status in [400, 403] ->
          if expired_token?(body) and is_function(config[:credentials], 0) do
            with {:ok, renewed} <- renew(config, creds),
                 do: signed_request(config, renewed, method, key, opts)
          else
            resp
          end

        other ->
          other
      end
    end
  end

  defp signed_request(config, {access_key_id, secret_access_key, token}, method, key, opts) do
    sigv4 = [
      access_key_id: access_key_id,
      secret_access_key: secret_access_key,
      service: "s3",
      region: region(config)
    ]

    base = [
      method: method,
      url: url(config, key),
      aws_sigv4: if(token, do: [{:token, token} | sigv4], else: sigv4),
      retry: false,
      redirect: false,
      decode_body: false,
      receive_timeout: Map.get(config, :receive_timeout, @default_timeout)
    ]

    base
    |> Keyword.merge(opts)
    |> Keyword.merge(Map.get(config, :req_options, []))
    |> send_request()
  end

  # The client also raises or exits, for example when its connection pool is
  # exhausted; every operation reports those as errors.
  defp send_request(options) do
    Req.request(options)
  catch
    kind, reason -> {:error, {:request_crashed, kind, reason}}
  end

  @impl true
  def location(config) do
    uri = URI.parse(endpoint(config))
    path = String.trim_trailing(uri.path || "", "/")
    {uri.scheme, String.downcase(uri.host), uri.port, path, fetch!(config, :bucket)}
  end

  defp url(config, key) do
    uri = URI.parse(endpoint(config))
    bucket = fetch!(config, :bucket)
    path = if key, do: "/" <> encode_path(key), else: "/"

    if Map.get(config, :path_style, true) do
      URI.to_string(%{uri | path: "/" <> bucket <> path})
    else
      URI.to_string(%{uri | host: bucket <> "." <> uri.host, path: path})
    end
  end

  # Errors name the key, never the config: it holds the credentials.
  defp fetch!(config, key) when is_map(config) do
    case config do
      %{^key => value} -> value
      _ -> raise ArgumentError, "the S3 store config has no #{inspect(key)}"
    end
  end

  defp fetch!(_config, _key), do: raise(ArgumentError, "the S3 store config must be a map")

  defp region(config), do: Map.get(config, :region, "us-east-1")

  defp endpoint(config),
    do: Map.get(config, :endpoint) || "https://s3.#{region(config)}.amazonaws.com"

  defp resolve(fun) when is_function(fun, 0), do: fun.()
  defp resolve(value), do: value

  # The key pair and token of a request: from the `:credentials` function,
  # cached, or from the static options.
  defp credentials(%{credentials: fun} = config) when is_function(fun, 0) do
    now = System.monotonic_time(:millisecond)

    case :persistent_term.get({__MODULE__, fun}, nil) do
      {creds, refresh_at} when now < refresh_at -> {:ok, creds}
      _ -> fetch_credentials(config, fun, now)
    end
  end

  defp credentials(config) do
    {:ok,
     {resolve(fetch!(config, :access_key_id)), resolve(fetch!(config, :secret_access_key)),
      resolve(Map.get(config, :session_token))}}
  end

  # Called after an expired-token answer to `used`. Another request may have
  # renewed them already: then those are used and the function is not called.
  defp renew(%{credentials: fun} = config, used) do
    case :persistent_term.get({__MODULE__, fun}, nil) do
      {^used, _} -> fetch_credentials(config, fun, System.monotonic_time(:millisecond))
      _ -> credentials(config)
    end
  end

  defp fetch_credentials(config, fun, now) do
    case call_credentials(fun) do
      {:ok, creds, nil} ->
        cache(fun, creds, now + Map.get(config, :credentials_ttl_ms, @credentials_ttl_ms))

      {:ok, creds, %DateTime{} = expires_at} ->
        margin = Map.get(config, :credentials_refresh_margin_ms, @credentials_margin_ms)
        left = DateTime.diff(expires_at, DateTime.utc_now(), :millisecond)
        cache(fun, creds, now + left - margin)

      {:error, reason} ->
        {:error, {:credentials, reason}}
    end
  end

  defp cache(fun, creds, refresh_at) do
    :persistent_term.put({__MODULE__, fun}, {creds, refresh_at})
    {:ok, creds}
  end

  # The error never carries the returned value: it holds the secrets.
  defp call_credentials(fun) do
    case fun.() do
      {access_key_id, secret_access_key, token}
      when is_binary(access_key_id) and is_binary(secret_access_key) and
             (is_binary(token) or is_nil(token)) ->
        {:ok, {access_key_id, secret_access_key, token}, nil}

      {access_key_id, secret_access_key, token, %DateTime{} = expires_at}
      when is_binary(access_key_id) and is_binary(secret_access_key) and
             (is_binary(token) or is_nil(token)) ->
        {:ok, {access_key_id, secret_access_key, token}, expires_at}

      {:error, reason} ->
        {:error, reason}

      _ ->
        {:error, :invalid_credentials}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp expired_token?(body),
    do: is_binary(body) and body =~ ~r{<Code>ExpiredToken(Exception)?</Code>}

  defp encode_path(key) do
    key
    |> String.split("/")
    |> Enum.map_join("/", &URI.encode(&1, fn c -> URI.char_unreserved?(c) end))
  end

  defp condition_headers(:none), do: []
  defp condition_headers(:if_none_match), do: [{"if-none-match", "*"}]
  defp condition_headers({:if_match, etag}), do: [{"if-match", ~s("#{etag}")}]

  defp meta_headers(meta), do: for({k, v} <- meta, do: {"x-amz-meta-" <> k, v})

  defp etag(resp) do
    resp |> Req.Response.get_header("etag") |> List.first("") |> String.trim("\"")
  end

  # `Content-Range: bytes 0-99/12345` or, for an unsatisfiable range, `bytes */12345`.
  defp total_size(resp) do
    with [value | _] <- Req.Response.get_header(resp, "content-range"),
         [_, total] <- Regex.run(~r{/(\d+)\z}, value) do
      String.to_integer(total)
    else
      _ -> 0
    end
  end

  defp binary_part_at(body, offset, _length) when offset >= byte_size(body), do: ""

  defp binary_part_at(body, offset, length),
    do: binary_part(body, offset, min(length, byte_size(body) - offset))

  defp meta(resp) do
    for {"x-amz-meta-" <> k, [v | _]} <- resp.headers, into: %{}, do: {k, v}
  end

  defp error({:ok, %{status: 404}}), do: {:error, :not_found}
  defp error({:ok, %{status: 412}}), do: {:error, :precondition_failed}
  defp error({:ok, %{status: status, body: body}}), do: {:error, {:http, status, body}}
  defp error({:error, %{reason: reason}}), do: {:error, reason}
  defp error({:error, reason}), do: {:error, reason}

  defp unescape(s) do
    s
    |> String.replace("&lt;", "<")
    |> String.replace("&gt;", ">")
    |> String.replace("&quot;", "\"")
    |> String.replace("&apos;", "'")
    |> String.replace("&amp;", "&")
  end
end
