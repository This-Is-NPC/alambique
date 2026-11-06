defmodule Alambique.Store.S3 do
  @moduledoc """
  `Alambique.Store` over the S3 API (AWS S3, Cloudflare R2, SeaweedFS).

  Config is a map:

    * `:endpoint` - base URL, for example `"http://127.0.0.1:18333"`
    * `:bucket`, `:access_key_id`, `:secret_access_key` - required. The two
      credentials are binaries or functions of no arguments that return one.
      The library keeps them in functions (see `conceal/1`), which print as
      `#Function<...>`, so a crash report, a process dump or an error never
      shows them. A placement that builds its own store configs should do the
      same.
    * `:region` - signing region, default `"us-east-1"` (R2 uses `"auto"`)
    * `:path_style` - `true` (default) puts the bucket in the path, as
      SeaweedFS needs; `false` uses `bucket.host`
    * `:receive_timeout` - milliseconds, default `15_000`
    * `:req_options` - extra options merged into every `Req` request, for
      example `finch: [pool_timeout: 5_000]` for the wait for a pooled connection

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

  @default_timeout 15_000
  @credentials [:access_key_id, :secret_access_key]

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
    url = url(config, key)

    base = [
      method: method,
      url: url,
      aws_sigv4: [
        access_key_id: credential(config, :access_key_id),
        secret_access_key: credential(config, :secret_access_key),
        service: "s3",
        region: Map.get(config, :region, "us-east-1")
      ],
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
    uri = URI.parse(fetch!(config, :endpoint))
    path = String.trim_trailing(uri.path || "", "/")
    {uri.scheme, String.downcase(uri.host), uri.port, path, fetch!(config, :bucket)}
  end

  defp url(config, key) do
    uri = URI.parse(fetch!(config, :endpoint))
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

  defp credential(config, key) do
    case fetch!(config, key) do
      fun when is_function(fun, 0) -> fun.()
      value -> value
    end
  end

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
