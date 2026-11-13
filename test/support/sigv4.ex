defmodule Alambique.SigV4 do
  @moduledoc """
  An independent SigV4 signer for S3 requests, written from the AWS
  specification, so that tests can check the signature the store sends.
  """

  @doc "Signature (hex) of a request; `headers` holds every signed header."
  def signature(secret, method, path, query, headers, signed, payload_hash, amz_date, region) do
    canonical_headers =
      Enum.map_join(signed, fn name -> "#{name}:#{String.trim(header!(headers, name))}\n" end)

    canonical =
      Enum.join(
        [method, path, query, canonical_headers, Enum.join(signed, ";"), payload_hash],
        "\n"
      )

    date = binary_part(amz_date, 0, 8)
    scope = "#{date}/#{region}/s3/aws4_request"

    to_sign =
      Enum.join(["AWS4-HMAC-SHA256", amz_date, scope, sha256_hex(canonical)], "\n")

    key =
      Enum.reduce([date, region, "s3", "aws4_request"], "AWS4" <> secret, &hmac(&2, &1))

    key |> hmac(to_sign) |> Base.encode16(case: :lower)
  end

  @doc "Splits an `authorization` header into `{access_key_id, signed_headers, signature}`."
  def parse("AWS4-HMAC-SHA256 " <> rest) do
    [_, key_id] = Regex.run(~r{Credential=([^/]+)/}, rest)
    [_, signed] = Regex.run(~r{SignedHeaders=([^,]+)}, rest)
    [_, signature] = Regex.run(~r{Signature=([0-9a-f]+)}, rest)
    {key_id, String.split(signed, ";"), signature}
  end

  defp header!(headers, name) do
    {_, value} = Enum.find(headers, fn {k, _} -> String.downcase(k) == name end)
    value
  end

  defp hmac(key, data), do: :crypto.mac(:hmac, :sha256, key, data)
  defp sha256_hex(data), do: :sha256 |> :crypto.hash(data) |> Base.encode16(case: :lower)
end
