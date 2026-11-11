defmodule Alambique.Store.S3StubTest do
  @moduledoc """
  The S3 store against an in-process HTTP adapter (no network): what it signs
  and sends, and how it maps responses and failures (design §3).
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Store.S3

  defmodule Adapter do
    @moduledoc false
    # Req runs the adapter in the calling process, which holds the answers.
    def run(request) do
      %{test: test, respond: respond} = Process.get(:s3_stub)
      send(test, {:request, request})

      case respond.(request) do
        %Req.Response{} = response -> {request, response}
        {:exception, exception} -> {request, exception}
      end
    end
  end

  # Every request (signed by Req's steps) goes to the test and is answered by
  # `respond`.
  defp config(respond, extra \\ %{}) do
    Process.put(:s3_stub, %{test: self(), respond: respond})

    Map.merge(
      %{
        endpoint: "http://s3.test:9000",
        bucket: "bkt",
        access_key_id: "AKIDEXAMPLE",
        secret_access_key: "SEKRET",
        req_options: [adapter: Adapter]
      },
      extra
    )
  end

  defp reply(status, headers \\ [], body \\ ""),
    do: fn _ -> Req.Response.new(status: status, headers: headers, body: body) end

  defp sent do
    assert_received {:request, request}
    request
  end

  defp header(request, name), do: request.headers |> Map.fetch!(name) |> hd()

  describe "the request" do
    test "is signed for the region and credentials of the config" do
      c = config(reply(200), %{region: "auto"})
      assert {:ok, _, _} = S3.head(c, "k")
      request = sent()

      assert header(request, "authorization") =~
               ~r/^AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE\/\d{8}\/auto\/s3\/aws4_request,/

      assert header(request, "x-amz-date") =~ ~r/^\d{8}T\d{6}Z$/
    end

    test "uses us-east-1 unless a region is given, and calls credential functions" do
      c = config(reply(200), %{access_key_id: fn -> "FROM-FUN" end})
      assert {:ok, _, _} = S3.head(c, "k")
      assert header(sent(), "authorization") =~ "Credential=FROM-FUN/"
      assert header(sent_again(c), "authorization") =~ "/us-east-1/s3/aws4_request"
    end

    test "never retries, redirects or decodes, and honours receive_timeout" do
      assert {:ok, _, _} = S3.head(config(reply(200)), "k")
      options = sent().options
      assert options.retry == false
      assert options.redirect == false
      assert options.decode_body == false
      assert options.receive_timeout == 8_000

      assert {:ok, _, _} = S3.head(config(reply(200), %{receive_timeout: 77}), "k")
      assert sent().options.receive_timeout == 77
    end

    test "req_options win over the defaults" do
      c = config(reply(200), %{req_options: [receive_timeout: 5, adapter: Adapter]})
      assert {:ok, _, _} = S3.head(c, "k")
      assert sent().options.receive_timeout == 5
    end

    test "puts the bucket in the path, or in the host without path style" do
      assert {:ok, _, _} = S3.head(config(reply(200)), "a/b")
      request = sent()

      assert {request.url.host, request.url.port, request.url.path} ==
               {"s3.test", 9000, "/bkt/a/b"}

      assert {:ok, _, _} = S3.head(config(reply(200), %{path_style: false}), "a/b")
      request = sent()
      assert {request.url.host, request.url.path} == {"bkt.s3.test", "/a/b"}
      assert header(request, "host") == "bkt.s3.test:9000"
    end

    test "keeps the path of the endpoint out of the key" do
      c = config(reply(200), %{endpoint: "https://s3.test/base"})
      assert {:ok, _, _} = S3.head(c, "k")
      request = sent()
      assert request.url.scheme == "https"
      assert request.url.path == "/bkt/k"
    end

    property "encodes any key so that it reads back as the same key" do
      check all(key <- key()) do
        assert {:ok, _, _} = S3.head(config(reply(200)), key)
        request = sent()
        assert request.url.query == nil
        assert request.url.fragment == nil
        assert URI.decode(request.url.path) == "/bkt/" <> key
        refute request.url.path =~ ~r/[ ?#\[\]]/
      end
    end
  end

  # Letters mostly, with the characters a URL gives a meaning to.
  defp key do
    special = ~c" ?#%[]+&=@:;!$'()*,\\\"<>{}|^`~/é"
    StreamData.string([?a..?z, ?a..?z, ?0..?9 | special], min_length: 1)
  end

  describe "put" do
    test "sends the body, the condition and the metadata, and returns the bare etag" do
      c = config(reply(200, [{"etag", ~s("abc123")}]))
      assert {:ok, "abc123"} = S3.put(c, "k", ["he", [?l, "lo"]], :none, %{"ts" => "5"})
      request = sent()
      assert request.method == :put
      assert request.body == "hello"
      assert header(request, "x-amz-meta-ts") == "5"
      refute Map.has_key?(request.headers, "if-none-match")
      refute Map.has_key?(request.headers, "if-match")

      assert {:ok, _} = S3.put(c, "k", "x", :if_none_match, %{})
      assert header(sent(), "if-none-match") == "*"

      assert {:ok, _} = S3.put(c, "k", "x", {:if_match, "e1"}, %{})
      assert header(sent(), "if-match") == ~s("e1")
    end

    test "maps the statuses" do
      assert {:ok, _} = S3.put(config(reply(204)), "k", "x", :none, %{})
      assert {:error, :precondition_failed} = S3.put(config(reply(412)), "k", "x", :none, %{})
      assert {:error, :not_found} = S3.put(config(reply(404)), "k", "x", :none, %{})

      assert {:error, {:http, 500, "oops"}} =
               S3.put(config(reply(500, [], "oops")), "k", "x", :none, %{})

      assert {:error, {:http, 301, _}} = S3.put(config(reply(301)), "k", "x", :none, %{})
    end
  end

  describe "get and head" do
    test "return the body, the bare etag and the metadata" do
      headers = [
        {"etag", ~s("e9")},
        {"x-amz-meta-ts", "12"},
        {"x-amz-meta-epoch", "3"},
        {"other", "x"}
      ]

      c = config(reply(200, headers, "data"))
      assert {:ok, "data", "e9", %{"ts" => "12", "epoch" => "3"}} = S3.get(c, "k")
      assert sent().method == :get

      assert {:ok, "e9", %{"ts" => "12", "epoch" => "3"}} = S3.head(c, "k")
      assert sent().method == :head
    end

    test "a missing object is :not_found and a server error is not" do
      assert {:error, :not_found} = S3.get(config(reply(404)), "k")
      assert {:error, :not_found} = S3.head(config(reply(404)), "k")
      assert {:error, {:http, 503, _}} = S3.get(config(reply(503)), "k")
      assert {:error, {:http, 403, _}} = S3.head(config(reply(403)), "k")
      assert {:error, {:http, 206, _}} = S3.get(config(reply(206)), "k")
    end
  end

  describe "get_range" do
    test "asks for the inclusive byte range and reads the size from Content-Range" do
      headers = [{"content-range", "bytes 10-19/1234"}, {"x-amz-meta-a", "b"}]
      c = config(reply(206, headers, "0123456789"))
      assert {:ok, "0123456789", 1234, %{"a" => "b"}} = S3.get_range(c, "k", 10, 10)
      assert header(sent(), "range") == "bytes=10-19"
    end

    test "slices the body when the server ignored the range" do
      c = config(reply(200, [], "0123456789"))
      assert {:ok, "345", 10, _} = S3.get_range(c, "k", 3, 3)
      assert {:ok, "789", 10, _} = S3.get_range(c, "k", 7, 100)
      assert {:ok, "", 10, _} = S3.get_range(c, "k", 10, 5)
      assert {:ok, "", 10, _} = S3.get_range(c, "k", 50, 5)
    end

    test "an offset past the end is an empty body and the size of the object" do
      c = config(reply(416, [{"content-range", "bytes */1234"}]))
      assert {:ok, "", 1234, _} = S3.get_range(c, "k", 5000, 10)
    end

    test "other statuses are errors" do
      assert {:error, :not_found} = S3.get_range(config(reply(404)), "k", 0, 1)
      assert {:error, {:http, 500, _}} = S3.get_range(config(reply(500)), "k", 0, 1)
    end
  end

  describe "list" do
    defp page(keys, truncated?) do
      entries = Enum.map_join(keys, &"<Contents><Key>#{&1}</Key></Contents>")

      "<ListBucketResult>#{entries}<IsTruncated>#{truncated?}</IsTruncated></ListBucketResult>"
    end

    test "sends the prefix, the limit and the start-after key" do
      c = config(reply(200, [], page([], false)))
      assert {:ok, [], nil} = S3.list(c, "p/", nil, 25)
      request = sent()
      assert request.url.path == "/bkt/"

      assert URI.decode_query(request.url.query) ==
               %{"list-type" => "2", "prefix" => "p/", "max-keys" => "25"}

      assert {:ok, [], nil} = S3.list(c, "p/", "p/x y", 25)
      assert sent().url.query |> URI.decode_query() |> Map.get("start-after") == "p/x y"
    end

    test "returns the keys unescaped, and the last one to continue after when truncated" do
      keys = ["a&amp;b", "c&lt;d&gt;", "e&quot;f&apos;g", "x&amp;lt;y", "multi\nline"]
      c = config(reply(200, [], page(keys, true)))
      expected = ["a&b", "c<d>", "e\"f'g", "x&lt;y", "multi\nline"]
      assert {:ok, ^expected, "multi\nline"} = S3.list(c, "", nil, 10)

      c = config(reply(200, [], page(keys, false)))
      assert {:ok, ^expected, nil} = S3.list(c, "", nil, 10)
    end

    test "errors keep their status" do
      assert {:error, {:http, 403, "denied"}} =
               S3.list(config(reply(403, [], "denied")), "", nil, 1)
    end
  end

  describe "delete" do
    test "succeeds for an object that is gone too" do
      for status <- [200, 204, 404] do
        assert :ok = S3.delete(config(reply(status)), "k")
        assert sent().method == :delete
      end
    end

    test "fails otherwise" do
      assert {:error, {:http, 500, _}} = S3.delete(config(reply(500)), "k")
      assert {:error, {:http, 403, _}} = S3.delete(config(reply(403)), "k")
    end
  end

  describe "failures of the client" do
    test "a transport timeout is :timeout and another reason is passed on" do
      timeout = fn _ -> {:exception, %Req.TransportError{reason: :timeout}} end
      closed = fn _ -> {:exception, %Req.TransportError{reason: :econnrefused}} end
      assert {:error, :timeout} = S3.get(config(timeout), "k")
      assert {:error, :timeout} = S3.put(config(timeout), "k", "x", :none, %{})
      assert {:error, :econnrefused} = S3.head(config(closed), "k")
      assert {:error, :econnrefused} = S3.delete(config(closed), "k")
      assert {:error, :econnrefused} = S3.list(config(closed), "", nil, 1)
      assert {:error, :econnrefused} = S3.get_range(config(closed), "k", 0, 1)
    end

    test "an exception, a throw or an exit in the client is an error" do
      for {thrower, kind} <- [
            {fn _ -> raise "boom" end, :error},
            {fn _ -> throw(:ball) end, :throw},
            {fn _ -> exit(:gone) end, :exit}
          ] do
        assert {:error, {:request_crashed, ^kind, _}} = S3.get(config(thrower), "k")
      end
    end
  end

  describe "the config" do
    test "a missing setting is named without printing the config" do
      c = config(reply(200)) |> Map.delete(:bucket)

      error = assert_raise ArgumentError, fn -> S3.get(c, "k") end
      assert error.message == "the S3 store config has no :bucket"
      refute error.message =~ "SEKRET"

      c = config(reply(200)) |> Map.delete(:secret_access_key)
      error = assert_raise ArgumentError, fn -> S3.get(c, "k") end
      assert error.message == "the S3 store config has no :secret_access_key"

      assert_raise ArgumentError, "the S3 store config must be a map", fn ->
        S3.get(Process.get(:none, endpoint: "http://x"), "k")
      end
    end

    test "location is the endpoint and bucket, free of credentials and spelling" do
      a = %{
        endpoint: "http://S3.Test:9000/",
        bucket: "b",
        access_key_id: "x",
        secret_access_key: "y"
      }

      b = %{a | endpoint: "http://s3.test:9000", access_key_id: "z"}
      assert S3.location(a) == {"http", "s3.test", 9000, "", "b"}
      assert S3.location(a) == S3.location(b)
      refute S3.location(a) == S3.location(%{b | bucket: "c"})

      assert S3.location(%{a | endpoint: "https://s3.test/base/"}) ==
               {"https", "s3.test", 443, "/base", "b"}
    end

    test "conceal hides binary credentials and leaves other values alone" do
      concealed = S3.conceal(%{access_key_id: "id", secret_access_key: "sk", bucket: "b"})
      assert concealed.bucket == "b"
      assert concealed.access_key_id.() == "id"
      assert concealed.secret_access_key.() == "sk"
      refute inspect(concealed) =~ "sk"

      assert S3.conceal(%{bucket: "b"}) == %{bucket: "b"}
      assert S3.conceal(:not_a_map) == :not_a_map
    end
  end

  # The request a second call of the same config sends.
  defp sent_again(c) do
    assert {:ok, _, _} = S3.head(c, "k")
    sent()
  end
end
