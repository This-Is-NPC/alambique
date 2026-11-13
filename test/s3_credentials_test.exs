defmodule Alambique.Store.S3CredentialsTest do
  @moduledoc """
  The S3 store against AWS: the endpoint derived from the region, the session
  token in the signature, and credentials that come from a function and renew.
  A stub adapter answers in place of the network.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.SigV4
  alias Alambique.Store.S3

  @expired ~s(<?xml version="1.0"?><Error><Code>ExpiredToken</Code><Message>x</Message></Error>)

  # Req options that answer from `responder` and report each request.
  defp stub(responder \\ fn _ -> 200 end) do
    test = self()

    Alambique.StubAdapter.install(fn request ->
      send(test, {:request, request})
      responder.(request)
    end)
  end

  defp config(extra) do
    %{bucket: "b", region: "eu-west-1"}
    |> Map.merge(Map.new(extra))
    |> Map.put_new_lazy(:req_options, &stub/0)
  end

  defp next_request do
    assert_received {:request, request}
    request
  end

  defp headers(request), do: for({k, [v | _]} <- request.headers, do: {k, v})

  defp header(request, name), do: request |> headers() |> List.keyfind(name, 0) |> elem(1)

  # `{access_key_id, signed header names, whether the signature is valid for secret}`.
  defp signed_by(request, secret) do
    {key_id, signed, signature} = SigV4.parse(header(request, "authorization"))
    uri = request.url

    expected =
      SigV4.signature(
        secret,
        "GET",
        uri.path,
        uri.query || "",
        headers(request),
        signed,
        header(request, "x-amz-content-sha256"),
        header(request, "x-amz-date"),
        "eu-west-1"
      )

    {key_id, signed, signature == expected}
  end

  describe "the independent signer" do
    test "reproduces the AWS documentation example" do
      empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

      headers = [
        {"host", "examplebucket.s3.amazonaws.com"},
        {"range", "bytes=0-9"},
        {"x-amz-content-sha256", empty},
        {"x-amz-date", "20130524T000000Z"}
      ]

      assert SigV4.signature(
               "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
               "GET",
               "/test.txt",
               "",
               headers,
               ["host", "range", "x-amz-content-sha256", "x-amz-date"],
               empty,
               "20130524T000000Z",
               "us-east-1"
             ) == "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
    end
  end

  describe "endpoint" do
    test "is derived from the region, path style by default" do
      S3.get(config(access_key_id: "AK", secret_access_key: "SK"), "a/b")
      assert to_string(next_request().url) == "https://s3.eu-west-1.amazonaws.com/b/a/b"
    end

    test "is derived from the region, virtual hosted without path style" do
      c = config(access_key_id: "AK", secret_access_key: "SK", path_style: false)
      S3.get(c, "a/b")
      assert to_string(next_request().url) == "https://b.s3.eu-west-1.amazonaws.com/a/b"
    end

    test "an explicit endpoint wins" do
      c = config(access_key_id: "AK", secret_access_key: "SK", endpoint: "http://127.0.0.1:9000")
      S3.get(c, "k")
      assert to_string(next_request().url) == "http://127.0.0.1:9000/b/k"
    end

    test "names the store location" do
      c = config(access_key_id: "AK", secret_access_key: "SK")
      assert S3.location(c) == {"https", "s3.eu-west-1.amazonaws.com", 443, "", "b"}
    end
  end

  describe "session token" do
    test "is sent and signed" do
      c = config(access_key_id: "AK", secret_access_key: "SK", session_token: "TOKEN")
      S3.get(c, "k")
      request = next_request()

      assert header(request, "x-amz-security-token") == "TOKEN"
      assert {"AK", signed, true} = signed_by(request, "SK")
      assert "x-amz-security-token" in signed
    end

    test "may be a function, and is concealed" do
      c = S3.conceal(config(access_key_id: "AK", secret_access_key: "SK", session_token: "TOKEN"))
      assert is_function(c.session_token, 0)
      refute inspect(c) =~ "TOKEN"
      S3.get(c, "k")
      assert header(next_request(), "x-amz-security-token") == "TOKEN"
    end
  end

  describe "credentials function" do
    # A function that returns a new triple on every call, and its call counter.
    defp source(opts \\ []) do
      {:ok, counter} = Agent.start_link(fn -> 0 end)
      ref = make_ref()
      expires = Keyword.get(opts, :expires)

      fun = fn ->
        _ = ref
        n = Agent.get_and_update(counter, &{&1 + 1, &1 + 1})
        creds = {"AK#{n}", "SK#{n}", "TOKEN#{n}"}
        if expires, do: Tuple.insert_at(creds, 3, expires.()), else: creds
      end

      {fun, counter}
    end

    defp calls(counter), do: Agent.get(counter, & &1)

    test "is cached across requests" do
      {fun, counter} = source()
      c = config(credentials: fun)
      for _ <- 1..5, do: S3.get(c, "k")

      assert calls(counter) == 1
      request = next_request()
      assert {"AK1", _, true} = signed_by(request, "SK1")
      assert header(request, "x-amz-security-token") == "TOKEN1"
    end

    test "is called again once the expiry is within the margin" do
      {fun, counter} = source(expires: fn -> DateTime.add(DateTime.utc_now(), 60) end)

      c = config(credentials: fun, credentials_refresh_margin_ms: 1_000)
      S3.get(c, "k")
      S3.get(c, "k")
      assert calls(counter) == 1

      {fun, counter} = source(expires: fn -> DateTime.add(DateTime.utc_now(), 60) end)
      c = config(credentials: fun, credentials_refresh_margin_ms: 120_000)
      S3.get(c, "k")
      S3.get(c, "k")
      assert calls(counter) == 2
    end

    test "is called again after the ttl when it gives no expiry" do
      {fun, counter} = source()
      c = config(credentials: fun, credentials_ttl_ms: 0)
      S3.get(c, "k")
      S3.get(c, "k")
      assert calls(counter) == 2
    end

    test "renews on an expired-token answer and repeats the request" do
      {fun, counter} = source()

      responder = fn request ->
        if header(request, "x-amz-security-token") == "TOKEN1", do: {400, @expired}, else: 200
      end

      c = config(credentials: fun, req_options: stub(responder))
      assert {:ok, _, _, _} = S3.get(c, "k")
      assert calls(counter) == 2
      _first = next_request()
      second = next_request()
      assert {"AK2", _, true} = signed_by(second, "SK2")
      assert header(second, "x-amz-security-token") == "TOKEN2"
    end

    test "gives up when the renewed credentials are refused too" do
      {fun, counter} = source()
      c = config(credentials: fun, req_options: stub(fn _ -> {403, @expired} end))

      assert {:error, {:http, 403, @expired}} = S3.get(c, "k")
      assert calls(counter) == 2
    end

    test "an expired-token answer is final without a credentials function" do
      c =
        config(
          access_key_id: "AK",
          secret_access_key: "SK",
          session_token: "T",
          req_options: stub(fn _ -> {400, @expired} end)
        )

      assert {:error, {:http, 400, _}} = S3.get(c, "k")
      next_request()
      refute_received {:request, _}
    end

    test "an error from the function is the result" do
      c = config(credentials: fn -> {:error, :no_credentials} end)
      assert {:error, {:credentials, :no_credentials}} = S3.get(c, "k")
      refute_received {:request, _}
    end

    test "a raising function is an error" do
      c = config(credentials: fn -> raise "boom" end)
      assert {:error, {:credentials, {:error, %RuntimeError{}}}} = S3.get(c, "k")
    end

    test "the function is concealed in the config" do
      {fun, _} = source()
      c = S3.conceal(config(credentials: fun))
      assert c.credentials == fun
    end

    property "the token and the signature always come from the same credentials" do
      check all(steps <- list_of(member_of([:request, :expire, :margin]), min_length: 1)) do
        {fun, _counter} = source(expires: fn -> DateTime.add(DateTime.utc_now(), 3_600) end)
        test = self()

        # `:expire` makes the next request answer an expired-token error once.
        responder = fn request ->
          if Process.delete(:expire) do
            {400, @expired}
          else
            send(test, {:seen, request})
            200
          end
        end

        base = [credentials: fun, req_options: stub(responder)]

        for step <- steps do
          case step do
            :request ->
              S3.get(config(base), "k")

            :expire ->
              Process.put(:expire, true)
              S3.get(config(base), "k")

            :margin ->
              S3.get(config(base ++ [credentials_refresh_margin_ms: 7_200_000]), "k")
          end
        end

        for request <- drain_seen() do
          {"AK" <> n, _, _} = SigV4.parse(header(request, "authorization"))
          assert {_, _, true} = signed_by(request, "SK" <> n)
          assert header(request, "x-amz-security-token") == "TOKEN" <> n
        end
      end
    end

    defp drain_seen do
      receive do
        {:seen, request} -> [request | drain_seen()]
      after
        0 -> []
      end
    end
  end

  describe "from_url/2" do
    test "builds the config and the prefix" do
      assert {:ok, config, "a/b"} =
               S3.from_url("s3://my-bucket/a/b/", region: "eu-west-1", access_key_id: "AK")

      assert config == %{bucket: "my-bucket", region: "eu-west-1", access_key_id: "AK"}
    end

    test "without a path there is no prefix" do
      assert {:ok, %{bucket: "my-bucket"}, nil} = S3.from_url("s3://my-bucket")
      assert {:ok, %{bucket: "my-bucket"}, nil} = S3.from_url("s3://my-bucket/")
    end

    test "the bucket of the url wins over the options" do
      assert {:ok, %{bucket: "from-url"}, _} = S3.from_url("s3://from-url/p", bucket: "other")
    end

    test "rejects what is not an s3 url with a bucket" do
      for url <- ["https://bucket/p", "s3:///p", "bucket/p", "s3://bucket?x=1"] do
        assert {:error, {:invalid_s3_url, ^url}} = S3.from_url(url)
      end
    end
  end
end
