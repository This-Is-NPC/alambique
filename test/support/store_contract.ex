defmodule Alambique.StoreContract do
  @moduledoc """
  Contract tests every `Alambique.Store` must pass.

  Pass `tags:` to tag every test (for example `:seaweedfs`). The using module defines `new_config/0`, which returns a config for a
  fresh store (or a shared one: every test uses its own key prefix).

      use Alambique.StoreContract, store: AlambiqueTestkit.MemoryStore
  """

  defmacro __using__(opts) do
    store = Keyword.fetch!(opts, :store)

    quote do
      use ExUnit.Case, async: false

      for tag <- unquote(List.wrap(Keyword.get(opts, :tags, []))), do: @moduletag(tag)

      @store unquote(store)
      @racers 50

      setup do
        prefix = "contract/" <> Base.url_encode64(:crypto.strong_rand_bytes(9)) <> "/"
        {:ok, config: new_config(), prefix: prefix}
      end

      test "put, get, head and delete", %{config: c, prefix: p} do
        key = p <> "a"
        assert {:error, :not_found} = @store.get(c, key)
        assert {:error, :not_found} = @store.head(c, key)

        assert {:ok, etag} = @store.put(c, key, ["hel", "lo"], :none, %{})
        assert {:ok, "hello", ^etag, _} = @store.get(c, key)
        assert {:ok, ^etag, _} = @store.head(c, key)

        assert {:ok, etag2} = @store.put(c, key, "world", :none, %{})
        assert etag2 != etag
        assert {:ok, "world", ^etag2, _} = @store.get(c, key)

        assert :ok = @store.delete(c, key)
        assert {:error, :not_found} = @store.get(c, key)
        assert :ok = @store.delete(c, key)
      end

      test "get_range reads a part of an object", %{config: c, prefix: p} do
        key = p <> "r"
        body = String.duplicate("0123456789", 10)
        {:ok, _} = @store.put(c, key, body, :none, %{"epoch" => "3"})

        assert {:ok, "0123456789", 100, %{"epoch" => "3"}} = @store.get_range(c, key, 0, 10)
        assert {:ok, "56789012", 100, _} = @store.get_range(c, key, 5, 8)
        assert {:ok, "6789", 100, _} = @store.get_range(c, key, 96, 50)
        assert {:ok, ^body, 100, _} = @store.get_range(c, key, 0, 1000)
        assert {:ok, "", 100, _} = @store.get_range(c, key, 100, 10)
        assert {:ok, "", 100, _} = @store.get_range(c, key, 5000, 10)
        assert {:error, :not_found} = @store.get_range(c, p <> "missing", 0, 10)
      end

      test "metadata round trip", %{config: c, prefix: p} do
        meta = %{"epoch" => "7", "node" => "a@host"}
        {:ok, etag} = @store.put(c, p <> "m", "x", :none, meta)
        assert {:ok, "x", ^etag, ^meta} = @store.get(c, p <> "m")
        assert {:ok, ^etag, ^meta} = @store.head(c, p <> "m")
      end

      test "list filters by prefix, orders and paginates with start_after", %{
        config: c,
        prefix: p
      } do
        keys = for i <- 1..7, do: p <> "seg/" <> String.pad_leading("#{i}", 3, "0")
        for k <- keys, do: {:ok, _} = @store.put(c, k, "x", :none, %{})
        {:ok, _} = @store.put(c, p <> "other", "x", :none, %{})

        assert {:ok, ^keys, nil} = @store.list(c, p <> "seg/", nil, 100)

        {:ok, page1, next1} = @store.list(c, p <> "seg/", nil, 3)
        assert page1 == Enum.take(keys, 3)
        assert next1 == List.last(page1)

        {:ok, page2, next2} = @store.list(c, p <> "seg/", next1, 3)
        assert page2 == Enum.slice(keys, 3, 3)

        {:ok, page3, next3} = @store.list(c, p <> "seg/", next2, 3)
        assert page3 == [List.last(keys)]
        assert next3 == nil

        assert {:ok, [], nil} = @store.list(c, p <> "none/", nil, 10)
      end

      test "if_none_match succeeds on a new key and fails on an existing one", %{
        config: c,
        prefix: p
      } do
        assert {:ok, etag} = @store.put(c, p <> "n", "one", :if_none_match, %{})

        assert {:error, :precondition_failed} =
                 @store.put(c, p <> "n", "two", :if_none_match, %{})

        assert {:ok, "one", ^etag, _} = @store.get(c, p <> "n")
      end

      test "if_match succeeds with the current etag and fails otherwise", %{config: c, prefix: p} do
        {:ok, etag} = @store.put(c, p <> "i", "one", :none, %{})

        assert {:error, :precondition_failed} =
                 @store.put(c, p <> "i", "bad", {:if_match, "wrong"}, %{})

        assert {:ok, etag2} = @store.put(c, p <> "i", "two", {:if_match, etag}, %{})

        assert {:error, :precondition_failed} =
                 @store.put(c, p <> "i", "stale", {:if_match, etag}, %{})

        assert {:ok, "two", ^etag2, _} = @store.get(c, p <> "i")
      end

      test "if_match replaces a segment-shaped key with its new metadata (design §6, R3)", %{
        config: c,
        prefix: p
      } do
        key = p <> "cells/c/0000/0000000000000005-0000000000000005.ltx"
        {:ok, etag} = @store.put(c, key, "zombie", :if_none_match, %{"epoch" => "1"})

        assert {:error, :precondition_failed} =
                 @store.put(c, key, "new", :if_none_match, %{"epoch" => "2"})

        assert {:ok, etag2} = @store.put(c, key, "new", {:if_match, etag}, %{"epoch" => "2"})
        assert etag2 != etag
        assert {:ok, "new", ^etag2, %{"epoch" => "2"}} = @store.get(c, key)
        assert {:ok, [^key], nil} = @store.list(c, p <> "cells/c/0000/", nil, 10)

        assert {:error, :precondition_failed} =
                 @store.put(c, key, "late", {:if_match, etag}, %{"epoch" => "1"})
      end

      test "if_match on a missing key fails", %{config: c, prefix: p} do
        assert {:error, :precondition_failed} =
                 @store.put(c, p <> "missing", "x", {:if_match, "any"}, %{})
      end

      test "race: exactly one if_none_match put wins, over several keys", %{config: c, prefix: p} do
        for round <- 1..5 do
          key = p <> "race-none-#{round}"

          results = race(@racers, fn i -> put_retrying(c, key, i, :if_none_match) end)
          assert_one_winner(c, key, results)
        end
      end

      test "race: exactly one if_match put with the same etag wins", %{config: c, prefix: p} do
        for round <- 1..5 do
          key = p <> "race-match-#{round}"
          {:ok, etag} = @store.put(c, key, "base", :none, %{})

          results = race(@racers, fn i -> put_retrying(c, key, i, {:if_match, etag}) end)
          assert_one_winner(c, key, results)
        end
      end

      # A put that timed out may have landed, so it is repeated a few times;
      # a `:precondition_failed` after a timeout is then `{:maybe, i}`: racer
      # `i` lost, or won on the first try and only the stored object tells.
      defp put_retrying(c, key, i, condition, timed_out? \\ false, attempts \\ 3) do
        case @store.put(c, key, "writer-#{i}", condition, %{}) do
          {:error, :timeout} when attempts > 1 ->
            put_retrying(c, key, i, condition, true, attempts - 1)

          {:error, :precondition_failed} when timed_out? ->
            {:maybe, i}

          result ->
            result
        end
      end

      # Exactly one writer won: the one that got `{:ok, etag}`, and the object
      # has that etag; or, when none did, a racer that timed out, and the
      # object holds its body. Every other racer got `:precondition_failed`.
      defp assert_one_winner(c, key, results) do
        {oks, others} = Enum.split_with(results, &match?({:ok, _}, &1))
        maybes = for {:maybe, i} <- others, do: i
        lost = Enum.count(others, &(&1 == {:error, :precondition_failed}))
        report = lost_report(results)

        assert length(oks) <= 1, report
        assert lost + length(maybes) == length(others), report

        case oks do
          [{:ok, etag}] ->
            assert @racers - 1 == length(others), report
            assert {:ok, _, ^etag, _} = @store.get(c, key)

          [] ->
            assert @racers == length(others) and maybes != [], report
            assert {:ok, body, _, _} = @store.get(c, key)
            assert body in Enum.map(maybes, &"writer-#{&1}"), report
        end
      end

      # What the racers that did not win got, for a failure message.
      defp lost_report(results) do
        lost = Enum.reject(results, &match?({:ok, _}, &1))

        "lost racers: #{inspect(Enum.frequencies(lost))}; first raw results: " <>
          inspect(Enum.take(Enum.reject(lost, &(&1 == {:error, :precondition_failed})), 5),
            limit: :infinity,
            printable_limit: 1_000
          )
      end

      defp race(n, fun) do
        1..n
        |> Task.async_stream(fun, max_concurrency: n, ordered: false, timeout: 60_000)
        |> Enum.map(fn {:ok, result} -> result end)
      end
    end
  end
end
