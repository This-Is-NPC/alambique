defmodule Alambique.LtxFormatTest do
  @moduledoc """
  The LTX format against the specification (docs/segment-format.md): the
  upstream examples, then our files read by `Alambique.LtxReader`, which
  shares no code with the Rust encoder.
  """
  use ExUnit.Case, async: false

  import Bitwise
  import Alambique.CompactionHelpers
  import Alambique.ReplicationHelpers, only: [tmp_dir: 0]

  alias Alambique.{LtxExample, LtxReader}
  alias Alambique.LtxReader.{Crc64, Lz4}
  alias Alambique.Replication
  alias Alambique.Replication.{Keys, Native}

  describe "upstream examples" do
    test "the reader reproduces the file checksums upstream states" do
      assert {:ok, sparse} = LtxReader.decode(LtxExample.sparse())
      assert sparse.file_checksum == LtxExample.sparse_checksum()
      assert sparse.header.min_txid == 2 and sparse.header.max_txid == 3
      assert sparse.header.commit == 16 and sparse.header.timestamp == 1000
      assert Enum.map(sparse.pages, &elem(&1, 0)) == [2, 5, 9, 16]
      assert Enum.all?(sparse.pages, fn {pgno, data} -> data == LtxExample.page(pgno) end)

      assert {:ok, snap} = LtxReader.decode(LtxExample.snapshot())
      assert snap.file_checksum == LtxExample.snapshot_checksum()
      assert snap.pages == LtxExample.snapshot_pages()
    end

    test "CRC-64/ISO has the check value of the catalogue" do
      assert Crc64.checksum("123456789") == 0xB90956C775A41001
    end

    test "the LZ4 decompressor handles overlapping matches and rejects bad blocks" do
      # 1 literal, a 9-byte match at offset 1, then 5 literals.
      block = <<0x15, ?a, 1::16-little, 0x50, "bcdef">>
      assert Lz4.decompress(block, 15) == "aaaaaaaaaabcdef"
      assert catch_throw(Lz4.decompress(block, 9)) == {:ltx, {:lz4_match, 1, 9}}
      assert catch_throw(Lz4.decompress(<<0x30, ?a>>, 15)) == {:ltx, :lz4_truncated}
    end

    test "our decoder reads the examples with the same pages" do
      sparse = LtxExample.sparse()
      assert {:ok, {2, 3, 1000, 16, pre, 512}, post} = Native.ltx_info(sparse, true)
      assert {pre, post} == {1 <<< 63 ||| 1, 1 <<< 63 ||| 2}

      # The pages equal the reader's because the verified file checksum covers
      # the decompressed pages. Applying a snapshot with a real post-apply
      # checksum then shows the pages themselves.
      {:ok, ref} = LtxReader.decode(LtxExample.snapshot())
      real = LtxReader.database_checksum(ref.pages)
      snapshot = LtxExample.snapshot(real)

      path = Path.join(tmp_dir(), "example.db")
      on_exit(fn -> File.rm_rf(Path.dirname(path)) end)
      assert {:ok, {1, 4, 1000, 6, 0, 512}, ^real} = Native.apply_ltx(path, snapshot, 0, 512)
      assert File.read!(path) == IO.iodata_to_binary(Enum.map(ref.pages, &elem(&1, 1)))
    end
  end

  describe "files written by Alambique" do
    setup do
      env = setup_world()
      on_exit(fn -> File.rm_rf(env.dir) end)
      Map.put(env, :owner, workload(env))
    end

    # Key-value writes, tables, a drop and a VACUUM that shrinks the database,
    # compacted twice so merges and two snapshots exist.
    defp workload(env) do
      compact = fn ->
        {:ok, _} =
          Alambique.Compaction.run(env.ctx, cell(),
            fanout: 2,
            max_level: 3,
            snapshot_interval_txids: 8
          )
      end

      owner = history(env, new_owner(env, "a.db"), 6)
      owner = write!(env, owner, [{:fill, 1}, {:fill, 2}, {:put, 3, 9_000}])
      owner = write!(env, owner, [{:fill, 3}, {:fill, 4}])
      owner = write!(env, owner, [{:drop, 2}, {:drop, 3}])
      compact.()
      owner = write!(env, owner, [:vacuum])
      owner = history(env, owner, 7)
      owner = write!(env, owner, [{:del, 3}, {:fill, 5}])
      compact.()
      history(env, owner, 3)
    end

    defp stored_files(env) do
      for key <- keys(env), String.ends_with?(key, ".ltx"), into: %{} do
        {:ok, body, _etag, _meta} =
          env.mod.get(env.cfg, Keys.cell_prefix(env.ctx.prefix, cell()) <> "/" <> key)

        {key, body}
      end
    end

    # Snapshot at or below the target, then the file reaching furthest from
    # the next TXID, from the highest level down.
    defp chain(files, target) do
      parsed =
        for {key, body} <- files do
          [level, name] = String.split(key, "/")
          <<min::binary-16, "-", max::binary-16, ".ltx">> = name

          {String.to_integer(level, 16), String.to_integer(min, 16), String.to_integer(max, 16),
           body}
        end

      snaps = for {9, _, max, _} = f <- parsed, max <= target, do: f
      start = Enum.max_by(snaps, &elem(&1, 2), fn -> nil end)
      rest = Enum.reject(parsed, &(elem(&1, 0) == 9))

      next_files(
        rest,
        if(start, do: elem(start, 2), else: 0),
        target,
        if(start, do: [start], else: [])
      )
    end

    defp next_files(_rest, at, target, acc) when at == target, do: Enum.reverse(acc)

    defp next_files(rest, at, target, acc) do
      candidates = for {_, min, max, _} = f <- rest, min == at + 1, max <= target, do: f
      next = Enum.max_by(candidates, &elem(&1, 2))
      next_files(rest, elem(next, 2), target, [next | acc])
    end

    defp decode!(body) do
      {:ok, file} = LtxReader.decode(body)
      file
    end

    defp reader_image(files, target) do
      {:ok, db} =
        files |> chain(target) |> Enum.map(&decode!(elem(&1, 3))) |> LtxReader.apply_chain()

      assert db.txid == target
      {:ok, image} = LtxReader.image(db)
      image
    end

    test "the reader decodes every file and applies the chains to our restores", env do
      files = stored_files(env)
      by_level = files |> Map.keys() |> Enum.map(&String.slice(&1, 0, 4)) |> Enum.frequencies()

      # Level 0 batches, merged files and snapshots are all present.
      assert Map.keys(by_level) |> Enum.sort() == ["0000", "0001", "0002", "0003", "0009"]
      assert by_level["0009"] == 2

      for {key, body} <- files do
        assert {:ok, file} = LtxReader.decode(body), key
        assert file.header.flags == 2
        assert Native.ltx_info(body, true) |> elem(0) == :ok
      end

      head = env.owner.state.txid

      for target <- [head, head - 4, 9, 3] do
        path = Path.join(env.dir, "restored-#{target}.db")

        assert {:ok, %{txid: ^target}} =
                 Replication.restore(env.ctx, cell(), path, {:txid, target})

        assert reader_image(Map.new(files), target) == File.read!(path), "TXID #{target}"
      end
    end

    test "the reader rejects a flipped bit unless the file still decodes to the same content",
         env do
      files = stored_files(env)
      {_key, small} = Enum.min_by(files, fn {_k, b} -> byte_size(b) end)
      {_key, large} = Enum.max_by(files, fn {_k, b} -> byte_size(b) end)
      step = div(byte_size(large), 97)

      # Every byte of the smallest file, a spread over the largest.
      for {body, positions} <- [
            {small, 0..(byte_size(small) - 1)},
            {large,
             [
               0,
               83,
               95,
               99,
               byte_size(large) - 1 | Enum.take_every(0..(byte_size(large) - 1), step)
             ]}
          ],
          pos <- positions,
          bit <- [0, 7] do
        <<head::binary-size(^pos), byte, tail::binary>> = body
        flipped = <<head::binary, bxor(byte, 1 <<< bit), tail::binary>>

        case LtxReader.decode(flipped) do
          {:error, _} ->
            :ok

          # The file checksum covers the decompressed pages, not the LZ4 bytes,
          # so a flip that leaves the pages as they were is allowed.
          {:ok, file} ->
            assert pos >= 100 and pos < byte_size(body) - 40, "byte #{pos} bit #{bit} accepted"
            assert file == decode!(body), "byte #{pos} bit #{bit} changed the content"
        end
      end
    end
  end
end
