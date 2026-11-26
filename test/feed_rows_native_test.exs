defmodule Alambique.FeedRowsNativeTest do
  @moduledoc """
  `Capture.send_rows/6` (the library builds the text of the records of the
  feed's rows, design §13) against the Elixir path that reads the rows and
  encodes them with `Record.encode/2`: the object bodies are the same bytes,
  whatever the bytes of the columns and however the rows are cut into calls.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Feed.{Capture, Pieces, Record}
  alias Alambique.Sqlite

  setup do
    dir = Alambique.TestDirs.new("rows-native")
    {:ok, conn} = Sqlite.open(Path.join(dir, "c.db"))
    on_exit(fn -> Sqlite.close(conn) end)
    :ok = Sqlite.execute(conn, "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)")
    :ok = Capture.install(conn, Capture.everything())
    %{conn: conn}
  end

  # A text column with exactly these bytes: a cast keeps invalid UTF-8 and NUL.
  defp text(nil), do: "NULL"
  defp text(bytes), do: "CAST(X'#{Base.encode16(bytes)}' AS TEXT)"

  defp put(conn, txid, {tbl, op, key, vals}) do
    :ok =
      Sqlite.execute(
        conn,
        "INSERT INTO _alambique_feed (txid, ts, tbl, op, key, vals) VALUES " <>
          "(#{txid}, #{txid * 10}, #{text(tbl)}, #{text(op)}, #{text(key)}, #{text(vals)})"
      )
  end

  defp load(conn, batches),
    do: for({txid, changes} <- batches, c <- changes, do: put(conn, txid, c))

  # What the cell does without the library: reads pieces and joins the pieces
  # of a batch, as the sender does.
  defp reference(conn, upto, bound) do
    reference(conn, {1, -1}, nil, upto, bound, [])
  end

  defp reference(conn, position, resumed, upto, bound, acc) do
    {:ok, pieces, more?} = Pieces.read(conn, position, upto, bound, resumed)
    acc = Enum.reduce(pieces, acc, fn p, acc -> [{p.txid, p.changes} | acc] end)

    if more? do
      last = List.last(pieces)
      next = if last.partial, do: {last.txid, last.seq}, else: {last.txid + 1, -1}
      reference(conn, next, if(last.partial, do: next), upto, bound, acc)
    else
      join(Enum.reverse(acc))
    end
  end

  defp join(parts) do
    parts
    |> Enum.chunk_by(&elem(&1, 0))
    |> Enum.map(fn [{txid, _} | _] = same ->
      {txid, Enum.concat(Enum.map(same, &elem(&1, 1)))}
    end)
  end

  # What the cell does with the library: `{:error, :bad_table}` as it is.
  defp native(conn, upto, bound) do
    native(conn, {1, -1}, nil, upto, bound, nil, [])
  end

  defp native(conn, {txid, seq}, resumed, upto, bound, room, acc) do
    token = make_ref()
    plan = {bound, resumed, room}

    case Capture.send_rows(conn, {txid, seq}, upto, plan, self(), token) do
      {:ok, [], _, _} ->
        {:ok, finish(acc)}

      {:ok, batches, more?, cut?} ->
        assert_receive {:rows, ^token, bodies}, 1_000
        assert length(bodies) == length(batches)

        parts =
          for {{t, _ts, rows, _seq, size, {_first, partial, rewind}}, body} <-
                Enum.zip(batches, bodies) do
            assert size == byte_size(body)
            {t, body, rows, partial, rewind}
          end

        acc = Enum.reduce(parts, acc, &[&1 | &2])
        {t, _ts, rows, last_seq, _size, {first, partial, _}} = List.last(batches)

        cond do
          not more? or cut? ->
            {:ok, finish(acc)}

          partial ->
            native(
              conn,
              {t, last_seq},
              if(first, do: rows, else: resumed + rows),
              upto,
              bound,
              room,
              acc
            )

          true ->
            native(conn, {t + 1, -1}, nil, upto, bound, room, acc)
        end

      {:error, :bad_table} ->
        :bad_table
    end
  end

  defp finish(acc) do
    acc
    |> Enum.reverse()
    |> Enum.chunk_by(&elem(&1, 0))
    |> Enum.map(fn [{txid, _, _, _, rewind} | _] = same ->
      bodies = Enum.map(same, &elem(&1, 1))
      count = same |> Enum.map(&elem(&1, 2)) |> Enum.sum()

      if rewind,
        do: {txid, [{"", "rewind", "{}", hd(bodies)}]},
        else: {txid, {:encoded, Enum.intersperse(bodies, ?,), count}}
    end)
  end

  defp encode(batches), do: Enum.map(batches, &Record.encode("cell", [&1]))

  defp change do
    gen all(
          tbl <-
            frequency([
              {6,
               member_of(["notes", "a\"b", "sl/ash", "tab\t", "nul\0x", "é", "\u2028", "del\x7f"])},
              {2, constant("c\x01\x1F\b\f\r\n\\ ")},
              {1, constant("bad\xFF")}
            ]),
          op <- member_of(["insert", "update", "delete"]),
          key <- binary(max_length: 6),
          vals <- one_of([constant(nil), constant(""), binary(max_length: 6)])
        ) do
      {tbl, op, key, vals}
    end
  end

  defp rows(:rewind), do: [{"", "rewind", "{}", ~s({"a":1})}]

  property "the object bodies are the bytes of the Elixir path", %{conn: conn} do
    check all(
            changes <- list_of(change(), min_length: 1, max_length: 8),
            cuts <- list_of(integer(1..3), min_length: 1, max_length: 4),
            rewind? <- boolean(),
            bound <- member_of([nil, 1, 2, 3, 4]),
            max_runs: 60
          ) do
      :ok = Sqlite.execute(conn, "DELETE FROM _alambique_feed")
      batches = split(changes, cuts, rewind?)
      load(conn, batches)
      upto = 1_000

      # `Jason.encode!/1` raises on a table name that is not UTF-8.
      expected =
        try do
          reference = reference(conn, upto, bound)
          {:ok, {Enum.map(reference, &elem(&1, 0)), encode(reference)}}
        rescue
          Jason.EncodeError -> :bad_table
        end

      case {expected, native(conn, upto, bound)} do
        {:bad_table, :bad_table} ->
          :ok

        {{:ok, expected}, {:ok, library}} ->
          assert {Enum.map(library, &elem(&1, 0)), encode(library)} == expected

        {a, b} ->
          flunk("the paths differ: #{inspect(a)} and #{inspect(b)}")
      end
    end
  end

  # Batches of the sizes in `cuts`, cycled; a rewind is a batch of its own.
  defp split(changes, cuts, rewind?) do
    {groups, _} =
      Enum.reduce_while(Stream.cycle(cuts), {[], changes}, fn size, {acc, left} ->
        case Enum.split(left, size) do
          {[], _} -> {:halt, {acc, []}}
          {group, rest} -> {:cont, {[group | acc], rest}}
        end
      end)

    groups = Enum.reverse(groups)
    groups = if rewind?, do: groups ++ [:rewind], else: groups

    for {group, i} <- Enum.with_index(groups, 1) do
      {i * 2, if(group == :rewind, do: rows(:rewind), else: group)}
    end
  end

  describe "a batch cut into calls" do
    test "keeps the n of its records and reports where it was cut", %{conn: conn} do
      load(conn, [{2, for(i <- 1..7, do: {"notes", "insert", ~s({"id":#{i}}), ~s({"v":#{i}})})}])

      assert {:ok, [{2, 20, 3, s1, _, {true, true, false}}], true, false} =
               Capture.send_rows(conn, {1, -1}, 9, {3, nil, nil}, self(), make_ref())

      assert {:ok, [{2, 20, 3, s2, _, {false, true, false}}], true, false} =
               Capture.send_rows(conn, {2, s1}, 9, {3, 3, nil}, self(), make_ref())

      assert {:ok, [{2, 20, 1, _, _, {false, false, false}}], false, false} =
               Capture.send_rows(conn, {2, s2}, 9, {3, 6, nil}, self(), make_ref())

      # The ids of the records are those of the batch before it was cut.
      assert [{2, {:encoded, bodies, 7}}] =
               native_pieces(conn, [{3, nil}, {3, 3}, {3, 6}], 9)

      ids =
        for [_, n] <- Regex.scan(~r/"n":(\d+)/, IO.iodata_to_binary(bodies)),
            do: String.to_integer(n)

      assert ids == Enum.to_list(0..6)
    end

    defp native_pieces(conn, plans, upto) do
      {parts, _} =
        Enum.reduce(plans, {[], {1, -1}}, fn {bound, resumed}, {acc, pos} ->
          token = make_ref()

          {:ok, [{t, _, rows, seq, _, {_, partial, _}}], _, _} =
            Capture.send_rows(conn, pos, upto, {bound, resumed, nil}, self(), token)

          assert_receive {:rows, ^token, [body]}
          {[{t, body, rows, partial, false} | acc], {t, seq}}
        end)

      finish(parts)
    end
  end

  describe "the room" do
    test "a batch that would begin over it is not read, and is not sent", %{conn: conn} do
      load(conn, for(t <- [2, 4, 6], do: {t, [{"notes", "insert", ~s({"id":#{t}}), nil}]}))
      token = make_ref()

      assert {:ok, [{2, _, 1, _, _, {true, false, false}}], true, true} =
               Capture.send_rows(conn, {1, -1}, 9, {nil, nil, 1}, self(), token)

      assert_receive {:rows, ^token, [_one]}
      refute_receive {:rows, ^token, _}, 20
    end

    test "a batch begun before takes none", %{conn: conn} do
      load(conn, [
        {2, for(i <- 1..3, do: {"notes", "insert", ~s({"id":#{i}}), nil})},
        {4, [{"notes", "insert", ~s({"id":9}), nil}]}
      ])

      # The rest of batch 2 goes on with no room; batch 4 does not begin.
      assert {:ok, [{2, _, 2, _, _, {false, false, false}}], true, true} =
               Capture.send_rows(conn, {2, 1}, 9, {nil, 1, 0}, self(), make_ref())
    end
  end

  describe "what the library refuses or has nothing for" do
    test "a table name that is not UTF-8", %{conn: conn} do
      load(conn, [{2, [{"bad\xFF", "insert", "{}", nil}]}])

      assert {:error, :bad_table} =
               Capture.send_rows(conn, {1, -1}, 9, {nil, nil, nil}, self(), make_ref())

      refute_receive {:rows, _, _}, 20
    end

    test "a pid that no longer exists", %{conn: conn} do
      load(conn, [{2, [{"notes", "insert", "{}", nil}]}])
      {pid, ref} = spawn_monitor(fn -> :ok end)
      assert_receive {:DOWN, ^ref, _, _, _}

      assert {:error, :sender_gone} =
               Capture.send_rows(conn, {1, -1}, 9, {nil, nil, nil}, pid, make_ref())
    end

    test "no rows, and a file without the feed's tables", %{conn: conn} do
      assert {:ok, [], false, false} =
               Capture.send_rows(conn, {1, -1}, 9, {nil, nil, nil}, self(), make_ref())

      dir = Alambique.TestDirs.new("rows-native-empty")
      {:ok, bare} = Sqlite.open(Path.join(dir, "b.db"))
      on_exit(fn -> Sqlite.close(bare) end)

      assert {:ok, [], false, false} =
               Capture.send_rows(bare, {1, -1}, 9, {nil, nil, nil}, self(), make_ref())

      refute_receive {:rows, _, _}, 20
    end

    test "a process that is not the user of the connection", %{conn: conn} do
      task =
        Task.async(fn ->
          Capture.send_rows(conn, {1, -1}, 9, {nil, nil, nil}, self(), make_ref())
        end)

      assert {:error, :not_owner} = Task.await(task)
    end
  end
end
