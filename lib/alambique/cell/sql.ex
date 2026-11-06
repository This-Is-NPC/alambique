defmodule Alambique.Cell.Sql do
  @moduledoc false
  # Splits a SQL script into statements, so a migration can run each one
  # through `Alambique.Tx` (design §13). Semicolons inside quotes, comments and
  # trigger bodies do not end a statement.

  @word ~r/\A[A-Za-z_][A-Za-z0-9_]*/

  @spec split(String.t()) :: [String.t()]
  def split(sql), do: scan(sql, [], new(), [])

  defp new, do: %{words: [], body: false, cases: 0, done: false}

  # `current` is the reversed iodata of the statement being read.
  defp scan("", current, _st, acc), do: finish(current, acc) |> Enum.reverse()

  defp scan(<<"--", rest::binary>>, cur, st, acc) do
    {comment, rest} = take_until(rest, "\n")
    scan(rest, [["--", comment] | cur], st, acc)
  end

  defp scan(<<"/*", rest::binary>>, cur, st, acc) do
    {comment, rest} = take_until(rest, "*/")
    scan(rest, [["/*", comment] | cur], st, acc)
  end

  defp scan(<<q, rest::binary>>, cur, st, acc) when q in [?', ?", ?`] do
    {quoted, rest} = quoted(rest, q, [<<q>>])
    scan(rest, [quoted | cur], st, acc)
  end

  defp scan(<<?[, rest::binary>>, cur, st, acc) do
    {text, rest} = take_until(rest, "]")
    scan(rest, [["[", text] | cur], st, acc)
  end

  defp scan(<<?;, rest::binary>>, cur, st, acc) do
    if st.words |> Enum.take(3) |> trigger?() and not st.done do
      scan(rest, [";" | cur], st, acc)
    else
      scan(rest, [], new(), finish([";" | cur], acc))
    end
  end

  defp scan(<<c, _::binary>> = sql, cur, st, acc) when c in ?a..?z or c in ?A..?Z or c == ?_ do
    [word] = Regex.run(@word, sql)
    rest = binary_part(sql, byte_size(word), byte_size(sql) - byte_size(word))
    scan(rest, [word | cur], track(st, String.downcase(word)), acc)
  end

  defp scan(<<c, rest::binary>>, cur, st, acc), do: scan(rest, [<<c>> | cur], st, acc)

  # Words of the statement so far, to know whether it is a trigger and where
  # its BEGIN ... END body ends (CASE ... END inside it does not).
  defp track(st, word) do
    st = %{st | words: st.words ++ [word]}
    trigger? = st.words |> Enum.take(3) |> trigger?()

    cond do
      not trigger? -> %{st | words: Enum.take(st.words, 3)}
      word == "begin" -> %{st | body: true}
      word == "case" and st.body -> %{st | cases: st.cases + 1}
      word == "end" and st.body and st.cases > 0 -> %{st | cases: st.cases - 1}
      word == "end" and st.body -> %{st | done: true}
      true -> %{st | words: Enum.take(st.words, 3)}
    end
  end

  defp trigger?(["create", "trigger" | _]), do: true
  defp trigger?(["create", t, "trigger"]) when t in ["temp", "temporary"], do: true
  defp trigger?(_), do: false

  defp finish(current, acc) do
    statement = current |> Enum.reverse() |> IO.iodata_to_binary() |> String.trim()
    if blank?(statement), do: acc, else: [statement | acc]
  end

  # Only whitespace, semicolons and comments (the last one may be left open).
  defp blank?(text) do
    text
    |> String.replace(~r/--[^\n]*|\/\*.*?(?:\*\/|\z)/s, "")
    |> String.trim()
    |> String.trim(";")
    |> String.trim() == ""
  end

  defp take_until(text, stop) do
    case :binary.split(text, stop) do
      [before, rest] -> {before <> stop, rest}
      [all] -> {all, ""}
    end
  end

  defp quoted(<<q, q, rest::binary>>, q, acc), do: quoted(rest, q, [<<q, q>> | acc])
  defp quoted(<<q, rest::binary>>, q, acc), do: {Enum.reverse([<<q>> | acc]), rest}
  defp quoted(<<c, rest::binary>>, q, acc), do: quoted(rest, q, [<<c>> | acc])
  defp quoted("", _q, acc), do: {Enum.reverse(acc), ""}
end
