defmodule Alambique.Cell.Sql do
  @moduledoc false
  # Splits a SQL script into statements, so a migration can run each one
  # through `Alambique.Tx` (design §13). Semicolons inside quotes, comments and
  # trigger bodies do not end a statement. It is not a safety gate: each piece
  # goes to SQLite alone, which refuses text after the first statement.

  # A word as SQLite tokenizes it: letters, digits, `_`, `$` and every byte of
  # a UTF-8 character; a variable (`:a`, `@a`, `$a`, `#a`) is one word too,
  # and `::` may join the parts of a variable. A keyword inside one is not a
  # keyword. Matched as bytes (no `u` flag).
  @word ~r/\A(?:[:@$]?(?:[A-Za-z0-9_$\x80-\xff]|::)+|#[A-Za-z0-9_$\x80-\xff]+)/

  # The UTF-8 byte order mark: SQLite reads it as a blank wherever a token
  # could start.
  @bom <<0xEF, 0xBB, 0xBF>>

  defguardp word_start(c)
            when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c in [?_, ?$, ?:, ?@, ?#] or
                   c >= 0x80

  @spec split(String.t()) :: [String.t()]
  def split(sql), do: scan(sql, [], new(), [])

  defp new, do: %{words: [], body: false, after_semi: false, done: false}

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
    scan(rest, [quoted | cur], %{st | after_semi: false}, acc)
  end

  defp scan(<<?[, rest::binary>>, cur, st, acc) do
    {text, rest} = take_until(rest, "]")
    scan(rest, [["[", text] | cur], %{st | after_semi: false}, acc)
  end

  defp scan(<<?;, rest::binary>>, cur, st, acc) do
    if st.words |> Enum.take(3) |> trigger?() and not st.done do
      scan(rest, [";" | cur], %{st | after_semi: st.body}, acc)
    else
      scan(rest, [], new(), finish([";" | cur], acc))
    end
  end

  defp scan(<<@bom, rest::binary>>, cur, st, acc), do: scan(rest, [@bom | cur], st, acc)

  defp scan(<<c, _::binary>> = sql, cur, st, acc) when word_start(c) do
    case Regex.run(@word, sql) do
      [word] ->
        rest = binary_part(sql, byte_size(word), byte_size(sql) - byte_size(word))
        {word, rest} = variable_arguments(word, rest)
        scan(rest, [word | cur], track(st, String.downcase(word)), acc)

      nil ->
        <<c, rest::binary>> = sql
        scan(rest, [<<c>> | cur], st, acc)
    end
  end

  defp scan(<<c, rest::binary>>, cur, st, acc) when c in [?\s, ?\t, ?\n, ?\f, ?\r],
    do: scan(rest, [<<c>> | cur], st, acc)

  defp scan(<<c, rest::binary>>, cur, st, acc),
    do: scan(rest, [<<c>> | cur], %{st | after_semi: false}, acc)

  # `$a(...)`, `:a(...)` and `@a(...)` are one variable up to the `)`, which a
  # blank before it spoils (SQLite then reads the `(` apart).
  defp variable_arguments(<<v, _::binary>> = word, <<?(, _::binary>> = text)
       when v in [?$, ?:, ?@] and byte_size(word) > 1 do
    case Regex.run(~r/\A\(([^)\s]*)\)/, text) do
      [args | _] ->
        {word <> args, binary_part(text, byte_size(args), byte_size(text) - byte_size(args))}

      nil ->
        {word, text}
    end
  end

  defp variable_arguments(word, rest), do: {word, rest}

  # Words of the statement so far, to know whether it is a trigger and where
  # its BEGIN ... END body ends: an `END` that starts a statement of the body
  # (after a `;`). Any other `end` is a CASE's or an identifier's.
  defp track(st, word) do
    st = %{st | words: st.words ++ [word]}
    trigger? = st.words |> Enum.take(3) |> trigger?()
    ends? = st.after_semi and word == "end"
    st = %{st | after_semi: false}

    cond do
      not trigger? -> %{st | words: Enum.take(st.words, 3)}
      word == "begin" -> %{st | body: true}
      ends? -> %{st | done: true}
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
    |> String.replace(@bom, "")
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
