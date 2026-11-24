defmodule Alambique.Foreign do
  @moduledoc """
  Test support: a writer that the library cannot prevent (design §15), as
  another process or code that opened the file before the cell would be.
  """
  alias Alambique.Replication.Native

  @doc "Opens `path` read-write with SQLite's default VFS, past the table of held files."
  @spec open(Path.t()) :: {:ok, reference} | {:error, term}
  def open(path) do
    case Native.foreign_open(path) do
      {:ok, ref, 0} -> {:ok, ref}
      error -> error
    end
  end

  @doc "Runs `sql` on a connection of its own that is closed after it: SQLite's result code."
  @spec exec(Path.t(), String.t()) :: non_neg_integer
  def exec(path, sql), do: Native.foreign_exec(path, sql)
end
