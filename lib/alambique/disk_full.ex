defmodule Alambique.DiskFull do
  @moduledoc false
  # A full local disk is `:disk_full` for the caller (design §6.4), whatever
  # step hit it. SQLite and the native library already report it by code
  # (`SQLITE_FULL`, `ENOSPC`, `EDQUOT`); the file operations of Elixir give the
  # POSIX atoms. A step that wraps the reason of the step below it
  # (`{:migration, version, reason}`, `{:restore, key, reason}`) is a full disk
  # when the reason it wraps is.

  @codes [:disk_full, :enospc, :edquot]

  @doc "`:disk_full` for a full-disk reason, any other reason as it is."
  @spec classify(term) :: term
  def classify(reason) when reason in @codes, do: :disk_full

  def classify(reason) when is_tuple(reason) and tuple_size(reason) > 1 do
    if classify(elem(reason, tuple_size(reason) - 1)) == :disk_full, do: :disk_full, else: reason
  end

  def classify(reason), do: reason
end
