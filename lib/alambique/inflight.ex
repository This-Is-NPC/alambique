defmodule Alambique.Inflight do
  @moduledoc false
  # Markers of work in flight (design §10). A process that is about to run
  # native code on the data of a cell holds a file under `<data_dir>/inflight/`
  # whose first byte is 1 while that code runs and 0 otherwise. A VM crash
  # leaves the byte at 1; `Alambique.Inflight.Tracker` reads those files when
  # the instance starts and records the crash.
  #
  # The flag is a `pwrite` to a file the process keeps open: no fsync (a VM
  # crash keeps the page cache; a machine crash is not covered), no new file
  # per call. The file is created once per process and removed by the tracker
  # when the process ends, so only a VM crash leaves a busy file behind.

  require Logger

  alias Alambique.Inflight.Tracker

  @busy <<1>>
  @idle <<0>>

  @enforce_keys [:fd, :path]
  defstruct [:fd, :path]

  @type t :: %__MODULE__{fd: :file.fd(), path: Path.t()} | nil

  @doc "Directory of the markers of an instance."
  @spec dir(Alambique.Config.t()) :: Path.t()
  def dir(config), do: Path.join(config.data_dir, "inflight")

  @doc """
  Creates the marker of the calling process for `cell`, idle. `nil` when
  quarantine is off or the file cannot be created: a cell without a marker
  works, it is only not protected.
  """
  @spec open(Alambique.Config.t(), String.t(), atom) :: t
  def open(%{quarantine: nil}, _cell, _kind), do: nil

  def open(config, cell, kind) do
    dir = dir(config)
    path = Path.join(dir, "#{System.os_time(:microsecond)}-#{System.unique_integer([:positive])}")

    body = [@idle, Jason.encode!(%{cell: cell, kind: kind})]

    with {:ok, fd} <- open_file(path, dir, body) do
      Tracker.track(config, path)
      %__MODULE__{fd: fd, path: path}
    else
      {:error, reason} ->
        Logger.warning("Alambique could not create the marker of #{cell}: #{inspect(reason)}")
        nil
    end
  end

  # The tracker creates the directory at instance start. Avoiding `mkdir_p`
  # here matters while a dirty I/O scheduler waits for a page: this process may
  # be the one that must fetch it, and `File.mkdir_p` needs a dirty I/O
  # scheduler too.
  defp open_file(path, dir, body) do
    case :file.open(path, [:raw, :binary, :write, :read, :exclusive]) do
      {:ok, fd} ->
        case :file.write(fd, body) do
          :ok ->
            {:ok, fd}

          {:error, _} = error ->
            :file.close(fd)
            error
        end

      {:error, :enoent} ->
        with :ok <- File.mkdir_p(dir), do: open_file(path, dir, body)

      error ->
        error
    end
  end

  @doc "Marks the marker busy for work that outlives one callback."
  @spec hold(t) :: :ok
  def hold(nil), do: :ok
  def hold(%__MODULE__{fd: fd}), do: :file.pwrite(fd, 0, @busy)

  @doc "Marks the marker idle, closes it and removes its file."
  @spec close(Alambique.Config.t(), t) :: :ok
  def close(_config, nil), do: :ok

  def close(config, %__MODULE__{fd: fd, path: path}) do
    :file.pwrite(fd, 0, @idle)
    :file.close(fd)
    File.rm(path)
    Tracker.untrack(config, path)
  end

  @doc "Runs `fun` with the marker busy."
  @spec busy(t, (-> result)) :: result when result: var
  def busy(nil, fun), do: fun.()

  def busy(%__MODULE__{fd: fd}, fun) do
    :file.pwrite(fd, 0, @busy)

    try do
      fun.()
    after
      :file.pwrite(fd, 0, @idle)
    end
  end

  @doc "Runs `fun` with a marker of its own, for work outside a cell process."
  @spec run(Alambique.Config.t(), String.t(), atom, (-> result)) :: result
        when result: var
  def run(config, cell, kind, fun) do
    marker = open(config, cell, kind)

    try do
      busy(marker, fun)
    after
      close(config, marker)
    end
  end
end
