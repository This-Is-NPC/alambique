defmodule Mix.Tasks.Alambique.Inspect do
  @shortdoc "Shows the lease, head, files and freeze state of a cell"

  @moduledoc """
  Shows what the store holds for one cell: whether it is frozen, the VM crashes recorded for it, its lease
  (epoch, owner, state), the head TXID, the segments by level and the
  snapshots, with sizes.

      mix alambique.inspect CELL [options]

  Options:

    * `--json` - print one JSON object instead of text
    * `--no-sizes` - do not fetch the objects to measure them (fast for cells
      with many files)

  and the store options described in `Mix.Tasks.Alambique.Restore`.
  """

  use Mix.Task

  alias Alambique.Ops
  alias Alambique.Ops.Options

  @impl Mix.Task
  def run(args) do
    {parsed, rest} =
      OptionParser.parse!(args,
        strict: [json: :boolean, sizes: :boolean] ++ Options.switches()
      )

    cell = cell!(rest)
    Options.boot()
    ctx = Options.ctx(parsed, cell)

    case Ops.describe(ctx, cell, sizes: Keyword.get(parsed, :sizes, true)) do
      {:ok, info} ->
        if parsed[:json], do: Mix.shell().info(Jason.encode!(info)), else: print(info)

      {:error, reason} ->
        Mix.raise("could not read #{cell}: #{inspect(reason)}")
    end
  end

  defp cell!([cell]), do: cell
  defp cell!(_), do: Mix.raise("usage: mix alambique.inspect CELL [options]")

  defp print(info) do
    shell = Mix.shell()
    out = &shell.info/1
    out.("Cell:       #{info.cell}")
    out.("Prefix:     #{info.prefix}")
    out.("Frozen:     #{if info.frozen, do: "yes", else: "no"}")
    out.("Crashes:    #{crashes(info.crashes)}")
    out.("Lease:      #{lease(info.lease)}")
    out.("Head:       #{head(info.head)}")

    out.("Segments:")
    if info.levels == [], do: out.("  none")

    for l <- info.levels,
        do:
          out.(
            "  level #{l.level}: #{l.files} #{plural(l.files, "file")}, TXID #{l.min}-#{l.max}#{size(l.bytes)}"
          )

    out.("Snapshots:")
    if info.snapshots == [], do: out.("  none")

    for s <- info.snapshots,
        do: out.("  TXID #{s.txid}#{size(s.bytes)}, epoch #{s.epoch}#{stamp(s.ts)}")
  end

  defp crashes(nil), do: "none"

  defp crashes(%{total: total, recent: recent}),
    do: "#{total} recorded, #{length(recent)} in the window"

  defp lease(nil), do: "none"

  defp lease(l) do
    state =
      case l.state do
        :held -> "held, expires in #{seconds(l.expires_in_ms)}"
        :expired -> "expired #{seconds(-l.expires_in_ms)} ago"
        :released -> "released"
      end

    moved = if l.moved_to, do: ", moved to location #{l.moved_to}", else: ""
    "epoch #{l.epoch}, owner #{l.owner}, #{state}#{moved}"
  end

  defp head(nil), do: "empty"
  defp head(h), do: "TXID #{h.txid}, epoch #{h.epoch}, page size #{h.page_size}"

  defp seconds(ms), do: "#{Float.round(ms / 1000, 1)} s"

  defp size(nil), do: ""
  defp size(false), do: ""
  defp size(bytes) when bytes < 1024, do: ", #{bytes} B"
  defp size(bytes) when bytes < 1_048_576, do: ", #{Float.round(bytes / 1024, 1)} KiB"
  defp size(bytes), do: ", #{Float.round(bytes / 1_048_576, 1)} MiB"

  defp stamp(nil), do: ""

  defp stamp(ms),
    do: ", state of #{ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()}"

  defp plural(1, word), do: word
  defp plural(_, word), do: word <> "s"
end
