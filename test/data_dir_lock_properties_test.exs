defmodule Alambique.DataDirLockPropertiesTest do
  @moduledoc """
  A data directory has at most one live instance (design §1): over random
  sequences of starts and stops on a few directories, a start succeeds exactly
  when no live instance holds the directory.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Alambique.CellHelpers

  @moduletag capture_log: true

  setup do
    %{ctx: setup_store()}
  end

  property "a start succeeds exactly when the directory has no live instance", %{ctx: ctx} do
    command =
      one_of([
        tuple({constant(:start), integer(0..2)}),
        tuple({constant(:stop), integer(0..2)})
      ])

    check all(commands <- list_of(command, max_length: 20), max_runs: 40) do
      # holders: dir index => the instance name that holds it.
      holders =
        Enum.reduce(commands, %{}, fn
          {:start, dir}, holders ->
            name = :"holder_#{System.unique_integer([:positive])}"
            started = start(ctx, name, dir)
            assert started == not Map.has_key?(holders, dir)
            if started, do: Map.put(holders, dir, name), else: holders

          {:stop, dir}, holders ->
            case Map.pop(holders, dir) do
              {nil, holders} ->
                holders

              {name, holders} ->
                :ok = stop_supervised(name)
                holders
            end
        end)

      for {_dir, name} <- holders, do: stop_supervised(name)
    end
  end

  defp start(ctx, name, dir) do
    opts = [
      name: name,
      store: ctx.store,
      prefix: "cells-test",
      data_dir: Path.join(ctx.dir, "d#{dir}"),
      node_id: Atom.to_string(name)
    ]

    case start_supervised(Supervisor.child_spec({Alambique, opts}, id: name)) do
      {:ok, _} -> true
      {:error, {{:data_dir_in_use, _}, _}} -> false
    end
  end
end
