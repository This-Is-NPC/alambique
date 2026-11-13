defmodule Alambique.Cell.Pipe do
  @moduledoc false
  # The commit pipeline of one cell (design §6): batches that were encoded and
  # sent to upload processes, oldest first. Only the head of the queue is ever
  # acknowledged, so callers are answered in TXID order whatever order the
  # uploads land in. `acked` is the newest acknowledged TXID; the replicator
  # state is ahead of it by the batches in the queue.

  alias Alambique.Replication.Replicator

  defstruct [
    :depth,
    queue: [],
    flush_wanted: false,
    acked: %{txid: 0, epoch: 0, data: 0}
  ]

  @type batch :: %{
          txid: pos_integer,
          ref: reference,
          # `nil` while the batch waits to be uploaded again
          pid: pid | nil,
          landed: boolean,
          # failed uploads so far, and the reason of the last
          attempts: non_neg_integer,
          last: term,
          # newest first
          items: list,
          writes: non_neg_integer,
          started: integer,
          inflight: pos_integer,
          epoch: pos_integer,
          data: non_neg_integer,
          # held for the compactor once acknowledged
          upload: map
        }

  @type t :: %__MODULE__{}

  @spec new(Alambique.Config.t()) :: t
  def new(config), do: %__MODULE__{depth: config.pipeline_depth}

  @doc "What a replicator state has acknowledged, for a state without a pipeline in flight."
  @spec acked(Replicator.t()) :: map
  def acked(repl), do: %{txid: repl.txid, epoch: repl.last_epoch, data: repl.data}

  @spec full?(t) :: boolean
  def full?(pipe), do: length(pipe.queue) >= pipe.depth

  @spec push(t, batch) :: t
  def push(pipe, batch), do: %{pipe | queue: pipe.queue ++ [batch]}

  @spec find(t, reference) :: batch | nil
  def find(pipe, ref), do: Enum.find(pipe.queue, &(&1.ref == ref))

  @spec update(t, reference, (batch -> batch)) :: t
  def update(pipe, ref, fun),
    do: %{pipe | queue: Enum.map(pipe.queue, &if(&1.ref == ref, do: fun.(&1), else: &1))}

  @spec land(t, reference) :: t
  def land(pipe, ref), do: update(pipe, ref, &%{&1 | landed: true})

  @doc """
  Makes `items` (newest first, like the items of a batch) wait for the newest
  batch; the queue must not be empty. The queue holds at most the pipeline
  depth, and the items go in front of the batch's, so the cost is that of
  `items` alone.
  """
  @spec attach(t, list) :: t
  def attach(%__MODULE__{queue: queue} = pipe, items) do
    {last, rest} = List.pop_at(queue, -1)
    %{pipe | queue: rest ++ [%{last | items: items ++ last.items}]}
  end

  @doc "Batch pids by pid, for selective receives."
  @spec pids(t) :: %{pid => true}
  def pids(pipe), do: for(%{pid: pid} <- pipe.queue, pid != nil, into: %{}, do: {pid, true})

  @doc "Every waiting item of the queue."
  @spec items(t) :: list
  def items(pipe), do: Enum.flat_map(pipe.queue, & &1.items)

  @doc "Removes the batch of `ref` from the queue."
  @spec drop(t, reference) :: t
  def drop(pipe, ref), do: %{pipe | queue: Enum.reject(pipe.queue, &(&1.ref == ref))}

  @doc "Stops the upload processes."
  @spec kill(t) :: :ok
  def kill(pipe) do
    for %{pid: pid} <- pipe.queue, pid != nil do
      Process.unlink(pid)
      Process.exit(pid, :kill)
    end

    :ok
  end
end
