defmodule Alambique.StubAdapter do
  @moduledoc """
  A `Req` adapter that answers from a function instead of the network. The
  adapter runs in the process that makes the request, so the function is kept
  in its process dictionary by `install/1`.
  """

  @doc "Answers the requests of this process from `responder`; returns the `Req` options."
  def install(responder) do
    Process.put(__MODULE__, responder)
    [adapter: __MODULE__]
  end

  @doc false
  def run(request) do
    case Process.get(__MODULE__).(request) do
      status when is_integer(status) -> {request, Req.Response.new(status: status, body: "")}
      {status, body} -> {request, Req.Response.new(status: status, body: body)}
    end
  end
end
