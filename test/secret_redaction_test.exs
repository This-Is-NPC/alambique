defmodule Alambique.SecretRedactionTest do
  @moduledoc """
  Store credentials never appear in crash reports, process state, errors or
  logs (design §3): the S3 store holds them in closures, which print as
  `#Function<...>` whatever formats them.
  """
  use ExUnit.Case, async: false

  import Alambique.CellHelpers
  import ExUnit.CaptureLog

  alias Alambique.Store.{Guard, S3}

  @secret "CANARY-SECRET-KEY"
  @key_id "CANARY-KEY-ID"

  # A store that keeps S3-style credentials in its config and serves from memory.
  defmodule CredStore do
    @moduledoc false
    @behaviour Alambique.Store

    alias AlambiqueTestkit.MemoryStore

    defdelegate conceal(config), to: Alambique.Store.S3
    def location(config), do: config.inner
    def put(c, k, b, cond, m), do: MemoryStore.put(c.inner, k, b, cond, m)
    def get(c, k), do: MemoryStore.get(c.inner, k)
    def head(c, k), do: MemoryStore.head(c.inner, k)
    def get_range(c, k, o, l), do: MemoryStore.get_range(c.inner, k, o, l)
    def list(c, p, s, l), do: MemoryStore.list(c.inner, p, s, l)
    def delete(c, k), do: MemoryStore.delete(c.inner, k)
  end

  setup ctx do
    base = setup_store()
    config = %{inner: base.store_config, access_key_id: @key_id, secret_access_key: @secret}
    Map.merge(ctx, %{base | store: {CredStore, config}})
  end

  defp everywhere(term) do
    inspect(term, limit: :infinity, printable_limit: :infinity) <>
      IO.iodata_to_binary(:io_lib.format("~p", [term]))
  end

  test "a crashed cell leaves no credential in the crash report or its state", ctx do
    inst = start_instance(ctx, migrations: [{1, "CREATE TABLE t (n INTEGER)"}])
    cell = unique_cell()
    assert {:ok, _} = Alambique.execute(inst, cell, "INSERT INTO t VALUES (1)")
    pid = Alambique.Cell.whereis(Alambique.Config.fetch!(inst), cell)

    refute everywhere(:sys.get_state(pid)) =~ @secret
    refute everywhere(:sys.get_status(pid)) =~ @secret
    refute everywhere(Alambique.Config.fetch!(inst)) =~ @secret

    ref = Process.monitor(pid)

    log =
      capture_log(fn ->
        GenServer.cast(pid, :boom)
        assert_receive {:DOWN, ^ref, _, _, _}, 5_000
      end)

    assert log =~ "terminating"
    refute log =~ @secret
    refute log =~ @key_id
  end

  test "a store config with a missing key raises without printing the credentials" do
    config = %{secret_access_key: @secret, access_key_id: @key_id, endpoint: "http://x"}

    result = Guard.get({S3, config}, "k")
    assert {:error, {:store_crashed, :error, %ArgumentError{message: message}}} = result
    assert message =~ "bucket"
    refute everywhere(result) =~ @secret

    assert_raise ArgumentError, fn -> S3.location(%{secret_access_key: @secret}) end
  end

  test "a config that is not a map raises without printing it" do
    assert {:error, {:store_crashed, :error, error}} = Guard.get({S3, [secret: @secret]}, "k")
    refute everywhere(error) =~ @secret
  end

  test "concealed credentials print as closures and conceal is idempotent" do
    config = S3.conceal(%{access_key_id: @key_id, secret_access_key: @secret, endpoint: "x"})
    refute everywhere(config) =~ @secret
    refute everywhere(config) =~ @key_id
    assert is_function(config.secret_access_key, 0)
    assert S3.conceal(config) == config
  end
end
