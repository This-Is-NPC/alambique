defmodule Alambique.CellWakeLeaseTest do
  @moduledoc "A cell that woke slowly does not serve on a lease it can no longer use (design §3, §7)."
  use ExUnit.Case, async: false

  import Alambique.CellHelpers

  @schema [{1, "CREATE TABLE t (n INTEGER PRIMARY KEY)"}]

  setup do
    ctx = setup_store()
    %{inst: start_instance(ctx, migrations: @schema, lease_ttl_ms: 3_000, skew_margin_ms: 500)}
  end

  test "a wake that outlasts the usable lease does not hand its caller :lease_expired", %{
    inst: inst
  } do
    # The migration step comes after the last lease check of the wake; the
    # pause leaves the cell with no usable lease behind it.
    Alambique.Fault.install(fn
      :schema_before_put, _ -> Process.sleep(2_700)
      _, _ -> :ok
    end)

    on_exit(&Alambique.Fault.uninstall/0)

    assert {:ok, _} = Alambique.touch(inst, "slow")
  end

  test "the first renewal comes a third of the TTL after the lease was put, however slow the wake",
       %{inst: inst} do
    test = self()

    # Short of the half window that makes the wake renew: the cell serves on
    # the lease it acquired, 0.9 s old.
    Alambique.Fault.install(fn
      :schema_before_put, _ ->
        Process.sleep(900)

      :lease_after_put, %{op: op, cell: "slow"} ->
        send(test, {op, System.monotonic_time(:millisecond)})

      _, _ ->
        :ok
    end)

    on_exit(&Alambique.Fault.uninstall/0)

    assert {:ok, _} = Alambique.touch(inst, "slow")
    assert_receive {:acquire, acquired}
    assert_receive {:renew, renewed}, 5_000

    # Counted from the end of the wake, the renewal would come at least
    # 0.9 + 0.9 s after the put, with less than one interval of the lease left.
    assert renewed - acquired < 1_800
  end
end
