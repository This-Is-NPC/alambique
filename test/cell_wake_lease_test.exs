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
end
