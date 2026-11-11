defmodule Alambique.Load.CapacityTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Alambique.Load.Capacity

  @mib 1_048_576

  describe "data tmpfs" do
    test "the bytes of an asleep cell are measured from the growth, or the default before any" do
      assert Capacity.cell_bytes(10.0, 10.0, 0, 160_000) == 160_000
      # 7070 cells took 1000 MiB over the base of 5 MiB.
      bytes = Capacity.cell_bytes(1_005.0, 5.0, 7_070, 160_000)
      assert_in_delta bytes, 1_000 * @mib / 7_070, 1.0e-6
    end

    test "a level whose estimate goes over the fraction of the tmpfs is refused" do
      # 40/s for 180 s is 7200 cells of 0.153 MiB: 1100 MiB against 0.9 * 1024.
      per_cell = 0.153 * @mib
      assert Capacity.data_exceeds?(0.0, 7_200, per_cell, 1_024, 0.9)
      refute Capacity.data_exceeds?(0.0, 5_000, per_cell, 1_024, 0.9)
      # What already sits on the tmpfs counts.
      assert Capacity.data_exceeds?(400.0, 5_000, per_cell, 1_024, 0.9)
    end

    test "the limit itself is allowed" do
      refute Capacity.data_exceeds?(900.0, 0, 1.0, 1_000, 0.9)
      assert Capacity.data_exceeds?(900.1, 0, 1.0, 1_000, 0.9)
    end

    property "more fresh cells or a smaller tmpfs never make a refused level fit" do
      check all(
              used <- float(min: 0.0, max: 2_000.0),
              fresh <- integer(0..20_000),
              extra <- integer(0..1_000),
              bytes <- float(min: 1.0, max: 500_000.0),
              data_mb <- integer(64..4_096)
            ) do
        if Capacity.data_exceeds?(used, fresh, bytes, data_mb, 0.9) do
          assert Capacity.data_exceeds?(used, fresh + extra, bytes, data_mb, 0.9)
          assert Capacity.data_exceeds?(used, fresh, bytes, max(data_mb - 1, 1), 0.9)
        end
      end
    end

    test "a failed preparation is data_full at 95% of the tmpfs, else prepare_failed" do
      assert Capacity.prepare_failure(972.8, 1_024, 0.95) == :data_full
      assert Capacity.prepare_failure(972.7, 1_024, 0.95) == :prepare_failed
    end
  end

  describe "store" do
    @limits %{store_mb: 4_096, store_full_fraction: 0.95, store_min_free_mb: 1_024}

    test "a tmpfs store is full at 95% of its size" do
      refute Capacity.store_full?(%{kind: :tmpfs, used_mb: 3_891.1}, @limits)
      assert Capacity.store_full?(%{kind: :tmpfs, used_mb: 3_891.2}, @limits)
    end

    test "a store on disk is full below 1 GiB free" do
      refute Capacity.store_full?(%{kind: :disk, free_mb: 1_024.0, used_mb: 0}, @limits)
      assert Capacity.store_full?(%{kind: :disk, free_mb: 1_023.9, used_mb: 0}, @limits)
    end

    test "free space comes from the df line, in MiB" do
      text = """
      Filesystem        1-blocks        Used   Available Capacity Mounted on
      /dev/nvme0n1p2 500000000000 200000000000 #{2 * @mib * 1_024} 41% /
      """

      assert Capacity.parse_df_free_mb(text) == 2.0 * 1_024
      assert Capacity.parse_df_free_mb("") == nil
      assert Capacity.parse_df_free_mb("df: no such file") == nil
    end
  end
end
