defmodule Alambique.Load.ResultsTest do
  use ExUnit.Case, async: true

  alias Alambique.Load.Results

  @moduletag :tmp_dir

  test "writes the JSON and leaves no temporary file", %{tmp_dir: dir} do
    path = Path.join(dir, "nested/step.json")
    :ok = Results.write!(path, %{level: 50, verdict: "good"})

    assert Jason.decode!(File.read!(path)) == %{"level" => 50, "verdict" => "good"}
    assert File.ls!(Path.dirname(path)) == ["step.json"]
  end

  test "a failed write keeps the previous file whole and leaves nothing behind", %{tmp_dir: dir} do
    path = Path.join(dir, "step.json")
    :ok = Results.write!(path, %{level: 1})

    # A tuple has no JSON form: the encoding fails before the file is touched.
    assert_raise Protocol.UndefinedError, fn -> Results.write!(path, %{level: {1, 2}}) end

    assert Jason.decode!(File.read!(path)) == %{"level" => 1}
    assert File.ls!(dir) == ["step.json"]
  end

  test "a failed rename removes the temporary file and keeps the old one", %{tmp_dir: dir} do
    # The destination is a directory, so the rename fails after the write.
    path = Path.join(dir, "step.json")
    File.mkdir_p!(Path.join(path, "keep"))

    assert_raise File.RenameError, fn -> Results.write!(path, %{level: 2}) end
    assert File.ls!(dir) == ["step.json"]
  end

  test "a step is named by its level, the confirmation by a second run of it" do
    assert Results.step_name(50, :ramp) == "step-50.json"
    assert Results.step_name(75, :refine) == "step-75.json"
    assert Results.step_name(50, :confirm) == "step-50-2.json"
  end

  describe "read_done/2" do
    defp stored(dir, name, fields) do
      path = Path.join(dir, name)

      Results.write!(
        path,
        Map.merge(
          %{level: 50, phase: "ramp", verdict: "good", reason: "", config_hash: "h1"},
          fields
        )
      )

      path
    end

    test "a good or bad step of the same settings is done, with its verdict as a term", %{
      tmp_dir: dir
    } do
      assert {:ok, %{level: 50, phase: :ramp, verdict_term: :good}} =
               Results.read_done(stored(dir, "good.json", %{}), "h1")

      bad = stored(dir, "bad.json", %{verdict: "bad", reason: "{:latency, :p99, 250.0}"})

      assert {:ok, %{verdict_term: {:bad, {:latency, :p99, 250.0}}}} =
               Results.read_done(bad, "h1")

      conf = stored(dir, "conf.json", %{phase: "confirm", verdict: "bad", reason: ":errors"})

      assert {:ok, %{phase: :confirm, verdict_term: {:bad, :errors}}} =
               Results.read_done(conf, "h1")
    end

    test "other settings, an invalid step and a missing or broken file are not done", %{
      tmp_dir: dir
    } do
      assert Results.read_done(stored(dir, "other.json", %{}), "h2") == :none

      invalid = stored(dir, "invalid.json", %{verdict: "invalid", reason: ":timeout"})
      assert Results.read_done(invalid, "h1") == :none

      assert Results.read_done(Path.join(dir, "missing.json"), "h1") == :none
      File.write!(Path.join(dir, "broken.json"), "{")
      assert Results.read_done(Path.join(dir, "broken.json"), "h1") == :none
    end

    test "a reason that is not a literal term stays text", %{tmp_dir: dir} do
      path = stored(dir, "odd.json", %{verdict: "bad", reason: "System.halt()"})
      assert {:ok, %{verdict_term: {:bad, "System.halt()"}}} = Results.read_done(path, "h1")
    end
  end
end
