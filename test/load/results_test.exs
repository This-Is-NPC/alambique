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

  test "step files sort in the order the steps ran" do
    names = [
      Results.step_name(10, :ramp, 5),
      Results.step_name(2, :refine, 900),
      Results.step_name(1, :ramp, 50)
    ]

    assert Enum.sort(names) == [
             "step-001-ramp-50.json",
             "step-002-refine-900.json",
             "step-010-ramp-5.json"
           ]
  end
end
