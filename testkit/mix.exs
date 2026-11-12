defmodule AlambiqueTestkit.MixProject do
  use Mix.Project

  def project do
    [
      app: :alambique_testkit,
      version: "0.4.0", # x-release-please-version
      elixir: "~> 1.17",
      start_permanent: false,
      description: "Test helpers for applications built on Alambique.",
      package: [licenses: ["MIT"]],
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger, :tools]]
  end

  defp deps do
    [{:alambique, path: ".."}]
  end
end
