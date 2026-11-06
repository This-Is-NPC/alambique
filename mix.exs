defmodule Alambique.MixProject do
  use Mix.Project

  def project do
    [
      app: :alambique,
      version: "0.1.0",
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: test_coverage(),
      description:
        "SQLite databases that wake on demand and replicate to S3-compatible storage, " <>
          "one lease-fenced owner per cell.",
      package: package()
    ]
  end

  # Cargo.toml and the sources of the NIF are shipped because Rustler
  # compiles the crate on the host; the fuzz crate and its corpus are not.
  defp package do
    [
      licenses: ["MIT"],
      files:
        ~w(lib docs native/alambique_nif/src native/alambique_nif/.cargo) ++
          ~w(native/alambique_nif/Cargo.toml native/alambique_nif/Cargo.lock) ++
          ~w(mix.exs README.md CHANGELOG.md LICENSE)
    ]
  end

  # `mix test --cover` fails below the threshold. The environment variable
  # lets scripts/test-coverage keep one documented value for both languages.
  # The ignored modules are test support and the testkit, which the test build
  # compiles into the application, and the NIF stubs of Replication.Native
  # (their bodies never run; scripts/test-coverage measures the Rust side).
  # Nothing else is excluded.
  defp test_coverage do
    threshold = String.to_integer(System.get_env("ALAMBIQUE_COVERAGE_MIN", "90"))

    [
      summary: [threshold: threshold],
      ignore_modules: [
        ~r/^Alambique\.(AuditStore|CellHelpers|ClusterFuns|CompactionHelpers|CountingStore)$/,
        ~r/^Alambique\.(LazyFailStore|LeaseContract|LeaseHelpers|LtxExample|LtxReader)(\.|$)/,
        ~r/^Alambique\.Replication\.Native$/,
        ~r/^Alambique\.(OptionFuns|RecordingStore|ReplicationHelpers|StoreContract)$/,
        ~r/^Alambique\.(BenchText|TestDirs)(\.|$)/,
        ~r/^AlambiqueTestkit\./
      ]
    ]
  end

  def application do
    [extra_applications: [:logger | test_applications(Mix.env())]]
  end

  # The testkit uses :cover, which ships in :tools.
  defp test_applications(:test), do: [:tools]
  defp test_applications(_), do: []

  # The testkit sources are compiled into the test build instead of being a
  # path dependency, because alambique_testkit itself depends on alambique
  # and Mix rejects that cycle.
  defp elixirc_paths(:test), do: ["lib", "test/support", "testkit/lib"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:req, "~> 0.5"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.3"},
      {:rustler, "~> 0.38", runtime: false},
      {:broadway, "~> 1.1", optional: true},
      {:stream_data, "~> 1.1", only: :test},
      {:recon, "~> 2.5", only: :test},
      {:telemetry_poller, "~> 1.1", optional: true}
    ]
  end
end
