import Config

# The test suite runs with the page arena on; ALAMBIQUE_PAGE_ARENA=0 runs it
# off (`mise run test:page-arena-off`). ALAMBIQUE_PAGE_ARENA_OPTS is an Elixir
# keyword list of other :page_arena settings (the page arena benchmark sets
# the extent size and the regions this way). The library ships it off.
if config_env() == :test do
  extra =
    case System.get_env("ALAMBIQUE_PAGE_ARENA_OPTS") do
      nil -> []
      code -> code |> Code.eval_string() |> elem(0)
    end

  config :alambique,
         :page_arena,
         Keyword.merge(
           [
             enabled: System.get_env("ALAMBIQUE_PAGE_ARENA") != "0",
             reserved_mib: 1024,
             keep_kib: 256,
             trim_after_kib: 64,
             trim_idle_ms: 600_000
           ],
           extra
         )
end
