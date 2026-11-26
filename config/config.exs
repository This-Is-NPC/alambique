import Config

# The test suite runs with the page arena on; ALAMBIQUE_PAGE_ARENA=0 runs it
# off (`mise run test:page-arena-off`). The library itself ships it off.
if config_env() == :test do
  config :alambique, :page_arena,
    enabled: System.get_env("ALAMBIQUE_PAGE_ARENA") != "0",
    reserved_mib: 1024,
    keep_kib: 256,
    trim_after_kib: 64
end
