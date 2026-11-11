# Refresh of one large cached replica after a small write (design §14).
#
#   mise run bench:query-refresh            # 128 MiB
#   mise run bench:query-refresh -- 512     # another size, in MiB
#
# Builds a cell of the given size, queries it (a full restore into the cache),
# writes one small row, and queries again (a refresh). It prints the time of
# the refresh and of each pass over the file: copy, checksum read, flush. Set
# ALAMBIQUE_BENCH_DIR to a directory on the filesystem to measure (a tmpfs has
# no reflinks; the default is the system temporary directory).

defmodule Bench do
  alias Alambique.Query
  alias Alambique.Query.Cache
  alias Alambique.Replication.Native
  alias AlambiqueTestkit.MemoryStore

  @migrations [{1, "CREATE TABLE blobs (id INTEGER PRIMARY KEY, body BLOB)"}]
  @sql "SELECT count(*) FROM blobs"

  def run(mib) do
    base = System.get_env("ALAMBIQUE_BENCH_DIR") || Alambique.TestDirs.new("bench")
    dir = Path.join(base, "refresh-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    {:ok, store} = MemoryStore.start_link()
    store = {MemoryStore, MemoryStore.config(store)}

    {:ok, sup} =
      Supervisor.start_link(
        [
          {Alambique,
           name: :bench_writer,
           store: store,
           data_dir: Path.join(dir, "writer"),
           migrations: @migrations,
           lease_ttl_ms: 60_000},
          Supervisor.child_spec(
            {Alambique,
             name: :bench_reader,
             store: store,
             data_dir: Path.join(dir, "reader"),
             migrations: @migrations},
            id: :reader
          )
        ],
        strategy: :one_for_one
      )

    try do
      for _ <- 1..mib do
        {:ok, _} =
          Alambique.execute(:bench_writer, "big", "INSERT INTO blobs (body) VALUES (randomblob(1048576))")
      end

      :ok = Alambique.hibernate(:bench_writer, "big")

      {cold, _} = :timer.tc(&query/0)
      {:ok, _} = Alambique.execute(:bench_writer, "big", "INSERT INTO blobs (body) VALUES (x'00')")
      :ok = Alambique.hibernate(:bench_writer, "big")
      {refresh, _} = :timer.tc(&query/0)

      [path] = Path.wildcard(Path.join([dir, "reader", "replicas", "*.r", "*.db"]))
      size = File.stat!(path).size
      {:ok, page_size} = Cache.page_size(path)
      probe = path <> ".probe"

      {copy, :ok} = :timer.tc(fn -> File.cp(path, probe) end)
      {read, {:ok, _, _, _}} = :timer.tc(fn -> Native.file_state(probe, page_size) end)
      {flush, :ok} = :timer.tc(fn -> Cache.sync(probe) end)
      File.rm(probe)

      IO.puts("replica:   #{div(size, 1_048_576)} MiB, refreshed by one small segment")
      IO.puts("cold restore            #{ms(cold)}")
      IO.puts("refresh (end to end)    #{ms(refresh)}")
      IO.puts("a full copy, read and flush of the final file, measured apart:")
      IO.puts("  copy                  #{ms(copy)}")
      IO.puts("  checksum read         #{ms(read)}")
      IO.puts("  flush                 #{ms(flush)}")
    after
      Supervisor.stop(sup)
      GenServer.stop(elem(store, 1).server)
      File.rm_rf(dir)
    end
  end

  defp query do
    result = Query.all(:bench_reader, ["big"], @sql, [])
    if result.errors != [], do: raise("errors: #{inspect(result.errors)}")
    result
  end

  defp ms(us), do: :io_lib.format("~8.1f ms", [us / 1000])
end

mib =
  case System.argv() do
    [n | _] -> String.to_integer(n)
    [] -> 128
  end

Alambique.TestDirs.sweep()

try do
  Bench.run(mib)
after
  Alambique.TestDirs.clean()
end
