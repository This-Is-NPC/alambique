defmodule Alambique.Load.LoadConfigTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  # scripts/load_config.py: the round file of the load test, read, merged over
  # the defaults, overridden by ALAMBIQUE_LOAD_*, validated and expanded into
  # cases. Nothing here starts a container.

  @script "scripts/load_config.py"

  # Without the overrides of the machine that runs the tests, with `extra` instead.
  defp clean_env(extra \\ []) do
    set = Enum.map(extra, &elem(&1, 0))

    unset =
      for {name, _} <- System.get_env(),
          String.starts_with?(name, "ALAMBIQUE_LOAD_"),
          name not in set,
          do: {name, nil}

    unset ++ extra
  end

  defp run(args, env \\ []) do
    System.cmd("python3", ["-I", @script | args], env: clean_env(env), stderr_to_stdout: true)
  end

  defp check(path, env \\ []) do
    {out, 0} = System.cmd("python3", ["-I", @script, "--check", path], env: clean_env(env))
    :json.decode(out)
  end

  # Runs python code with the module imported; the code prints one JSON value.
  defp py(code, env \\ []) do
    prelude = "import json, sys; sys.path.insert(0, 'scripts'); import load_config as lc\n"

    {out, 0} =
      System.cmd("python3", ["-I", "-B", "-c", prelude <> code],
        env: clean_env(env),
        stderr_to_stdout: true
      )

    out |> String.split("\n", trim: true) |> List.last() |> :json.decode()
  end

  defp write(dir, name \\ "round.toml", text) do
    path = Path.join(dir, name)
    File.write!(path, text)
    path
  end

  defp problems(dir, text, env \\ []) do
    {out, status} = run(["--check", write(dir, text)], env)
    assert status == 1, out

    out
    |> String.split("\n", trim: true)
    |> Enum.filter(&String.starts_with?(&1, "  - "))
    |> Enum.map(&String.replace_prefix(&1, "  - ", ""))
  end

  describe "defaults" do
    test "example.toml is the built-in defaults, key for key" do
      {defaults, 0} = System.cmd("python3", ["-I", @script, "--defaults"], env: clean_env())
      assert check("load/rounds/example.toml") == :json.decode(defaults)
    end

    test "the default ramps are today's levels" do
      levels =
        py("""
        c = lc.load_config(None, {})
        print(json.dumps({f'{m}/{v}': lc.levels(lc.case_config(c, 'A', m, v)['steps']) for s, m, v in lc.cases(c)}))
        """)

      assert levels["commits/small"] == [50, 100, 200, 400, 800, 1600, 3200]
      assert levels["commits/large"] == [5, 10, 20, 40, 80, 160, 320]
      assert levels["cells/default"] == [500, 1000, 2000, 4000, 8000, 16000, 32000]
      assert levels["cells/cache256"] == levels["cells/default"]
      assert levels["wakes/None"] == [5, 10, 20, 40, 80, 160, 320]
      assert levels["query/None"] == [1, 2, 4, 8, 16, 32, 64]
    end

    test "the default round is scenario A, every measure and variant, in today's order" do
      assert py(
               "c = lc.load_config(None, {}); print(json.dumps([lc.tag(c_) for c_ in lc.cases(c)]))"
             ) ==
               ~w(A/commits/small A/commits/large A/cells/default A/cells/cache256 A/wakes A/query)
    end

    test "the criteria of a measure are today's" do
      c =
        py("""
        c = lc.load_config(None, {})
        j = lambda m, v=None: lc.case_config(c, 'A', m, v)['judging']
        print(json.dumps({'commits': j('commits', 'small'), 'cells': j('cells', 'default'), 'wakes': j('wakes'), 'query': j('query')}))
        """)

      assert c["commits"]["latency"] == %{
               "enabled" => true,
               "percentile" => "p99",
               "max_ms" => 250.0
             }

      assert c["wakes"]["latency"] == %{
               "enabled" => true,
               "percentile" => "p95",
               "max_ms" => 500.0
             }

      assert c["query"]["latency"]["max_ms"] == 1000.0
      assert c["cells"]["latency"]["enabled"] == false
      assert c["cells"]["memory"]["max_pct"] == 90.0
      assert c["commits"]["memory"]["max_pct"] == 100.0
      assert c["commits"]["busy"]["max_pct"] == 1.0 and c["commits"]["errors"]["max_pct"] == 0.0
    end
  end

  describe "reading a file" do
    test "a file keeps its keys and the rest is default", %{tmp_dir: dir} do
      path =
        write(dir, """
        [judging.latency]
        max_ms = 300
        [measures.wakes.steps]
        duration_s = 30
        [scenarios.B]
        memory = "3g"
        """)

      c = check(path)

      assert c["judging"]["latency"] == %{
               "enabled" => true,
               "percentile" => "p99",
               "max_ms" => 300.0
             }

      assert c["measures"]["wakes"]["steps"]["duration_s"] == 30.0
      assert c["measures"]["wakes"]["steps"]["factor"] == 2.0
      assert c["scenarios"]["B"]["memory"] == "3g" and c["scenarios"]["B"]["cpus"] == 2.0
      # a measure keeps its own default under the global key a file sets
      assert c["measures"]["wakes"]["judging"]["latency"]["max_ms"] == 500.0
    end

    test "a measure and a variant override the global store, only for their case", %{tmp_dir: dir} do
      path =
        write(dir, """
        [round]
        scenarios = ["A", "B"]
        measures = ["commits", "wakes"]
        [store]
        latency_ms = 10
        [measures.wakes.store]
        latency_ms = 50
        [measures.commits.variant.large.store]
        backing = "disk"
        """)

      stores =
        py("""
        c = lc.load_config(#{inspect(path)}, {})
        print(json.dumps({lc.tag(k): lc.case_config(c, *k)['store'] for k in lc.cases(c)}))
        """)

      assert stores["A/commits/small"]["backing"] == "tmpfs"
      assert stores["B/commits/large"]["backing"] == "disk"
      assert stores["A/commits/small"]["latency_ms"] == 10.0
      assert stores["A/wakes"]["latency_ms"] == 50.0
    end

    test "round.cases replaces scenarios and measures, with the variants of a bare measure", %{
      tmp_dir: dir
    } do
      path =
        write(dir, """
        [round]
        cases = ["B/cells", "A/commits/large", "A/wakes"]
        scenarios = ["A"]
        """)

      assert py(
               "c = lc.load_config(#{inspect(path)}, {}); print(json.dumps([lc.tag(c_) for c_ in lc.cases(c)]))"
             ) ==
               ~w(B/cells/default B/cells/cache256 A/commits/large A/wakes)
    end

    test "the variants list chooses which run, a cells variant can be added", %{tmp_dir: dir} do
      path =
        write(dir, """
        [round]
        measures = ["cells"]
        [measures.cells]
        variants = ["default", "cache1024"]
        [measures.cells.variant.cache1024.service]
        cache_kib = 1024
        """)

      c =
        py("""
        c = lc.load_config(#{inspect(path)}, {})
        print(json.dumps([[lc.tag(k), lc.case_config(c, *k)['service']['cache_kib']] for k in lc.cases(c)]))
        """)

      assert c == [["A/cells/default", 0], ["A/cells/cache1024", 1024]]
    end

    test "a wakes variant can be added", %{tmp_dir: dir} do
      path =
        write(dir, """
        [round]
        measures = ["wakes"]
        [measures.wakes]
        variants = ["plain", "verified"]
        [measures.wakes.variant.plain]
        [measures.wakes.variant.verified.service]
        opts = "[lazy_restore: [verify_reads: true]]"
        """)

      c =
        py("""
        c = lc.load_config(#{inspect(path)}, {})
        print(json.dumps([[lc.tag(k), lc.case_config(c, *k)['service']['opts']] for k in lc.cases(c)]))
        """)

      assert c == [
               ["A/wakes/plain", ""],
               ["A/wakes/verified", "[lazy_restore: [verify_reads: true]]"]
             ]
    end

    test "a scenario sets service keys under the measure and variant", %{tmp_dir: dir} do
      path =
        write(dir, """
        [round]
        scenarios = ["A", "B"]
        measures = ["wakes"]
        [service]
        erl_aflags = "+SDio 10"
        [scenarios.A.service]
        erl_aflags = "+SDio 1"
        [scenarios.B.service]
        erl_aflags = "+SDio 2"
        opts = "[]"
        [measures.wakes]
        variants = ["plain", "verified"]
        [measures.wakes.variant.plain]
        [measures.wakes.variant.verified.service]
        opts = "[lazy_restore: [verify_reads: true]]"
        """)

      c =
        py("""
        c = lc.load_config(#{inspect(path)}, {})
        print(json.dumps([[lc.tag(k), lc.case_config(c, *k)['service']['erl_aflags'], lc.case_config(c, *k)['service']['opts']] for k in lc.cases(c)]))
        """)

      assert c == [
               ["A/wakes/plain", "+SDio 1", ""],
               ["A/wakes/verified", "+SDio 1", "[lazy_restore: [verify_reads: true]]"],
               ["B/wakes/plain", "+SDio 2", "[]"],
               ["B/wakes/verified", "+SDio 2", "[lazy_restore: [verify_reads: true]]"]
             ]
    end

    test "o022vm.toml is valid and its cases are the pending ones, in order", %{tmp_dir: _dir} do
      c =
        py("""
        c = lc.load_config('load/rounds/o022vm.toml', {})
        out = []
        for k in lc.cases(c):
            cc = lc.case_config(c, *k)
            out.append([lc.tag(k), cc['store']['backing'], cc['store']['tmpfs_size']])
        print(json.dumps({'cases': out, 'name': c['output']['name'], 'ssh': c['host']['ssh'], 'repo': c['host']['repo']}))
        """)

      assert c["name"] == "o022vm" and c["ssh"] == "192.168.0.101" and c["repo"] == "~/alambique"

      assert Enum.map(c["cases"], &hd/1) ==
               ~w(A/commits/small A/commits/large B/commits/large A/cells/default A/cells/cache256
                  B/cells/default B/cells/cache256 A/wakes B/wakes)

      disk = for [tag, "disk", size] <- c["cases"], do: {tag, size}
      assert disk == [{"A/commits/large", "12288m"}, {"B/commits/large", "12288m"}]

      assert Enum.all?(c["cases"], fn [tag, backing, _] ->
               backing == "disk" or not String.contains?(tag, "large")
             end)
    end
  end

  describe "the environment overrides" do
    test "a variable wins over the file and over a measure's default", %{tmp_dir: dir} do
      path = write(dir, "[judging.busy]\nmax_pct = 2\n[measures.cells.steps]\nduration_s = 90\n")

      c =
        check(path, [
          {"ALAMBIQUE_LOAD_MAX_BUSY_PCT", "3"},
          {"ALAMBIQUE_LOAD_STEP_S", "15"},
          {"ALAMBIQUE_LOAD_IDLE_TIMEOUT_MS", "1234"},
          {"ALAMBIQUE_LOAD_STORE_LATENCY_MS", "0"},
          {"ALAMBIQUE_LOAD_DATA_MB", "768"},
          {"ALAMBIQUE_LOAD_WAKES_P95_MS", "700"}
        ])

      assert c["judging"]["busy"]["max_pct"] == 3.0
      assert c["measures"]["cells"]["steps"]["duration_s"] == 15.0
      assert c["measures"]["commits"]["variant"]["large"]["steps"]["duration_s"] == 15.0
      # the measure's own default idle timeout gives way to the variable
      assert c["measures"]["cells"]["service"]["idle_timeout_ms"] == 1234
      assert c["measures"]["wakes"]["service"]["idle_timeout_ms"] == 1234
      assert c["store"]["latency_ms"] == 0.0

      assert c["scenarios"]["A"]["data_tmpfs"] == "768m" and
               c["scenarios"]["B"]["data_tmpfs"] == "768m"

      assert c["measures"]["wakes"]["judging"]["latency"]["max_ms"] == 700.0
    end

    test "LEVELS makes every ramp a ladder", %{tmp_dir: _dir} do
      levels =
        py(
          "c = lc.load_config(None); print(json.dumps([lc.levels(lc.case_config(c, *k)['steps']) for k in lc.cases(c)]))",
          [{"ALAMBIQUE_LOAD_LEVELS", "10, 20,40"}]
        )

      assert Enum.uniq(levels) == [[10, 20, 40]]
    end

    test "a variable the file cannot hold is refused by name", %{tmp_dir: dir} do
      assert problems(dir, "", [
               {"ALAMBIQUE_LOAD_STEP_S", "abc"},
               {"ALAMBIQUE_LOAD_STORE_ON", "ssd"},
               {"ALAMBIQUE_LOAD_MAX_BUSY_PCT", "500"},
               {"ALAMBIQUE_LOAD_LEVELS", "5,3"}
             ]) == [
               "ALAMBIQUE_LOAD_STEP_S: 'abc' is not a number",
               "ALAMBIQUE_LOAD_MAX_BUSY_PCT: 500 is out of range, expected >= 0 and <= 100",
               "ALAMBIQUE_LOAD_STORE_ON: 'ssd' is not one of tmpfs, disk",
               "ALAMBIQUE_LOAD_LEVELS: the values must grow, each above the one before"
             ]
    end
  end

  describe "validation" do
    test "an unknown key names the closest one", %{tmp_dir: dir} do
      assert problems(dir, "[measures.cells.steps]\nfactr = 2\n[judgng]\n") == [
               "measures.cells.steps.factr: unknown key (did you mean 'factor'?)",
               "judgng: unknown key (did you mean 'judging'?)"
             ]
    end

    test "a wrong type or a value out of range names the key", %{tmp_dir: dir} do
      assert problems(dir, """
             [judging.latency]
             max_ms = "fast"
             [judging.delivery]
             min_ratio = 1.5
             enabled = 1
             [judging.errors]
             max_pct = -1
             [measures.cells.steps]
             refine = 1.5
             """) == [
               "judging.latency.max_ms: expected a number, got a string",
               "judging.delivery.min_ratio: 1.5 is out of range, expected > 0 and <= 1",
               "judging.delivery.enabled: expected true or false, got a whole number",
               "judging.errors.max_pct: -1 is out of range, expected >= 0 and <= 100",
               "measures.cells.steps.refine: expected a whole number, got a number"
             ]
    end

    test "a size, a choice and a name are checked", %{tmp_dir: dir} do
      assert problems(dir, """
             [scenarios.A]
             memory = "lots"
             [store]
             backing = "ssd"
             [output]
             name = "a b"
             [scenarios.C]
             cpus = 1
             """) == [
               "scenarios.A.memory: 'lots' is not a size, use a whole number of at least 1 MiB with k, m or g (512m, 2g)",
               "scenarios.C: unknown scenario 'C', expected one of A, B",
               "store.backing: 'ssd' is not one of tmpfs, disk",
               "output.name: use letters, digits, dot, dash and underscore only"
             ]
    end

    test "an unknown scenario, measure or variant in the cases", %{tmp_dir: dir} do
      assert problems(dir, """
             [round]
             cases = ["C/cells", "A/disk", "A/commits/huge", "A/query/small", "A/cells/default", "A/cells/default", "A"]
             measures = ["disk"]
             """) == [
               "round.measures[0]: 'disk' is not one of commits, cells, wakes, query",
               "round.cases[0]: unknown scenario 'C', expected one of A, B",
               "round.cases[1]: unknown measure 'disk', expected one of commits, cells, wakes, query",
               "round.cases[2]: unknown variant 'huge' of commits, expected one of small, large",
               "round.cases[3]: unknown variant 'small' of query, expected one of none (it has no variants)",
               "round.cases[5]: 'A/cells/default' is listed twice",
               "round.cases[6]: 'A' is not scenario/measure or scenario/measure/variant"
             ]
    end

    test "a variants entry the measure does not know", %{tmp_dir: dir} do
      assert problems(
               dir,
               "[measures.commits]\nvariants = [\"small\", \"huge\"]\n[measures.query]\nvariants = []\n"
             ) == [
               "measures.commits.variants: unknown variant 'huge', expected one of small, large"
             ]

      assert problems(dir, "[measures.commits]\nvariants = []\n") == [
               "measures.commits.variants: must not be empty"
             ]
    end

    test "an incoherent step plan", %{tmp_dir: dir} do
      assert problems(dir, """
             [measures.commits.steps]
             kind = "ladder"
             [measures.cells.steps]
             start = 90000
             [measures.wakes.steps]
             values = [10, 20]
             [measures.query.steps]
             kind = "ladder"
             values = [4, 2]
             [measures.commits.variant.large.steps]
             start = 9
             """) == [
               "measures.query.steps.values: the values must grow, each above the one before",
               "measures.commits.steps.values: a ladder needs values",
               "measures.cells.steps: start 90000 is above max 32000",
               "measures.wakes.steps.values: only a ladder uses values; set kind = \"ladder\" or remove them"
             ]

      # a variant's own plan is checked against the measure's
      assert problems(dir, "[measures.commits.variant.large.steps]\nstart = 900\n") == [
               "measures.commits.variant.large.steps: start 900 is above max 320"
             ]
    end

    test "host.ssh needs host.repo and an output name, and a query cannot read more cells than it has",
         %{tmp_dir: dir} do
      assert problems(
               dir,
               "[host]\nssh = \"x\"\n[measures.query.workload]\ncells_per_query = 500\n"
             ) == [
               "measures.query.workload.cells_per_query: more than workload.population",
               "host.repo: required when host.ssh is set",
               "output.name: required when host.ssh is set, so that --fetch finds the results"
             ]
    end

    test "the host's values are checked for what a remote command cannot take", %{tmp_dir: dir} do
      assert problems(
               dir,
               "[output]\nname = \"x\"\n[host]\nssh = \"h; rm\"\nrepo = \"~/a b\"\nresults = \"r$x\"\n"
             ) == [
               "host.ssh: 'h; rm' has characters a remote command cannot take safely",
               "host.repo: '~/a b' has characters a remote command cannot take safely",
               "host.results: 'r$x' has characters a remote command cannot take safely"
             ]
    end

    test "every problem is reported, from several tables", %{tmp_dir: dir} do
      list =
        problems(dir, """
        [round]
        scenarios = ["Z"]
        cases = ["A/commits/huge"]
        [judging.busy]
        max_pct = "lots"
        [store]
        nope = 1
        [measures.cells.steps]
        start = 90000
        [host]
        ssh = "x"
        """)

      assert length(list) == 7
      assert "round.scenarios[0]: 'Z' is not one of A, B" in list

      assert "store.nope: unknown key, expected one of backing, tmpfs_size, dir, latency_ms, image" in list
    end

    test "a file that is not TOML or not there", %{tmp_dir: dir} do
      assert [<<"not valid TOML", _::binary>>] = problems(dir, "[round\n")

      {out, 1} = run(["--check", Path.join(dir, "none.toml")])
      assert out =~ "cannot read the file: No such file or directory"
    end
  end

  describe "the plan" do
    test "levels grow by factor each above the last, or are the ladder" do
      levels =
        py("""
        steps = lambda **k: {'kind': 'bisect', 'values': [], 'start': 5.0, 'factor': 2.0, 'max': 320.0, **k}
        print(json.dumps([lc.levels(steps()), lc.levels(steps(start=1.0, factor=1.25, max=8.0)),
                          lc.levels(steps(kind='ladder', values=[7, 9])), lc.levels(steps(start=10.0, max=10.0))]))
        """)

      assert levels == [[5, 10, 20, 40, 80, 160, 320], [1, 2, 3, 4, 5, 6, 8], [7, 9], [10]]
    end

    test "every case has a time bound: computed from its plan, or the one the file sets", %{
      tmp_dir: dir
    } do
      path = write(dir, "[round]\ncases = [\"A/commits/small\", \"A/cells/default\"]\n")

      bounds =
        py("""
        c = lc.load_config(#{inspect(path)}, {})
        print(json.dumps([lc.case_bound(c, lc.case_config(c, *k)) for k in lc.cases(c)]))
        """)

      # commits: 7 levels + 2 refinements + 1 confirmation, cells: 7 levels
      assert bounds == [10 * 1800 + 600 + 1800, 7 * 1800 + 600 + 1800]

      explicit = write(dir, "e.toml", "[round]\ncase_timeout_s = 90\nstep_timeout_s = 10\n")

      assert py(
               "c = lc.load_config(#{inspect(explicit)}, {}); print(json.dumps(lc.case_bound(c, lc.case_config(c, *lc.cases(c)[0]))))"
             ) ==
               90.0

      shorter = write(dir, "s.toml", "[round]\nstep_timeout_s = 100\n")

      assert py(
               "c = lc.load_config(#{inspect(shorter)}, {}); print(json.dumps(lc.case_bound(c, lc.case_config(c, *lc.cases(c)[0]))))"
             ) ==
               10 * 100 + 600 + 1800
    end

    test "--dry-run shows the case bound and --prepare writes it", %{tmp_dir: dir} do
      path = write(dir, "[round]\ncases = [\"A/wakes\"]\n[output]\ndir = \"#{dir}/out\"\n")
      {out, 0} = run(["--dry-run", path])
      assert out =~ "timeouts: 1800 s a step, 20400 s a case (computed), round none"
      assert out =~ "case limit 20400 s; steps bisect"

      explicit =
        write(
          dir,
          "e.toml",
          "[round]\ncases = [\"A/wakes\"]\ncase_timeout_s = 900\n[output]\ndir = \"#{dir}/out\"\n"
        )

      {out, 0} = run(["--dry-run", explicit])
      assert out =~ "timeouts: 1800 s a step, 900 s a case, round none"

      {_, 0} = run(["--prepare", path])
      case = dir |> Path.join("out/A/wakes/case.json") |> File.read!() |> :json.decode()
      assert case["timeouts"]["case_s"] == 20400.0
      {shell, 0} = run(["--shell", Path.join(dir, "out/A/wakes/case.json")])
      assert shell =~ "case_s=20400\n"
    end

    test "--dry-run prints the cases, their levels and the criteria", %{tmp_dir: dir} do
      path =
        write(
          dir,
          "[round]\ncases = [\"A/commits/large\", \"B/cells/cache256\"]\n[output]\nname = \"plan\"\n"
        )

      {out, 0} = run(["--dry-run", path])

      assert out =~ "round plan: 2 cases, results in results/load/plan"

      assert out =~
               "1. A/commits/large: 1 CPU, 512 MB, data tmpfs 1024 MB; store on tmpfs 4g, +25 ms"

      assert out =~
               "steps bisect [5, 10, 20, 40, 80, 160, 320] then 2 refinements, 1 confirmation, 180 s each, at most 10"

      assert out =~ "2. B/cells/cache256: 2 CPU, 2048 MB"
      assert out =~ "off: latency"
    end
  end

  describe "the hash of a case" do
    test "changing a measure's plan changes only its cases", %{tmp_dir: dir} do
      same =
        write(dir, "same.toml", "[output]\nname = \"a\"\n[host]\nssh = \"h\"\nrepo = \"r\"\n")

      changed = write(dir, "changed.toml", "[measures.wakes.steps]\nrefine = 1\n")

      hashes = fn path ->
        py("""
        c = lc.load_config(#{inspect(path)}, {})
        print(json.dumps({lc.tag(k): lc.config_hash(lc.case_config(c, *k)) for k in lc.cases(c)}))
        """)
      end

      base = hashes.("load/rounds/example.toml")
      assert hashes.(same) == base, "output.* and host.* are not hashed"

      other = hashes.(changed)
      assert other["A/wakes"] != base["A/wakes"]

      assert for({k, v} <- other, k != "A/wakes", do: {k, v}) ==
               for({k, v} <- base, k != "A/wakes", do: {k, v})

      assert String.length(base["A/wakes"]) == 64
    end

    test "a criterion changes the hash of every case", %{tmp_dir: dir} do
      path = write(dir, "[judging.busy]\nmax_pct = 2\n")

      both =
        py("""
        a = lc.load_config(None, {}); b = lc.load_config(#{inspect(path)}, {})
        print(json.dumps([[lc.config_hash(lc.case_config(a, *k)) != lc.config_hash(lc.case_config(b, *k)) for k in lc.cases(a)]]))
        """)

      assert both == [[true, true, true, true, true, true]]
    end
  end

  describe "--prepare and skip-done" do
    defp round_file(dir) do
      write(dir, """
      [round]
      cases = ["A/commits/small", "A/wakes"]
      [output]
      dir = "#{dir}/out"
      """)
    end

    defp prepare(dir, extra \\ [], env \\ []),
      do: run(["--prepare", round_file(dir) | extra], env)

    defp case_hash(dir, tag) do
      round_file(dir)

      py("""
      c = lc.load_config(#{inspect(Path.join(dir, "round.toml"))}, {})
      print(json.dumps(lc.config_hash(lc.case_config(c, *[k for k in lc.cases(c) if lc.tag(k) == #{inspect(tag)}][0]))))
      """)
    end

    defp step(dir, tag, name, hash, verdict) do
      case_dir = Path.join([dir, "out" | String.split(tag, "/")])
      File.mkdir_p!(case_dir)

      File.write!(
        Path.join(case_dir, name),
        :json.encode(%{"verdict" => verdict, "config_hash" => hash}) |> IO.iodata_to_binary()
      )
    end

    test "writes config.json and a case.json per case and lists them", %{tmp_dir: dir} do
      {out, 0} = prepare(dir)
      [first | lines] = String.split(out, "\n", trim: true)
      assert first == "#{dir}/out\t0"

      assert lines == [
               "A commits small #{dir}/out/A/commits/small/case.json",
               "A wakes default #{dir}/out/A/wakes/case.json"
             ]

      case = dir |> Path.join("out/A/commits/small/case.json") |> File.read!() |> :json.decode()
      assert case["levels"] == [50, 100, 200, 400, 800, 1600, 3200]
      assert case["hash"] == case_hash(dir, "A/commits/small")
      assert case["skip_done"] == true
      assert case["command"] =~ "mise run load -- --config"
      assert case["dir"] == "#{dir}/out/A/commits/small"

      assert case["store"]["tmpfs_mb"] == 4096 and case["memory_mb"] == 512 and
               case["data_mb"] == 1024

      all = dir |> Path.join("out/config.json") |> File.read!() |> :json.decode()
      assert all["cases"] == ["A/commits/small", "A/wakes"]
    end

    test "the command carries the overrides in effect", %{tmp_dir: dir} do
      {_, 0} = prepare(dir, ["--no-skip-done"], [{"ALAMBIQUE_LOAD_STEP_S", "15"}])
      case = dir |> Path.join("out/A/wakes/case.json") |> File.read!() |> :json.decode()

      assert case["command"] =~
               ~r/^ALAMBIQUE_LOAD_STEP_S=15 mise run load -- --config .*round.toml --no-skip-done$/

      assert case["skip_done"] == false
      assert case["steps"]["duration_s"] == 15.0
    end

    test "a stored step of the same settings is accepted; one of other settings is refused", %{
      tmp_dir: dir
    } do
      step(dir, "A/commits/small", "step-50.json", case_hash(dir, "A/commits/small"), "good")
      assert {_, 0} = prepare(dir)

      step(dir, "A/wakes", "step-5.json", "0000000000000000", "bad")
      {out, status} = prepare(dir)
      assert status == 1

      assert out =~
               "A/wakes/step-5.json: made with other settings (hash 000000000000, this config "

      assert out =~ "use --no-skip-done to run it again, or another output.name"
      refute out =~ "A/commits/small"
    end

    test "--no-skip-done runs over results of other settings", %{tmp_dir: dir} do
      step(dir, "A/wakes", "step-5.json", "0000000000000000", "bad")
      assert {_, 0} = prepare(dir, ["--no-skip-done"])
    end

    test "an invalid step never counts as done, whatever its hash", %{tmp_dir: dir} do
      hash = case_hash(dir, "A/wakes")
      step(dir, "A/wakes", "step-5.json", "0000000000000000", "invalid")
      step(dir, "A/wakes", "step-10.json", hash, "invalid")
      step(dir, "A/wakes", "step-20.json", hash, "good")
      step(dir, "A/wakes", "step-20-attempt1.json", hash, "good")
      assert {_, 0} = prepare(dir)

      done =
        py("""
        c = lc.load_config(#{inspect(Path.join(dir, "round.toml"))}, {})
        print(json.dumps(sorted(lc.done_steps(#{inspect(Path.join(dir, "out/A/wakes"))}, #{inspect(hash)}))))
        """)

      assert done == ["step-20.json"]
    end

    test "an invalid file is refused before anything is written", %{tmp_dir: dir} do
      path = write(dir, "[measures.cells.steps]\nfactr = 2\n[output]\ndir = \"#{dir}/out\"\n")
      {out, 1} = run(["--prepare", path])
      assert out =~ "measures.cells.steps.factr: unknown key (did you mean 'factor'?)"
      refute File.exists?(Path.join(dir, "out"))
    end
  end

  describe "what load-case reads" do
    test "--shell and --service-env describe the case", %{tmp_dir: dir} do
      path =
        write(dir, "[round]\ncases = [\"B/cells/cache256\"]\n[output]\ndir = \"#{dir}/out\"\n")

      {_, 0} = run(["--prepare", path])
      case_path = Path.join(dir, "out/B/cells/cache256/case.json")

      {shell, 0} = run(["--shell", case_path])

      assigns =
        for line <- String.split(shell, "\n", trim: true),
            [k, v] = String.split(line, "=", parts: 2),
            into: %{},
            do: {k, v}

      assert %{"scenario" => "B", "measure" => "cells", "variant" => "cache256", "cpus" => "2.0"} =
               assigns

      assert %{
               "limit_mb" => "2048",
               "data_mb" => "1024",
               "store_on" => "tmpfs",
               "store_mb" => "4096"
             } = assigns

      assert %{"skip_done" => "1", "start_load" => "8.0", "min_gib" => "6.0"} = assigns

      assert assigns["image"] == "alambique-load:local" and
               assigns["out"] == "#{dir}/out/B/cells/cache256"

      {env, 0} = run(["--service-env", case_path])

      assert String.split(env, "\n", trim: true) == [
               "ALAMBIQUE_LOAD_STORE_LATENCY_MS=25",
               "ALAMBIQUE_LOAD_WAL_CHECKPOINT_BYTES=8388608",
               "ALAMBIQUE_LOAD_IDLE_TIMEOUT_MS=86400000",
               "ALAMBIQUE_LOAD_MAX_CELLS=10000000",
               "ALAMBIQUE_LOAD_CACHE_KIB=256"
             ]
    end
  end
end
