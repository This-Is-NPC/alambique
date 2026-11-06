Alambique.TestDirs.sweep()
ExUnit.after_suite(fn _ -> Alambique.TestDirs.clean() end)
ExUnit.start(exclude: [:seaweedfs, :cluster])
