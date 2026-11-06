defmodule Alambique.Ops.CookieTest do
  @moduledoc "The cluster cookie of the Mix tasks can come from the environment, off the command line."
  use ExUnit.Case, async: false

  alias Alambique.Ops.Cluster

  setup do
    old = System.get_env("ALAMBIQUE_COOKIE")

    on_exit(fn ->
      if old,
        do: System.put_env("ALAMBIQUE_COOKIE", old),
        else: System.delete_env("ALAMBIQUE_COOKIE")
    end)
  end

  test "ALAMBIQUE_COOKIE gives the cookie and the flag wins" do
    System.delete_env("ALAMBIQUE_COOKIE")
    assert Cluster.cookie([]) == nil

    System.put_env("ALAMBIQUE_COOKIE", "from-env")
    assert Cluster.cookie([]) == "from-env"
    assert Cluster.cookie(cookie: "from-flag") == "from-flag"
  end
end
