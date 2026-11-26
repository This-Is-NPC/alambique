# Credo, run with --strict by scripts/check-elixir-static.
%{
  configs: [
    %{
      name: "default",
      files: %{included: ["lib/"], excluded: []},
      strict: true,
      # `extra` overrides the parameters of these checks and keeps the default ones.
      checks: %{
        extra: [
          {Credo.Check.Refactor.CyclomaticComplexity, [max_complexity: 9]},
          {Credo.Check.Refactor.Nesting, [max_nesting: 2]},
          {Credo.Check.Refactor.FunctionArity, [max_arity: 6]},
          {Credo.Check.Refactor.ABCSize, [max_size: 40]},
          {Credo.Check.Readability.Specs, []}
        ]
      }
    }
  ]
}
