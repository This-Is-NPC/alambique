# Agent instructions

## Applicability

Apply each instruction according to the request, the affected files, the
project's interfaces, and the impact of the change. Perform a step only when
it contributes to completing or verifying the task. Create supporting
artifacts only when the work calls for them.

Read `README.md` to understand the project, then follow the links relevant to
the task. Project behavior, architecture, setup, and commands belong in the
README and its linked documentation. Treat setup instructions as preparation
for work that needs that environment, rather than a prerequisite for every
request.

## Development

- Keep changes focused on the request. Preserve existing user changes.
- Prefer simple solutions and one implementation of each behavior. Remove
  code, imports, files, and obsolete paths made redundant by the change.
- Complete replacements within the agreed scope, including affected callers
  and documentation. Introduce abstractions when concrete uses justify them.
- Treat performance improvements as worthwhile even when the gain is small.
  Measure when evidence is needed to support the decision.
- Keep comments short: explain constraints, intent, or behavior that needs
  clarification. Put extended explanations and guides in `docs/`.
- Write code, comments, documentation, task descriptions, and commits in
  English.
- Continue authorized work through implementation and relevant verification.
  Resolve routine choices independently; ask when missing information changes
  the intended outcome or scope.

## Workflow and verification

- Choose verification according to the behavior changed and the consequences
  of a mistake. Start with the narrowest useful check. Broaden coverage when
  integration impact or remaining uncertainty warrants it.
- For text changes, review accuracy, structure, and affected links. Run
  documentation tooling when the change depends on generated output or other
  rules enforced by that tooling.
- For behavior changes, exercise the affected paths. Add regression coverage
  for fixes when useful, proving that the check detects the original defect.
  Use mutation checks or benchmarks when they provide necessary evidence.
- Review experiments and design alternatives with the smallest working
  demonstration that makes them assessable. Apply production verification
  when integrating the chosen behavior.
- Batch related edits before running checks. Repeat checks when new changes,
  failures, or unresolved concerns justify another run.
- Tests wait for observable state, output, or signals. Timeouts bound failure.
- Isolate checks that could affect user settings, accounts, files, or running
  services. Use disposable environments and synthetic fixtures where needed.
- Clean up temporary processes, sessions, sockets, and files started by the
  task, including after failure. Keep the intended results.

## Coverage

- `mise run test:coverage` measures line coverage of the Elixir code and of
  the NIF the BEAM loads over the default suite. It fails when either is
  below `ALAMBIQUE_COVERAGE_MIN` (`mise.toml`, default 90). Keep both above it.
- Excluded: test support and testkit modules, the NIF stubs of
  `Alambique.Replication.Native`, fuzz targets, and the panic test NIFs. A new
  exclusion needs a reason in `mix.exs` or the script.
- New tests must be able to fail on a real bug. Do not add tests that only
  raise the number. Remove dead code found, rather than testing it.

## Tooling and scripts

- Where the repository uses mise, keep it as the entry point for project
  commands and pinned tools. Install tools only when the task requires them
  and the required versions are unavailable.
- Reuse existing tasks. Invoke pinned tools through mise when needed.
- Keep executable task implementations in `scripts/`, with a shebang and one
  responsibility per script. Make scripts safe to rerun with the same inputs.
- Declare tasks in `mise.toml`, with descriptions and relevant dependencies,
  environment, arguments, inputs, and outputs. Compose existing tasks or
  scripts for aggregate commands.
- Keep one implementation shared by mise, hooks, and other callers.

## Documentation

Write for someone using the project, one question at a time. Use short,
concrete sentences, verified facts, and real command output. Identify
placeholders and distinguish observed behavior, measurements, and inference.

Use the following conventions when the corresponding content is needed.
Follow existing equivalent pages and names when updating a repository.

- `README.md` is the front door: purpose, installation of the released
  program, first steps, key concepts, interface, requirements, limitations,
  license, and links to detailed documentation and contributor instructions.
- `docs/README.md` maps the documentation in reading order, with one line
  explaining the question each page answers and a short recommended path.
- `docs/how-to-*.md` answers one practical question per page. Open with
  **The question:** and say whether the guide stands alone. Include
  prerequisites, numbered steps with commands or actions and expected
  results, troubleshooting, and links to next steps. Link to existing guides
  for tasks they already explain.
- `docs/design.md` serves contributors: model, architecture, state,
  protocols, packaging, constraints, and measurements. Keep numbered section
  identifiers stable when used as references. Development and build
  instructions belong in contributor documentation.
- `docs/screens.md` explains graphical screens in order of use, including
  controls, navigation, and relevant illustrations.
- A CLI reference documents commands, options, examples, and output.
  Generate reference material from command definitions when supported, and
  verify freshness when those definitions change.
- `docs/what-does-not-work.md` tracks known defects while they remain open.
  Each entry explains what happens, the available workaround, and the
  evidence. Link it from the README and remove resolved entries. Document
  platform and provider limitations in the README and relevant guides.

Update documentation alongside the behavior it describes. Keep affected
links, images, and anchors valid. Create pages when there is useful content
for their readers.

### Images

Use images when they help explain graphical interfaces and visual behavior
relevant to the task. Generate captures with synthetic data, inspect the
results, and update images affected by the change. Store documentation images
under `docs/img/`. Record their source and generation method so they can be
reproduced where practical.

## Delivery, commits, and pull requests

- Report what changed, the relevant verification results, and any material
  limitations. State clearly when a check could not be completed.
- When creating commits, inspect the working tree and staged diff, and group
  changes by intent. Stage specific paths or hunks non-interactively while
  preserving unrelated work.
- Use Conventional Commits: `type(scope): subject`, with an English imperative
  subject of at most 50 characters, lowercase after the colon, and no trailing
  period. Mark breaking changes with `!` and a `BREAKING CHANGE:` footer.
- Show the proposed commit message before committing. Explain non-obvious
  reasons in the body.
- Keep authorship with the human. Commit messages and pull request titles and
  descriptions must contain no AI or agent attribution of any kind: trailers,
  signatures, badges, credits, or generated-by links. This includes
  `Co-Authored-By` entries naming agents or models, `Generated with` notices,
  and equivalent wording. This rule applies whenever creating or editing a
  commit or pull request.
- Leave publication to the user: never run `git push`.
