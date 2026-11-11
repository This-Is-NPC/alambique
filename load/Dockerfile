# The service of the load test: a mix release of Alambique on the S3 store,
# built at the versions pinned in mise.toml (scripts/load-image passes them).
# The build stage compiles the dependencies and the Rust crate's dependencies
# in their own layers, so a change to the sources rebuilds only what it touches.

# scripts/load-image passes the real values; these only keep FROM valid for
# a build without them (which fails at the pull, naming the missing version).
ARG ERLANG=unset
ARG ELIXIR=unset
ARG DEBIAN=unset
FROM hexpm/elixir:${ELIXIR}-erlang-${ERLANG}-debian-${DEBIAN} AS build
ARG RUST

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential curl ca-certificates git \
 && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/opt/rustup CARGO_HOME=/opt/cargo PATH=/opt/cargo/bin:$PATH MIX_ENV=load
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
  | sh -s -- -y --profile minimal --default-toolchain ${RUST}

WORKDIR /src

# Elixir dependencies.
COPY mix.exs mix.lock ./
RUN mix local.hex --force && mix local.rebar --force \
 && mix deps.get --only load && mix deps.compile

# The crate's dependencies (SQLite is the slow one), built against an empty
# library; the sources replace it below.
COPY native/alambique_nif/Cargo.toml native/alambique_nif/Cargo.lock native/alambique_nif/
COPY native/alambique_nif/.cargo native/alambique_nif/.cargo
RUN mkdir native/alambique_nif/src && touch native/alambique_nif/src/lib.rs \
 && cd native/alambique_nif && cargo build --release --locked

COPY native/alambique_nif/src native/alambique_nif/src
COPY lib lib
COPY load/lib load/lib
COPY test/support/bench_text.ex test/support/bench_text.ex
# Cargo compares modification times, and the copied sources are older than the
# build of the empty library.
RUN find native -name '*.rs' -exec touch {} + \
 && mix release alambique_load --path /opt/alambique_load

FROM hexpm/erlang:${ERLANG}-debian-${DEBIAN} AS run
RUN apt-get update \
 && apt-get install -y --no-install-recommends libstdc++6 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /opt/alambique_load /opt/alambique_load
ENV LANG=C.UTF-8 RELEASE_DISTRIBUTION=name
ENTRYPOINT ["/opt/alambique_load/bin/alambique_load"]
CMD ["start"]
