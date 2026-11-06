defmodule Alambique.Query do
  @moduledoc """
  Read-only SQL over many cells at once, for reports and operator views
  (design §14).

  A query never wakes a cell, takes a lease or reads an owner's file. It runs
  on a local read-only replica of each cell, built from the store into a
  bounded on-disk cache and refreshed by applying only the segments that are
  new. Every row says which cell it came from and at which TXID.

      Alambique.Query.all(MyApp.Cells, {:prefix, "org-"}, "SELECT count(*) FROM notes")
      #=> %{rows: [%Alambique.Query.Row{cell: "org-1", txid: 7, columns: ["count(*)"], values: [3]}, ...],
      #     errors: [{"org-9", :timeout}]}

  **A replica is as of the last batch the store acknowledged when the cell was
  read.** A write is acknowledged to its caller only after it is in the store,
  so a query that starts after a write returned sees it. The one thing an
  awake owner can show that a replica cannot is a commit still waiting for its
  upload, at most one upload (`:op_timeout_ms`) old. With `:max_age_ms` a
  replica may be older by that much on purpose.

  This is not the way to analyse all organizations together: that is the job
  of the change feed (`Alambique.Feed`). Use queries for a bounded set of
  cells, such as one organization, a list, or an occasional report.

  ## Cells

  The `cells` argument is a list or any enumerable of cell ids, `:all`, or
  `{:prefix, id_prefix}`. The last two scan the keys of the location with
  `cells/2`, one paginated request per page and streamed; they are explicit
  scans and never happen unless asked for. Prefer a list of ids when the cells
  are known.

  ## Options

    * `:at` - `:latest` (default), `{:txid, n}` or `{:timestamp, ms}`: the
      state to read. A timestamp always restores the cell from scratch.
    * `:concurrency` - cells read at once (instance option `:query`).
    * `:timeout_ms` - time one cell may take, including building its replica
      (instance option `:query`). A running statement is interrupted at the
      deadline.
    * `:page_size` - ids per listing request when `:all` or `{:prefix, _}`
      scans a location (instance option `:query`, default 1 000).
    * `:max_age_ms` - use a cached `:latest` replica used less than this long
      ago without asking the store (default 0: always check). The age has a
      resolution of one second.
    * `:location` - where to read instead of where the placement puts the
      cells: an `Alambique.Location`, the id of one, `{store, prefix}`, or a
      list of them for `:all` and `{:prefix, _}`. By default a cell is read in
      the location the placement gives it, and `:all` and `{:prefix, _}` scan
      every location of the placement (`locations/1`). A cache entry belongs
      to its location.
    * `:moved` - for `:all` and `{:prefix, _}`: `:skip` (default) leaves out a
      cell that the placement now puts in another location than the one
      listed, because a relocated cell keeps its old objects until their
      cleanup; `:include` reads them anyway (the old copy is frozen at the
      relocation). Cells of a `{store, prefix}` given in `:location` are not
      checked.
    * `:ordered` - return the cells in the order of the input (default
      `false`: the order they finish in).
  """

  alias Alambique.{Config, Inflight, Location, Placement, Telemetry}
  alias Alambique.Cell.Db
  alias Alambique.Query.{Cache, Replica}
  alias Alambique.Replication.Retry

  defmodule Row do
    @moduledoc """
    One row of a cross-cell query: `values` in the order of `columns`, read
    from `cell` at `txid`.
    """
    defstruct [:cell, :txid, :columns, :values]

    @type t :: %__MODULE__{
            cell: String.t(),
            txid: non_neg_integer,
            columns: [String.t()],
            values: [term]
          }
  end

  defmodule ListError do
    @moduledoc "Raised when listing the cells in the store fails."
    defexception [:reason]

    @impl true
    def message(%{reason: reason}), do: "listing cells failed: #{inspect(reason)}"
  end

  @type cells :: Enumerable.t() | :all | {:prefix, String.t()}
  @type item :: Row.t() | {:error, String.t(), term}

  @doc """
  Streams the ids of the cells in the store, in the order of their keys, one
  location after the other (the `:location` option, by default every location
  of the placement).

  `{:prefix, p}` or the `:prefix` option lists only ids that start with `p`.
  Listing is by prefix pages of the store: one request returns up to
  `:page_size` keys (default 1 000), and after each page the listing skips the
  rest of the last cell it saw, so a cell with many segments costs one
  request at most. A failing request raises `Alambique.Query.ListError`.
  """
  @spec cells(Config.t() | atom, keyword) :: Enumerable.t()
  def cells(instance, opts \\ []) do
    config = config(instance)

    config
    |> targets!(opts)
    |> Stream.flat_map(&list_cells(config, &1, opts))
  end

  defp list_cells(config, target, opts) do
    prefix = Keyword.get(opts, :prefix, "")
    page_size = Keyword.get(opts, :page_size, config.query.page_size)
    timeout = Keyword.get(opts, :timeout_ms, config.query.timeout_ms)
    %{ctx: %{store: {store, store_config}, prefix: location_prefix}} = target
    base = "#{location_prefix}/cells/"

    Stream.resource(
      fn -> {:start, nil} end,
      fn
        :done ->
          {:halt, :done}

        {:start, start_after} ->
          started = System.monotonic_time()
          deadline = Retry.deadline_in(timeout)

          result =
            Retry.run(target.ctx, deadline, fn ->
              case store.list(store_config, base <> prefix, start_after, page_size) do
                {:ok, keys, next} -> {:ok, keys, next}
                {:error, reason} -> {:retry, reason}
              end
            end)

          case result do
            {:ok, keys, next} ->
              ids = keys |> Enum.map(&cell_of(&1, base)) |> Enum.reject(&is_nil/1) |> Enum.dedup()

              Telemetry.span_stop(
                [:query, :list],
                started,
                %{keys: length(keys), cells: length(ids)},
                %{
                  result: :ok
                }
              )

              {ids, continue(next, ids, base)}

            {:error, reason} ->
              Telemetry.span_stop([:query, :list], started, %{keys: 0, cells: 0}, %{
                result: :error,
                reason: reason
              })

              raise ListError, reason: reason
          end
      end,
      fn _ -> :ok end
    )
  end

  defp continue(nil, _ids, _base), do: :done
  # Everything under the last cell is known: jump past it. Names under a cell
  # (`lease`, the level directories, `frozen`) all sort before `~`.
  defp continue(_next, ids, base), do: {:start, base <> List.last(ids) <> "/~"}

  defp cell_of(key, base) do
    case key |> String.replace_prefix(base, "") |> String.split("/", parts: 2) do
      [id, _rest] -> id
      _ -> nil
    end
  end

  @doc """
  Runs `sql` on each cell and streams the rows.

  Elements are `%Alambique.Query.Row{}` and, for a cell that failed,
  `{:error, cell, reason}`: a timeout, a store error, a SQL error or
  `:read_only` for a statement that would write. Other cells are unaffected.
  Rows are not held back: a cell's rows arrive when that cell is done.
  """
  @spec stream(atom, cells, String.t(), list, keyword) :: Enumerable.t()
  def stream(instance, cells, sql, params \\ [], opts \\ [])
      when is_binary(sql) and is_list(params) do
    config = config(instance)
    concurrency = Keyword.get(opts, :concurrency, config.query.concurrency)
    timeout = Keyword.get(opts, :timeout_ms, config.query.timeout_ms)
    target = Keyword.get(opts, :at, :latest)
    explicit = explicit_target(config, cells, opts)

    work = fn {cell, location} ->
      {cell, query_cell(config, location, cell, sql, params, target, timeout, opts)}
    end

    config.query_supervisor
    |> Task.Supervisor.async_stream_nolink(ids(config, cells, explicit, opts), work,
      max_concurrency: concurrency,
      # The statement is interrupted at `timeout`; this only backs it up.
      timeout: timeout + 1_000,
      on_timeout: :kill_task,
      ordered: Keyword.get(opts, :ordered, false),
      zip_input_on_exit: true
    )
    |> trimmed(config)
    |> Stream.flat_map(&items/1)
  end

  @doc """
  Runs `sql` on each cell and collects the result: `%{rows: [Row], errors:
  [{cell, reason}]}`.
  """
  @spec all(atom, cells, String.t(), list, keyword) :: %{rows: [Row.t()], errors: [tuple]}
  def all(instance, cells, sql, params \\ [], opts \\ []) do
    {rows, errors} =
      instance
      |> stream(cells, sql, params, opts)
      |> Enum.reduce({[], []}, fn
        %Row{} = row, {rows, errors} -> {[row | rows], errors}
        {:error, cell, reason}, {rows, errors} -> {rows, [{cell, reason} | errors]}
      end)

    %{rows: Enum.reverse(rows), errors: Enum.reverse(errors)}
  end

  # The elements of the work stream: a cell with the target to read it in
  # (`nil` until the worker asks the placement).
  defp ids(config, :all, _explicit, opts), do: scan(config, [], opts)
  defp ids(config, {:prefix, prefix}, _explicit, opts), do: scan(config, [prefix: prefix], opts)
  defp ids(_config, cells, explicit, _opts), do: Stream.map(cells, &{&1, explicit})

  defp scan(config, list_opts, opts) do
    list_opts = list_opts ++ Keyword.take(opts, [:page_size, :timeout_ms])
    moved = Keyword.get(opts, :moved, :skip)

    config
    |> targets!(opts)
    |> Stream.flat_map(fn target ->
      config
      |> list_cells(target, list_opts)
      |> Stream.map(&{&1, target})
      |> Stream.reject(fn {cell, target} -> moved == :skip and moved?(config, cell, target) end)
    end)
  end

  # A relocated cell keeps its old objects until their cleanup: the listing of
  # that location still shows it, but the cell lives elsewhere now.
  defp moved?(config, cell, %{location: id}) when id != nil do
    case Placement.locate(config, cell) do
      {:ok, %Location{id: current}} -> current != id
      {:error, _} -> false
    end
  end

  defp moved?(_config, _cell, _target), do: false

  defp items({:ok, {cell, {:ok, txid, columns, rows, _built}}}),
    do: for(values <- rows, do: %Row{cell: cell, txid: txid, columns: columns, values: values})

  defp items({:ok, {cell, {:error, reason}}}), do: [{:error, cell, reason}]
  defp items({:exit, {cell, reason}}), do: [{:error, cell, {:exit, reason}}]

  # Keeps the cache within its bound while a long run goes on, and once at the
  # end (also when the consumer stops early). A trim lists the whole cache, so
  # it runs when the replicas this run built since the last trim reach a
  # tenth of `cache_bytes`, not at every few results. The cache can pass
  # `cache_bytes` by about that much.
  defp trimmed(stream, config) do
    step = max(div(config.query.cache_bytes, 10), 1)

    Stream.transform(
      stream,
      fn -> 0 end,
      fn item, built ->
        built = built + built(item)

        if built >= step do
          trim(config)
          {[item], 0}
        else
          {[item], built}
        end
      end,
      fn _built -> trim(config) end
    )
  end

  defp built({:ok, {_cell, {:ok, _txid, _columns, _rows, built}}}), do: built
  defp built(_item), do: 0

  defp trim(config) do
    started = System.monotonic_time()

    {deleted, bytes} =
      Cache.trim(Cache.dir(config), config.query.cache_bytes, config.query.stale_tmp_seconds)

    Telemetry.span_stop([:query, :trim], started, %{deleted: deleted, bytes: bytes}, %{})
  end

  defp query_cell(config, location, cell, sql, params, target, timeout, opts) do
    started = System.monotonic_time()
    deadline = Retry.deadline_in(timeout)

    result =
      try do
        run_cell(config, location, cell, sql, params, target, deadline, opts, 2)
      rescue
        exception -> {:error, {:raised, exception}}
      catch
        kind, reason -> {:error, {kind, reason}}
      end

    meta =
      case result do
        {:ok, txid, _columns, rows, _built} ->
          {%{rows: length(rows)}, %{result: :ok, txid: txid}}

        {:error, reason} ->
          {%{rows: 0}, %{result: :error, reason: reason}}
      end

    Telemetry.span_stop(
      [:query, :cell],
      started,
      elem(meta, 0),
      Map.put(elem(meta, 1), :cell, cell)
    )

    result
  end

  defp run_cell(_config, _location, cell, _sql, _params, _target, _deadline, _opts, _tries)
       when not is_binary(cell),
       do: {:error, :invalid_cell}

  defp run_cell(config, location, cell, sql, params, target, deadline, opts, tries) do
    Inflight.run(config, cell, :query, fn ->
      read_cell(config, location, cell, sql, params, target, deadline, opts, tries)
    end)
  end

  defp read_cell(config, location, cell, sql, params, target, deadline, opts, tries) do
    with {:ok, location} <- place(config, location, cell),
         {:ok, replica} <- Replica.ensure(config, location, cell, target, deadline, opts) do
      case read(config, replica, sql, params, deadline) do
        # An eviction removed the file between choosing and opening it.
        {:error, :gone} when tries > 1 ->
          read_cell(config, location, cell, sql, params, target, deadline, opts, tries - 1)

        {:error, :gone} ->
          {:error, :evicted}

        {:ok, columns, rows} ->
          {:ok, replica.txid, columns, rows, replica.bytes}

        {:error, _} = error ->
          error
      end
    end
  end

  defp place(_config, %{} = location, _cell), do: {:ok, location}

  defp place(config, nil, cell) do
    case Placement.validate_id(config, cell) do
      :ok ->
        with {:ok, location} <- Placement.locate(config, cell),
             do: {:ok, target(config, location)}

      {:error, _} ->
        {:error, :invalid_cell}
    end
  end

  defp read(config, replica, sql, params, deadline) do
    case File.exists?(replica.path) && Db.open_read_only(replica.path) do
      false ->
        {:error, :gone}

      {:ok, conn} ->
        try do
          case Db.read_within(
                 conn,
                 sql,
                 params,
                 max(deadline - System.monotonic_time(:millisecond), 1),
                 Db.limits(config)
               ) do
            {:ok, %{columns: columns, rows: rows}} -> {:ok, columns, rows}
            {:error, _} = error -> error
          end
        after
          Db.close(conn)
        end

      # A trim removed the file between the check and the open.
      {:error, reason} ->
        if File.exists?(replica.path), do: {:error, {:open, reason}}, else: {:error, :gone}
    end
  end

  # Where to read: the targets of the `:location` option, or every location of
  # the placement. A target carries the replication context and the scope that
  # keys the cache.
  defp targets!(config, opts) do
    case Keyword.fetch(opts, :location) do
      {:ok, given} -> given |> wrap() |> Enum.map(&target!(config, &1))
      :error -> all_targets!(config)
    end
  end

  defp all_targets!(config) do
    case Placement.locations(config) do
      {:ok, locations} -> Enum.map(locations, &target(config, &1))
      {:error, reason} -> raise ListError, reason: reason
    end
  end

  # Explicit cells go to the location of the option, or each to its own.
  defp explicit_target(config, cells, opts)
       when is_list(cells) or is_map(cells) or is_function(cells) do
    case Keyword.fetch(opts, :location) do
      {:ok, given} ->
        case targets!(config, location: given) do
          [target] -> target
          _ -> raise ArgumentError, "listed cells are read in one :location"
        end

      :error ->
        nil
    end
  end

  defp explicit_target(_config, _scan, _opts), do: nil

  defp wrap(given) when is_list(given), do: given
  defp wrap(given), do: [given]

  defp target!(config, id) when is_binary(id) do
    case Placement.location(config, id) do
      {:ok, location} ->
        target(config, location)

      {:error, reason} ->
        raise ArgumentError, "unknown :location #{inspect(id)}: #{inspect(reason)}"
    end
  end

  defp target!(config, %Location{} = location), do: target(config, location)

  defp target!(config, {{mod, _} = store, prefix}) when is_atom(mod) and is_binary(prefix),
    do: target(config, %Location{id: "given", store: store, prefix: prefix})

  defp target(config, %Location{store: store, prefix: prefix} = location) do
    scope = :erlang.phash2({store, prefix}) |> Integer.to_string(16)
    # A location given as a store and prefix is not one of the placement.
    id = if location.id == "given", do: nil, else: location.id
    %{ctx: Config.repl_ctx(config, location), scope: scope, location: id}
  end

  defp config(name) when is_atom(name), do: Config.fetch!(name)
  defp config(%{query: _} = config), do: config
end
