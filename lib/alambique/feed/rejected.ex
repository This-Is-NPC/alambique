defmodule Alambique.Feed.Rejected do
  @moduledoc """
  A feed object that could not be decoded (design §13).

  It takes the place of the records of the object, and the position moves
  past it, so one malformed object, from a faulty or hostile writer of the
  store, cannot stop a consumer. Each one also emits a
  `[:alambique, :feed, :read, :rejected]` telemetry event.

    * `:key` - the store key of the object
    * `:reason` - why: `:invalid_json`, `:bad_object` (not the shape of a feed
      object), `{:bad_op, op}` or `:bad_record`
    * `:id` - an idempotency key, `"rejected:<key>"`
    * `:at` - publication time in milliseconds, from the key

  A consumer must handle this struct next to `Alambique.Feed.Record` and
  `Alambique.Feed.Rewind`. The usual handling is to log it and carry on.
  """

  @enforce_keys [:key, :reason, :id, :at]
  defstruct [:key, :reason, :id, :at]

  @type t :: %__MODULE__{
          key: String.t(),
          reason: atom | {:bad_op, term},
          id: String.t(),
          at: non_neg_integer
        }
end
