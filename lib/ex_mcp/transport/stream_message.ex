defmodule ExMCP.Transport.StreamMessage do
  @moduledoc false

  @enforce_keys [:payload, :response_bytes]
  defstruct [:payload, :response_bytes]

  @type t :: %__MODULE__{payload: term(), response_bytes: non_neg_integer()}
end
