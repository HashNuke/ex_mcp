defmodule ExMCP.Client.ResponseBudget do
  @moduledoc false

  defstruct requests: %{}, progress_tokens: %{}, uncorrelated_remaining: nil

  @type request_id :: ExMCP.Types.request_id()
  @type progress_token :: String.t() | integer()
  @type request_budget :: %{remaining: non_neg_integer(), progress_token: progress_token() | nil}
  @type t :: %__MODULE__{
          requests: %{optional(request_id()) => request_budget()},
          progress_tokens: %{optional(progress_token()) => MapSet.t(request_id())},
          uncorrelated_remaining: non_neg_integer() | nil
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec track(t() | nil, request_id(), map(), pos_integer()) :: t()
  def track(budgets, request_id, request, limit)
      when is_map(request) and is_integer(limit) and limit > 0 do
    budgets = normalize(budgets) |> drop(request_id)
    progress_token = get_in(request, ["params", "_meta", "progressToken"])
    entry = %{remaining: limit, progress_token: progress_token}

    %{
      budgets
      | requests: Map.put(budgets.requests, request_id, entry),
        progress_tokens: put_progress_token(budgets.progress_tokens, progress_token, request_id),
        uncorrelated_remaining: uncorrelated_limit(budgets.uncorrelated_remaining, limit)
    }
  end

  @spec consume(t() | nil, term(), non_neg_integer()) ::
          {:ok, t()} | {:error, [request_id()] | :uncorrelated, t()}
  def consume(budgets, message, bytes)
      when is_integer(bytes) and bytes >= 0 do
    budgets = normalize(budgets)

    case targets(budgets, message) do
      {:requests, request_ids} -> consume_requests(budgets, request_ids, bytes)
      :uncorrelated -> consume_uncorrelated(budgets, bytes)
    end
  end

  @spec consume_request(t() | nil, request_id(), non_neg_integer()) ::
          {:ok, t()} | {:error, [request_id()], t()}
  def consume_request(budgets, request_id, bytes)
      when is_integer(bytes) and bytes >= 0 do
    budgets = normalize(budgets)
    {budgets, exceeded} = consume_request(budgets, request_id, bytes, [])

    case exceeded do
      [] -> {:ok, budgets}
      request_ids -> {:error, request_ids, budgets}
    end
  end

  @spec drop(t() | nil, request_id()) :: t()
  def drop(budgets, request_id) do
    budgets = normalize(budgets)

    case Map.pop(budgets.requests, request_id) do
      {nil, _requests} ->
        budgets

      {%{progress_token: progress_token}, requests} ->
        budgets = %{
          budgets
          | requests: requests,
            progress_tokens:
              drop_progress_token(budgets.progress_tokens, progress_token, request_id)
        }

        reset_uncorrelated_if_idle(budgets)
    end
  end

  @spec tracked?(t() | nil, request_id()) :: boolean()
  def tracked?(budgets, request_id), do: Map.has_key?(normalize(budgets).requests, request_id)

  @spec prune(t() | nil, map()) :: t()
  def prune(budgets, pending_requests) when is_map(pending_requests) do
    budgets = normalize(budgets)

    Enum.reduce(Map.keys(budgets.requests), budgets, fn request_id, acc ->
      if Map.has_key?(pending_requests, request_id), do: acc, else: drop(acc, request_id)
    end)
  end

  defp normalize(%__MODULE__{} = budgets), do: budgets
  defp normalize(_budgets), do: new()

  defp targets(budgets, %{"id" => request_id} = message) do
    if final_response?(message) and Map.has_key?(budgets.requests, request_id),
      do: {:requests, [request_id]},
      else: :uncorrelated
  end

  defp targets(
         budgets,
         %{
           "method" => "notifications/progress",
           "params" => %{"progressToken" => progress_token}
         }
       ) do
    case Map.get(budgets.progress_tokens, progress_token) do
      %MapSet{} = request_ids -> {:requests, MapSet.to_list(request_ids)}
      nil -> :uncorrelated
    end
  end

  defp targets(_budgets, _message), do: :uncorrelated

  defp final_response?(message) do
    not Map.has_key?(message, "method") and
      Map.has_key?(message, "result") != Map.has_key?(message, "error")
  end

  defp consume_request(budgets, request_id, bytes, exceeded) do
    case Map.get(budgets.requests, request_id) do
      %{remaining: remaining} = entry when bytes <= remaining ->
        requests = Map.put(budgets.requests, request_id, %{entry | remaining: remaining - bytes})
        {%{budgets | requests: requests}, exceeded}

      %{remaining: _remaining} ->
        {drop(budgets, request_id), [request_id | exceeded]}

      nil ->
        {budgets, exceeded}
    end
  end

  defp consume_requests(budgets, request_ids, bytes) do
    {budgets, exceeded} =
      Enum.reduce(request_ids, {budgets, []}, fn request_id, {acc, exceeded} ->
        consume_request(acc, request_id, bytes, exceeded)
      end)

    case Enum.sort(exceeded) do
      [] -> {:ok, budgets}
      request_ids -> {:error, request_ids, budgets}
    end
  end

  defp consume_uncorrelated(%{uncorrelated_remaining: nil} = budgets, _bytes),
    do: {:ok, budgets}

  defp consume_uncorrelated(budgets, bytes) when bytes <= budgets.uncorrelated_remaining do
    {:ok, %{budgets | uncorrelated_remaining: budgets.uncorrelated_remaining - bytes}}
  end

  defp consume_uncorrelated(budgets, _bytes) do
    {:error, :uncorrelated, %{budgets | uncorrelated_remaining: 0}}
  end

  defp uncorrelated_limit(nil, limit), do: limit
  defp uncorrelated_limit(remaining, limit), do: min(remaining, limit)

  defp reset_uncorrelated_if_idle(%{requests: requests} = budgets)
       when map_size(requests) == 0 do
    %{budgets | uncorrelated_remaining: nil}
  end

  defp reset_uncorrelated_if_idle(budgets), do: budgets

  defp put_progress_token(tokens, nil, _request_id), do: tokens

  defp put_progress_token(tokens, progress_token, request_id) do
    Map.update(tokens, progress_token, MapSet.new([request_id]), &MapSet.put(&1, request_id))
  end

  defp drop_progress_token(tokens, nil, _request_id), do: tokens

  defp drop_progress_token(tokens, progress_token, request_id) do
    case Map.get(tokens, progress_token) do
      %MapSet{} = request_ids ->
        remaining = MapSet.delete(request_ids, request_id)

        if MapSet.size(remaining) == 0,
          do: Map.delete(tokens, progress_token),
          else: Map.put(tokens, progress_token, remaining)

      nil ->
        tokens
    end
  end
end
