defmodule ExMCP.Client.ResponseBudgetTest do
  use ExUnit.Case, async: true

  alias ExMCP.Client
  alias ExMCP.Client.ResponseBudget
  alias ExMCP.Error
  alias ExMCP.Transport.HTTP

  test "keeps request budgets isolated by progress token across stream recovery" do
    budgets =
      ResponseBudget.new()
      |> ResponseBudget.track(1, request("alpha"), 10)
      |> ResponseBudget.track(2, request("beta"), 10)

    alpha_progress = progress("alpha")

    assert {:ok, budgets} = ResponseBudget.consume(budgets, alpha_progress, 6)
    assert {:error, [1], budgets} = ResponseBudget.consume(budgets, alpha_progress, 5)

    refute ResponseBudget.tracked?(budgets, 1)
    assert ResponseBudget.tracked?(budgets, 2)
  end

  test "bounds uncorrelated stream traffic without charging an arbitrary request" do
    budgets =
      ResponseBudget.new()
      |> ResponseBudget.track(1, request("alpha"), 4)
      |> ResponseBudget.track(2, request("beta"), 6)

    message = %{"jsonrpc" => "2.0", "method" => "notifications/message", "params" => %{}}

    assert {:error, :uncorrelated, budgets} = ResponseBudget.consume(budgets, message, 5)
    assert ResponseBudget.tracked?(budgets, 1)
    assert ResponseBudget.tracked?(budgets, 2)
  end

  test "the client fails only the request whose resumed stream exceeds its budget" do
    alpha_reply = make_ref()
    beta_reply = make_ref()

    budgets =
      ResponseBudget.new()
      |> ResponseBudget.track(1, request("alpha"), 10)
      |> ResponseBudget.track(2, request("beta"), 10)

    state =
      struct!(Client,
        pending_requests: %{
          1 => {{self(), alpha_reply}, :single, "tools/call"},
          2 => {{self(), beta_reply}, :single, "tools/call"}
        },
        pending_batches: %{},
        cancelled_requests: MapSet.new(),
        response_budgets: budgets,
        subscriptions: %{}
      )

    message = progress("alpha")

    assert {:noreply, state} = Client.handle_info({:transport_message, message, 6}, state)
    refute_receive {^alpha_reply, _response}
    refute_receive {^beta_reply, _response}

    assert {:noreply, state} = Client.handle_info({:transport_message, message, 5}, state)

    assert_receive {^alpha_reply,
                    {:error,
                     %Error.TransportError{
                       transport: :http,
                       reason: :response_too_large
                     }}}

    refute_receive {^beta_reply, _response}
    refute Map.has_key?(state.pending_requests, 1)
    assert Map.has_key?(state.pending_requests, 2)
  end

  test "SSE data returned by the request POST shares the request budget" do
    reply_tag = make_ref()
    budgets = ResponseBudget.track(ResponseBudget.new(), 1, request("alpha"), 4)

    state =
      struct!(Client,
        pending_requests: %{1 => {{self(), reply_tag}, :single, "tools/call"}},
        pending_batches: %{},
        cancelled_requests: MapSet.new(),
        response_budgets: budgets,
        subscriptions: %{},
        async_post_tasks: %{}
      )

    meta = %{request_id: 1, response_bytes: 5, state_changes: %{}}

    assert {:noreply, state} =
             Client.handle_info({:async_post_result, {:ok, %HTTP{}}, meta}, state)

    assert_receive {^reply_tag, {:error, %Error.TransportError{reason: :response_too_large}}}

    refute Map.has_key?(state.pending_requests, 1)
  end

  defp request(token) do
    %{
      "jsonrpc" => "2.0",
      "id" => System.unique_integer([:positive]),
      "method" => "tools/call",
      "params" => %{"_meta" => %{"progressToken" => token}}
    }
  end

  defp progress(token) do
    %{
      "jsonrpc" => "2.0",
      "method" => "notifications/progress",
      "params" => %{"progressToken" => token, "progress" => 1}
    }
  end
end
