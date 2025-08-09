defmodule Commands.Blpop do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([key, timeout_str]) do
    # Parse timeout
    timeout =
      try do
        String.to_float(timeout_str)
      rescue
        ArgumentError ->
          String.to_integer(timeout_str) * 1.0
      end

    IO.puts("BLPOP: Trying to pop from key '#{key}' with timeout #{timeout}")

    # Check if there are already blocked clients for this key
    # If so, we should block immediately to maintain FIFO order
    case BlockingQueue.has_blocked_clients?(key) do
      true ->
        IO.puts("BLPOP: Other clients already waiting, blocking immediately for key '#{key}'")
        {:block, {key, timeout}}

      false ->
        # No other clients waiting, try to pop immediately
        case Store.lpop(key) do
          {:ok, :not_found} ->
            IO.puts("BLPOP: List empty, blocking client for key '#{key}'")
            {:block, {key, timeout}}

          {:ok, element} ->
            IO.puts("BLPOP: Got element immediately: '#{element}'")

            {:ok,
             "*2\r\n$#{byte_size(key)}\r\n#{key}\r\n$#{byte_size(element)}\r\n#{element}\r\n"}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  def execute(_), do: {:error, "BLPOP command expects exactly two arguments: key and timeout"}
end
