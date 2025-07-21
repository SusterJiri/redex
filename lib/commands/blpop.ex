defmodule Commands.Blpop do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([key, timeout_str]) do
    # Parse timeout - Redis accepts both integers and floats
    timeout = try do
      String.to_float(timeout_str)
    rescue
      ArgumentError ->
        # If it's not a valid float, try integer and convert to float
        String.to_integer(timeout_str) * 1.0
    end

    IO.puts("BLPOP: Trying to pop from key '#{key}' with timeout #{timeout}")

    case Store.lpop(key) do
      {:ok, :not_found} ->
        # List is empty - signal the main server to block this client
        IO.puts("BLPOP: List empty, blocking client for key '#{key}'")
        {:block, {key, timeout}}
      {:ok, element} ->
        IO.puts("BLPOP: Got element immediately: '#{element}'")
        {:ok, "*2\r\n$#{byte_size(key)}\r\n#{key}\r\n$#{byte_size(element)}\r\n#{element}\r\n"}
      {:error, reason} ->
        {:error, reason}
    end
  end

  def execute(_), do: {:error, "BLPOP command expects exactly two arguments: key and timeout"}

end
