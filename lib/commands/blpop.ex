defmodule Commands.Blpop do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([key, timeout_str]) do
    timeout = String.to_integer(timeout_str)
    IO.puts("BLPOP: Trying to pop from key '#{key}'")

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
