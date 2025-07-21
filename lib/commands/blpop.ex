defmodule Commands.Blpop do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([key, timeout_str]) do
    timeout = String.to_integer(timeout_str)

    case Store.lpop(key) do
      {:ok, :not_found} ->
        # List is empty - signal the main server to block this client
        {:block, {key, timeout}}
      {:ok, element} ->
        {:ok, "*2\r\n$#{byte_size(key)}\r\n#{key}\r\n$#{byte_size(element)}\r\n#{element}\r\n"}
      {:error, reason} ->
        {:error, reason}
    end
  end

  def execute(_), do: {:error, "BLPOP command expects exactly two arguments: key and timeout"}

end
