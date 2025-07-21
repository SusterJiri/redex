defmodule Commands.Blpop do
  @behaviour RedisCommand

  @impl RedisCommand
  @spec execute([...]) :: {:block, any()} | {:error, <<_::520>>} | {:ok, <<_::64, _::_*8>>}
  def execute([key, timeout_str]) do
    timeout = String.to_integer(timeout_str)

    case Store.lpop(key) do
      {:ok, :not_found} ->
        # List is empty - signal the main server to block this client
        {:block, key, timeout}
      {:ok, element} ->
        {:ok, "*2\r\n$#{byte_size(key)}\r\n#{key}\r\n$#{byte_size(element)}\r\n#{element}\r\n"}
      {:error, reason} ->
        {:error, reason}
    end
  end
end
