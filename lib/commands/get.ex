defmodule Commands.Get do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([key]) do
    case Store.get(key) do
      {:ok, value} when is_binary(value) ->
        response = "$#{byte_size(value)}\r\n#{value}\r\n"
        {:ok, response}
      {:ok, value} when is_integer(value) ->
        response = "$#{byte_size(Integer.to_string(value))}\r\n#{value}\r\n"
        {:ok, response}
      {:ok, value} when is_list(value) ->
        {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

      {:error, :not_found} ->
        {:ok, "$-1\r\n"}
    end
  end

  def execute(_), do: {:error, "GET command expects exactly one argument"}
end
