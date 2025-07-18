defmodule Commands.Get do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([key]) do
    case Store.get(key) do
      {:ok, value} ->
        response = "$#{byte_size(value)}\r\n#{value}\r\n"
        {:ok, response}

      {:error, :not_found} ->
        {:ok, "$-1\r\n"}
    end
  end

  def execute(_), do: {:error, "GET command expects exactly one argument"}
end
