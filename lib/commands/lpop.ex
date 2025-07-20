defmodule Commands.Lpop do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([key]) do
    case Store.lpop(key) do
      {:ok, :not_found} ->
        {:ok, "$-1\r\n"}
      {:ok, value} ->
        response = if value, do: "$#{byte_size(value)}\r\n#{value}\r\n", else: "$-1\r\n"
        {:ok, response}
      {:error, reason} ->
        {:error, reason}
    end
  end

  def execute(_), do: {:error, "LPOP command expects exactly one argument"}
end
