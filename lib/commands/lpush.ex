defmodule Commands.Lpush do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([_key]) do
    {:error, "LPUSH command expects at least one value"}
  end

  def execute([key | values]) when length(values) > 0 do
    case Store.lpush(key, values) do
      {:ok, count} ->
        {:ok, ":#{count}\r\n"}
      {:error, reason} ->
        {:error, reason}
    end
  end

  def execute(_), do: {:error, "LPUSH command expects at least two arguments: key and at least one value"}
end
