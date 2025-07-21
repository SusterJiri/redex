defmodule Commands.Rpush do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([_key]) do
    {:error, "RPUSH command expects at least one value"}
  end

  def execute([key | values]) when length(values) > 0 do
    case Store.rpush(key, values) do
      {:ok, count} ->
        BlockingQueue.notify_client(key)
        {:ok, ":#{count}\r\n"}
      {:error, reason} ->
        {:error, reason}
    end
  end

  def execute(_), do: {:error, "RPUSH command expects at least two arguments: key and at least one value"}

end
