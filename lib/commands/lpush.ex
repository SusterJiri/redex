defmodule Commands.Lpush do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute(args) do
    case args do
      [_key | []] ->
        {:error, "LPUSH command expects at least one value"}

      [key | values] when is_list(values) and length(values) > 0 ->
        case Store.lpush(key, values) do
          {:ok, count} ->
            {:ok, ":#{count}\r\n"}
          {:error, reason} ->
            {:error, reason}
        end

      _ ->
        {:error, "RPUSH command expects a key and at least one value"}
    end
  end
end
