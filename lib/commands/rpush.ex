defmodule Commands.Rpush do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute(args) do
    case args do
      [_key | []] ->
        {:error, "RPUSH command expects at least one value"}

      [key | values] when is_list(values) and length(values) > 0 ->
        case Store.rpush(key, values) do
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
