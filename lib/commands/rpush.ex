defmodule Commands.Rpush do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute(args) do
    case args do
      [key, value] ->
        case Store.rpush(key, value) do
          {:ok, count} ->
            {:ok, ":#{count}\r\n"}
          {:error, reason} ->
            {:error, reason}
        end

      _ ->
        {:error, "RPUSH command expects exactly two arguments"}
    end
  end

end
