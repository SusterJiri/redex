defmodule Commands.Llen do

  @behaviour RedisCommand

  @impl RedisCommand
  def execute([key]) do
        case Store.llen(key) do
          {:ok, count} ->
            {:ok, ":#{count}\r\n"}
          {:error, reason} ->
            {:error, reason}
        end
  end

  def execute(_), do: {:error, "LLEN command expects exactly one argument"}

end
