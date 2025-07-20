defmodule Commands.Llen do

  @behaviour RedisCommand

  @impl RedisCommand
  def execute(args) do
    case args do
      [key] ->
        case Store.llen(key) do
          {:ok, count} ->
            {:ok, ":#{count}\r\n"}
          {:error, reason} ->
            {:error, reason}
        end

      _ ->
        {:error, "LLEN command expects exactly one key"}
    end

  end

end
