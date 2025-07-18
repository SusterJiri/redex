defmodule Commands.Ping do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([]), do: {:ok, "+PONG\r\n"}
  def execute(_), do: {:error, "PING command expects no arguments"}
end
