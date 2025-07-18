defmodule Commands.Echo do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([message]) do
    response = "$#{byte_size(message)}\r\n#{message}\r\n"
    {:ok, response}
  end

  def execute(_), do: {:error, "ECHO command expects exactly one argument"}
end
