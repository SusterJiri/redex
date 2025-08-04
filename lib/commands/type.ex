defmodule Commands.Type do

  @behaviour RedisCommand

  @impl RedisCommand
  def execute([key]) do
    case Store.type(key) do
      {:ok, :string} ->
        {:ok, "+string\r\n"}

      {:ok, :list} ->
        {:ok, "+list\r\n"}

      {:ok, :stream} ->
        {:ok, "+stream\r\n"}

      {:not_found, _} ->
        {:ok, "+none\r\n"}
    end
  end

  def execute(_), do: {:error, "TYPE command expects exactly one argument: key"}

end
