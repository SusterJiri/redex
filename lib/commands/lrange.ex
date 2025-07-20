defmodule Commands.Lrange do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([key, start, stop]) do
    case Store.lrange(key, String.to_integer(start), String.to_integer(stop)) do
      {:ok, values} when is_list(values) ->
        bulk_strings = Enum.map(values, fn value ->
          "$#{byte_size(value)}\r\n#{value}\r\n"
        end)
        response = "*#{length(values)}\r\n" <> Enum.join(bulk_strings)
        {:ok, response}

      {:ok, _} ->
        {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

      {:error, _} ->
        {:ok, "*0\r\n"}
    end
  end

  def execute(_), do: {:error, "LRANGE command expects exactly three arguments: key, start, stop"}
end
