defmodule Commands.Incr do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([key]) do
    case Store.incr(key) do
      {:ok, new_value} when is_integer(new_value) ->
        response = ":#{new_value}\r\n"
        {:ok, response}

      {:error, reason} ->
        {:error, reason}
    end
  end

end
