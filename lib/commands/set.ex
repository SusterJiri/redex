defmodule Commands.Set do
  @behaviour RedisCommand

  @impl RedisCommand
  def execute([key, value]) do
    case Store.set(key, value) do
      {:ok, _value} ->
        {:ok, "+OK\r\n"}
    end
  end

  @impl RedisCommand
  def execute([key, value, time_unit, ttl_val]) do
    case parse_ttl(ttl_val) do
      {:error, error_val} ->
        {:error, error_val}

      {:ok, ttl} ->
        case String.upcase(time_unit) do
          "EX" -> handle_ex(key, value, ttl)
          "PX" -> handle_px(key, value, ttl)
          _ -> {:error, "Invalid time unit: #{time_unit}"}
        end
    end
  end

  def execute(_), do: {:error, "SET command expects exactly two arguments"}

  defp parse_ttl(ttl_string) do
    case Integer.parse(ttl_string) do
      {ttl, ""} when ttl > 0 -> {:ok, ttl}
      _ -> {:error, "Invalid TTL value"}
    end
  end

  defp handle_ex(key, value, ttl) do
    case Store.set_with_ttl(key, value, ttl * 1000) do
      {:ok, _value} ->
        {:ok, "+OK\r\n"}
    end
  end

  defp handle_px(key, value, ttl) do
    case Store.set_with_ttl(key, value, ttl) do
      {:ok, _value} ->
        {:ok, "+OK\r\n"}
    end
  end
end
