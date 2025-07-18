defmodule Store do

  def setup_store() do
    :ets.new(:redis_store, [:named_table, :set, :public, read_concurrency: true])
    {:ok, :redis_store}
  end

  def get(key) do
    case :ets.lookup(:redis_store, key) do
      [{^key, {value, expiry_time}}] when is_integer(expiry_time) ->
        now = :os.system_time(:millisecond)
        if now < expiry_time do
          {:ok, value}
        else
          :ets.delete(:redis_store, key)
          {:error, :not_found}
        end
      [{^key, value}] ->
        {:ok, value}
      [] ->
        {:error, :not_found}
    end
  end

  def set(key, value) do
    :ets.insert(:redis_store, {key, value})
    {:ok, value}
  end

  def set_with_ttl(key, value, ttl_ms) do
    now = :os.system_time(:millisecond)
    expiry_time = now + ttl_ms
    :ets.insert(:redis_store, {key, {value, expiry_time}})
    {:ok, value}
  end

  def rpush(key, value) do
    case :ets.lookup(:redis_store, key) do
      [] ->
        :ets.insert(:redis_store, {key, value})
        {:ok, "#{length(value)}"}
      [{^key, list}] when is_list(list) ->
        new_list = list ++ value
        :ets.insert(:redis_store, {key, new_list})
        {:ok, length(new_list)}
      _ ->
        {:error, "Invalid data type for key #{key}"}
    end
  end

  def lrange(key, start, stop) do
    case :ets.lookup(:redis_store, key) do
      [{^key, list}] when is_list(list) ->
        length = length(list)
        start = max(0, start)
        stop = min(length - 1, stop)
        if start > stop or start >= length do
          {:ok, []}
        else
          result = Enum.slice(list, start, stop - start + 1)
          {:ok, result}
        end
      _ ->
        {:error, "Key #{key} does not exist or is not a list"}
    end
  end

end
