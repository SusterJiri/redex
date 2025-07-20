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

  def lpush(key, value) do
    case :ets.lookup(:redis_store, key) do
      [] ->
        :ets.insert(:redis_store, {key, Enum.reverse(value)})
        {:ok, "#{length(value)}"}

      [{^key, list}] when is_list(list) ->
        new_list = Enum.reduce(value, list, fn x, acc -> [x | acc] end)
        :ets.insert(:redis_store, {key, new_list})
        {:ok, "#{length(new_list)}"}

      _ ->
        {:error, "Invalid data type for key #{key}"}
    end
  end

  def lrange(key, start, stop) do
    case :ets.lookup(:redis_store, key) do
      [{^key, list}] when is_list(list) ->
        list_length = length(list)

        # Handle negative indices (count from end)
        actual_start = if start < 0, do: max(0, list_length + start), else: start
        actual_stop = if stop < 0, do: max(-1, list_length + stop), else: stop

        # Redis LRANGE behavior:
        # - If start > stop, return empty list
        # - If start >= list_length, return empty list
        # - If stop >= list_length, use list_length - 1
        cond do
          actual_start > actual_stop ->
            {:ok, []}

          actual_start >= list_length ->
            {:ok, []}

          true ->
            # Clamp stop to valid range
            clamped_stop = min(actual_stop, list_length - 1)

            # Calculate slice range
            slice_start = actual_start
            slice_count = clamped_stop - actual_start + 1

            result = Enum.slice(list, slice_start, slice_count)
            {:ok, result}
        end

      [{^key, _}] ->
        # Key exists but is not a list
        {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

      [] ->
        # Key doesn't exist - return empty list
        {:ok, []}
    end
  end

  def llen(key) do
    case :ets.lookup(:redis_store, key) do
      [{^key, list}] when is_list(list) ->
        {:ok, length(list)}

      [{^key, _}] ->
        {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

      [] ->
        {:ok, 0}
    end
  end

  def lpop(key) do
    case :ets.lookup(:redis_store, key) do
      [{^key, list}] when is_list(list) ->
        case list do
          [] ->
            {:ok, :not_found}

          [head | tail] ->
            :ets.insert(:redis_store, {key, tail})
            {:ok, head}
        end

      [{^key, _}] ->
        {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
    end
  end

  def lpop(key, number_of_elements_to_pop) do
    # Convert string to integer if needed
    count = case number_of_elements_to_pop do
      count when is_integer(count) -> count
      count when is_binary(count) -> String.to_integer(count)
      [count_str] when is_binary(count_str) -> String.to_integer(count_str)
      _ -> 0
    end

    if count <= 0 do
      {:error, "Invalid number of elements to pop"}
    else
      case :ets.lookup(:redis_store, key) do
        [{^key, list}] when is_list(list) ->
          {popped_elements, remaining_list} = Enum.split(list, count)
          :ets.insert(:redis_store, {key, remaining_list})
          {:ok, popped_elements}

        [{^key, _}] ->
          {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

        [] ->
          {:ok, :not_found}
      end
    end
  end
end
