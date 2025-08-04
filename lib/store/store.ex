defmodule Store do
  @moduledoc """
  Sharded key-value store using multiple ETS tables for better concurrency.
  """

  # Number of shards - should be a power of 2 for efficient distribution
  @shard_count 16

  def setup_store() do
    # Create multiple ETS tables, one per shard
    for shard_id <- 0..(@shard_count - 1) do
      table_name = shard_table_name(shard_id)
      :ets.new(table_name, [:set, :public, :named_table, read_concurrency: true])
    end

    {:ok, :sharded_redis_store}
  end

  @doc """
  Get the shard ID for a given key using hash-based sharding
  """
  def get_shard_id(key) do
    :erlang.phash2(key, @shard_count)
  end

  @doc """
  Get the ETS table name for a shard
  """
  def shard_table_name(shard_id) do
    :"redis_store_shard_#{shard_id}"
  end

  @doc """
  Get the ETS table for a given key
  """
  def get_table_for_key(key) do
    shard_id = get_shard_id(key)
    shard_table_name(shard_id)
  end

  def get(key) do
    table = get_table_for_key(key)

    case :ets.lookup(table, key) do
      [{^key, {value, expiry_time}}] when is_integer(expiry_time) ->
        now = :os.system_time(:millisecond)

        if now < expiry_time do
          {:ok, value}
        else
          :ets.delete(table, key)
          {:error, :not_found}
        end

      [{^key, value}] ->
        {:ok, value}

      [] ->
        {:error, :not_found}
    end
  end

  def set(key, value) do
    table = get_table_for_key(key)
    :ets.insert(table, {key, value})
    {:ok, value}
  end

  def set_with_ttl(key, value, ttl_ms) do
    table = get_table_for_key(key)
    now = :os.system_time(:millisecond)
    expiry_time = now + ttl_ms
    :ets.insert(table, {key, {value, expiry_time}})
    {:ok, value}
  end

  def rpush(key, value) do
    table = get_table_for_key(key)

    case :ets.lookup(table, key) do
      [] ->
        :ets.insert(table, {key, value})
        {:ok, "#{length(value)}"}

      [{^key, list}] when is_list(list) ->
        new_list = list ++ value
        :ets.insert(table, {key, new_list})
        {:ok, length(new_list)}

      _ ->
        {:error, "Invalid data type for key #{key}"}
    end
  end

  def lpush(key, value) do
    table = get_table_for_key(key)

    case :ets.lookup(table, key) do
      [] ->
        :ets.insert(table, {key, Enum.reverse(value)})
        {:ok, "#{length(value)}"}

      [{^key, list}] when is_list(list) ->
        new_list = Enum.reduce(value, list, fn x, acc -> [x | acc] end)
        :ets.insert(table, {key, new_list})
        {:ok, "#{length(new_list)}"}

      _ ->
        {:error, "Invalid data type for key #{key}"}
    end
  end

  def lrange(key, start, stop) do
    table = get_table_for_key(key)

    case :ets.lookup(table, key) do
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
    table = get_table_for_key(key)

    case :ets.lookup(table, key) do
      [{^key, list}] when is_list(list) ->
        {:ok, length(list)}

      [{^key, _}] ->
        {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

      [] ->
        {:ok, 0}
    end
  end

  def lpop(key) do
    table = get_table_for_key(key)

    case :ets.lookup(table, key) do
      [{^key, list}] when is_list(list) ->
        case list do
          [] ->
            {:ok, :not_found}

          [head | tail] ->
            :ets.insert(table, {key, tail})
            {:ok, head}
        end

      [{^key, _}] ->
        {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

      [] ->
        {:ok, :not_found}
    end
  end

  def lpop(key, number_of_elements_to_pop) do
    # Convert string to integer if needed
    count =
      case number_of_elements_to_pop do
        count when is_integer(count) -> count
        count when is_binary(count) -> String.to_integer(count)
        [count_str] when is_binary(count_str) -> String.to_integer(count_str)
        _ -> 0
      end

    table = get_table_for_key(key)

    if count <= 0 do
      {:error, "Invalid number of elements to pop"}
    else
      case :ets.lookup(table, key) do
        [{^key, list}] when is_list(list) ->
          {popped_elements, remaining_list} = Enum.split(list, count)
          :ets.insert(table, {key, remaining_list})
          {:ok, popped_elements}

        [{^key, _}] ->
          {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

        [] ->
          {:ok, :not_found}
      end
    end
  end

  def type(key) do
    table = get_table_for_key(key)

    case :ets.lookup(table, key) do
      [] ->
        {:not_found, "none"}

      [{^key, value}] when is_binary(value) ->
        {:ok, :string}

      [{^key, list}] when is_list(list) ->
        {:ok, :list}

      [{^key, {:stream, _stream_map}}] ->
        {:ok, :stream}

      [{^key, {type, _value}}] ->
        {:ok, type}
    end
  end

  def xadd(stream_key, entry_id, field_value_pairs) do
    table = get_table_for_key(stream_key)

    case :ets.lookup(table, stream_key) do
      [] ->
        # Create new stream with first entry
        stream_map = %{entry_id => field_value_pairs}
        :ets.insert(table, {stream_key, {:stream, stream_map}})
        {:ok, entry_id}

      [{^stream_key, {:stream, existing_stream}}] when is_map(existing_stream) ->
        # Add new entry to existing stream
        updated_stream = Map.put(existing_stream, entry_id, field_value_pairs)
        :ets.insert(table, {stream_key, {:stream, updated_stream}})
        {:ok, entry_id}

      [{^stream_key, {type, _}}] ->
        {:error, "WRONGTYPE Operation against a key holding the wrong kind of value, expected: stream, got: #{type}"}

      _ ->
        {:error, "Invalid data type for key #{stream_key}"}
    end
  end

  def xrange(stream_key, _start_id \\ "-", _end_id \\ "+") do
    table = get_table_for_key(stream_key)

    case :ets.lookup(table, stream_key) do
      [] ->
        {:ok, []}

      [{^stream_key, {:stream, stream_map}}] when is_map(stream_map) ->
        # For now, return all entries (later we'll implement proper range filtering)
        entries = Enum.map(stream_map, fn {id, field_value_pairs} ->
          {id, field_value_pairs}
        end)
        |> Enum.sort_by(fn {id, _} -> id end)

        {:ok, entries}

      [{^stream_key, {type, _}}] ->
        {:error, "WRONGTYPE Operation against a key holding the wrong kind of value, expected: stream, got: #{type}"}
    end
  end
end
