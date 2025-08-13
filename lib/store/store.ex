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

  def xadd(stream_key, {timestamp, sequence}, field_value_pairs) do
    table = get_table_for_key(stream_key)

    case :ets.lookup(table, stream_key) do
      [] ->
        # Create new stream with first entry
        # Store as a list with newest entries at the head for easy access
        case validate_entry_id({timestamp, sequence}, []) do
          {:ok, new_timestamp, new_sequence} ->
            entry_id = "#{new_timestamp}-#{new_sequence}"
            stream_entries = [{entry_id, field_value_pairs}]
            :ets.insert(table, {stream_key, {:stream, stream_entries}})
            {:ok, entry_id}

          {:error, reason} ->
            {:error, reason}
        end

      [{^stream_key, {:stream, existing_entries}}] when is_list(existing_entries) ->
        case validate_entry_id({timestamp, sequence}, existing_entries) do
          {:ok, new_timestamp, new_sequence} ->
            # Add new entry to the head of the list (most recent first)
            entry_id = "#{new_timestamp}-#{new_sequence}"

            updated_entries = [
              {entry_id, field_value_pairs} | existing_entries
            ]

            :ets.insert(table, {stream_key, {:stream, updated_entries}})
            {:ok, entry_id}

          {:error, reason} ->
            {:error, reason}
        end

      [{^stream_key, {type, _}}] ->
        {:error,
         "WRONGTYPE Operation against a key holding the wrong kind of value, expected: stream, got: #{type}"}

      _ ->
        {:error, "Invalid data type for key #{stream_key}"}
    end
  end

  # Validate that the new entry ID is greater than the last entry
  defp validate_entry_id({new_timestamp, new_sequence}, []) do
    cond do
      new_timestamp == 0 and new_sequence == :generate ->
        {:ok, new_timestamp, 1}

      new_sequence == :generate ->
        {:ok, new_timestamp, 0}

      new_timestamp == 0 and new_sequence == 0 ->
        {:error, "The ID specified in XADD must be greater than 0-0"}

      true ->
        {:ok, new_timestamp, new_sequence}
    end
  end

  defp validate_entry_id({new_timestamp, new_sequence}, [
         {last_entry_id, _} | _
       ]) do
    [last_timestamp, last_sequence] = String.split(last_entry_id, "-")
    last_timestamp = String.to_integer(last_timestamp)
    last_sequence = String.to_integer(last_sequence)

    cond do
      new_timestamp > last_timestamp and new_sequence == :generate ->
        {:ok, new_timestamp, 0}

      new_sequence == :generate ->
        {:ok, new_timestamp, last_sequence + 1}

      new_timestamp == 0 and new_sequence == 0 ->
        {:error, "The ID specified in XADD must be greater than 0-0"}

      new_timestamp > last_timestamp ->
        {:ok, new_timestamp, new_sequence}

      new_timestamp == last_timestamp and new_sequence > last_sequence ->
        {:ok, new_timestamp, new_sequence}

      new_timestamp == last_timestamp and new_sequence == last_sequence ->
        {:error, "The ID specified in XADD is equal or smaller than the target stream top item"}

      true ->
        {:error, "The ID specified in XADD is equal or smaller than the target stream top item"}
    end
  end

  def xrange(stream_key, start_id \\ "-", end_id \\ "+") do
    table = get_table_for_key(stream_key)

    case :ets.lookup(table, stream_key) do
      [] ->
        {:ok, []}

      [{^stream_key, {:stream, stream_entries}}] when is_list(stream_entries) ->
        # Parse start and end IDs
        {start_ts, start_seq} = parse_range_id(start_id, :start)
        {end_ts, end_seq} = parse_range_id(end_id, :end)

        # Filter entries within the range
        filtered_entries =
          stream_entries
          |> Enum.filter(fn {entry_id, _field_value_pairs} ->
            [timestamp, sequence] = String.split(entry_id, "-")

            in_range?(
              {String.to_integer(timestamp), String.to_integer(sequence)},
              {start_ts, start_seq},
              {end_ts, end_seq}
            )
          end)
          # Reverse to get chronological order (oldest first)
          |> Enum.reverse()
          |> Enum.map(fn {entry_id, field_value_pairs} ->
            {entry_id, field_value_pairs}
          end)

        IO.inspect(filtered_entries, label: "Filtered entries in xrange")

        {:ok, filtered_entries}

      [{^stream_key, {type, _}}] ->
        {:error,
         "WRONGTYPE Operation against a key holding the wrong kind of value, expected: stream, got: #{type}"}
    end
  end

  # Add this function to your Store module

  def get_max_id_from_stream(stream_key) do
    table = get_table_for_key(stream_key)

    case :ets.lookup(table, stream_key) do
      [] ->
        # Stream doesn't exist, return 0-0
        {0, 0}

      [{^stream_key, {:stream, entries}}] when is_list(entries) ->
        case entries do
          [] ->
            # Empty stream, return 0-0
            {0, 0}

          [{first_entry_id, _} | _] ->
            # entries are stored with newest first, so first entry is the maximum
            [timestamp_str, sequence_str] = String.split(first_entry_id, "-")
            {String.to_integer(timestamp_str), String.to_integer(sequence_str)}
        end

      [{^stream_key, {_type, _}}] ->
        # Wrong type, return 0-0
        {0, 0}
    end
  end

  # Helper function to parse range IDs
  defp parse_range_id("-", :start), do: {0, 0}
  defp parse_range_id("+", :end), do: {:infinity, :infinity}

  defp parse_range_id("$", stream_key) when is_binary(stream_key) do
    get_max_id_from_stream(stream_key)
  end

  defp parse_range_id(id, _) when is_binary(id) do
    case String.split(id, "-") do
      [timestamp_str] ->
        # Only timestamp provided, default sequence to 0 for start, max for end
        timestamp = String.to_integer(timestamp_str)
        {timestamp, 0}

      [timestamp_str, sequence_str] ->
        timestamp = String.to_integer(timestamp_str)
        sequence = String.to_integer(sequence_str)
        {timestamp, sequence}
    end
  end

  # Helper function to check if an entry is within range
  defp in_range?({ts, seq}, {start_ts, start_seq}, {end_ts, end_seq}) do
    # Check if entry timestamp/sequence is >= start
    start_ok =
      cond do
        # "-" means from beginning
        start_ts == 0 and start_seq == 0 -> true
        ts > start_ts -> true
        ts == start_ts and seq >= start_seq -> true
        true -> false
      end

    # Check if entry timestamp/sequence is <= end
    end_ok =
      cond do
        # "+" means to end
        end_ts == :infinity -> true
        ts < end_ts -> true
        ts == end_ts and seq <= end_seq -> true
        true -> false
      end

    start_ok and end_ok
  end

  def xread(stream_keys, ids) do
    # Zip the two lists together so we can iterate over pairs
    response =
      Enum.zip(stream_keys, ids)
      |> Enum.map(fn {stream_key, start_id} ->
        table = get_table_for_key(stream_key)
        IO.inspect({stream_key, start_id}, label: "Processing stream")

        case :ets.lookup(table, stream_key) do
          [{^stream_key, {:stream, entries}}] ->
            IO.inspect(entries, label: "Entries in stream")

            # Parse start_id - pass stream_key for $ handling
            {start_ts, start_seq} =
              if start_id == "$" do
                parse_range_id(start_id, stream_key)
              else
                parse_range_id(start_id, :start)
              end

            filtered_entries =
              entries
              |> Enum.filter(fn {entry_id, _field_value_pairs} ->
                # Parse entry_id into timestamp and sequence
                {entry_ts, entry_seq} = parse_entry_id(entry_id)

                # Check if entry is greater than start (exclusive)
                in_range?({entry_ts, entry_seq}, {start_ts, start_seq})
              end)
              # Reverse to get chronological order
              |> Enum.reverse()

            # Include stream key in result
            {stream_key, filtered_entries}

          _ ->
            # Return empty list for non-existent streams
            {stream_key, []}
        end
      end)
      # Remove streams with no new entries
      |> Enum.reject(fn {_stream_key, entries} -> entries == [] end)

    {:ok, response}
  end

  # Helper function to parse entry ID string into {timestamp, sequence}
  defp parse_entry_id(entry_id) do
    [timestamp_str, sequence_str] = String.split(entry_id, "-")
    {String.to_integer(timestamp_str), String.to_integer(sequence_str)}
  end

  defp in_range?({ts, seq}, {start_ts, start_seq}) do
    # Check if entry timestamp/sequence is = start
    start_ok =
      cond do
        start_ts == 0 and start_seq == 0 -> true
        ts > start_ts -> true
        ts == start_ts and seq > start_seq -> true
        true -> false
      end

    start_ok
  end

  def incr(key) do
    case get(key) do
      {:ok, value} when is_integer(value) ->
        new_value = value + 1
        set(key, new_value)
        {:ok, new_value}

      {:ok, value} when is_binary(value) ->
        case Integer.parse(value) do
          {int_value, ""} ->
            new_value = int_value + 1
            set(key, new_value)
            {:ok, new_value}

          _ ->
            {:error, "Value is not an integer"}
        end

      {:error, _reason} ->
        # If the key does not exist or has no value, initialize it to 1
        set(key, 1)
        {:ok, 1}

      {:error, _reason} ->
        {:error, "Key not found"}
    end
  end
end
